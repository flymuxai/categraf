package supplier

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// epayQueryResponse is the merchant query of 彩虹易支付-style platforms:
// GET /api.php?act=query&pid=..&key=..
//
// The response echoes the merchant key, so the struct deliberately has no
// field for it and the raw body is never logged.
type epayQueryResponse struct {
	Code          flexNumber `json:"code"`
	Msg           string     `json:"msg"`
	Active        flexNumber `json:"active"`
	Money         flexNumber `json:"money"`
	Orders        flexNumber `json:"orders"`
	OrdersToday   flexNumber `json:"orders_today"`
	OrdersLastday flexNumber `json:"orders_lastday"`
	// Field names used by the upstream 彩虹易支付 docs; some deployments
	// return these instead of the orders_* spelling.
	OrderToday   flexNumber `json:"order_today"`
	OrderLastday flexNumber `json:"order_lastday"`
}

func (ins *Instance) fetchEpay(ctx context.Context) (*reading, error) {
	query := url.Values{
		"act": {"query"},
		"pid": {string(ins.PID)},
		"key": {ins.apiKey},
	}
	var resp epayQueryResponse
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/api.php", query, nil, nil, &resp); err != nil {
		return nil, fmt.Errorf("query merchant: %w", ins.describeHTTPError(err))
	}
	// A wrong key or pid still comes back as HTTP 200 with code != 1.
	if !resp.Code.set || resp.Code.value != 1 {
		code := "missing"
		if resp.Code.set {
			code = strconv.FormatFloat(resp.Code.value, 'f', -1, 64)
		}
		return nil, fmt.Errorf("query merchant: code=%s msg=%q", code, ins.sanitize(resp.Msg))
	}
	if !resp.Money.set {
		return nil, fmt.Errorf("query merchant: response missing money")
	}

	r := &reading{balance: resp.Money.value, unit: strings.ToUpper(ins.Unit)}
	if resp.Active.set {
		r.extras = append(r.extras, extraSample{metric: "account_active", value: resp.Active.value})
	}
	if resp.Orders.set {
		r.extras = append(r.extras, extraSample{metric: "orders_total", value: resp.Orders.value})
	}
	if today := firstSet(resp.OrdersToday, resp.OrderToday); today.set {
		r.extras = append(r.extras, extraSample{metric: "orders_today", value: today.value})
	}
	if yesterday := firstSet(resp.OrdersLastday, resp.OrderLastday); yesterday.set {
		r.extras = append(r.extras, extraSample{metric: "orders_yesterday", value: yesterday.value})
	}
	return r, nil
}

// flexNumber decodes a JSON number or a numeric string ("200.44"). Missing,
// null and empty-string values leave it unset.
type flexNumber struct {
	value float64
	set   bool
}

func (n *flexNumber) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "null" {
		return nil
	}
	if strings.HasPrefix(raw, `"`) {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		raw = strings.TrimSpace(s)
		if raw == "" {
			return nil
		}
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("expected a number")
	}
	n.value, n.set = value, true
	return nil
}

func firstSet(values ...flexNumber) flexNumber {
	for _, v := range values {
		if v.set {
			return v
		}
	}
	return flexNumber{}
}
