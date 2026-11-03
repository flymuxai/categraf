package supplier

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
)

// lynxtonBalanceResponse is GET /v1/account/balance. balance and used_quota
// are raw quota units; balance_display is balance converted to display_unit.
type lynxtonBalanceResponse struct {
	Balance        *float64 `json:"balance"`
	BalanceDisplay *float64 `json:"balance_display"`
	DisplayUnit    string   `json:"display_unit"`
	BalanceScope   string   `json:"balance_scope"`
	UsedQuota      *float64 `json:"used_quota"`
}

func (ins *Instance) fetchLynxton(ctx context.Context) (*reading, error) {
	headers := map[string]string{"Authorization": "Bearer " + ins.apiKey}
	var resp lynxtonBalanceResponse
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/v1/account/balance", nil, headers, nil, &resp); err != nil {
		return nil, fmt.Errorf("fetch balance: %w", ins.describeHTTPError(err))
	}
	if resp.BalanceDisplay == nil {
		return nil, fmt.Errorf("balance response missing balance_display")
	}

	// The scope follows the account that owns the key: owner/admin keys see
	// the organization pool, member keys see their own allowance. A swapped
	// key would otherwise silently change what the number means.
	scope := strings.TrimSpace(resp.BalanceScope)
	if ins.ExpectScope != "" && scope != ins.ExpectScope {
		return nil, fmt.Errorf("balance_scope=%q, expected %q; check which account owns the api_key", scope, ins.ExpectScope)
	}

	unit := strings.ToUpper(strings.TrimSpace(resp.DisplayUnit))
	if unit == "" {
		unit = strings.ToUpper(ins.Unit)
	}
	if unit == "" {
		return nil, fmt.Errorf("balance response missing display_unit; set unit in the instance config")
	}

	r := &reading{balance: *resp.BalanceDisplay, unit: unit}
	if scope != "" {
		r.labels = map[string]string{"scope": scope}
	}

	if resp.UsedQuota != nil {
		if observed, ok := ins.lynxtonQuotaRatioMatches(resp); ok {
			r.extras = append(r.extras, extraSample{
				metric: "spent_total",
				value:  *resp.UsedQuota / ins.QuotaPerUnit,
				labels: mergeLabels(r.labels, map[string]string{"unit": unit}),
			})
		} else {
			log.Printf("W! supplier=%s: balance/balance_display=%.6g differs from quota_per_unit=%.6g; skipping supplier_spent_total",
				ins.Supplier, observed, ins.QuotaPerUnit)
		}
	}
	return r, nil
}

// lynxtonQuotaRatioMatches checks that the configured quota_per_unit agrees
// with the ratio the upstream used for balance_display. spent_total is
// converted with the fixed configured ratio, not the observed one, so that
// rounding noise cannot make the counter go backwards.
func (ins *Instance) lynxtonQuotaRatioMatches(resp lynxtonBalanceResponse) (float64, bool) {
	// Below one display unit the rounding in balance_display dominates.
	if resp.Balance == nil || *resp.BalanceDisplay < 1 {
		return 0, true
	}
	observed := *resp.Balance / *resp.BalanceDisplay
	return observed, math.Abs(observed-ins.QuotaPerUnit)/ins.QuotaPerUnit <= 0.01
}
