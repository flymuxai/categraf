package supplier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
	"flashcat.cloud/categraf/types"
)

// reading is what a provider extracts from one upstream response.
type reading struct {
	balance float64
	unit    string
	// labels are added to supplier_balance and to provider extras that
	// describe the same balance (for example lynxton's scope).
	labels map[string]string
	extras []extraSample
}

type extraSample struct {
	metric string
	value  any
	labels map[string]string
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx, gatherID, ok := ins.beginGather()
	if !ok {
		return
	}
	defer ins.endGather(gatherID)

	base := ins.baseLabels()
	start := time.Now()
	r, err := ins.fetch(ctx)
	slist.PushSample(inputName, "scrape_duration_seconds", time.Since(start).Seconds(), base)
	if err != nil {
		log.Printf("E! supplier=%s provider=%s: %v", ins.Supplier, ins.Provider, ins.redactError(err))
		slist.PushSample(inputName, "up", 0, base)
		return
	}
	slist.PushSample(inputName, "up", 1, base)
	slist.PushSample(inputName, "last_success_timestamp_seconds", time.Now().Unix(), base)

	slist.PushSample(inputName, "balance", r.balance, mergeLabels(base, r.labels, map[string]string{"unit": r.unit}))
	for _, extra := range r.extras {
		slist.PushSample(inputName, extra.metric, extra.value, mergeLabels(base, extra.labels))
	}
}

func (ins *Instance) fetch(ctx context.Context) (*reading, error) {
	switch ins.Provider {
	case providerLynxton:
		return ins.fetchLynxton(ctx)
	case providerEpay:
		return ins.fetchEpay(ctx)
	default:
		return nil, fmt.Errorf("unknown provider %q", ins.Provider)
	}
}

func (ins *Instance) baseLabels() map[string]string {
	return map[string]string{"supplier": ins.Supplier, "provider": ins.Provider}
}

func mergeLabels(sets ...map[string]string) map[string]string {
	size := 0
	for _, set := range sets {
		size += len(set)
	}
	result := make(map[string]string, size)
	for _, set := range sets {
		for key, value := range set {
			result[key] = value
		}
	}
	return result
}

// describeHTTPError keeps the status code and the upstream's own error
// message, never the raw response body.
func (ins *Instance) describeHTTPError(err error) error {
	var statusErr *llmupstream.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return err
	}
	fields := fmt.Sprintf("status=%d", statusErr.StatusCode)
	var payload struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
		Msg     string `json:"msg"`
	}
	if json.Unmarshal([]byte(statusErr.Body), &payload) == nil {
		message := payload.Message
		if payload.Error != nil && payload.Error.Message != "" {
			message = payload.Error.Message
		}
		if message == "" {
			message = payload.Msg
		}
		if message != "" {
			fields += fmt.Sprintf(" message=%q", ins.sanitize(message))
		}
	}
	return errors.New(fields)
}

// secretForms lists every spelling of the API key that can show up in an
// error: as configured, and URL-encoded (epay sends it as a query parameter,
// and Go's *url.Error includes the full request URL).
func (ins *Instance) secretForms() []string {
	if ins.apiKey == "" {
		return nil
	}
	forms := []string{ins.apiKey}
	for _, encoded := range []string{url.QueryEscape(ins.apiKey), url.PathEscape(ins.apiKey)} {
		if encoded != ins.apiKey {
			forms = append(forms, encoded)
		}
	}
	return forms
}

func (ins *Instance) redact(value string) string {
	for _, secret := range ins.secretForms() {
		value = strings.ReplaceAll(value, secret, "[redacted]")
	}
	return value
}

func (ins *Instance) redactError(err error) error {
	return errors.New(ins.redact(err.Error()))
}

// sanitize prepares an upstream-provided string for a log line.
func (ins *Instance) sanitize(value string) string {
	value = ins.redact(value)
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	const maxRunes = 200
	if utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	return string([]rune(value)[:maxRunes]) + "..."
}
