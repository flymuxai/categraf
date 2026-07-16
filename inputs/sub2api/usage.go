package sub2api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

const usagePageSize = 20

type usageRecord struct {
	ID                  int64     `json:"id"`
	Model               string    `json:"model"`
	RequestType         string    `json:"request_type"`
	InputTokens         int64     `json:"input_tokens"`
	OutputTokens        int64     `json:"output_tokens"`
	CacheCreationTokens int64     `json:"cache_creation_tokens"`
	CacheReadTokens     int64     `json:"cache_read_tokens"`
	TotalCost           float64   `json:"total_cost"`
	ActualCost          float64   `json:"actual_cost"`
	DurationMS          *int64    `json:"duration_ms"`
	FirstTokenMS        *int64    `json:"first_token_ms"`
	CreatedAt           time.Time `json:"created_at"`
	Group               *struct {
		Name     string `json:"name"`
		Platform string `json:"platform"`
	} `json:"group"`
}

type usagePage struct {
	Items    []json.RawMessage `json:"items"`
	Total    int64             `json:"total"`
	Page     int               `json:"page"`
	PageSize int               `json:"page_size"`
	Pages    int               `json:"pages"`
}

type usageScanResult struct {
	Pages            int
	RecordsFetched   int
	NewRecords       int
	Complete         bool
	HighestID        int64
	HighestCreatedAt time.Time
}

func (ins *Instance) scanUsageWithHandler(ctx context.Context, handle func(usageRecord) error) (usageScanResult, error) {
	result := usageScanResult{}
	if ins.usageLocation == nil {
		return result, fmt.Errorf("usage timezone is not initialized")
	}
	if handle == nil {
		return result, fmt.Errorf("usage record handler is required")
	}

	cursor := ins.state.LastUsageID
	initializing := cursor == 0 && !ins.state.UsageInitialized
	start := ins.startedAt
	if cursor > 0 || ins.state.UsageInitialized {
		if ins.state.LastUsageCreatedAt == nil {
			return result, fmt.Errorf("usage cursor %d has no baseline timestamp", cursor)
		}
		start = *ins.state.LastUsageCreatedAt
	}
	if start.IsZero() {
		start = time.Now()
	}

	startDate := start.In(ins.usageLocation).Format("2006-01-02")
	endDate := time.Now().In(ins.usageLocation).Format("2006-01-02")
	seen := make(map[int64]struct{})

scanPages:
	for page := 1; ; page++ {
		query := url.Values{
			"page":       {strconv.Itoa(page)},
			"page_size":  {strconv.Itoa(usagePageSize)},
			"start_date": {startDate},
			"end_date":   {endDate},
			"sort_by":    {"id"},
			"sort_order": {"desc"},
			"timezone":   {ins.UsageTimezone},
		}
		var response envelope[usagePage]
		if err := ins.doUserJSON(ctx, http.MethodGet, "/api/v1/usage", query, nil, &response); err != nil {
			return result, ins.formatUsageRequestError(err, page, result, startDate, endDate)
		}
		if response.Code != 0 {
			return result, fmt.Errorf("usage API returned code %d message=%q page=%d page_size=%d scanned_pages=%d scanned_records=%d start_date=%s end_date=%s sort_by=id sort_order=desc",
				response.Code, ins.sanitizeErrorField(response.Message), page, usagePageSize, result.Pages, result.RecordsFetched, startDate, endDate)
		}
		result.Pages = page
		result.RecordsFetched += len(response.Data.Items)

		for _, rawRecord := range response.Data.Items {
			var identity struct {
				ID int64 `json:"id"`
			}
			if err := json.Unmarshal(rawRecord, &identity); err != nil || identity.ID <= 0 {
				return result, fmt.Errorf("usage API returned invalid id on page %d", page)
			}
			if cursor > 0 && identity.ID <= cursor {
				result.Complete = true
				break scanPages
			}
			if _, ok := seen[identity.ID]; ok {
				continue
			}
			var record usageRecord
			if err := json.Unmarshal(rawRecord, &record); err != nil {
				return result, fmt.Errorf("decode usage record %d: %w", identity.ID, err)
			}
			if record.ID != identity.ID {
				return result, fmt.Errorf("usage record id changed while decoding")
			}
			if record.ID > result.HighestID {
				result.HighestID = record.ID
				result.HighestCreatedAt = record.CreatedAt
			}
			if cursor == 0 {
				if record.CreatedAt.IsZero() {
					return result, fmt.Errorf("usage record %d has no created_at", record.ID)
				}
				if !initializing && record.CreatedAt.Before(start) {
					result.Complete = true
					break scanPages
				}
			}
			if initializing {
				continue
			}
			seen[record.ID] = struct{}{}
			if err := handle(record); err != nil {
				return result, fmt.Errorf("process usage record %d: %w", record.ID, err)
			}
			result.NewRecords++
		}

		if initializing || len(response.Data.Items) == 0 || len(response.Data.Items) < usagePageSize || (response.Data.Pages > 0 && page >= response.Data.Pages) {
			result.Complete = true
			break
		}
	}
	return result, nil
}

type sub2APIErrorEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

func (ins *Instance) formatUsageRequestError(err error, page int, result usageScanResult, startDate, endDate string) error {
	contextFields := fmt.Sprintf("page=%d page_size=%d scanned_pages=%d scanned_records=%d start_date=%s end_date=%s sort_by=id sort_order=desc",
		page, usagePageSize, result.Pages, result.RecordsFetched, startDate, endDate)
	var statusErr *llmupstream.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return fmt.Errorf("usage request failed %s: %w", contextFields, err)
	}

	fields := fmt.Sprintf("status=%d", statusErr.StatusCode)
	var response sub2APIErrorEnvelope
	if json.Unmarshal([]byte(statusErr.Body), &response) == nil {
		if response.Code != 0 {
			fields += fmt.Sprintf(" code=%d", response.Code)
		}
		if message := ins.sanitizeErrorField(response.Message); message != "" {
			fields += fmt.Sprintf(" message=%q", message)
		}
		if reason := ins.sanitizeErrorField(response.Reason); reason != "" {
			fields += fmt.Sprintf(" reason=%q", reason)
		}
	}
	return fmt.Errorf("usage request failed %s %s", fields, contextFields)
}

func sanitizeErrorField(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "sk-", "rt_"} {
		if strings.Contains(lower, marker) {
			return "[redacted]"
		}
	}
	for _, field := range strings.Fields(value) {
		if isLikelyJWT(strings.Trim(field, `"'(),;`)) {
			return "[redacted]"
		}
	}
	const maxRunes = 256
	if utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes]) + "..."
}

func (ins *Instance) updateRedactionSecrets() {
	secrets := make([]string, 0, 2+len(ins.modelAPIKeys))
	for _, secret := range []string{ins.accessToken, ins.refreshToken} {
		if secret != "" {
			secrets = append(secrets, secret)
		}
	}
	for _, key := range ins.modelAPIKeys {
		if key.APIKey != "" {
			secrets = append(secrets, key.APIKey)
		}
	}
	ins.redactionMu.Lock()
	ins.redactionSecrets = secrets
	ins.redactionMu.Unlock()
}

func (ins *Instance) sanitizeErrorField(value string) string {
	ins.redactionMu.RLock()
	secrets := append([]string(nil), ins.redactionSecrets...)
	ins.redactionMu.RUnlock()
	for _, secret := range secrets {
		if strings.Contains(value, secret) {
			value = strings.ReplaceAll(value, secret, "[redacted]")
		}
	}
	return sanitizeErrorField(value)
}

func isLikelyJWT(value string) bool {
	if len(value) < 32 {
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				return false
			}
		}
	}
	return true
}
