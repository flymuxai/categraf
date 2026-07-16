package newapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

const (
	consumeLogType      = 2
	newAPIUsagePageSize = 100
)

type usageRecord struct {
	ID               int    `json:"id"`
	RequestID        string `json:"request_id"`
	CreatedAt        int64  `json:"created_at"`
	Type             int    `json:"type"`
	ModelName        string `json:"model_name"`
	Group            string `json:"group"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	Quota            int64  `json:"quota"`
	UseTime          *int64 `json:"use_time"`
	IsStream         bool   `json:"is_stream"`
	Other            string `json:"other"`
}

type usagePage struct {
	Items    []usageRecord `json:"items"`
	Total    int           `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
}

type usageScanResult struct {
	Pages             int
	RecordsFetched    int
	NewRecords        int
	Complete          bool
	Deferred          bool
	HighestTimestamp  int64
	HighestRequestIDs []string
}

func (ins *Instance) scanUsageWithHandler(ctx context.Context, handle func(usageRecord) error) (usageScanResult, error) {
	result := usageScanResult{}
	if handle == nil {
		return result, fmt.Errorf("usage record handler is required")
	}
	now := ins.now
	if now == nil {
		now = time.Now
	}

	startTimestamp := ins.startedAt.Unix()
	knownBoundary := make(map[string]struct{})
	if ins.state.UsageInitialized {
		startTimestamp = ins.state.LastUsageTimestamp
		if startTimestamp <= 0 {
			return result, fmt.Errorf("usage state has invalid timestamp %d", startTimestamp)
		}
		for _, requestID := range ins.state.LastUsageRequestIDs {
			knownBoundary[requestID] = struct{}{}
		}
	}
	endTimestamp := now().Unix() - 1
	if endTimestamp < startTimestamp {
		result.Deferred = true
		result.Complete = true
		return result, nil
	}

	result.HighestTimestamp = startTimestamp
	highestIDs := make(map[string]struct{}, len(knownBoundary))
	for requestID := range knownBoundary {
		highestIDs[requestID] = struct{}{}
	}
	seen := make(map[string]struct{})
	expectedTotal := -1

	for page := 1; ; page++ {
		query := url.Values{
			"type":            {strconv.Itoa(consumeLogType)},
			"p":               {strconv.Itoa(page)},
			"page_size":       {strconv.Itoa(newAPIUsagePageSize)},
			"start_timestamp": {strconv.FormatInt(startTimestamp, 10)},
			"end_timestamp":   {strconv.FormatInt(endTimestamp, 10)},
		}
		var response response[usagePage]
		if err := ins.client.DoJSON(ctx, http.MethodGet, "/api/log/self", query, ins.authHeaders(), nil, &response); err != nil {
			return result, ins.formatUsageRequestError(err, page, result, startTimestamp, endTimestamp, expectedTotal)
		}
		if !response.Success {
			return result, fmt.Errorf("usage API failed message=%q page=%d page_size=%d scanned_pages=%d scanned_records=%d start_timestamp=%d end_timestamp=%d expected_total=%s",
				ins.sanitizeErrorField(response.Message), page, newAPIUsagePageSize, result.Pages, result.RecordsFetched,
				startTimestamp, endTimestamp, formatExpectedTotal(expectedTotal))
		}
		if response.Data.Total < 0 {
			return result, fmt.Errorf("usage API returned negative total %d", response.Data.Total)
		}
		if expectedTotal == -1 {
			expectedTotal = response.Data.Total
		} else if response.Data.Total != expectedTotal {
			return result, fmt.Errorf("usage total changed from %d to %d during scan", expectedTotal, response.Data.Total)
		}
		result.Pages = page
		result.RecordsFetched += len(response.Data.Items)

		for _, record := range response.Data.Items {
			record.RequestID = strings.TrimSpace(record.RequestID)
			if record.RequestID == "" {
				return result, fmt.Errorf("usage API returned empty request_id")
			}
			if record.Type != consumeLogType {
				return result, fmt.Errorf("usage record has unexpected type %d", record.Type)
			}
			if record.CreatedAt < startTimestamp || record.CreatedAt > endTimestamp {
				return result, fmt.Errorf("usage record timestamp %d is outside query range %d..%d", record.CreatedAt, startTimestamp, endTimestamp)
			}
			if _, ok := seen[record.RequestID]; ok {
				continue
			}
			seen[record.RequestID] = struct{}{}

			if record.CreatedAt > result.HighestTimestamp {
				result.HighestTimestamp = record.CreatedAt
				highestIDs = map[string]struct{}{record.RequestID: {}}
			} else if record.CreatedAt == result.HighestTimestamp {
				highestIDs[record.RequestID] = struct{}{}
			}

			if !ins.state.UsageInitialized && record.CreatedAt <= ins.startedAt.Unix() {
				continue
			}
			if ins.state.UsageInitialized && record.CreatedAt == startTimestamp {
				if _, ok := knownBoundary[record.RequestID]; ok {
					continue
				}
			}
			if err := handle(record); err != nil {
				return result, fmt.Errorf("process usage record: %w", err)
			}
			result.NewRecords++
		}

		if len(response.Data.Items) == 0 || len(response.Data.Items) < newAPIUsagePageSize || page*newAPIUsagePageSize >= expectedTotal {
			if len(seen) != expectedTotal {
				return result, fmt.Errorf("usage scan returned %d unique records, expected %d", len(seen), expectedTotal)
			}
			result.Complete = true
			break
		}
	}
	result.HighestRequestIDs = make([]string, 0, len(highestIDs))
	for requestID := range highestIDs {
		result.HighestRequestIDs = append(result.HighestRequestIDs, requestID)
	}
	sort.Strings(result.HighestRequestIDs)
	return result, nil
}

type newAPIErrorEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

func (ins *Instance) formatUsageRequestError(err error, page int, result usageScanResult, startTimestamp, endTimestamp int64, expectedTotal int) error {
	contextFields := fmt.Sprintf("page=%d page_size=%d scanned_pages=%d scanned_records=%d start_timestamp=%d end_timestamp=%d expected_total=%s",
		page, newAPIUsagePageSize, result.Pages, result.RecordsFetched, startTimestamp, endTimestamp, formatExpectedTotal(expectedTotal))
	var statusErr *llmupstream.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return fmt.Errorf("usage request failed %s: %w", contextFields, err)
	}

	fields := fmt.Sprintf("status=%d", statusErr.StatusCode)
	var response newAPIErrorEnvelope
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

func formatExpectedTotal(total int) string {
	if total < 0 {
		return "unknown"
	}
	return strconv.Itoa(total)
}

func (ins *Instance) sanitizeErrorField(value string) string {
	if ins.accessToken != "" {
		value = strings.ReplaceAll(value, ins.accessToken, "[redacted]")
	}
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "sk-"} {
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
	return string([]rune(value)[:maxRunes]) + "..."
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
