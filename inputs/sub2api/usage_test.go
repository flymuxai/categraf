package sub2api

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

type collectedUsageScanResult struct {
	usageScanResult
	Records []usageRecord
}

func (ins *Instance) scanUsage(ctx context.Context) (collectedUsageScanResult, error) {
	collected := collectedUsageScanResult{Records: make([]usageRecord, 0)}
	result, err := ins.scanUsageWithHandler(ctx, func(record usageRecord) error {
		collected.Records = append(collected.Records, record)
		return nil
	})
	collected.usageScanResult = result
	return collected, err
}

func TestUsageScanResultDoesNotRetainDecodedRecords(t *testing.T) {
	if _, ok := reflect.TypeOf(usageScanResult{}).FieldByName("Records"); ok {
		t.Fatal("usageScanResult retains decoded records; uncapped scans must stream records into a candidate state")
	}
}

func TestScanUsageStopsImmediatelyWhenIDEqualsCursor(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		requests++
		assertUsageQuery(t, req, 1)
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 5, 20,
			usageJSON(103, "2026-07-15T10:03:00+08:00"),
			usageJSON(102, "2026-07-15T10:02:00+08:00"),
			usageJSON(100, "2026-07-15T10:00:00+08:00"),
			usageJSON(99, "2026-07-15T09:59:00+08:00"),
		)), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
	if got := usageIDs(result.Records); fmt.Sprint(got) != "[103 102]" {
		t.Fatalf("record IDs = %v, want [103 102]", got)
	}
	if !result.Complete || result.Pages != 1 {
		t.Fatalf("result = %+v, want complete single-page scan", result)
	}
}

func TestScanUsageStopsImmediatelyWhenIDIsBelowCursor(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 5, 10,
			usageJSON(99, "2026-07-15T09:59:00+08:00"),
			usageJSON(150, "2026-07-15T10:50:00+08:00"),
		)), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 1 || len(result.Records) != 0 {
		t.Fatalf("requests/records = %d/%d, want 1/0", requests, len(result.Records))
	}
}

func TestScanUsageStopsBeforeDecodingMalformedCursorRow(t *testing.T) {
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 2,
			usageJSON(101, "2026-07-15T10:01:00+08:00"),
			`{"id":100,"created_at":"not-a-time","duration_ms":"invalid","first_token_ms":{}}`,
			`{"id":150,"created_at":"also-invalid"}`,
		)), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v, cursor row fields must not be decoded", err)
	}
	if got := usageIDs(result.Records); fmt.Sprint(got) != "[101]" {
		t.Fatalf("record IDs = %v, want only [101]", got)
	}
}

func TestScanUsageFindsCursorOnLaterPageAndDeduplicates(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		requests++
		page := req.URL.Query().Get("page")
		switch page {
		case "1":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 2, usageRangeJSON(120, 101)...)), nil
		case "2":
			return jsonResponse(req, http.StatusOK, usagePageJSON(2, 20, 2,
				usageJSON(101, "2026-07-15T10:01:00+08:00"),
				usageJSON(100, "2026-07-15T10:00:00+08:00"),
				usageJSON(99, "2026-07-15T09:59:00+08:00"),
			)), nil
		default:
			t.Fatalf("unexpected page %q", page)
			return nil, nil
		}
	})
	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
	if len(result.Records) != 20 || result.Records[0].ID != 120 || result.Records[19].ID != 101 {
		t.Fatalf("record IDs = %v, want 120..101 exactly once", usageIDs(result.Records))
	}
}

func TestScanUsageContinuesPastFivePages(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, 150, func(req *http.Request) (*http.Response, error) {
		requests++
		page, err := strconv.Atoi(req.URL.Query().Get("page"))
		if err != nil {
			t.Fatalf("invalid page: %v", err)
		}
		if page < 1 || page > 6 {
			t.Fatalf("unexpected page %d", page)
		}
		high := int64(270 - (page-1)*20)
		rows := usageRangeJSON(high, high-19)
		if page == 6 {
			rows = append(rows, usageJSON(150, "2026-07-15T10:00:00+08:00"))
		}
		return jsonResponse(req, http.StatusOK, usagePageJSON(page, 20, 10, rows...)), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 6 || result.Pages != 6 || !result.Complete {
		t.Fatalf("requests/result = %d/%+v, want complete six-page scan", requests, result)
	}
	if len(result.Records) != 120 {
		t.Fatalf("records = %d, want 120", len(result.Records))
	}
}

func TestScanUsageInitializationUsesNewestRecordAsBaselineWithoutAggregation(t *testing.T) {
	ins := usageTestInstance(t, 0, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 5, 1,
			usageJSON(11, "2026-07-15T10:01:00+08:00"),
			usageJSON(10, "2026-07-15T09:59:00+08:00"),
			usageJSON(9, "2026-07-15T09:58:00+08:00"),
		)), nil
	})
	ins.startedAt = time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC) // 10:00 Asia/Shanghai

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if got := usageIDs(result.Records); len(got) != 0 || result.NewRecords != 0 {
		t.Fatalf("record IDs/new records = %v/%d, want baseline only", got, result.NewRecords)
	}
	if result.HighestID != 11 {
		t.Fatalf("highest ID = %d, want 11", result.HighestID)
	}
}

func TestScanUsageInitializationUsesLatestOldRecordAsEmptyBaseline(t *testing.T) {
	ins := usageTestInstance(t, 0, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 5, 1,
			usageJSON(10, "2026-07-15T09:59:00+08:00"),
			usageJSON(9, "2026-07-15T09:58:00+08:00"),
		)), nil
	})
	ins.startedAt = time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if len(result.Records) != 0 || result.HighestID != 10 {
		t.Fatalf("records/highest ID = %d/%d, want 0/10", len(result.Records), result.HighestID)
	}
}

func TestScanUsageInitializationUsesSingleTwentyRecordBaselinePage(t *testing.T) {
	requests := 0
	requestedPageSizes := make([]int, 0, 2)
	ins := usageTestInstance(t, 0, func(req *http.Request) (*http.Response, error) {
		requests++
		page, err := strconv.Atoi(req.URL.Query().Get("page"))
		if err != nil {
			t.Fatalf("invalid page: %v", err)
		}
		pageSize, err := strconv.Atoi(req.URL.Query().Get("page_size"))
		if err != nil {
			t.Fatalf("invalid page_size: %v", err)
		}
		requestedPageSizes = append(requestedPageSizes, pageSize)
		high := int64(pageSize - (page-1)*pageSize)
		low := high - int64(pageSize) + 1
		if high < 1 {
			return jsonResponse(req, http.StatusOK, usagePageJSON(page, pageSize, 3)), nil
		}
		return jsonResponse(req, http.StatusOK, usagePageJSON(page, pageSize, 3, usageRangeJSON(high, low)...)), nil
	})
	ins.startedAt = time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 1 || fmt.Sprint(requestedPageSizes) != "[20]" {
		t.Fatalf("requests/page sizes = %d/%v, want one page of 20", requests, requestedPageSizes)
	}
	if len(result.Records) != 0 || result.NewRecords != 0 {
		t.Fatalf("initial records/new records = %d/%d, want zero baseline-only records", len(result.Records), result.NewRecords)
	}
	if result.HighestID != 20 || result.Pages != 1 || !result.Complete {
		t.Fatalf("result = %+v, want complete baseline at ID 20 from one page", result)
	}
}

func TestScanUsageDoesNotRetryRateLimitResponse(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(req, http.StatusTooManyRequests, `{"code":429,"message":"rate limited"}`), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil {
		t.Fatal("scanUsage() error = nil, want rate-limit error")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want exactly 1", requests)
	}
}

func TestScanUsageReportsSanitizedHTTPErrorContext(t *testing.T) {
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusInternalServerError, `{"code":500,"message":"database unavailable","reason":"QUERY_FAILED","data":"secret-body"}`), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err == nil {
		t.Fatal("scanUsage() error = nil, want HTTP error")
	}
	for _, want := range []string{"status=500", "code=500", `message="database unavailable"`, `reason="QUERY_FAILED"`, "page=1", "page_size=20"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("scanUsage() error = %q, want %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "secret-body") {
		t.Fatalf("scanUsage() error leaked unparsed response data: %v", err)
	}
	if result.Pages != 0 || result.RecordsFetched != 0 {
		t.Fatalf("failed result = %+v, want no completed response pages", result)
	}
}

func TestSanitizeErrorFieldRedactsCredentialShapes(t *testing.T) {
	tests := []string{
		"Bearer access-token",
		"failed for sk-secret-value",
		"refresh rt_secret_value",
		"token eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxfQ.signaturevalue",
	}
	for _, input := range tests {
		if got := sanitizeErrorField(input); got != "[redacted]" {
			t.Fatalf("sanitizeErrorField(%q) = %q, want [redacted]", input, got)
		}
	}
}

func TestInstanceSanitizeErrorFieldRedactsResolvedOpaqueSecrets(t *testing.T) {
	ins := &Instance{
		accessToken:  "opaque-access-value",
		refreshToken: "custom-refresh-value",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "custom-model-value"}},
	}
	ins.updateRedactionSecrets()
	input := "failed opaque-access-value custom-refresh-value custom-model-value"
	if got := ins.sanitizeErrorField(input); strings.Contains(got, "opaque-access-value") || strings.Contains(got, "custom-refresh-value") || strings.Contains(got, "custom-model-value") {
		t.Fatalf("sanitizeErrorField() leaked a resolved secret: %q", got)
	}
}

func TestScanUsageWithEmptyPersistedCursorUsesOriginalBaselineAfterRestart(t *testing.T) {
	baseline := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC) // 10:00
	ins := usageTestInstance(t, 0, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 5, 1,
			usageJSON(1, "2026-07-15T10:30:00+08:00"),
		)), nil
	})
	ins.state.UsageInitialized = true
	ins.state.LastUsageCreatedAt = &baseline
	ins.startedAt = time.Date(2026, 7, 15, 3, 0, 0, 0, time.UTC) // restarted at 11:00

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if got := usageIDs(result.Records); fmt.Sprint(got) != "[1]" {
		t.Fatalf("record IDs = %v, want downtime record [1]", got)
	}
}

func usageTestInstance(t *testing.T, cursor int64, transport roundTripFunc) *Instance {
	t.Helper()
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	createdAt := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	return &Instance{
		Site:          "supplier-a",
		UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"),
		client:        client,
		accessToken:   "user-token",
		state: persistedState{
			Version: stateVersion, LastUsageID: cursor, LastUsageCreatedAt: &createdAt,
			UsageSeries: make(map[string]*usageSeriesState),
		},
	}
}

func assertUsageQuery(t *testing.T, req *http.Request, page int) {
	t.Helper()
	q := req.URL.Query()
	if req.URL.Path != "/api/v1/usage" || q.Get("sort_by") != "id" || q.Get("sort_order") != "desc" || q.Get("page_size") != "20" {
		t.Fatalf("usage request = %s, want ID-desc usage query", req.URL.String())
	}
	if q.Get("page") != fmt.Sprint(page) || q.Get("timezone") != "Asia/Shanghai" {
		t.Fatalf("usage query = %v", q)
	}
	if q.Get("start_date") != "2026-07-15" {
		t.Fatalf("start_date = %q, want 2026-07-15", q.Get("start_date"))
	}
}

func usageRangeJSON(high, low int64) []string {
	rows := make([]string, 0, high-low+1)
	for id := high; id >= low; id-- {
		createdAt := time.Date(2026, 7, 15, 2, int(id%60), 0, 0, time.UTC).Format(time.RFC3339)
		rows = append(rows, usageJSON(id, createdAt))
	}
	return rows
}

func usagePageJSON(page, pageSize, pages int, rows ...string) string {
	return fmt.Sprintf(`{"code":0,"message":"success","data":{"items":[%s],"total":999,"page":%d,"page_size":%d,"pages":%d}}`, strings.Join(rows, ","), page, pageSize, pages)
}

func usageJSON(id int64, createdAt string) string {
	return fmt.Sprintf(`{"id":%d,"model":"claude-sonnet-4-5","request_type":"stream","created_at":%q,"group":{"name":"pro","platform":"anthropic"}}`, id, createdAt)
}

func usageIDs(records []usageRecord) []int64 {
	ids := make([]int64, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return location
}
