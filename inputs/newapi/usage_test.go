package newapi

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
		t.Fatal("usageScanResult retains decoded records; unbounded scans must stream records into candidate state")
	}
}

func TestScanUsageIgnoresDisplayIDAndScansEntireBoundarySecond(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		assertNewAPIUsageQuery(t, req, 1, 100, 199)
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 3, 3,
			usageJSON(1, "req-new", 100),
			usageJSON(2, "req-old-a", 100),
			usageJSON(3, "req-old-b", 100),
		)), nil
	})
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 100
	ins.state.LastUsageRequestIDs = []string{"req-old-a", "req-old-b"}

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
	if got := requestIDs(result.Records); fmt.Sprint(got) != "[req-new]" {
		t.Fatalf("new request IDs = %v, want [req-new]", got)
	}
	if got := fmt.Sprint(result.HighestRequestIDs); got != "[req-new req-old-a req-old-b]" {
		t.Fatalf("highest request IDs = %s", got)
	}
}

func TestScanUsageDeduplicatesAcrossPages(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		switch req.URL.Query().Get("p") {
		case "1":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 101, usageRowsJSON(1, 100, 101)...)), nil
		case "2":
			return jsonResponse(req, http.StatusOK, usagePageJSON(2, 100, 101,
				usageJSON(1, "req-001", 101), usageJSON(101, "req-101", 101))), nil
		default:
			t.Fatalf("unexpected page %q", req.URL.Query().Get("p"))
			return nil, nil
		}
	})
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 99

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if len(result.Records) != 101 || result.Records[len(result.Records)-1].RequestID != "req-101" {
		t.Fatalf("new records = %d, last=%q; want 101 unique records", len(result.Records), result.Records[len(result.Records)-1].RequestID)
	}
	if result.Pages != 2 || !result.Complete {
		t.Fatalf("result = %+v, want complete two-page scan", result)
	}
}

func TestScanUsageRejectsChangingTotal(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Get("p") == "1" {
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 101, usageRowsJSON(1, 100, 101)...)), nil
		}
		return jsonResponse(req, http.StatusOK, usagePageJSON(2, 100, 102, usageJSON(101, "req-101", 101))), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "total changed") {
		t.Fatalf("scanUsage() error = %v, want total change error", err)
	}
}

func TestScanUsageContinuesPastFivePagesWithFixedPageSize(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		page, err := strconv.Atoi(req.URL.Query().Get("p"))
		if err != nil || page < 1 || page > 6 {
			t.Fatalf("unexpected page %q", req.URL.Query().Get("p"))
		}
		if got := req.URL.Query().Get("page_size"); got != "100" {
			t.Fatalf("page_size = %q, want fixed 100", got)
		}
		count := 100
		if page == 6 {
			count = 1
		}
		rows := make([]string, 0, count)
		for i := 0; i < count; i++ {
			index := (page-1)*100 + i
			rows = append(rows, usageJSON(index+1, fmt.Sprintf("req-%03d", index+1), 101))
		}
		return jsonResponse(req, http.StatusOK, usagePageJSON(page, 100, 501, rows...)), nil
	})
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 100

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if requests != 6 || result.Pages != 6 || !result.Complete {
		t.Fatalf("requests/result = %d/%+v, want complete six-page scan", requests, result)
	}
}

func TestScanUsageInitializationSkipsStartupSecond(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		assertNewAPIUsageQuery(t, req, 1, 100, 199)
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 10, 2,
			usageJSON(1, "req-after", 101),
			usageJSON(2, "req-start", 100),
		)), nil
	})
	ins.startedAt = time.Unix(100, 500)

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if got := requestIDs(result.Records); fmt.Sprint(got) != "[req-after]" {
		t.Fatalf("new request IDs = %v, want only post-startup-second request", got)
	}
	if result.HighestTimestamp != 101 || fmt.Sprint(result.HighestRequestIDs) != "[req-after]" {
		t.Fatalf("baseline = %d/%v, want 101/[req-after]", result.HighestTimestamp, result.HighestRequestIDs)
	}
}

func TestScanUsageDefersUntilStartupSecondCloses(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		return nil, fmt.Errorf("unexpected request")
	})
	ins.startedAt = time.Unix(200, 0)
	ins.now = func() time.Time { return time.Unix(200, 500) }

	result, err := ins.scanUsage(context.Background())
	if err != nil {
		t.Fatalf("scanUsage() error = %v", err)
	}
	if !result.Deferred || requests != 0 {
		t.Fatalf("result/requests = %+v/%d, want deferred without request", result, requests)
	}
}

func TestScanUsageRejectsMissingRequestID(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 10, 1, usageJSON(1, "", 101))), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "empty request_id") {
		t.Fatalf("scanUsage() error = %v, want request ID error", err)
	}
}

func TestScanUsageRejectsDuplicateRequestIDWhenUniqueCountDoesNotMatchTotal(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 10, 2,
			usageJSON(1, "req-duplicate", 101), usageJSON(2, "req-duplicate", 101))), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 unique records, expected 2") {
		t.Fatalf("scanUsage() error = %v, want unique count error", err)
	}
}

func TestScanUsageDoesNotRetryUnauthorizedResponse(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(req, http.StatusUnauthorized, `{"success":false,"message":"invalid token"}`), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil || requests != 1 {
		t.Fatalf("error/requests = %v/%d, want one failed request", err, requests)
	}
}

func TestScanUsageDoesNotRetryRateLimitResponse(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(req, http.StatusTooManyRequests, `{"success":false,"message":"rate limited"}`), nil
	})

	_, err := ins.scanUsage(context.Background())
	if err == nil || requests != 1 {
		t.Fatalf("error/requests = %v/%d, want one failed request", err, requests)
	}
}

func TestScanUsageReportsSanitizedHTTPErrorContext(t *testing.T) {
	requests := 0
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		requests++
		return jsonResponse(req, http.StatusInternalServerError,
			`{"code":500,"message":"database failed for system-access-token","reason":"QUERY_FAILED","data":"secret-body"}`), nil
	})

	result, err := ins.scanUsage(context.Background())
	if err == nil {
		t.Fatal("scanUsage() error = nil, want HTTP error")
	}
	for _, want := range []string{"status=500", "code=500", `reason="QUERY_FAILED"`, "page=1", "page_size=100", "scanned_pages=0", "scanned_records=0", "expected_total=unknown"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("scanUsage() error = %q, want %q", err, want)
		}
	}
	for _, forbidden := range []string{"system-access-token", "secret-body"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("scanUsage() error leaked %q: %v", forbidden, err)
		}
	}
	if requests != 1 || result.Pages != 0 || result.RecordsFetched != 0 {
		t.Fatalf("requests/result = %d/%+v, want one failed first-page request", requests, result)
	}
}

func usageTestInstance(t *testing.T, transport roundTripFunc) *Instance {
	t.Helper()
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return &Instance{
		Site:        "supplier-a",
		UserID:      1001,
		client:      client,
		accessToken: "system-access-token",
		startedAt:   time.Unix(100, 0),
		now:         func() time.Time { return time.Unix(200, 0) },
		state: persistedState{
			Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState),
		},
	}
}

func usageRowsJSON(start, count int, createdAt int64) []string {
	rows := make([]string, 0, count)
	for index := start; index < start+count; index++ {
		rows = append(rows, usageJSON(index, fmt.Sprintf("req-%03d", index), createdAt))
	}
	return rows
}

func assertNewAPIUsageQuery(t *testing.T, req *http.Request, page int, start, end int64) {
	t.Helper()
	q := req.URL.Query()
	if req.URL.Path != "/api/log/self" || q.Get("type") != "2" {
		t.Fatalf("usage request = %s, want consume log query", req.URL.String())
	}
	if q.Get("p") != fmt.Sprint(page) || q.Get("page_size") != "100" {
		t.Fatalf("usage paging query = %v", q)
	}
	if q.Get("start_timestamp") != fmt.Sprint(start) || q.Get("end_timestamp") != fmt.Sprint(end) {
		t.Fatalf("usage time query = %v, want %d..%d", q, start, end)
	}
}

func usagePageJSON(page, pageSize, total int, rows ...string) string {
	return fmt.Sprintf(`{"success":true,"message":"","data":{"items":[%s],"total":%d,"page":%d,"page_size":%d}}`, strings.Join(rows, ","), total, page, pageSize)
}

func usageJSON(displayID int, requestID string, createdAt int64) string {
	return fmt.Sprintf(`{"id":%d,"request_id":%q,"created_at":%d,"type":2,"model_name":"gpt-4o","group":"pro","prompt_tokens":10,"completion_tokens":5,"quota":500,"use_time":2,"is_stream":true,"other":%q}`, displayID, requestID, createdAt, `{"frt":250}`)
}

func requestIDs(records []usageRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.RequestID)
	}
	return ids
}
