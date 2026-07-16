package sub2api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
	"flashcat.cloud/categraf/types"
)

func TestAccumulateUsageRecordsBuildsCountersAndHistograms(t *testing.T) {
	duration := int64(1500)
	ttft := int64(300)
	record := usageRecord{
		ID: 101, Model: "claude-sonnet-4-5", RequestType: "stream",
		InputTokens: 10, OutputTokens: 20, CacheCreationTokens: 3, CacheReadTokens: 4,
		TotalCost: 0.25, ActualCost: 0.2, DurationMS: &duration, FirstTokenMS: &ttft,
		CreatedAt: time.Date(2026, 7, 15, 2, 1, 0, 0, time.UTC),
		Group: &struct {
			Name     string `json:"name"`
			Platform string `json:"platform"`
		}{Name: "pro", Platform: "anthropic"},
	}
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := accumulateUsageRecords(&state, []usageRecord{record}); err != nil {
		t.Fatalf("accumulateUsageRecords() error = %v", err)
	}
	series := state.UsageSeries[usageSeriesKey("pro", "anthropic", record.Model, record.RequestType)]
	if series == nil {
		t.Fatal("usage series not created")
	}
	if series.Requests != 1 || series.InputTokens != 10 || series.OutputTokens != 20 || series.TotalTokens != 37 {
		t.Fatalf("request/token counters = %+v", series)
	}
	if series.StandardCostUSD != 0.25 || series.ActualCostUSD != 0.2 {
		t.Fatalf("cost counters = %+v", series)
	}
	if series.Duration.Count != 1 || series.Duration.Sum != 1.5 || series.Duration.Buckets[len(series.Duration.Buckets)-1] != 1 {
		t.Fatalf("duration histogram = %+v", series.Duration)
	}
	if series.FirstToken.Count != 1 || series.FirstToken.Sum != 0.3 {
		t.Fatalf("TTFT histogram = %+v", series.FirstToken)
	}
	if series.FirstToken.Buckets[1] != 0 || series.FirstToken.Buckets[2] != 1 {
		t.Fatalf("TTFT buckets = %v, want 0 through 0.25s and 1 at 0.5s", series.FirstToken.Buckets)
	}
}

func TestAccumulateUsageRecordsSkipsNullLatencySamples(t *testing.T) {
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}
	record := usageRecord{ID: 101, Model: "gpt-5", RequestType: "sync", CreatedAt: time.Now()}

	if err := accumulateUsageRecords(&state, []usageRecord{record}); err != nil {
		t.Fatalf("accumulateUsageRecords() error = %v", err)
	}
	series := state.UsageSeries[usageSeriesKey("unknown", "unknown", "gpt-5", "sync")]
	if series.Duration.Count != 0 || series.FirstToken.Count != 0 {
		t.Fatalf("latency counts = %d/%d, want zero", series.Duration.Count, series.FirstToken.Count)
	}
}

func TestEmitUsageMetricsIncludesCountersAndCumulativeBuckets(t *testing.T) {
	ins := &Instance{Site: "supplier-a", state: persistedState{
		Version: stateVersion,
		UsageSeries: map[string]*usageSeriesState{
			"series": {
				Group: "pro", Platform: "anthropic", Model: "claude-sonnet-4-5", RequestType: "stream",
				Requests: 2, InputTokens: 10, OutputTokens: 20, TotalTokens: 30,
				Duration:   histogramState{Buckets: []uint64{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}, Sum: 3, Count: 2},
				FirstToken: histogramState{Buckets: []uint64{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}, Sum: 0.5, Count: 2},
			},
		},
		LastUsageID: 123,
	}}
	slist := types.NewSampleList()
	ins.emitUsageMetrics(slist, usageScanResult{Pages: 1, RecordsFetched: 3, NewRecords: 2, Complete: true})
	samples := slist.PopBackAll()
	labels := map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "model": "claude-sonnet-4-5", "request_type": "stream"}

	assertSample(t, samples, "sub2api_usage_model_requests_total", 2, labels)
	assertSample(t, samples, "sub2api_usage_model_tokens_total", 10, mergeLabels(labels, map[string]string{"kind": "input"}))
	assertSample(t, samples, "sub2api_usage_model_duration_seconds_count", 2, labels)
	assertSample(t, samples, "sub2api_usage_model_first_token_latency_seconds_bucket", 1, mergeLabels(labels, map[string]string{"le": "0.25"}))
	assertSample(t, samples, "sub2api_usage_model_first_token_latency_seconds_bucket", 2, mergeLabels(labels, map[string]string{"le": "+Inf"}))
	assertSample(t, samples, "sub2api_usage_last_processed_id", 123, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_usage_new_records", 2, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_usage_scan_records", 3, map[string]string{"site": "supplier-a"})
}

func TestGatherAlwaysCollectsAllResourcesAndPersistsUsageAtomically(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/user/profile":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":12,"frozen_balance":1}}`), nil
		case "/api/v1/groups/available":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1.2}]}`), nil
		case "/api/v1/groups/rates":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"7":1.1}}`), nil
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			return jsonResponse(req, http.StatusOK, `{"data":[{"id":"claude-sonnet-4-5"}]}`), nil
		case "/api/v1/usage":
			duration, ttft := 1500, 300
			row := fmt.Sprintf(`{"id":101,"model":"claude-sonnet-4-5","request_type":"stream","input_tokens":10,"output_tokens":20,"total_cost":0.25,"actual_cost":0.2,"duration_ms":%d,"first_token_ms":%d,"created_at":"2026-07-15T10:01:00+08:00","group":{"name":"pro","platform":"anthropic"}}`, duration, ttft)
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1, row, usageJSON(100, "2026-07-15T10:00:00+08:00"))), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		Site: "supplier-a", StateFile: stateFile, UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"),
		client:        client, accessToken: "user-token",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		state:        persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime, UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	for _, resource := range []string{"balance", "groups", "models", "usage"} {
		assertSample(t, samples, "sub2api_scrape_success", 1, map[string]string{"site": "supplier-a", "resource": resource})
	}
	labels := map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "model": "claude-sonnet-4-5", "request_type": "stream"}
	assertSample(t, samples, "sub2api_usage_model_requests_total", 1, labels)
	assertSample(t, samples, "sub2api_usage_last_processed_id", 101, map[string]string{"site": "supplier-a"})

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state.LastUsageID != 101 || len(state.UsageSeries) != 1 {
		t.Fatalf("persisted state = %+v", state)
	}
}

func TestGatherUsageDoesNotAdvanceStateWhenPersistenceFails(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocked path: %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1,
			usageJSON(101, "2026-07-15T10:01:00+08:00"),
			usageJSON(100, "2026-07-15T10:00:00+08:00"),
		)), nil
	})
	ins.StateFile = filepath.Join(blocked, "state.json")
	ins.state.LastUsageCreatedAt = &cursorTime
	slist := types.NewSampleList()

	err := ins.gatherUsage(context.Background(), slist)
	if err == nil {
		t.Fatal("gatherUsage() error = nil, want persistence failure")
	}
	if ins.state.LastUsageID != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("state advanced after persistence failure: %+v", ins.state)
	}
	assertSample(t, slist.PopBackAll(), "sub2api_usage_scan_complete", 0, map[string]string{"site": "supplier-a"})
}

func TestGatherUsageDiscardsStreamedCandidateWhenLaterPageFails(t *testing.T) {
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := usageTestInstance(t, 100, func(req *http.Request) (*http.Response, error) {
		switch req.URL.Query().Get("page") {
		case "1":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 2, usageRangeJSON(120, 101)...)), nil
		case "2":
			return jsonResponse(req, http.StatusInternalServerError, `{"code":500,"message":"database unavailable"}`), nil
		default:
			t.Fatalf("unexpected usage page %q", req.URL.Query().Get("page"))
			return nil, nil
		}
	})
	ins.StateFile = filepath.Join(t.TempDir(), "state.json")
	ins.state.LastUsageCreatedAt = &cursorTime

	slist := types.NewSampleList()
	err := ins.gatherUsage(context.Background(), slist)
	if err == nil {
		t.Fatal("gatherUsage() error = nil, want second-page failure")
	}
	if ins.state.LastUsageID != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("state advanced after a partial streamed scan: %+v", ins.state)
	}
	samples := slist.PopBackAll()
	assertSample(t, samples, "sub2api_usage_scan_pages", 1, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_usage_scan_records", 20, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_usage_scan_complete", 0, map[string]string{"site": "supplier-a"})
}

func TestGatherUsagePersistsStartupTimeForEmptyInitialBaseline(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	startedAt := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := usageTestInstance(t, 0, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1)), nil
	})
	ins.StateFile = stateFile
	ins.startedAt = startedAt
	ins.state.LastUsageCreatedAt = nil

	if err := ins.gatherUsage(context.Background(), types.NewSampleList()); err != nil {
		t.Fatalf("gatherUsage() error = %v", err)
	}
	if !ins.state.UsageInitialized || ins.state.LastUsageID != 0 || ins.state.LastUsageCreatedAt == nil {
		t.Fatalf("empty initialized state = %+v", ins.state)
	}
	if !ins.state.LastUsageCreatedAt.Equal(startedAt) {
		t.Fatalf("baseline time = %s, want %s", ins.state.LastUsageCreatedAt, startedAt)
	}
}
