package newapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"

	"flashcat.cloud/categraf/types"
)

type cancelOnReadBody struct {
	io.Reader
	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelOnReadBody) Read(data []byte) (int, error) {
	count, err := b.Reader.Read(data)
	if count > 0 {
		b.once.Do(b.cancel)
	}
	return count, err
}

func (b *cancelOnReadBody) Close() error { return nil }

func TestAccumulateUsageRecordsBuildsCountersAndHistograms(t *testing.T) {
	duration := int64(2)
	record := usageRecord{
		RequestID: "req-1", CreatedAt: 101, Type: consumeLogType,
		ModelName: "gpt-4o", Group: "pro", PromptTokens: 10, CompletionTokens: 20,
		Quota: 500, UseTime: &duration, IsStream: true,
		Other: `{"frt":300,"cache_tokens":4,"cache_creation_tokens":3,"cache_creation_tokens_5m":5,"cache_creation_tokens_1h":7}`,
	}
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := accumulateUsageRecords(&state, []usageRecord{record}, 500000); err != nil {
		t.Fatalf("accumulateUsageRecords() error = %v", err)
	}
	series := state.UsageSeries[usageSeriesKey("pro", "gpt-4o", "stream")]
	if series == nil {
		t.Fatal("usage series not created")
	}
	if series.Requests != 1 || series.InputTokens != 10 || series.OutputTokens != 20 || series.TotalTokens != 30 {
		t.Fatalf("request/token counters = %+v", series)
	}
	if series.CacheReadTokens != 4 || series.CacheCreationTokens != 12 {
		t.Fatalf("cache counters = %+v", series)
	}
	if math.Abs(series.ActualCostUSD-0.001) > 1e-12 {
		t.Fatalf("actual cost = %v, want 0.001", series.ActualCostUSD)
	}
	if series.Duration.Count != 1 || series.Duration.Sum != 2 {
		t.Fatalf("duration histogram = %+v", series.Duration)
	}
	if series.FirstToken.Count != 1 || series.FirstToken.Sum != 0.3 {
		t.Fatalf("TTFT histogram = %+v", series.FirstToken)
	}
}

func TestAccumulateUsageRecordsSkipsTTFTForNonStreamAndDurationForTask(t *testing.T) {
	duration := int64(8)
	record := usageRecord{
		RequestID: "req-task", CreatedAt: 101, Type: consumeLogType,
		ModelName: "image-model", UseTime: &duration,
		Other: `{"frt":200,"is_task":true}`,
	}
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := accumulateUsageRecords(&state, []usageRecord{record}, 500000); err != nil {
		t.Fatalf("accumulateUsageRecords() error = %v", err)
	}
	series := state.UsageSeries[usageSeriesKey("unknown", "image-model", "non_stream")]
	if series.Duration.Count != 0 || series.FirstToken.Count != 0 {
		t.Fatalf("latency counts = %d/%d, want zero", series.Duration.Count, series.FirstToken.Count)
	}
}

func TestAccumulateUsageRecordsRejectsInvalidOtherJSON(t *testing.T) {
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}
	record := usageRecord{RequestID: "req-1", Type: consumeLogType, Other: "{"}

	err := accumulateUsageRecords(&state, []usageRecord{record}, 500000)
	if err == nil {
		t.Fatal("accumulateUsageRecords() error = nil, want invalid other error")
	}
}

func TestEmitUsageMetricsIncludesCountersAndCumulativeBuckets(t *testing.T) {
	ins := &Instance{Site: "supplier-a", state: persistedState{
		Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 123,
		UsageSeries: map[string]*usageSeriesState{
			"series": {
				Group: "pro", Model: "gpt-4o", RequestType: "stream",
				Requests: 2, InputTokens: 10, OutputTokens: 20, TotalTokens: 30, ActualCostUSD: 0.5,
				Duration:   histogramState{Buckets: []uint64{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}, Sum: 3, Count: 2},
				FirstToken: histogramState{Buckets: []uint64{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2}, Sum: 0.5, Count: 2},
			},
		},
	}}
	slist := types.NewSampleList()
	ins.emitUsageMetrics(slist, usageScanResult{Pages: 1, RecordsFetched: 3, NewRecords: 2, Complete: true})
	samples := slist.PopBackAll()
	labels := map[string]string{"site": "supplier-a", "group": "pro", "platform": "newapi", "model": "gpt-4o", "request_type": "stream"}

	assertSample(t, samples, "newapi_usage_model_requests_total", 2, labels)
	assertSample(t, samples, "newapi_usage_model_tokens_total", 10, mergeLabels(labels, map[string]string{"kind": "input"}))
	assertSample(t, samples, "newapi_usage_model_cost_usd_total", 0.5, mergeLabels(labels, map[string]string{"kind": "actual"}))
	assertSample(t, samples, "newapi_usage_model_duration_seconds_count", 2, labels)
	assertSample(t, samples, "newapi_usage_model_first_token_latency_seconds_bucket", 1, mergeLabels(labels, map[string]string{"le": "0.25"}))
	assertSample(t, samples, "newapi_usage_last_processed_timestamp_seconds", 123, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "newapi_usage_new_records", 2, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "newapi_usage_scan_records", 3, map[string]string{"site": "supplier-a"})
}

func TestGatherUsageDoesNotAdvanceStateWhenPersistenceFails(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocked path: %v", err)
	}
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 10, 1, usageJSON(1, "req-new", 101))), nil
	})
	ins.StateFile = filepath.Join(blocked, "state.json")
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 100
	ins.state.LastUsageRequestIDs = []string{"req-old"}
	slist := types.NewSampleList()

	err := ins.gatherUsage(context.Background(), slist, 500000)
	if err == nil {
		t.Fatal("gatherUsage() error = nil, want persistence failure")
	}
	if ins.state.LastUsageTimestamp != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("state advanced after persistence failure: %+v", ins.state)
	}
	assertSample(t, slist.PopBackAll(), "newapi_usage_scan_complete", 0, map[string]string{"site": "supplier-a"})
}

func TestGatherUsagePersistsEmptyInitialBaseline(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 10, 0)), nil
	})
	ins.StateFile = stateFile
	ins.startedAt = time.Unix(100, 0)
	ins.state = persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := ins.gatherUsage(context.Background(), types.NewSampleList(), 500000); err != nil {
		t.Fatalf("gatherUsage() error = %v", err)
	}
	if !ins.state.UsageInitialized || ins.state.LastUsageTimestamp != 100 {
		t.Fatalf("initialized state = %+v, want timestamp 100", ins.state)
	}
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("state file not persisted: %v", err)
	}
}

func TestGatherAlwaysCollectsAllResourcesAndSharesStatusRequest(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	statusRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/status":
			statusRequests++
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota_per_unit":500000}}`), nil
		case "/api/user/self":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota":2500000}}`), nil
		case "/api/user/self/groups":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"pro":{"ratio":1.2}}}`), nil
		case "/api/user/models":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":["gpt-4o"]}`), nil
		case "/api/log/self":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 1, usageJSON(1, "req-new", 101))), nil
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
		Site: "supplier-a", UserID: 1001, StateFile: stateFile,
		client: client, accessToken: "system-access-token",
		startedAt: time.Unix(100, 0), now: func() time.Time { return time.Unix(200, 0) },
		state: persistedState{
			Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100,
			LastUsageRequestIDs: []string{"req-old"}, UsageSeries: make(map[string]*usageSeriesState),
		},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	if statusRequests != 1 {
		t.Fatalf("status requests = %d, want 1", statusRequests)
	}
	for _, resource := range []string{"balance", "groups", "models", "usage"} {
		assertSample(t, samples, "newapi_scrape_success", 1, map[string]string{"site": "supplier-a", "resource": resource})
	}
	assertSample(t, samples, "newapi_balance_usd", 5, map[string]string{"site": "supplier-a"})
	labels := map[string]string{"site": "supplier-a", "group": "pro", "platform": "newapi", "model": "gpt-4o", "request_type": "stream"}
	assertSample(t, samples, "newapi_usage_model_requests_total", 1, labels)
	assertSample(t, samples, "newapi_up", 1, map[string]string{"site": "supplier-a"})

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if strings.Contains(string(data), "system-access-token") {
		t.Fatal("persisted state contains the system access token")
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state.LastUsageTimestamp != 101 || len(state.UsageSeries) != 1 {
		t.Fatalf("persisted state = %+v", state)
	}
}

func TestGatherUsageFailureKeepsOtherResources(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/status":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota_per_unit":500000}}`), nil
		case "/api/user/self":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota":1000000}}`), nil
		case "/api/user/self/groups":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"pro":{"ratio":1}}}`), nil
		case "/api/user/models":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":["gpt-4o"]}`), nil
		case "/api/log/self":
			row := `{"id":1,"request_id":"req-new","created_at":101,"type":2,"model_name":"gpt-4o","group":"pro","prompt_tokens":10,"completion_tokens":5,"quota":500,"use_time":2,"is_stream":true,"other":"{"}`
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 1, row)), nil
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
		Site: "supplier-a", UserID: 1001, StateFile: filepath.Join(t.TempDir(), "state.json"),
		client: client, accessToken: "system-access-token",
		startedAt: time.Unix(100, 0), now: func() time.Time { return time.Unix(200, 0) },
		state: persistedState{Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	assertSample(t, samples, "newapi_balance_usd", 2, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "newapi_group_available", 1, map[string]string{"site": "supplier-a", "group": "pro"})
	assertSample(t, samples, "newapi_model_available", 1, map[string]string{"site": "supplier-a", "model": "gpt-4o"})
	assertSample(t, samples, "newapi_scrape_success", 0, map[string]string{"site": "supplier-a", "resource": "usage"})
	assertSample(t, samples, "newapi_up", 0, map[string]string{"site": "supplier-a"})
	if ins.state.LastUsageTimestamp != 100 {
		t.Fatalf("usage timestamp advanced to %d", ins.state.LastUsageTimestamp)
	}
}

func TestGatherUsageErrorDoesNotExposeRequestID(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		row := `{"id":1,"request_id":"req-sensitive-value","created_at":101,"type":2,"model_name":"gpt-4o","group":"pro","prompt_tokens":10,"completion_tokens":5,"quota":500,"use_time":2,"is_stream":true,"other":"{"}`
		return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 1, row)), nil
	})
	ins.StateFile = filepath.Join(t.TempDir(), "state.json")

	err := ins.gatherUsage(context.Background(), types.NewSampleList(), 500000)
	if err == nil {
		t.Fatal("gatherUsage() error = nil, want invalid usage record")
	}
	if strings.Contains(err.Error(), "req-sensitive-value") {
		t.Fatalf("gatherUsage() error leaked request_id: %v", err)
	}
}

func TestGatherUsageDiscardsStreamedCandidateWhenLaterPageFails(t *testing.T) {
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		switch req.URL.Query().Get("p") {
		case "1":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 101, usageRowsJSON(1, 100, 101)...)), nil
		case "2":
			return jsonResponse(req, http.StatusInternalServerError, `{"code":500,"message":"database unavailable"}`), nil
		default:
			t.Fatalf("unexpected page %q", req.URL.Query().Get("p"))
			return nil, nil
		}
	})
	ins.StateFile = filepath.Join(t.TempDir(), "state.json")
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 100

	slist := types.NewSampleList()
	err := ins.gatherUsage(context.Background(), slist, 500000)
	if err == nil {
		t.Fatal("gatherUsage() error = nil, want second-page failure")
	}
	if ins.state.LastUsageTimestamp != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("state advanced after partial streamed scan: %+v", ins.state)
	}
	samples := slist.PopBackAll()
	assertSample(t, samples, "newapi_usage_scan_pages", 1, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "newapi_usage_scan_records", 100, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "newapi_usage_scan_complete", 0, map[string]string{"site": "supplier-a"})
}

func TestGatherUsageDoesNotCommitWhenContextIsCanceledAfterFinalResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ins := usageTestInstance(t, func(req *http.Request) (*http.Response, error) {
		response := jsonResponse(req, http.StatusOK, usagePageJSON(1, 100, 1, usageJSON(1, "req-new", 101)))
		response.Body = &cancelOnReadBody{Reader: response.Body, cancel: cancel}
		return response, nil
	})
	ins.StateFile = filepath.Join(t.TempDir(), "state.json")
	ins.state.UsageInitialized = true
	ins.state.LastUsageTimestamp = 100

	err := ins.gatherUsage(ctx, types.NewSampleList(), 500000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("gatherUsage() error = %v, want context.Canceled", err)
	}
	if ins.state.LastUsageTimestamp != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("state advanced after context cancellation: %+v", ins.state)
	}
}

func TestCommitUsageStateRefusesCommitAfterDropBegins(t *testing.T) {
	ins := &Instance{
		StateFile: filepath.Join(t.TempDir(), "state.json"),
		state: persistedState{
			Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100,
			UsageSeries: make(map[string]*usageSeriesState),
		},
	}
	ins.beginDrop()
	candidate := clonePersistedState(ins.state)
	candidate.LastUsageTimestamp = 200

	if err := ins.commitUsageState(context.Background(), candidate); !errors.Is(err, context.Canceled) {
		t.Fatalf("commitUsageState() error = %v, want context.Canceled", err)
	}
	if ins.state.LastUsageTimestamp != 100 {
		t.Fatalf("state timestamp = %d, want unchanged 100", ins.state.LastUsageTimestamp)
	}
	if _, err := os.Stat(ins.StateFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state file error = %v, want not created", err)
	}
}
