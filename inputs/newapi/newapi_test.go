package newapi

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs/internal/llmupstream"
	"flashcat.cloud/categraf/pkg/cfg"
	"flashcat.cloud/categraf/types"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDropCancelsActiveGatherWithoutAdvancingUsageState(t *testing.T) {
	statusStarted := make(chan struct{})
	cancelObserved := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/status" {
			startedOnce.Do(func() { close(statusStarted) })
			<-req.Context().Done()
			canceledOnce.Do(func() { close(cancelObserved) })
		}
		return nil, req.Context().Err()
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		Site: "supplier-newapi", UserID: 1001, client: client, accessToken: "system-access-token",
		startedAt: time.Unix(100, 0), now: func() time.Time { return time.Unix(200, 0) },
		state: persistedState{Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100, UsageSeries: make(map[string]*usageSeriesState)},
	}
	plugin := &NewAPI{Instances: []*Instance{ins}}
	gatherDone := make(chan struct{})
	go func() {
		ins.Gather(types.NewSampleList())
		close(gatherDone)
	}()
	<-statusStarted
	dropDone := make(chan struct{})
	go func() {
		plugin.Drop()
		close(dropDone)
	}()
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("Drop() did not cancel the active request")
	}
	<-gatherDone
	<-dropDone
	if ins.state.LastUsageTimestamp != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("usage state advanced during canceled Gather: %+v", ins.state)
	}
	if ctx, _, ok := ins.beginGather(); ok || ctx != nil {
		t.Fatal("beginGather() accepted work after Drop")
	}
}

func TestGatherCollectsNewAPIBalanceGroupsAndModels(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assertHeader(t, req, "Authorization", "user-access-token")
		assertHeader(t, req, "New-Api-User", "1001")
		body := ""
		switch req.URL.Path {
		case "/api/status":
			body = `{"success":true,"message":"","data":{"quota_per_unit":500000}}`
		case "/api/user/self":
			body = `{"success":true,"message":"","data":{"quota":2500000}}`
		case "/api/user/self/groups":
			body = `{"success":true,"message":"","data":{"pro":{"ratio":1.2,"desc":"Pro"},"auto":{"ratio":"自动","desc":"Auto"}}}`
		case "/api/user/models":
			switch req.URL.Query().Get("group") {
			case "pro":
				body = `{"success":true,"message":"","data":["gpt-4o","claude-sonnet-4-5"]}`
			case "auto":
				body = `{"success":true,"message":"","data":[]}`
			default:
				t.Fatalf("unexpected model group %q", req.URL.Query().Get("group"))
			}
		case "/api/log/self":
			body = `{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":100}}`
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
		}
		return jsonResponse(req, http.StatusOK, body), nil
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		Site: "supplier-newapi", UserID: 1001, StateFile: filepath.Join(t.TempDir(), "state.json"),
		client: client, accessToken: "user-access-token",
		startedAt: time.Unix(100, 0), now: func() time.Time { return time.Unix(200, 0) },
		state: persistedState{Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	assertSample(t, samples, "newapi_up", 1, map[string]string{"site": "supplier-newapi"})
	assertSample(t, samples, "newapi_balance_usd", 5, map[string]string{"site": "supplier-newapi"})
	assertSample(t, samples, "newapi_group_count", 2, map[string]string{"site": "supplier-newapi"})
	assertSample(t, samples, "newapi_group_available", 1, map[string]string{"site": "supplier-newapi", "group": "pro", "platform": "newapi"})
	assertSample(t, samples, "newapi_group_ratio", 1.2, map[string]string{"site": "supplier-newapi", "group": "pro", "platform": "newapi", "kind": "effective"})
	assertNoSample(t, samples, "newapi_group_ratio", map[string]string{"site": "supplier-newapi", "group": "auto"})
	assertSample(t, samples, "newapi_model_count", 2, map[string]string{"site": "supplier-newapi", "group": "pro", "platform": "newapi"})
	assertSample(t, samples, "newapi_model_available", 1, map[string]string{"site": "supplier-newapi", "group": "pro", "platform": "newapi", "model": "gpt-4o"})
	assertSample(t, samples, "newapi_model_count", 0, map[string]string{"site": "supplier-newapi", "group": "auto", "platform": "newapi"})
	assertSample(t, samples, "newapi_model_scrape_success", 1, map[string]string{"site": "supplier-newapi", "group": "auto", "platform": "newapi"})
	for _, resource := range []string{"balance", "groups", "models", "usage"} {
		assertSample(t, samples, "newapi_scrape_success", 1, map[string]string{"site": "supplier-newapi", "resource": resource})
		assertMetricWithLabels(t, samples, "newapi_scrape_duration_seconds", map[string]string{"site": "supplier-newapi", "resource": resource})
		assertMetricWithLabels(t, samples, "newapi_last_success_timestamp_seconds", map[string]string{"site": "supplier-newapi", "resource": resource})
	}
}

func TestGatherKeepsOtherResourcesWhenOneGroupModelRequestFails(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/status":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota_per_unit":500000}}`), nil
		case "/api/user/self":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"quota":1000000}}`), nil
		case "/api/user/self/groups":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"broken":{"ratio":1},"healthy":{"ratio":1}}}`), nil
		case "/api/user/models":
			if req.URL.Query().Get("group") == "broken" {
				return jsonResponse(req, http.StatusBadGateway, `{"message":"upstream unavailable"}`), nil
			}
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":["gpt-4o"]}`), nil
		case "/api/log/self":
			return jsonResponse(req, http.StatusOK, `{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100}}`), nil
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
		Site: "supplier", UserID: 1001, StateFile: filepath.Join(t.TempDir(), "state.json"),
		client: client, accessToken: "token",
		startedAt: time.Unix(100, 0), now: func() time.Time { return time.Unix(200, 0) },
		state: persistedState{Version: stateVersion, UsageInitialized: true, LastUsageTimestamp: 100, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	assertSample(t, samples, "newapi_balance_usd", 2, map[string]string{"site": "supplier"})
	assertSample(t, samples, "newapi_scrape_success", 1, map[string]string{"site": "supplier", "resource": "balance"})
	assertSample(t, samples, "newapi_scrape_success", 1, map[string]string{"site": "supplier", "resource": "groups"})
	assertSample(t, samples, "newapi_scrape_success", 0, map[string]string{"site": "supplier", "resource": "models"})
	assertSample(t, samples, "newapi_model_scrape_success", 0, map[string]string{"site": "supplier", "group": "broken", "platform": "newapi"})
	assertSample(t, samples, "newapi_model_scrape_success", 1, map[string]string{"site": "supplier", "group": "healthy", "platform": "newapi"})
	assertSample(t, samples, "newapi_model_available", 1, map[string]string{"site": "supplier", "group": "healthy", "platform": "newapi", "model": "gpt-4o"})
	assertSample(t, samples, "newapi_up", 0, map[string]string{"site": "supplier"})
}

func TestBusinessErrorMessageDoesNotExposeAccessToken(t *testing.T) {
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, `{"success":false,"message":"invalid system-access-token"}`), nil
	})}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{Site: "supplier-newapi", UserID: 1001, client: client, accessToken: "system-access-token"}

	err = ins.gatherBalance(context.Background(), types.NewSampleList(), 500000)
	if err == nil {
		t.Fatal("gatherBalance() error = nil, want business error")
	}
	if strings.Contains(err.Error(), "system-access-token") {
		t.Fatalf("gatherBalance() error leaked access token: %v", err)
	}
}

func TestConfigurationParsesUserCredentials(t *testing.T) {
	var plugin NewAPI
	err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{
		Format: cfg.TomlFormat,
		Config: `
[[instances]]
site = "supplier"
base_url = "https://example.com"
user_id = 1001
access_token = "user-token"
state_file = "/var/lib/categraf/newapi/state.json"
`,
	}, &plugin)
	if err != nil {
		t.Fatalf("LoadSingleConfig() error = %v", err)
	}
	if len(plugin.Instances) != 1 {
		t.Fatalf("parsed instances = %d, want 1", len(plugin.Instances))
	}
	if got := plugin.Instances[0].UserID; got != 1001 {
		t.Fatalf("user_id = %d, want 1001", got)
	}
	if got := plugin.Instances[0].StateFile; got != "/var/lib/categraf/newapi/state.json" {
		t.Fatalf("state_file = %q", got)
	}
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func assertHeader(t *testing.T, req *http.Request, name, want string) {
	t.Helper()
	if got := req.Header.Get(name); got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}

func assertSample(t *testing.T, samples []*types.Sample, metric string, want float64, labels map[string]string) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric != metric || !hasLabels(sample.Labels, labels) {
			continue
		}
		got, ok := numericValue(sample.Value)
		if !ok {
			t.Fatalf("metric %s value has type %T", metric, sample.Value)
		}
		if got != want {
			t.Fatalf("metric %s labels %v = %v, want %v", metric, labels, got, want)
		}
		return
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertMetricWithLabels(t *testing.T, samples []*types.Sample, metric string, labels map[string]string) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric == metric && hasLabels(sample.Labels, labels) {
			return
		}
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertNoSample(t *testing.T, samples []*types.Sample, metric string, labels map[string]string) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric == metric && hasLabels(sample.Labels, labels) {
			t.Fatalf("unexpected metric %s with labels %v", metric, labels)
		}
	}
}

func hasLabels(got, want map[string]string) bool {
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}

func numericValue(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func TestDistributedConfigurationResolvesAccessTokenFromEnvironment(t *testing.T) {
	t.Setenv("NEWAPI_ACCESS_TOKEN", "token-from-env")
	data, err := os.ReadFile("../../conf/input.newapi/newapi.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin NewAPI
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	ins := plugin.Instances[0]
	// The shipped state_file path is outside the test sandbox.
	ins.StateFile = filepath.Join(t.TempDir(), "state.json")
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer plugin.Drop()
	if ins.accessToken != "token-from-env" {
		t.Fatalf("access_token resolved to %q", ins.accessToken)
	}
}

func TestInitReportsUnsetAccessTokenVariable(t *testing.T) {
	ins := &Instance{
		Site:        "supplier",
		BaseURL:     "https://example.com",
		UserID:      1001,
		AccessToken: config.NewSecret([]byte("@{env:NEWAPI_TEST_UNSET_TOKEN}")),
		StateFile:   filepath.Join(t.TempDir(), "state.json"),
	}
	err := ins.Init()
	if err == nil {
		(&NewAPI{Instances: []*Instance{ins}}).Drop()
	}
	if err == nil || !strings.Contains(err.Error(), "NEWAPI_TEST_UNSET_TOKEN") {
		t.Fatalf("Init() error = %v, want the missing variable name", err)
	}
}
