package sub2api

import (
	"errors"
	"io"
	"log"
	"net/http"
	"os"
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

// -----------------------------------------------------------------------------
// /v1/usage — three modes
// -----------------------------------------------------------------------------

const walletUsageBody = `{
	"mode": "unrestricted",
	"isValid": true,
	"planName": "钱包余额",
	"remaining": 12.5,
	"balance": 12.5,
	"unit": "USD",
	"usage": {
		"today": {"requests": 3, "input_tokens": 30, "output_tokens": 40, "cache_creation_tokens": 5, "cache_read_tokens": 7, "total_tokens": 82, "cost": 0.1, "actual_cost": 0.09},
		"total": {"requests": 1200, "input_tokens": 12000, "output_tokens": 34000, "cache_creation_tokens": 500, "cache_read_tokens": 700, "total_tokens": 47200, "cost": 4.2, "actual_cost": 4.0},
		"average_duration_ms": 850,
		"rpm": 2.5,
		"tpm": 300
	},
	"daily_usage": [{"date":"2026-09-11","requests":10,"total_tokens":900,"cost":0.05,"actual_cost":0.04}],
	"model_stats": [
		{"model":"claude-sonnet-4-5","requests":800,"input_tokens":8000,"output_tokens":22000,"cache_creation_tokens":300,"cache_read_tokens":500,"total_tokens":30800,"cost":3.0,"actual_cost":2.8,"account_cost":2.5},
		{"model":"claude-opus-4-1","requests":400,"input_tokens":4000,"output_tokens":12000,"cache_creation_tokens":200,"cache_read_tokens":200,"total_tokens":16400,"cost":1.2,"actual_cost":1.2,"account_cost":1.1}
	]
}`

const quotaLimitedUsageBody = `{
	"mode": "quota_limited",
	"isValid": true,
	"status": "active",
	"quota": {"limit": 50, "used": 12.5, "remaining": 37.5, "unit": "USD"},
	"remaining": 37.5,
	"unit": "USD",
	"rate_limits": [
		{"window": "5h", "limit": 10, "used": 3, "remaining": 7, "reset_at": "2026-09-11T15:00:00Z"},
		{"window": "1d", "limit": 20, "used": 5, "remaining": 15, "reset_at": "2026-09-12T00:00:00Z"}
	],
	"expires_at": "2026-12-31T00:00:00Z",
	"days_until_expiry": 111,
	"usage": {
		"today": {"requests": 5, "total_tokens": 100, "cost": 0.2, "actual_cost": 0.2},
		"total": {"requests": 500, "total_tokens": 20000, "cost": 12.5, "actual_cost": 12.5},
		"average_duration_ms": 900,
		"rpm": 1,
		"tpm": 200
	}
}`

const subscriptionUsageBody = `{
	"mode": "unrestricted",
	"isValid": true,
	"planName": "Pro Monthly",
	"remaining": 42.0,
	"unit": "USD",
	"subscription": {
		"daily_usage_usd": 3.5,
		"weekly_usage_usd": 15.0,
		"monthly_usage_usd": 58.0,
		"daily_limit_usd": 10.0,
		"weekly_limit_usd": 60.0,
		"monthly_limit_usd": 100.0,
		"weekly_window_start": "2026-09-08T00:00:00Z",
		"expires_at": "2026-10-01T00:00:00Z"
	},
	"usage": {
		"today": {"requests": 5, "total_tokens": 100, "cost": 0.5, "actual_cost": 0.5},
		"total": {"requests": 1500, "total_tokens": 500000, "cost": 58.0, "actual_cost": 58.0},
		"average_duration_ms": 750,
		"rpm": 5,
		"tpm": 500
	}
}`

func TestGatherWalletModeEmitsRemainingAndTotals(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/usage" {
			t.Fatalf("unexpected request path %q", req.URL.Path)
		}
		assertHeader(t, req, "Authorization", "Bearer my-api-key")
		return jsonResponse(req, http.StatusOK, walletUsageBody), nil
	})
	ins := newTestInstance(t, transport)

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_up", 1, labels)
	assertSample(t, samples, "sub2api_key_remaining_usd", 12.5, mergeLabelsForTest(labels, "mode", "wallet"))
	assertSample(t, samples, "sub2api_key_wallet_balance_usd", 12.5, labels)
	assertSample(t, samples, "sub2api_key_requests_total", int64(1200), labels)
	assertSample(t, samples, "sub2api_key_tokens_total", int64(47200), mergeLabelsForTest(labels, "kind", "total"))
	assertSample(t, samples, "sub2api_key_cost_usd_total", 4.2, mergeLabelsForTest(labels, "kind", "standard"))
	assertSample(t, samples, "sub2api_key_cost_usd_total", 4.0, mergeLabelsForTest(labels, "kind", "actual"))
	assertSample(t, samples, "sub2api_key_average_duration_seconds", 0.85, labels)
	assertSample(t, samples, "sub2api_key_model_requests", int64(800),
		mergeLabelsForTest(labels, "model", "claude-sonnet-4-5", "window", "30d"))
	assertSample(t, samples, "sub2api_scrape_success", 1, mergeLabelsForTest(labels, "resource", "usage"))
	assertMetricWithLabels(t, samples, "sub2api_scrape_duration_seconds", mergeLabelsForTest(labels, "resource", "usage"))
	assertMetricWithLabels(t, samples, "sub2api_last_success_timestamp_seconds", mergeLabelsForTest(labels, "resource", "usage"))
}

func TestGatherQuotaLimitedModeEmitsQuotaAndRateLimits(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/usage" {
			t.Fatalf("unexpected request path %q", req.URL.Path)
		}
		return jsonResponse(req, http.StatusOK, quotaLimitedUsageBody), nil
	})
	ins := newTestInstance(t, transport)

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_key_remaining_usd", 37.5, mergeLabelsForTest(labels, "mode", "quota_limited"))
	assertSample(t, samples, "sub2api_key_quota_limit_usd", 50.0, labels)
	assertSample(t, samples, "sub2api_key_quota_used_usd", 12.5, labels)
	assertSample(t, samples, "sub2api_key_rate_limit_remaining", 7.0, mergeLabelsForTest(labels, "window", "5h"))
	assertSample(t, samples, "sub2api_key_rate_limit_remaining", 15.0, mergeLabelsForTest(labels, "window", "1d"))
	assertMetricWithLabels(t, samples, "sub2api_key_rate_limit_reset_timestamp", mergeLabelsForTest(labels, "window", "5h"))
	assertSample(t, samples, "sub2api_key_days_until_expiry", 111, labels)
	assertMetricWithLabels(t, samples, "sub2api_key_expires_timestamp", labels)
}

func TestGatherSubscriptionModeEmitsWindows(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusOK, subscriptionUsageBody), nil
	})
	ins := newTestInstance(t, transport)

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_key_remaining_usd", 42.0, mergeLabelsForTest(labels, "mode", "subscription"))
	assertSample(t, samples, "sub2api_key_subscription_usage_usd", 3.5, mergeLabelsForTest(labels, "window", "daily"))
	assertSample(t, samples, "sub2api_key_subscription_usage_usd", 15.0, mergeLabelsForTest(labels, "window", "weekly"))
	assertSample(t, samples, "sub2api_key_subscription_usage_usd", 58.0, mergeLabelsForTest(labels, "window", "monthly"))
	assertSample(t, samples, "sub2api_key_subscription_limit_usd", 10.0, mergeLabelsForTest(labels, "window", "daily"))
	assertSample(t, samples, "sub2api_key_subscription_limit_usd", 60.0, mergeLabelsForTest(labels, "window", "weekly"))
	assertSample(t, samples, "sub2api_key_subscription_limit_usd", 100.0, mergeLabelsForTest(labels, "window", "monthly"))
}

// -----------------------------------------------------------------------------
// Optional switches: collect_billing / collect_models
// -----------------------------------------------------------------------------

func TestGatherCollectsBillingWhenEnabled(t *testing.T) {
	t.Parallel()

	billingCalls := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/v1/usage":
			return jsonResponse(req, http.StatusOK, walletUsageBody), nil
		case "/v1/sub2api/billing":
			billingCalls++
			assertHeader(t, req, "Authorization", "Bearer my-api-key")
			return jsonResponse(req, http.StatusOK, `{
				"object":"sub2api.key_billing","schema_version":1,"billing_scope":"token",
				"group_rate_multiplier":1.0,"user_rate_multiplier":0.8,"resolved_rate_multiplier":0.8,
				"peak_rate_enabled":true,"peak_rate_multiplier":1.5,"applied_peak_multiplier":1.5,
				"effective_rate_multiplier":1.2
			}`), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	ins := newTestInstance(t, transport)
	ins.CollectBilling = true

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	if billingCalls != 1 {
		t.Fatalf("billing calls = %d, want 1", billingCalls)
	}
	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_key_rate_multiplier", 1.0, mergeLabelsForTest(labels, "kind", "group"))
	assertSample(t, samples, "sub2api_key_rate_multiplier", 0.8, mergeLabelsForTest(labels, "kind", "user"))
	assertSample(t, samples, "sub2api_key_rate_multiplier", 0.8, mergeLabelsForTest(labels, "kind", "resolved"))
	assertSample(t, samples, "sub2api_key_rate_multiplier", 1.5, mergeLabelsForTest(labels, "kind", "peak_configured"))
	assertSample(t, samples, "sub2api_key_rate_multiplier", 1.2, mergeLabelsForTest(labels, "kind", "effective"))
	assertSample(t, samples, "sub2api_key_peak_rate_enabled", 1, labels)
	assertSample(t, samples, "sub2api_scrape_success", 1, mergeLabelsForTest(labels, "resource", "billing"))
}

func TestGatherCollectsModelsWhenEnabled(t *testing.T) {
	t.Parallel()

	modelCalls := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/v1/usage":
			return jsonResponse(req, http.StatusOK, walletUsageBody), nil
		case "/v1/models":
			modelCalls++
			assertHeader(t, req, "Authorization", "Bearer my-api-key")
			return jsonResponse(req, http.StatusOK, `{"data":[{"id":"claude-opus-4-1"},{"id":"claude-sonnet-4-5"}]}`), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	ins := newTestInstance(t, transport)
	ins.CollectModels = true

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	if modelCalls != 1 {
		t.Fatalf("model calls = %d, want 1", modelCalls)
	}
	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_key_model_count", 2, labels)
	assertSample(t, samples, "sub2api_key_model_available", 1, mergeLabelsForTest(labels, "model", "claude-opus-4-1"))
	assertSample(t, samples, "sub2api_key_model_available", 1, mergeLabelsForTest(labels, "model", "claude-sonnet-4-5"))
}

func TestGatherSkipsOptionalCollectionByDefault(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/usage" {
			t.Fatalf("unexpected request path %q; billing/models should not be called by default", req.URL.Path)
		}
		return jsonResponse(req, http.StatusOK, walletUsageBody), nil
	})
	ins := newTestInstance(t, transport)

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	labels := map[string]string{"key": "prod-key"}
	assertNoSample(t, samples, "sub2api_scrape_success", mergeLabelsForTest(labels, "resource", "billing"))
	assertNoSample(t, samples, "sub2api_scrape_success", mergeLabelsForTest(labels, "resource", "models"))
	assertNoSample(t, samples, "sub2api_key_rate_multiplier", labels)
	assertNoSample(t, samples, "sub2api_key_model_count", labels)
}

// -----------------------------------------------------------------------------
// Error paths
// -----------------------------------------------------------------------------

func TestGatherReportsFailureOnUpstreamError(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusUnauthorized, `{"type":"error","error":{"type":"authentication_error","message":"Invalid API key"}}`), nil
	})
	ins := newTestInstance(t, transport)

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	labels := map[string]string{"key": "prod-key"}
	assertSample(t, samples, "sub2api_up", 0, labels)
	assertSample(t, samples, "sub2api_scrape_success", 0, mergeLabelsForTest(labels, "resource", "usage"))
	assertNoSample(t, samples, "sub2api_key_remaining_usd", labels)
}

func TestGatherRedactsRawAPIKeyInErrorMessages(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, http.StatusForbidden, `{"type":"error","error":{"type":"permission_error","message":"Key my-api-key rejected"}}`), nil
	})
	logs, restore := captureLogs()
	defer restore()

	ins := newTestInstance(t, transport)
	ins.Gather(types.NewSampleList())

	if strings.Contains(logs.String(), "my-api-key") {
		t.Fatalf("logs leaked raw api key: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "[redacted]") {
		t.Fatalf("logs missing redaction: %s", logs.String())
	}
}

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

func TestDropCancelsInFlightUsageCall(t *testing.T) {
	usageStarted := make(chan struct{})
	cancelObserved := make(chan struct{})
	allowReturn := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1/usage" {
			t.Fatalf("unexpected request path %q", req.URL.Path)
		}
		startedOnce.Do(func() { close(usageStarted) })
		select {
		case <-req.Context().Done():
			canceledOnce.Do(func() { close(cancelObserved) })
		case <-time.After(time.Second):
		}
		<-allowReturn
		err := req.Context().Err()
		if err == nil {
			err = errors.New("usage request was not canceled")
		}
		return nil, err
	})
	ins := newTestInstance(t, transport)

	done := make(chan struct{})
	go func() {
		ins.Gather(types.NewSampleList())
		close(done)
	}()
	<-usageStarted

	dropDone := make(chan struct{})
	go func() {
		ins.drop()
		close(dropDone)
	}()

	select {
	case <-cancelObserved:
	case <-time.After(500 * time.Millisecond):
		close(allowReturn)
		<-done
		<-dropDone
		t.Fatal("Drop() did not cancel the active /v1/usage request")
	}
	close(allowReturn)
	<-done
	<-dropDone
}

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

func TestInitRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*Instance)
		wantErr string
	}{
		{"missing name", func(i *Instance) { i.Name = "" }, "name is required"},
		{"missing base_url", func(i *Instance) { i.BaseURL = "" }, "base_url is required"},
		{"missing api_key", func(i *Instance) { i.APIKey = config.Secret{} }, "api_key is required"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ins := &Instance{
				Name:    "prod-key",
				BaseURL: "https://supplier.example.com",
				APIKey:  config.NewSecret([]byte("my-api-key")),
			}
			tt.mutate(ins)
			err := ins.Init()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Init() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestInitRejectsLegacyConfiguration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name: "legacy site",
			config: `
[[instances]]
site = "supplier"
base_url = "https://example.com"
api_key = "my-api-key"
`,
			wantErr: "'site' was renamed to 'name'",
		},
		{
			name: "legacy refresh_token",
			config: `
[[instances]]
name = "prod"
base_url = "https://example.com"
refresh_token = "rt"
api_key = "my-api-key"
`,
			wantErr: "'refresh_token' auth was removed",
		},
		{
			name: "legacy state_file",
			config: `
[[instances]]
name = "prod"
base_url = "https://example.com"
state_file = "/tmp/sub2api.json"
api_key = "my-api-key"
`,
			wantErr: "'state_file' was removed",
		},
		{
			name: "legacy model_api_keys",
			config: `
[[instances]]
name = "prod"
base_url = "https://example.com"
api_key = "my-api-key"

[[instances.model_api_keys]]
api_key = "group-token"
`,
			wantErr: "'model_api_keys' was removed",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var plugin Sub2API
			if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: tt.config}, &plugin); err != nil {
				t.Fatalf("LoadSingleConfig() error = %v", err)
			}
			if len(plugin.Instances) != 1 {
				t.Fatalf("instances = %d, want 1", len(plugin.Instances))
			}
			err := plugin.Instances[0].Init()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Init() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDistributedConfigurationLoads(t *testing.T) {
	data, err := os.ReadFile("../../conf/input.sub2api/sub2api.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin Sub2API
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	if len(plugin.Instances) < 1 {
		t.Fatalf("instances = %d, want at least 1", len(plugin.Instances))
	}
	if plugin.Instances[0].Name == "" || plugin.Instances[0].BaseURL == "" {
		t.Fatalf("first instance missing required fields: %+v", plugin.Instances[0])
	}
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func newTestInstance(t *testing.T, transport http.RoundTripper) *Instance {
	t.Helper()
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		Name:    "prod-key",
		BaseURL: "https://supplier.example.com",
		client:  client,
		apiKey:  "my-api-key",
	}
	ins.updateRedactionSecrets()
	return ins
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

func assertSample(t *testing.T, samples []*types.Sample, metric string, want any, labels map[string]string) {
	t.Helper()
	wantFloat, ok := numericValue(want)
	if !ok {
		t.Fatalf("assertSample called with non-numeric want %T", want)
	}
	for _, sample := range samples {
		if sample.Metric != metric || !hasLabels(sample.Labels, labels) {
			continue
		}
		got, ok := numericValue(sample.Value)
		if !ok {
			t.Fatalf("metric %s value has type %T", metric, sample.Value)
		}
		if got != wantFloat {
			t.Fatalf("metric %s labels %v = %v, want %v", metric, labels, got, wantFloat)
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

// mergeLabelsForTest lets tests declare label additions in a compact form.
func mergeLabelsForTest(base map[string]string, kv ...string) map[string]string {
	result := make(map[string]string, len(base)+len(kv)/2)
	for key, value := range base {
		result[key] = value
	}
	for i := 0; i+1 < len(kv); i += 2 {
		result[kv[i]] = kv[i+1]
	}
	return result
}

// captureLogs redirects the standard logger to a buffer so tests can assert
// that raw secrets never appear in log output. Parallel tests keep logging
// through the global logger while the buffer is installed, so the buffer is
// guarded and capturing tests run one at a time.
func captureLogs() (*lockedBuilder, func()) {
	captureLogsMu.Lock()
	buf := &lockedBuilder{}
	previous := log.Writer()
	log.SetOutput(buf)
	return buf, func() {
		log.SetOutput(previous)
		captureLogsMu.Unlock()
	}
}

var captureLogsMu sync.Mutex

type lockedBuilder struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *lockedBuilder) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

func (b *lockedBuilder) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

func TestDistributedConfigurationResolvesAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("SUB2API_API_KEY", "key-from-env")
	data, err := os.ReadFile("../../conf/input.sub2api/sub2api.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin Sub2API
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	ins := plugin.Instances[0]
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer ins.drop()
	if ins.apiKey != "key-from-env" {
		t.Fatalf("api_key resolved to %q", ins.apiKey)
	}
}

func TestInitReportsUnsetAPIKeyVariable(t *testing.T) {
	ins := &Instance{
		Name:    "prod-key",
		BaseURL: "https://supplier.example.com",
		APIKey:  config.NewSecret([]byte("@{env:SUB2API_TEST_UNSET_KEY}")),
	}
	err := ins.Init()
	if err == nil {
		ins.drop()
	}
	if err == nil || !strings.Contains(err.Error(), "SUB2API_TEST_UNSET_KEY") {
		t.Fatalf("Init() error = %v, want the missing variable name", err)
	}
}
