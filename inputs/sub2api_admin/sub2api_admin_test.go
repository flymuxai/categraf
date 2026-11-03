package sub2api_admin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs/internal/llmupstream"
	"flashcat.cloud/categraf/pkg/cfg"
	"flashcat.cloud/categraf/types"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestPluginDefaultsInterval(t *testing.T) {
	var plugin Sub2APIAdmin
	if err := plugin.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got := time.Duration(plugin.Interval); got != 5*time.Minute {
		t.Fatalf("interval = %s, want 5m", got)
	}
}

func TestInstanceConfiguration(t *testing.T) {
	valid := func() *Instance {
		return &Instance{Site: "production", BaseURL: "https://example.com", AdminAPIKey: config.NewSecret([]byte("admin-secret"))}
	}
	t.Run("defaults", func(t *testing.T) {
		ins := valid()
		t.Cleanup(ins.AdminAPIKey.Destroy)
		if err := ins.Init(); err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		if ins.Concurrency != 10 || time.Duration(ins.Timeout) != 25*time.Second ||
			time.Duration(ins.GatherTimeout) != 4*time.Minute || ins.MaxProxies != 100 {
			t.Fatalf("unexpected defaults: concurrency=%d timeout=%s gather_timeout=%s max=%d",
				ins.Concurrency, time.Duration(ins.Timeout), time.Duration(ins.GatherTimeout), ins.MaxProxies)
		}
	})

	tests := []struct {
		name string
		edit func(*Instance)
		want string
	}{
		{name: "missing site", edit: func(ins *Instance) { ins.Site = "" }, want: "site is required"},
		{name: "missing base URL", edit: func(ins *Instance) { ins.BaseURL = "" }, want: "base_url is required"},
		{name: "missing key", edit: func(ins *Instance) { ins.AdminAPIKey.Destroy() }, want: "admin_api_key is required"},
		{name: "high concurrency", edit: func(ins *Instance) { ins.Concurrency = 11 }, want: "concurrency"},
		{name: "short timeout", edit: func(ins *Instance) { ins.Timeout = config.Duration(time.Millisecond) }, want: "at least 1s"},
		{name: "short gather timeout", edit: func(ins *Instance) {
			ins.Timeout = config.Duration(2 * time.Second)
			ins.GatherTimeout = config.Duration(time.Second)
		}, want: "gather_timeout"},
		{name: "invalid maximum", edit: func(ins *Instance) { ins.MaxProxies = -1 }, want: "max_proxies"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := valid()
			t.Cleanup(ins.AdminAPIKey.Destroy)
			tt.edit(ins)
			err := ins.Init()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Init() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestGatherCollectsAliveAndSemanticDown(t *testing.T) {
	var requests atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("x-api-key"); got != "admin-secret" {
			t.Fatalf("x-api-key = %q, want admin-secret", got)
		}
		requests.Add(1)
		switch req.URL.Path {
		case "/api/v1/admin/proxies/all":
			if req.URL.Query().Get("with_count") != "true" {
				t.Fatalf("with_count = %q, want true", req.URL.Query().Get("with_count"))
			}
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[
				{"id":1,"name":"edge-a","protocol":"socks5","host":"192.0.2.1","port":1080,"status":"active","account_count":3,"password":"ignored"},
				{"id":2,"name":"edge-b","protocol":"http","host":"192.0.2.2","port":8080,"status":"active","account_count":1},
				{"id":3,"name":"disabled","status":"inactive"}]}`), nil
		case "/api/v1/admin/proxies/1/test":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"success":true,"latency_ms":250,"ip_address":"198.51.100.1"}}`), nil
		case "/api/v1/admin/proxies/2/test":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"success":false,"message":"unreachable"}}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
			return nil, nil
		}
	})
	ins := newTestInstance(t, transport, 2, 100)
	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	labelsA := testLabels("1", "edge-a", "socks5", "192.0.2.1", "1080")
	labelsB := testLabels("2", "edge-b", "http", "192.0.2.2", "8080")
	assertSample(t, samples, "sub2api_admin_up", 1, map[string]string{"site": "production"})
	assertSample(t, samples, "sub2api_admin_proxy_discovered_count", 2, map[string]string{"site": "production"})
	assertSample(t, samples, "sub2api_admin_proxy_account_count", 3, labelsA)
	assertSample(t, samples, "sub2api_admin_proxy_up", 1, labelsA)
	assertSample(t, samples, "sub2api_admin_proxy_probe_completed", 1, labelsA)
	assertSample(t, samples, "sub2api_admin_proxy_probe_latency_seconds", .25, labelsA)
	assertMetric(t, samples, "sub2api_admin_proxy_last_probe_timestamp_seconds", labelsA)
	assertSample(t, samples, "sub2api_admin_proxy_up", 0, labelsB)
	assertSample(t, samples, "sub2api_admin_proxy_probe_completed", 1, labelsB)
	assertNoMetric(t, samples, "sub2api_admin_proxy_probe_latency_seconds", labelsB)
	assertMetric(t, samples, "sub2api_admin_scrape_duration_seconds", map[string]string{"site": "production"})
	assertMetric(t, samples, "sub2api_admin_last_success_timestamp_seconds", map[string]string{"site": "production"})
	if requests.Load() != 3 {
		t.Fatalf("requests = %d, want 3", requests.Load())
	}
}

func TestProbeRequestFailureMarksComponentDown(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/v1/admin/proxies/all" {
			return jsonResponse(req, http.StatusOK, proxyListJSON(1)), nil
		}
		return jsonResponse(req, http.StatusBadGateway, `{"message":"failed"}`), nil
	})
	ins := newTestInstance(t, transport, 1, 100)
	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	labels := testLabels("1", "proxy-1", "http", "192.0.2.1", "8001")
	assertSample(t, samples, "sub2api_admin_up", 0, map[string]string{"site": "production"})
	assertSample(t, samples, "sub2api_admin_proxy_up", 0, labels)
	assertSample(t, samples, "sub2api_admin_proxy_probe_completed", 0, labels)
	assertNoMetric(t, samples, "sub2api_admin_last_success_timestamp_seconds", map[string]string{"site": "production"})
}

func TestListFailures(t *testing.T) {
	tests := []struct {
		name  string
		code  int
		body  string
		limit int64
	}{
		{name: "unauthorized", code: http.StatusUnauthorized, body: `{"message":"unauthorized"}`, limit: 1024},
		{name: "forbidden", code: http.StatusForbidden, body: `{"message":"forbidden"}`, limit: 1024},
		{name: "server error", code: http.StatusInternalServerError, body: `{"message":"failed"}`, limit: 1024},
		{name: "invalid JSON", code: http.StatusOK, body: `{invalid`, limit: 1024},
		{name: "oversized", code: http.StatusOK, body: `{"code":0,"data":[]}`, limit: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return jsonResponse(req, tt.code, tt.body), nil
			})
			ins := newTestInstanceWithLimit(t, transport, 1, 100, tt.limit)
			slist := types.NewSampleList()
			ins.Gather(slist)
			assertSample(t, slist.PopBackAll(), "sub2api_admin_up", 0, map[string]string{"site": "production"})
		})
	}
}

func TestRequestTimeout(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	client := &http.Client{Transport: transport, Timeout: 10 * time.Millisecond}
	upstream, err := llmupstream.NewClient("https://example.com", client, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ins := &Instance{Site: "production", client: upstream, adminAPIKey: "admin-secret", Concurrency: 1, GatherTimeout: config.Duration(time.Second), MaxProxies: 100}
	start := time.Now()
	if err := ins.gatherProxies(context.Background(), types.NewSampleList()); err == nil {
		t.Fatal("gatherProxies() error = nil, want timeout")
	}
	if time.Since(start) > time.Second {
		t.Fatal("request timeout was not honored")
	}
}

func TestMaximumRejectsAllProbes(t *testing.T) {
	var probes atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/v1/admin/proxies/all" {
			return jsonResponse(req, http.StatusOK, proxyListJSON(3)), nil
		}
		probes.Add(1)
		return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"success":true}}`), nil
	})
	ins := newTestInstance(t, transport, 2, 2)
	slist := types.NewSampleList()
	ins.Gather(slist)
	assertSample(t, slist.PopBackAll(), "sub2api_admin_up", 0, map[string]string{"site": "production"})
	if probes.Load() != 0 {
		t.Fatalf("probe requests = %d, want 0", probes.Load())
	}
}

func TestConcurrencyBoundWithOneHundredProxies(t *testing.T) {
	var current, maximum atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/v1/admin/proxies/all" {
			return jsonResponse(req, http.StatusOK, proxyListJSON(100)), nil
		}
		active := current.Add(1)
		defer current.Add(-1)
		for observed := maximum.Load(); active > observed && !maximum.CompareAndSwap(observed, active); observed = maximum.Load() {
		}
		time.Sleep(5 * time.Millisecond)
		return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"success":true,"latency_ms":1}}`), nil
	})
	ins := newTestInstance(t, transport, 3, 100)
	if err := ins.gatherProxies(context.Background(), types.NewSampleList()); err != nil {
		t.Fatalf("gatherProxies() error = %v", err)
	}
	if maximum.Load() != 3 {
		t.Fatalf("maximum concurrency = %d, want 3", maximum.Load())
	}
}

func TestDropCancelsWorkersWithoutFabricatedProbeMetrics(t *testing.T) {
	probeStarted := make(chan struct{}, 1)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api/v1/admin/proxies/all" {
			return jsonResponse(req, http.StatusOK, proxyListJSON(10)), nil
		}
		select {
		case probeStarted <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	ins := newTestInstance(t, transport, 2, 100)
	slist := types.NewSampleList()
	gatherDone := make(chan struct{})
	go func() {
		ins.Gather(slist)
		close(gatherDone)
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	dropDone := make(chan struct{})
	go func() { ins.drop(); close(dropDone) }()
	select {
	case <-gatherDone:
	case <-time.After(time.Second):
		t.Fatal("Gather was not canceled")
	}
	select {
	case <-dropDone:
	case <-time.After(time.Second):
		t.Fatal("Drop did not wait for Gather")
	}
	samples := slist.PopBackAll()
	assertNoMetric(t, samples, "sub2api_admin_proxy_up", map[string]string{"site": "production"})
}

func TestAdminAPIKeyRedaction(t *testing.T) {
	ins := &Instance{adminAPIKey: "admin-super-secret"}
	got := ins.sanitize("request rejected for admin-super-secret")
	if strings.Contains(got, "admin-super-secret") {
		t.Fatalf("sanitize() leaked admin API key: %q", got)
	}
}

func newTestInstance(t *testing.T, transport http.RoundTripper, concurrency, maximum int) *Instance {
	return newTestInstanceWithLimit(t, transport, concurrency, maximum, 1024*1024)
}

func newTestInstanceWithLimit(t *testing.T, transport http.RoundTripper, concurrency, maximum int, limit int64) *Instance {
	t.Helper()
	client, err := llmupstream.NewClient("https://example.com", &http.Client{Transport: transport}, limit)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return &Instance{Site: "production", client: client, adminAPIKey: "admin-secret", Concurrency: concurrency, GatherTimeout: config.Duration(time.Minute), MaxProxies: maximum}
}

func proxyListJSON(count int) string {
	rows := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, fmt.Sprintf(`{"id":%d,"name":"proxy-%d","protocol":"http","host":"192.0.2.%d","port":%d,"status":"active","account_count":%d}`, i, i, i, 8000+i, i))
	}
	return fmt.Sprintf(`{"code":0,"data":[%s]}`, strings.Join(rows, ","))
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
}

func testLabels(id, name, protocol, host, port string) map[string]string {
	return map[string]string{"site": "production", "proxy_id": id, "proxy_name": name, "protocol": protocol, "proxy_host": host, "proxy_port": port}
}

func assertSample(t *testing.T, samples []*types.Sample, metric string, want float64, labels map[string]string) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric != metric || !hasLabels(sample.Labels, labels) {
			continue
		}
		var got float64
		switch value := sample.Value.(type) {
		case int:
			got = float64(value)
		case int64:
			got = float64(value)
		case float64:
			got = value
		default:
			t.Fatalf("metric %s has unsupported value type %T", metric, sample.Value)
		}
		if got != want {
			t.Fatalf("metric %s = %v, want %v", metric, got, want)
		}
		return
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertMetric(t *testing.T, samples []*types.Sample, metric string, labels map[string]string) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric == metric && hasLabels(sample.Labels, labels) {
			return
		}
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertNoMetric(t *testing.T, samples []*types.Sample, metric string, labels map[string]string) {
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

func TestDistributedConfigurationResolvesAdminAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("SUB2API_ADMIN_API_KEY", "admin-key-from-env")
	data, err := os.ReadFile("../../conf/input.sub2api_admin/sub2api_admin.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin Sub2APIAdmin
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	ins := plugin.Instances[0]
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer ins.drop()
	if ins.adminAPIKey != "admin-key-from-env" {
		t.Fatalf("admin_api_key resolved to %q", ins.adminAPIKey)
	}
}

func TestInitReportsUnsetAdminAPIKeyVariable(t *testing.T) {
	ins := &Instance{
		Site:        "production",
		BaseURL:     "https://example.com",
		AdminAPIKey: config.NewSecret([]byte("@{env:SUB2API_ADMIN_TEST_UNSET_KEY}")),
	}
	err := ins.Init()
	if err == nil {
		ins.drop()
	}
	if err == nil || !strings.Contains(err.Error(), "SUB2API_ADMIN_TEST_UNSET_KEY") {
		t.Fatalf("Init() error = %v, want the missing variable name", err)
	}
}
