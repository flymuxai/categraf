package supplier

import (
	"encoding/json"
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

const (
	lynxtonKey = "sk-test-lynxton-key"
	epayKey    = "Ep4yK3y+with/special=chars"
)

// Real responses captured from the two suppliers, with keys replaced.
const lynxtonOrgPoolBody = `{"balance":51742826,"balance_display":103.485652,"balance_scope":"org_pool","display_unit":"CNY","object":"account_balance","used_quota":58257174}`

const epayQueryBody = `{"code":1,"pid":3407,"key":"Ep4yK3y+with/special=chars","active":"1","money":"200.44","type":"1","account":"","username":"","orders":"4813","orders_today":"21","orders_lastday":"35"}`

// -----------------------------------------------------------------------------
// lynxton
// -----------------------------------------------------------------------------

func TestLynxtonEmitsBalanceSpentAndThreshold(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/account/balance" {
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer "+lynxtonKey {
			t.Fatalf("Authorization = %q", got)
		}
		if req.URL.RawQuery != "" {
			t.Fatalf("unexpected query %q", req.URL.RawQuery)
		}
		return jsonResponse(req, http.StatusOK, lynxtonOrgPoolBody), nil
	})
	ins := newLynxtonInstance(t, transport)
	threshold := 50.0
	ins.BalanceLowThreshold = &threshold
	ins.ExpectScope = "org_pool"

	samples := gather(ins)

	base := map[string]string{"supplier": "lynxton", "provider": "lynxton"}
	assertSample(t, samples, "supplier_up", 1, base)
	assertSample(t, samples, "supplier_balance", 103.485652, with(base, "unit", "CNY", "scope", "org_pool"))
	assertSample(t, samples, "supplier_spent_total", 58257174.0/500000, with(base, "unit", "CNY", "scope", "org_pool"))
	assertSample(t, samples, "supplier_balance_low_threshold", 50, with(base, "unit", "CNY"))
	assertPresent(t, samples, "supplier_scrape_duration_seconds", base)
	assertPresent(t, samples, "supplier_last_success_timestamp_seconds", base)
}

func TestLynxtonRejectsUnexpectedScope(t *testing.T) {
	t.Parallel()

	body := strings.Replace(lynxtonOrgPoolBody, `"org_pool"`, `"personal"`, 1)
	ins := newLynxtonInstance(t, staticResponse(http.StatusOK, body))
	ins.ExpectScope = "org_pool"

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	base := map[string]string{"supplier": "lynxton"}
	assertSample(t, samples, "supplier_up", 0, base)
	assertAbsent(t, samples, "supplier_balance")
	if !strings.Contains(logs.String(), `balance_scope="personal", expected "org_pool"`) {
		t.Fatalf("log does not explain scope mismatch: %s", logs.String())
	}
}

func TestLynxtonSkipsSpentWhenQuotaRatioDiffers(t *testing.T) {
	t.Parallel()

	ins := newLynxtonInstance(t, staticResponse(http.StatusOK, lynxtonOrgPoolBody))
	ins.QuotaPerUnit = 1000000

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	assertSample(t, samples, "supplier_balance", 103.485652, map[string]string{"supplier": "lynxton"})
	assertAbsent(t, samples, "supplier_spent_total")
	if !strings.Contains(logs.String(), "differs from quota_per_unit") {
		t.Fatalf("missing ratio warning: %s", logs.String())
	}
}

func TestLynxtonReportsHTTPErrorWithoutKey(t *testing.T) {
	t.Parallel()

	body := `{"error":{"message":"Invalid token ` + lynxtonKey + `","type":"invalid_request_error"}}`
	ins := newLynxtonInstance(t, staticResponse(http.StatusUnauthorized, body))

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	assertSample(t, samples, "supplier_up", 0, map[string]string{"supplier": "lynxton"})
	assertAbsent(t, samples, "supplier_balance")
	if !strings.Contains(logs.String(), "status=401") || !strings.Contains(logs.String(), "Invalid token") {
		t.Fatalf("log missing status or upstream message: %s", logs.String())
	}
	assertNoSecret(t, logs.String(), lynxtonKey)
}

// -----------------------------------------------------------------------------
// epay
// -----------------------------------------------------------------------------

func TestEpayEmitsBalanceStatusAndOrders(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Path != "/api.php" {
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		query := req.URL.Query()
		if query.Get("act") != "query" || query.Get("pid") != "3407" || query.Get("key") != epayKey {
			t.Fatalf("unexpected query %v", query)
		}
		if req.Header.Get("Authorization") != "" {
			t.Fatalf("epay must not send an Authorization header")
		}
		return jsonResponse(req, http.StatusOK, epayQueryBody), nil
	})
	ins := newEpayInstance(t, transport)
	threshold := 100.0
	ins.BalanceLowThreshold = &threshold

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	base := map[string]string{"supplier": "payqixiang", "provider": "epay"}
	assertSample(t, samples, "supplier_up", 1, base)
	assertSample(t, samples, "supplier_balance", 200.44, with(base, "unit", "CNY"))
	assertSample(t, samples, "supplier_balance_low_threshold", 100, with(base, "unit", "CNY"))
	assertSample(t, samples, "supplier_account_active", 1, base)
	assertSample(t, samples, "supplier_orders_total", 4813, base)
	assertSample(t, samples, "supplier_orders_today", 21, base)
	assertSample(t, samples, "supplier_orders_yesterday", 35, base)
	assertAbsent(t, samples, "supplier_spent_total")
	assertNoSecret(t, logs.String(), epayKey)
	for _, s := range samples {
		for _, v := range s.Labels {
			if strings.Contains(v, epayKey) {
				t.Fatalf("label leaked key: %s %v", s.Metric, s.Labels)
			}
		}
	}
}

func TestEpayAcceptsDocumentedFieldNamesAndNumbers(t *testing.T) {
	t.Parallel()

	body := `{"code":"1","active":0,"money":15.5,"orders":10,"order_today":2,"order_lastday":3}`
	ins := newEpayInstance(t, staticResponse(http.StatusOK, body))

	samples := gather(ins)

	base := map[string]string{"supplier": "payqixiang"}
	assertSample(t, samples, "supplier_balance", 15.5, base)
	assertSample(t, samples, "supplier_account_active", 0, base)
	assertSample(t, samples, "supplier_orders_today", 2, base)
	assertSample(t, samples, "supplier_orders_yesterday", 3, base)
}

func TestEpayTreatsNonOneCodeAsFailure(t *testing.T) {
	t.Parallel()

	// Wrong keys come back as HTTP 200; some deployments echo the key in msg.
	body := `{"code":-3,"msg":"KEY校验失败 ` + epayKey + `"}`
	ins := newEpayInstance(t, staticResponse(http.StatusOK, body))

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	assertSample(t, samples, "supplier_up", 0, map[string]string{"supplier": "payqixiang"})
	assertAbsent(t, samples, "supplier_balance")
	if !strings.Contains(logs.String(), "code=-3") || !strings.Contains(logs.String(), "KEY校验失败") {
		t.Fatalf("log missing code or msg: %s", logs.String())
	}
	assertNoSecret(t, logs.String(), epayKey)
}

func TestEpayRequiresMoney(t *testing.T) {
	t.Parallel()

	ins := newEpayInstance(t, staticResponse(http.StatusOK, `{"code":1,"money":""}`))
	samples := gather(ins)
	assertSample(t, samples, "supplier_up", 0, map[string]string{"supplier": "payqixiang"})
	assertAbsent(t, samples, "supplier_balance")
}

func TestEpayRedactsKeyFromTransportErrors(t *testing.T) {
	t.Parallel()

	// http.Client wraps transport errors in *url.Error, whose message
	// includes the full request URL and therefore the URL-encoded key.
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: i/o timeout")
	})
	ins := newEpayInstance(t, transport)

	logs, restore := captureLogs()
	defer restore()
	samples := gather(ins)

	assertSample(t, samples, "supplier_up", 0, map[string]string{"supplier": "payqixiang"})
	if !strings.Contains(logs.String(), "i/o timeout") || !strings.Contains(logs.String(), "[redacted]") {
		t.Fatalf("log missing error or redaction: %s", logs.String())
	}
	assertNoSecret(t, logs.String(), epayKey)
}

func TestFlexNumber(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		want    float64
		set     bool
		wantErr bool
	}{
		{`"200.44"`, 200.44, true, false},
		{`200.44`, 200.44, true, false},
		{`" 7 "`, 7, true, false},
		{`""`, 0, false, false},
		{`null`, 0, false, false},
		{`"abc"`, 0, false, true},
		{`"NaN"`, 0, false, true},
	}
	for _, tt := range cases {
		var n flexNumber
		err := json.Unmarshal([]byte(tt.in), &n)
		if (err != nil) != tt.wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", tt.in, err, tt.wantErr)
		}
		if err == nil && (n.set != tt.set || n.value != tt.want) {
			t.Fatalf("%s: got %+v, want %v set=%v", tt.in, n, tt.want, tt.set)
		}
	}
}

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

func TestDropCancelsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-req.Context().Done():
			canceledOnce.Do(func() { close(canceled) })
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return nil, errors.New("request was not canceled")
		}
	})
	ins := newEpayInstance(t, transport)

	done := make(chan struct{})
	go func() {
		ins.Gather(types.NewSampleList())
		close(done)
	}()
	<-started
	ins.drop()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("drop() did not cancel the in-flight request")
	}
	<-done

	// A dropped instance never starts another request.
	slist := types.NewSampleList()
	ins.Gather(slist)
	if slist.Len() != 0 {
		t.Fatalf("dropped instance emitted %d samples", slist.Len())
	}
}

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

func TestInitRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*Instance)
		wantErr string
	}{
		{"missing supplier", func(i *Instance) { i.Supplier = "" }, "supplier is required"},
		{"missing base_url", func(i *Instance) { i.BaseURL = "" }, "base_url is required"},
		{"missing provider", func(i *Instance) { i.Provider = "" }, "provider is required"},
		{"unknown provider", func(i *Instance) { i.Provider = "stripe" }, `unknown provider "stripe"`},
		{"missing api_key", func(i *Instance) { i.APIKey = config.Secret{} }, "api_key is required"},
		{"epay missing pid", func(i *Instance) { i.PID = "" }, "pid is required"},
		{"epay full url", func(i *Instance) { i.BaseURL = "https://api.payqixiang.cn/api.php" }, "/api.php is added automatically"},
		{"query in base_url", func(i *Instance) { i.BaseURL = "https://api.payqixiang.cn/?key=x" }, "must not contain a query string"},
		{"expect_scope on epay", func(i *Instance) { i.ExpectScope = "org_pool" }, "expect_scope only applies"},
		{"quota_per_unit on epay", func(i *Instance) { i.QuotaPerUnit = 1 }, "quota_per_unit only applies"},
		{"pid on lynxton", func(i *Instance) { i.Provider = "lynxton" }, "pid only applies"},
		{"lynxton full url", func(i *Instance) {
			i.Provider, i.PID, i.BaseURL = "lynxton", "", "https://tokens.lynxtoncloud.com/v1/account/balance"
		}, "/v1/account/balance is added automatically"},
		{"lynxton bad scope", func(i *Instance) { i.Provider, i.PID, i.ExpectScope = "lynxton", "", "org" }, "expect_scope must be"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ins := &Instance{
				Supplier: "payqixiang",
				Provider: "epay",
				BaseURL:  "https://api.payqixiang.cn",
				APIKey:   config.NewSecret([]byte(epayKey)),
				PID:      "3407",
			}
			tt.mutate(ins)
			err := ins.Init()
			if err == nil {
				ins.drop()
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Init() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestPIDAcceptsIntegerOrString(t *testing.T) {
	for _, pid := range []string{`3407`, `"3407"`} {
		raw := `
[[instances]]
supplier = "payqixiang"
provider = "epay"
base_url = "https://api.payqixiang.cn"
pid = ` + pid + `
api_key = "secret"
`
		var plugin Supplier
		if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: raw}, &plugin); err != nil {
			t.Fatalf("pid = %s: LoadSingleConfig() error = %v", pid, err)
		}
		ins := plugin.Instances[0]
		if err := ins.Init(); err != nil {
			t.Fatalf("pid = %s: Init() error = %v", pid, err)
		}
		ins.drop()
		if ins.PID != "3407" || ins.Unit != "CNY" {
			t.Fatalf("pid = %s: got PID=%q Unit=%q", pid, ins.PID, ins.Unit)
		}
	}
}

func TestAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("SUPPLIER_TEST_EPAY_KEY", epayKey)
	raw := `
[[instances]]
supplier = "payqixiang"
provider = "epay"
base_url = "https://api.payqixiang.cn"
pid = 3407
api_key = "@{env:SUPPLIER_TEST_EPAY_KEY}"
`
	var plugin Supplier
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: raw}, &plugin); err != nil {
		t.Fatalf("LoadSingleConfig() error = %v", err)
	}
	ins := plugin.Instances[0]
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	ins.drop()
	if ins.apiKey != epayKey {
		t.Fatalf("api_key resolved to %q", ins.apiKey)
	}

	var unset Supplier
	missing := strings.Replace(raw, "SUPPLIER_TEST_EPAY_KEY", "SUPPLIER_TEST_UNSET_KEY", 1)
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: missing}, &unset); err != nil {
		t.Fatalf("LoadSingleConfig() error = %v", err)
	}
	err := unset.Instances[0].Init()
	if err == nil || !strings.Contains(err.Error(), "SUPPLIER_TEST_UNSET_KEY") {
		t.Fatalf("Init() error = %v, want the missing variable name", err)
	}
}

func TestDistributedConfigurationLoads(t *testing.T) {
	data, err := os.ReadFile("../../conf/input.supplier/supplier.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin Supplier
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	providers := map[string]bool{}
	for _, ins := range plugin.Instances {
		if ins.Supplier == "" || ins.BaseURL == "" {
			t.Fatalf("instance missing required fields: %+v", ins)
		}
		ins.APIKey = config.NewSecret([]byte("placeholder"))
		if err := ins.Init(); err != nil {
			t.Fatalf("Init(%s) error = %v", ins.Supplier, err)
		}
		ins.drop()
		providers[ins.Provider] = true
	}
	if !providers[providerLynxton] || !providers[providerEpay] {
		t.Fatalf("distributed config should show both providers, got %v", providers)
	}
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func newLynxtonInstance(t *testing.T, transport http.RoundTripper) *Instance {
	t.Helper()
	return newTestInstance(t, &Instance{
		Supplier: "lynxton",
		Provider: providerLynxton,
		BaseURL:  "https://tokens.lynxtoncloud.com",
	}, lynxtonKey, transport)
}

func newEpayInstance(t *testing.T, transport http.RoundTripper) *Instance {
	t.Helper()
	return newTestInstance(t, &Instance{
		Supplier: "payqixiang",
		Provider: providerEpay,
		BaseURL:  "https://api.payqixiang.cn",
		PID:      "3407",
	}, epayKey, transport)
}

// newTestInstance wires a fake transport without registering a rate limiter.
func newTestInstance(t *testing.T, ins *Instance, key string, transport http.RoundTripper) *Instance {
	t.Helper()
	if err := ins.validateProvider(); err != nil {
		t.Fatalf("validateProvider() error = %v", err)
	}
	client, err := llmupstream.NewClient(ins.BaseURL, &http.Client{Transport: transport}, 1<<20)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins.client = client
	ins.apiKey = key
	return ins
}

func staticResponse(status int, body string) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, status, body), nil
	})
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func gather(ins *Instance) []*types.Sample {
	slist := types.NewSampleList()
	ins.Gather(slist)
	return slist.PopBackAll()
}

func assertSample(t *testing.T, samples []*types.Sample, metric string, want float64, labels map[string]string) {
	t.Helper()
	for _, s := range samples {
		if s.Metric != metric || !hasLabels(s.Labels, labels) {
			continue
		}
		got, ok := numericValue(s.Value)
		if !ok {
			t.Fatalf("metric %s value has type %T", metric, s.Value)
		}
		if got != want {
			t.Fatalf("metric %s labels %v = %v, want %v", metric, s.Labels, got, want)
		}
		return
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertPresent(t *testing.T, samples []*types.Sample, metric string, labels map[string]string) {
	t.Helper()
	for _, s := range samples {
		if s.Metric == metric && hasLabels(s.Labels, labels) {
			return
		}
	}
	t.Fatalf("metric %s with labels %v not found", metric, labels)
}

func assertAbsent(t *testing.T, samples []*types.Sample, metric string) {
	t.Helper()
	for _, s := range samples {
		if s.Metric == metric {
			t.Fatalf("unexpected metric %s %v", metric, s.Labels)
		}
	}
}

func assertNoSecret(t *testing.T, logs, key string) {
	t.Helper()
	for _, form := range (&Instance{apiKey: key}).secretForms() {
		if strings.Contains(logs, form) {
			t.Fatalf("logs leaked key form %q: %s", form, logs)
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
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func with(base map[string]string, kv ...string) map[string]string {
	result := make(map[string]string, len(base)+len(kv)/2)
	for key, value := range base {
		result[key] = value
	}
	for i := 0; i+1 < len(kv); i += 2 {
		result[kv[i]] = kv[i+1]
	}
	return result
}

// captureLogs redirects the standard logger so tests can assert that keys
// never reach log output. Tests using it share the global logger, so the
// buffer is guarded.
func captureLogs() (*lockedBuilder, func()) {
	logMu.Lock()
	buf := &lockedBuilder{}
	previous := log.Writer()
	log.SetOutput(buf)
	return buf, func() {
		log.SetOutput(previous)
		logMu.Unlock()
	}
}

var logMu sync.Mutex

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
