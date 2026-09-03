package sub2api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

func TestGatherCollectsBalanceGroupsAndModels(t *testing.T) {
	t.Parallel()

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := ""
		switch req.URL.Path {
		case "/api/v1/user/profile":
			assertHeader(t, req, "Authorization", "Bearer user-token")
			body = `{"code":0,"message":"success","data":{"balance":12.5,"frozen_balance":1.25}}`
		case "/api/v1/groups/available":
			assertHeader(t, req, "Authorization", "Bearer user-token")
			body = `{"code":0,"message":"success","data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1.2,"peak_rate_enabled":true,"peak_rate_multiplier":1.5}]}`
		case "/api/v1/groups/rates":
			assertHeader(t, req, "Authorization", "Bearer user-token")
			body = `{"code":0,"message":"success","data":{"7":1.1}}`
		case "/api/v1/keys":
			assertHeader(t, req, "Authorization", "Bearer user-token")
			body = apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))
		case "/v1/models":
			assertHeader(t, req, "Authorization", "Bearer group-token")
			body = `{"object":"list","data":[{"id":"claude-sonnet-4-5"},{"id":"claude-opus-4-1"}]}`
		case "/api/v1/usage":
			body = usagePageJSON(1, 20, 1, usageJSON(100, "2026-07-15T10:00:00+08:00"))
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
		}
		return jsonResponse(req, http.StatusOK, body), nil
	})
	httpClient := &http.Client{Transport: transport}
	client, err := llmupstream.NewClient("https://supplier.example.com", httpClient, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", StateFile: filepath.Join(t.TempDir(), "state.json"),
		UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"), client: client, accessToken: "user-token",
		state:        persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime, UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()

	assertSample(t, samples, "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_balance_usd", 12.5, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_frozen_balance_usd", 1.25, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_group_count", 1, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_group_ratio", 1.2, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "kind": "default"})
	assertSample(t, samples, "sub2api_group_ratio", 1.1, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "kind": "effective"})
	assertSample(t, samples, "sub2api_group_ratio", 1.5, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "kind": "peak"})
	assertSample(t, samples, "sub2api_model_count", 2, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic"})
	assertSample(t, samples, "sub2api_model_available", 1, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic", "model": "claude-sonnet-4-5"})
	assertSample(t, samples, "sub2api_model_scrape_success", 1, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic"})
	for _, resource := range []string{"balance", "groups", "models", "usage"} {
		assertSample(t, samples, "sub2api_scrape_success", 1, map[string]string{"site": "supplier-a", "resource": resource})
		assertMetricWithLabels(t, samples, "sub2api_scrape_duration_seconds", map[string]string{"site": "supplier-a", "resource": resource})
		assertMetricWithLabels(t, samples, "sub2api_last_success_timestamp_seconds", map[string]string{"site": "supplier-a", "resource": resource})
	}
}

func TestGatherBootstrapsAccessTokenFromRefreshToken(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	refreshRequests := 0
	resourceRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/auth/refresh":
			if resourceRequests != 0 {
				t.Fatalf("refresh request followed %d resource requests", resourceRequests)
			}
			refreshRequests++
			var body struct {
				RefreshToken string `json:"refresh_token"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode refresh body: %v", err)
			}
			if body.RefreshToken != "configured-refresh" {
				t.Fatalf("refresh_token = %q, want configured-refresh", body.RefreshToken)
			}
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"access_token":"bootstrapped-access","refresh_token":"rotated-refresh"}}`), nil
		case "/api/v1/user/profile":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer bootstrapped-access")
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":1}}`), nil
		case "/api/v1/groups/available":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer bootstrapped-access")
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1}]}`), nil
		case "/api/v1/groups/rates":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer bootstrapped-access")
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{}}`), nil
		case "/api/v1/keys":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer bootstrapped-access")
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer group-token")
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		case "/api/v1/usage":
			resourceRequests++
			assertHeader(t, req, "Authorization", "Bearer bootstrapped-access")
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1)), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", StateFile: stateFile, UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"), client: client,
		refreshToken: "configured-refresh",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		state: persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime,
			UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	assertSample(t, slist.PopBackAll(), "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	if refreshRequests != 1 || resourceRequests == 0 {
		t.Fatalf("refresh/resource requests = %d/%d, want 1/>0", refreshRequests, resourceRequests)
	}

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", stateFile, err)
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode persisted state: %v", err)
	}
	if state.AccessToken != "bootstrapped-access" || state.RefreshToken != "rotated-refresh" {
		t.Fatalf("persisted tokens = %q/%q", state.AccessToken, state.RefreshToken)
	}
	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", stateFile, err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("state permissions = %o, want 600", got)
	}
}

func TestGatherRetriesBootstrapOnNextCollection(t *testing.T) {
	t.Parallel()

	refreshRequests := 0
	resourceRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/auth/refresh":
			refreshRequests++
			if refreshRequests == 1 {
				return jsonResponse(req, http.StatusServiceUnavailable, `{"code":503,"message":"unavailable"}`), nil
			}
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"access_token":"new-access","refresh_token":"new-refresh"}}`), nil
		case "/api/v1/user/profile":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":1}}`), nil
		case "/api/v1/groups/available":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1}]}`), nil
		case "/api/v1/groups/rates":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{}}`), nil
		case "/api/v1/keys":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		case "/api/v1/usage":
			resourceRequests++
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1)), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://bootstrap-retry.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", StateFile: filepath.Join(t.TempDir(), "state.json"), UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"), client: client,
		refreshToken: "configured-refresh",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		state: persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime,
			UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
	}

	first := types.NewSampleList()
	ins.Gather(first)
	firstSamples := first.PopBackAll()
	assertSample(t, firstSamples, "sub2api_up", 0, map[string]string{"site": "supplier-a"})
	for _, resource := range []string{"balance", "groups", "models", "usage"} {
		assertSample(t, firstSamples, "sub2api_scrape_success", 0, map[string]string{"site": "supplier-a", "resource": resource})
	}
	if refreshRequests != 1 || resourceRequests != 0 {
		t.Fatalf("first refresh/resource requests = %d/%d, want 1/0", refreshRequests, resourceRequests)
	}

	second := types.NewSampleList()
	ins.Gather(second)
	assertSample(t, second.PopBackAll(), "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	if refreshRequests != 2 || resourceRequests == 0 {
		t.Fatalf("total refresh/resource requests = %d/%d, want 2/>0", refreshRequests, resourceRequests)
	}
}

func TestGatherRefreshesExpiredAccessTokenAndPersistsRotation(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "token.json")
	profileRequests := 0
	refreshRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/user/profile":
			profileRequests++
			if profileRequests == 1 {
				assertHeader(t, req, "Authorization", "Bearer old-access")
				return jsonResponse(req, http.StatusUnauthorized, `{"code":401,"message":"expired"}`), nil
			}
			assertHeader(t, req, "Authorization", "Bearer new-access")
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":{"balance":9,"frozen_balance":0}}`), nil
		case "/api/v1/auth/refresh":
			refreshRequests++
			var body struct {
				RefreshToken string `json:"refresh_token"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode refresh body: %v", err)
			}
			if body.RefreshToken != "old-refresh" {
				t.Fatalf("refresh_token = %q, want old-refresh", body.RefreshToken)
			}
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600,"token_type":"Bearer"}}`), nil
		case "/api/v1/groups/available":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1}]}`), nil
		case "/api/v1/groups/rates":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{}}`), nil
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		case "/api/v1/usage":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1, usageJSON(100, "2026-07-15T10:00:00+08:00"))), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", StateFile: stateFile, UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"),
		client:        client, accessToken: "old-access", refreshToken: "old-refresh",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		state:        persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime, UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	assertSample(t, samples, "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	assertSample(t, samples, "sub2api_balance_usd", 9, map[string]string{"site": "supplier-a"})
	if profileRequests != 2 || refreshRequests != 1 {
		t.Fatalf("profile requests = %d, refresh requests = %d; want 2 and 1", profileRequests, refreshRequests)
	}

	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", stateFile, err)
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode token state: %v", err)
	}
	if state.AccessToken != "new-access" || state.RefreshToken != "new-refresh" {
		t.Fatalf("token state = %+v, want rotated tokens", state)
	}
	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", stateFile, err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("token state permissions = %o, want 600", got)
	}
}

func TestUserRequestRefreshesAndRetriesOnce(t *testing.T) {
	profileRequests := 0
	refreshRequests := 0
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/user/profile":
			profileRequests++
			if profileRequests == 1 {
				return jsonResponse(req, http.StatusUnauthorized, `{"code":401,"message":"expired"}`), nil
			}
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":1}}`), nil
		case "/api/v1/auth/refresh":
			refreshRequests++
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"access_token":"new-access","refresh_token":"new-refresh"}}`), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: base}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		StateFile: filepath.Join(t.TempDir(), "state.json"), client: client,
		accessToken: "old-access", refreshToken: "old-refresh",
		state: persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)},
	}
	var response envelope[profileData]
	if err := ins.doUserJSON(context.Background(), http.MethodGet, "/api/v1/user/profile", nil, nil, &response); err != nil {
		t.Fatalf("doUserJSON() error = %v", err)
	}
	if profileRequests != 2 || refreshRequests != 1 {
		t.Fatalf("profile/refresh requests = %d/%d, want 2/1", profileRequests, refreshRequests)
	}
}

func TestDropCancelsActiveUsageWithoutAdvancingState(t *testing.T) {
	usageStarted := make(chan struct{})
	cancelObserved := make(chan struct{})
	allowUsageReturn := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/user/profile":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":1}}`), nil
		case "/api/v1/groups/available":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1}]}`), nil
		case "/api/v1/groups/rates":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{}}`), nil
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		case "/api/v1/usage":
			startedOnce.Do(func() { close(usageStarted) })
			select {
			case <-req.Context().Done():
				canceledOnce.Do(func() { close(cancelObserved) })
			case <-time.After(time.Second):
			}
			<-allowUsageReturn
			err := req.Context().Err()
			if err == nil {
				err = errors.New("usage request was not canceled")
			}
			return nil, err
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://reload-active.example.com", &http.Client{Transport: base}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", client: client,
		accessToken: "user-token", modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		UsageTimezone: "Asia/Shanghai", usageLocation: mustLocation(t, "Asia/Shanghai"),
		state: persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime, UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
	}
	gatherDone := make(chan struct{})
	go func() {
		ins.Gather(types.NewSampleList())
		close(gatherDone)
	}()
	<-usageStarted
	dropDone := make(chan struct{})
	go func() {
		ins.drop()
		close(dropDone)
	}()
	select {
	case <-cancelObserved:
	case <-time.After(200 * time.Millisecond):
		close(allowUsageReturn)
		<-gatherDone
		<-dropDone
		t.Fatal("Drop() did not cancel the active Usage request")
	}

	close(allowUsageReturn)
	<-gatherDone
	<-dropDone
	if ins.state.LastUsageID != 100 || len(ins.state.UsageSeries) != 0 {
		t.Fatalf("usage state advanced during canceled gather: %+v", ins.state)
	}
}

func TestGatherSkipsAvailableGroupWithoutConfiguredModelKey(t *testing.T) {
	t.Parallel()

	modelRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/groups/available":
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1.2},{"id":8,"name":"new-group","platform":"openai","rate_multiplier":0.8}]}`), nil
		case "/api/v1/groups/rates":
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":{}}`), nil
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/api/v1/user/profile":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{"balance":1}}`), nil
		case "/v1/models":
			modelRequests++
			return jsonResponse(req, http.StatusOK, `{"object":"list","data":[{"id":"claude-sonnet-4-5"}]}`), nil
		case "/api/v1/usage":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1, usageJSON(100, "2026-07-15T10:00:00+08:00"))), nil
		default:
			t.Fatalf("unexpected request path %q", req.URL.Path)
			return nil, nil
		}
	})
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: transport}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	cursorTime := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ins := &Instance{
		Site: "supplier-a", StateFile: filepath.Join(t.TempDir(), "state.json"), UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"), client: client, accessToken: "user-token",
		state:        persistedState{Version: stateVersion, LastUsageID: 100, LastUsageCreatedAt: &cursorTime, UsageInitialized: true, UsageSeries: make(map[string]*usageSeriesState)},
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
	}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	assertSample(t, samples, "sub2api_group_available", 1, map[string]string{"site": "supplier-a", "group": "new-group", "platform": "openai"})
	assertNoSample(t, samples, "sub2api_model_scrape_success", map[string]string{"site": "supplier-a", "group": "new-group", "platform": "openai"})
	assertSample(t, samples, "sub2api_scrape_success", 1, map[string]string{"site": "supplier-a", "resource": "models"})
	assertSample(t, samples, "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	if modelRequests != 1 {
		t.Fatalf("model requests = %d, want only configured group request", modelRequests)
	}
}

func TestGatherModelsUsesCurrentGroupIdentityAndStopsKeyPagination(t *testing.T) {
	keyRequests := 0
	modelRequests := 0
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/keys":
			keyRequests++
			if req.URL.Query().Get("page_size") != "50" || req.URL.Query().Get("page") != "1" {
				t.Fatalf("key query = %v, want first page with fixed size 50", req.URL.Query())
			}
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 100, apiKeyJSON("group-token", 7, "active", "old-name", "openai"))), nil
		case "/v1/models":
			modelRequests++
			return jsonResponse(req, http.StatusOK, `{"data":[{"id":"gpt-5"}]}`), nil
		default:
			t.Fatalf("unexpected request %s", req.URL.String())
			return nil, nil
		}
	})}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{Site: "supplier-a", client: client, accessToken: "user-token", modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}}}
	slist := types.NewSampleList()
	err = ins.gatherModels(context.Background(), slist, []groupData{{ID: 7, Name: "renamed-group", Platform: "anthropic"}})
	if err != nil {
		t.Fatalf("gatherModels() error = %v", err)
	}
	samples := slist.PopBackAll()
	assertSample(t, samples, "sub2api_model_available", 1, map[string]string{"site": "supplier-a", "group": "renamed-group", "platform": "anthropic", "model": "gpt-5"})
	if keyRequests != 1 || modelRequests != 1 {
		t.Fatalf("key/model requests = %d/%d, want 1/1", keyRequests, modelRequests)
	}
}

func TestGatherModelsRejectsMissingAndInactiveConfiguredKeys(t *testing.T) {
	tests := []struct {
		name       string
		keyPage    string
		groups     []groupData
		wantError  string
		wantMetric bool
	}{
		{name: "missing", keyPage: apiKeyPageJSON(1, 1), groups: []groupData{{ID: 7, Name: "pro", Platform: "anthropic"}}, wantError: "was not found"},
		{name: "inactive", keyPage: apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "inactive", "pro", "anthropic")), groups: []groupData{{ID: 7, Name: "pro", Platform: "anthropic"}}, wantError: `status "inactive"`, wantMetric: true},
		{name: "unavailable group", keyPage: apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic")), wantError: "not currently available", wantMetric: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/api/v1/keys" {
					t.Fatalf("unexpected request %s", req.URL.String())
				}
				return jsonResponse(req, http.StatusOK, tt.keyPage), nil
			})}, 1024*1024)
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}
			ins := &Instance{Site: "supplier-a", client: client, accessToken: "user-token", modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}}}
			slist := types.NewSampleList()
			err = ins.gatherModels(context.Background(), slist, tt.groups)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("gatherModels() error = %v, want %q", err, tt.wantError)
			}
			samples := slist.PopBackAll()
			if tt.wantMetric {
				assertSample(t, samples, "sub2api_model_scrape_success", 0, map[string]string{"site": "supplier-a", "group": "pro", "platform": "anthropic"})
			} else {
				assertNoSample(t, samples, "sub2api_model_scrape_success", map[string]string{"site": "supplier-a"})
			}
		})
	}
}

func TestGatherModelsWarnsAndSkipsUnassignedConfiguredKeys(t *testing.T) {
	tests := []struct {
		name    string
		keyJSON string
	}{
		{name: "null group", keyJSON: `{"key":"group-token","group_id":null,"status":"active"}`},
		{name: "zero group", keyJSON: `{"key":"group-token","group_id":0,"status":"active"}`},
		{name: "negative group", keyJSON: `{"key":"group-token","group_id":-1,"status":"active"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logOutput bytes.Buffer
			previousOutput := log.Writer()
			log.SetOutput(&logOutput)
			defer log.SetOutput(previousOutput)

			modelRequests := 0
			client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/api/v1/keys":
					return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, tt.keyJSON)), nil
				case "/v1/models":
					modelRequests++
					return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
				default:
					t.Fatalf("unexpected request %s", req.URL.String())
					return nil, nil
				}
			})}, 1024*1024)
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}
			ins := &Instance{Site: "supplier-a", client: client, accessToken: "user-token", modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}}}
			slist := types.NewSampleList()
			if err := ins.gatherModels(context.Background(), slist, []groupData{{ID: 7, Name: "pro", Platform: "anthropic"}}); err != nil {
				t.Fatalf("gatherModels() error = %v, want unassigned key to be skipped", err)
			}
			if modelRequests != 0 {
				t.Fatalf("model requests = %d, want zero", modelRequests)
			}
			assertNoSample(t, slist.PopBackAll(), "sub2api_model_scrape_success", map[string]string{"site": "supplier-a"})

			warning := "W! sub2api site=supplier-a model_api_keys[0] is not assigned to a group; skipping model collection"
			if !strings.Contains(logOutput.String(), warning) {
				t.Fatalf("warning log = %q, want %q", logOutput.String(), warning)
			}
			if strings.Contains(logOutput.String(), "group-token") {
				t.Fatalf("warning log leaked model API key: %q", logOutput.String())
			}
		})
	}
}

func TestGatherModelsContinuesAfterUnassignedConfiguredKey(t *testing.T) {
	var logOutput bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logOutput)
	defer log.SetOutput(previousOutput)

	modelRequests := 0
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1,
				`{"key":"unassigned-token","group_id":null,"status":"active"}`,
				apiKeyJSON("assigned-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			modelRequests++
			assertHeader(t, req, "Authorization", "Bearer assigned-token")
			return jsonResponse(req, http.StatusOK, `{"data":[{"id":"claude-sonnet-4-5"}]}`), nil
		default:
			t.Fatalf("unexpected request %s", req.URL.String())
			return nil, nil
		}
	})}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{
		Site: "supplier-a", client: client, accessToken: "user-token",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "unassigned-token"}, {APIKey: "assigned-token"}},
	}
	slist := types.NewSampleList()
	if err := ins.gatherModels(context.Background(), slist, []groupData{{ID: 7, Name: "pro", Platform: "anthropic"}}); err != nil {
		t.Fatalf("gatherModels() error = %v", err)
	}
	if modelRequests != 1 {
		t.Fatalf("model requests = %d, want one request for the assigned key", modelRequests)
	}
	assertSample(t, slist.PopBackAll(), "sub2api_model_available", 1, map[string]string{
		"site": "supplier-a", "group": "pro", "platform": "anthropic", "model": "claude-sonnet-4-5",
	})
	if !strings.Contains(logOutput.String(), "model_api_keys[0] is not assigned to a group; skipping model collection") {
		t.Fatalf("warning log = %q, want unassigned key warning", logOutput.String())
	}
	if strings.Contains(logOutput.String(), "unassigned-token") || strings.Contains(logOutput.String(), "assigned-token") {
		t.Fatalf("warning log leaked model API key: %q", logOutput.String())
	}
}

func TestGatherModelsRejectsDuplicateTargetGroup(t *testing.T) {
	modelRequests := 0
	client, err := llmupstream.NewClient("https://supplier.example.com", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1,
				apiKeyJSON("key-one", 7, "active", "pro", "anthropic"),
				apiKeyJSON("key-two", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			modelRequests++
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		default:
			t.Fatalf("unexpected request %s", req.URL.String())
			return nil, nil
		}
	})}, 1024*1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ins := &Instance{Site: "supplier-a", client: client, accessToken: "user-token", modelAPIKeys: []resolvedModelAPIKey{{APIKey: "key-one"}, {APIKey: "key-two"}}}
	err = ins.gatherModels(context.Background(), types.NewSampleList(), []groupData{{ID: 7, Name: "pro", Platform: "anthropic"}})
	if err == nil || !strings.Contains(err.Error(), "multiple model API keys") {
		t.Fatalf("gatherModels() error = %v, want duplicate group error", err)
	}
	if modelRequests != 1 {
		t.Fatalf("model requests = %d, want one request for the first resolved key", modelRequests)
	}
}

func TestInitRestoresRotatedTokenState(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "token.json")
	data, err := json.Marshal(tokenState{Version: tokenStateVersion, AccessToken: "persisted-access", RefreshToken: "persisted-refresh"})
	if err != nil {
		t.Fatalf("marshal token state: %v", err)
	}
	if err := os.WriteFile(stateFile, data, 0o600); err != nil {
		t.Fatalf("write token state: %v", err)
	}
	ins := &Instance{
		Site:          "supplier-a",
		BaseURL:       "https://supplier.example.com",
		RefreshToken:  config.NewSecret([]byte("configured-refresh")),
		StateFile:     stateFile,
		UsageTimezone: "Asia/Shanghai",
		ModelAPIKeys:  []ModelAPIKey{{APIKey: config.NewSecret([]byte("group-token"))}},
	}
	defer ins.RefreshToken.Destroy()

	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.accessToken != "persisted-access" || ins.refreshToken != "persisted-refresh" {
		t.Fatalf("restored tokens = %q/%q, want persisted values", ins.accessToken, ins.refreshToken)
	}
}

func TestInitRejectsCorruptTokenState(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "token.json")
	if err := os.WriteFile(stateFile, []byte("not-json"), 0o600); err != nil {
		t.Fatalf("write token state: %v", err)
	}
	ins := &Instance{
		Site:          "supplier-a",
		BaseURL:       "https://supplier.example.com",
		RefreshToken:  config.NewSecret([]byte("configured-refresh")),
		StateFile:     stateFile,
		UsageTimezone: "Asia/Shanghai",
		ModelAPIKeys:  []ModelAPIKey{{APIKey: config.NewSecret([]byte("group-token"))}},
	}
	defer ins.RefreshToken.Destroy()

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "decode state") {
		t.Fatalf("Init() error = %v, want corrupt state error", err)
	}
}

func TestGatherRetriesPendingTokenStatePersistence(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	blockedParent := filepath.Join(tempDir, "blocked")
	if err := os.WriteFile(blockedParent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocked parent: %v", err)
	}
	validStateFile := filepath.Join(tempDir, "valid", "token.json")
	profileRequests := 0
	refreshRequests := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/v1/user/profile":
			profileRequests++
			if req.Header.Get("Authorization") == "Bearer old-access" {
				return jsonResponse(req, http.StatusUnauthorized, `{}`), nil
			}
			assertHeader(t, req, "Authorization", "Bearer new-access")
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":{"balance":3}}`), nil
		case "/api/v1/auth/refresh":
			refreshRequests++
			return jsonResponse(req, http.StatusOK, `{"code":0,"message":"success","data":{"access_token":"new-access","refresh_token":"new-refresh"}}`), nil
		case "/api/v1/groups/available":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":[{"id":7,"name":"pro","platform":"anthropic","rate_multiplier":1}]}`), nil
		case "/api/v1/groups/rates":
			return jsonResponse(req, http.StatusOK, `{"code":0,"data":{}}`), nil
		case "/api/v1/keys":
			return jsonResponse(req, http.StatusOK, apiKeyPageJSON(1, 1, apiKeyJSON("group-token", 7, "active", "pro", "anthropic"))), nil
		case "/v1/models":
			return jsonResponse(req, http.StatusOK, `{"data":[]}`), nil
		case "/api/v1/usage":
			return jsonResponse(req, http.StatusOK, usagePageJSON(1, 20, 1)), nil
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
		Site: "supplier-a", StateFile: filepath.Join(blockedParent, "state.json"), UsageTimezone: "Asia/Shanghai",
		usageLocation: mustLocation(t, "Asia/Shanghai"),
		client:        client, accessToken: "old-access", refreshToken: "old-refresh",
		modelAPIKeys: []resolvedModelAPIKey{{APIKey: "group-token"}},
		state:        persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)},
	}

	first := types.NewSampleList()
	ins.Gather(first)
	assertSample(t, first.PopBackAll(), "sub2api_up", 0, map[string]string{"site": "supplier-a"})

	ins.StateFile = validStateFile
	second := types.NewSampleList()
	ins.Gather(second)
	assertSample(t, second.PopBackAll(), "sub2api_up", 1, map[string]string{"site": "supplier-a"})
	if refreshRequests != 1 {
		t.Fatalf("refresh requests = %d, want 1", refreshRequests)
	}
	if profileRequests != 2 {
		t.Fatalf("profile requests = %d, want 2", profileRequests)
	}
	data, err := os.ReadFile(validStateFile)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", validStateFile, err)
	}
	if !strings.Contains(string(data), "new-refresh") {
		t.Fatalf("persisted token state = %s, want rotated refresh token", data)
	}
}

func TestConfigurationParsesNestedModelAPIKeysAndIgnoresAccessToken(t *testing.T) {
	var plugin Sub2API
	err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{
		Format: cfg.TomlFormat,
		Config: `
[[instances]]
site = "supplier"
base_url = "https://example.com"
access_token = "user-token"
refresh_token = "refresh-token"
state_file = "/tmp/sub2api-state.json"
usage_timezone = "Asia/Shanghai"

[[instances.model_api_keys]]
api_key = "group-token"
`,
	}, &plugin)
	if err != nil {
		t.Fatalf("LoadSingleConfig() error = %v", err)
	}
	if len(plugin.Instances) != 1 || len(plugin.Instances[0].ModelAPIKeys) != 1 {
		t.Fatalf("parsed instances/model keys = %d/%d, want 1/1", len(plugin.Instances), len(plugin.Instances[0].ModelAPIKeys))
	}
	if plugin.Instances[0].accessToken != "" {
		t.Fatalf("legacy access_token was not ignored: %q", plugin.Instances[0].accessToken)
	}
	key, err := secretString(&plugin.Instances[0].ModelAPIKeys[0].APIKey)
	if err != nil || key != "group-token" {
		t.Fatalf("model api key = %q/%v, want group-token", key, err)
	}
}

func TestConfigurationRetainsLegacyFieldsForMigrationError(t *testing.T) {
	var plugin Sub2API
	err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: `
[[instances]]
site = "supplier"
base_url = "https://example.com"
access_token = "user-token"
refresh_token = "refresh-token"
state_file = "/tmp/sub2api-state.json"
usage_timezone = "Asia/Shanghai"
usage_page_size = 50
usage_max_pages = 5
usage_max_count = 250
request_interval = "1s"

[[instances.group_api_keys]]
group = "pro"
platform = "anthropic"
api_key = "group-token"
`}, &plugin)
	if err != nil {
		t.Fatalf("LoadSingleConfig() error = %v", err)
	}
	ins := plugin.Instances[0]
	if ins.LegacyUsagePageSize == nil || ins.LegacyUsageMaxPages == nil || ins.LegacyUsageMaxCount == nil || ins.LegacyRequestInterval == nil || len(ins.LegacyGroupAPIKeys) != 1 {
		t.Fatalf("legacy fields were not retained: %+v", ins)
	}
}

func TestDistributedConfigurationKeepsInstanceFieldsOutsideModelKeyTable(t *testing.T) {
	data, err := os.ReadFile("../../conf/input.sub2api/sub2api.toml")
	if err != nil {
		t.Fatalf("read distributed config: %v", err)
	}
	var plugin Sub2API
	if err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: string(data)}, &plugin); err != nil {
		t.Fatalf("load distributed config: %v", err)
	}
	if len(plugin.Instances) != 1 || len(plugin.Instances[0].ModelAPIKeys) != 1 {
		t.Fatalf("instances/model keys = %d/%d, want 1/1", len(plugin.Instances), len(plugin.Instances[0].ModelAPIKeys))
	}
	if plugin.Instances[0].IntervalTimes != 1 {
		t.Fatalf("interval_times = %d, want instance value 1", plugin.Instances[0].IntervalTimes)
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

func apiKeyPageJSON(page, pages int, rows ...string) string {
	return fmt.Sprintf(`{"code":0,"message":"success","data":{"items":[%s],"total":%d,"page":%d,"page_size":50,"pages":%d}}`, strings.Join(rows, ","), len(rows), page, pages)
}

func apiKeyJSON(key string, groupID int64, status, group, platform string) string {
	return fmt.Sprintf(`{"key":%q,"group_id":%d,"status":%q,"group":{"id":%d,"name":%q,"platform":%q}}`, key, groupID, status, groupID, group, platform)
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
