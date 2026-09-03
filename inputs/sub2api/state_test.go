package sub2api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
)

func TestPluginInitDefaultsIntervalToFiveMinutes(t *testing.T) {
	var plugin Sub2API
	if err := plugin.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got, want := time.Duration(plugin.Interval), 5*time.Minute; got != want {
		t.Fatalf("interval = %s, want %s", got, want)
	}
}

func TestInstanceInitRequiresStateFile(t *testing.T) {
	ins := validConfigInstance(t)
	ins.StateFile = ""

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "state_file is required") {
		t.Fatalf("Init() error = %v, want state_file validation", err)
	}
}

func TestInstanceInitRequiresRefreshToken(t *testing.T) {
	ins := validConfigInstance(t)
	ins.RefreshToken.Destroy()

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "refresh_token is required") {
		t.Fatalf("Init() error = %v, want refresh_token validation", err)
	}
}

func TestInstanceInitDefaultsRequestRate(t *testing.T) {
	ins := validConfigInstance(t)

	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.RequestRPSLimit != 1 || ins.RequestBurst != 1 {
		t.Fatalf("request limiter = %v/%d, want 1/1", ins.RequestRPSLimit, ins.RequestBurst)
	}
	if ins.usageLocation == nil || ins.usageLocation.String() != "Asia/Shanghai" {
		t.Fatalf("usage location = %v, want Asia/Shanghai", ins.usageLocation)
	}
}

func TestInstanceInitConfiguresCustomRequestRate(t *testing.T) {
	ins := validConfigInstance(t)
	ins.BaseURL = "https://custom-rate.example.com"
	ins.RequestRPSLimit = 1.5
	ins.RequestBurst = 3
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.RequestRPSLimit != 1.5 || ins.RequestBurst != 3 {
		t.Fatalf("limiter settings = %v/%d, want 1.5/3", ins.RequestRPSLimit, ins.RequestBurst)
	}
}

func TestInstanceInitAllowsHTTPTimeoutIndependentOfRequestRate(t *testing.T) {
	tests := []struct {
		name    string
		rps     float64
		timeout time.Duration
	}{
		{name: "one RPS with ten second timeout", rps: 1, timeout: 10 * time.Second},
		{name: "point one RPS with ten second timeout", rps: 0.1, timeout: 10 * time.Second},
		{name: "point one RPS with twelve second timeout", rps: 0.1, timeout: 12 * time.Second},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := validConfigInstance(t)
			ins.BaseURL = "https://timeout-" + string(rune('a'+i)) + ".example.com"
			ins.RequestRPSLimit = tt.rps
			ins.Timeout = config.Duration(tt.timeout)

			if err := ins.Init(); err != nil {
				t.Fatalf("Init() error = %v", err)
			}
		})
	}
}

func TestInstanceInitRejectsNegativeRequestRateSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Instance)
	}{
		{name: "RPS", mutate: func(ins *Instance) { ins.RequestRPSLimit = -1 }},
		{name: "burst", mutate: func(ins *Instance) { ins.RequestBurst = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := validConfigInstance(t)
			tt.mutate(ins)
			if err := ins.Init(); err == nil {
				t.Fatal("Init() error = nil, want invalid limiter configuration")
			}
		})
	}
}

func TestInstanceInitRestoresVersionTwoState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	createdAt := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	want := persistedState{
		Version:            stateVersion,
		AccessToken:        "persisted-access",
		RefreshToken:       "persisted-refresh",
		LastUsageID:        42,
		LastUsageCreatedAt: &createdAt,
		UsageSeries: map[string]*usageSeriesState{
			"series": {Requests: 7},
		},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := os.WriteFile(stateFile, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	ins := validConfigInstance(t)
	ins.StateFile = stateFile
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.accessToken != "persisted-access" || ins.refreshToken != "persisted-refresh" {
		t.Fatalf("restored tokens = %q/%q", ins.accessToken, ins.refreshToken)
	}
	if ins.state.LastUsageID != 42 || ins.state.UsageSeries["series"].Requests != 7 {
		t.Fatalf("restored state = %+v", ins.state)
	}
}

func TestWriteStateFileDoesNotChangeExistingDirectoryPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir shared: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod shared: %v", err)
	}
	ins := &Instance{StateFile: filepath.Join(dir, "state.json")}
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := ins.writeStateFile(state); err != nil {
		t.Fatalf("writeStateFile() error = %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat directory: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("existing directory permissions = %o, want unchanged 755", got)
	}
	stateInfo, err := os.Stat(ins.StateFile)
	if err != nil {
		t.Fatalf("stat state file: %v", err)
	}
	if got := stateInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("state file permissions = %o, want 600", got)
	}
}

func TestInstanceInitRejectsCursorWithoutTimestamp(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	data, err := json.Marshal(persistedState{
		Version: stateVersion, LastUsageID: 42, UsageInitialized: true,
		UsageSeries: make(map[string]*usageSeriesState),
	})
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if err := os.WriteFile(stateFile, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	ins := validConfigInstance(t)
	ins.StateFile = stateFile

	err = ins.Init()
	if err == nil || !strings.Contains(err.Error(), "baseline timestamp") {
		t.Fatalf("Init() error = %v, want cursor timestamp validation", err)
	}
}

func validConfigInstance(t *testing.T) *Instance {
	t.Helper()
	ins := &Instance{
		Site:          "supplier-a",
		BaseURL:       "https://supplier.example.com",
		RefreshToken:  config.NewSecret([]byte("configured-refresh")),
		StateFile:     filepath.Join(t.TempDir(), "state.json"),
		UsageTimezone: "Asia/Shanghai",
		ModelAPIKeys: []ModelAPIKey{{
			APIKey: config.NewSecret([]byte("group-token")),
		}},
	}
	t.Cleanup(func() {
		ins.releaseRequestLimiter()
		ins.RefreshToken.Destroy()
		for i := range ins.ModelAPIKeys {
			ins.ModelAPIKeys[i].APIKey.Destroy()
		}
	})
	return ins
}

func TestInstanceInitRejectsLegacyConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Instance)
		want   string
	}{
		{name: "usage page size", mutate: func(ins *Instance) { value := 50; ins.LegacyUsagePageSize = &value }, want: "usage_page_size"},
		{name: "usage max pages", mutate: func(ins *Instance) { value := 5; ins.LegacyUsageMaxPages = &value }, want: "usage_max_pages"},
		{name: "usage max count", mutate: func(ins *Instance) { value := 250; ins.LegacyUsageMaxCount = &value }, want: "usage_max_count"},
		{name: "request interval", mutate: func(ins *Instance) { value := config.Duration(time.Second); ins.LegacyRequestInterval = &value }, want: "request_interval"},
		{name: "group api keys", mutate: func(ins *Instance) {
			ins.LegacyGroupAPIKeys = []GroupAPIKey{{Group: "pro", Platform: "anthropic", APIKey: config.NewSecret([]byte("legacy-key"))}}
		}, want: "group_api_keys"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := validConfigInstance(t)
			tt.mutate(ins)
			for i := range ins.LegacyGroupAPIKeys {
				t.Cleanup(ins.LegacyGroupAPIKeys[i].APIKey.Destroy)
			}
			err := ins.Init()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Init() error = %v, want legacy field %q rejection", err, tt.want)
			}
		})
	}
}
