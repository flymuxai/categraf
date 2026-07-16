package newapi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/pkg/cfg"
)

func TestPluginInitDefaultsIntervalToFiveMinutes(t *testing.T) {
	var plugin NewAPI
	if err := plugin.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got, want := time.Duration(plugin.Interval), 5*time.Minute; got != want {
		t.Fatalf("interval = %s, want %s", got, want)
	}
}

func TestInstanceInitRequiresStateFile(t *testing.T) {
	ins := validNewAPIConfigInstance(t)
	ins.StateFile = ""

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "state_file is required") {
		t.Fatalf("Init() error = %v, want state_file validation", err)
	}
}

func TestInstanceInitDefaultsRequestRate(t *testing.T) {
	ins := validNewAPIConfigInstance(t)

	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.RequestRPSLimit != 1 || ins.RequestBurst != 1 {
		t.Fatalf("request limiter = %v/%d, want 1/1", ins.RequestRPSLimit, ins.RequestBurst)
	}
}

func TestInstanceInitConfiguresCustomRequestRate(t *testing.T) {
	ins := validNewAPIConfigInstance(t)
	ins.BaseURL = "https://custom-newapi-rate.example.com"
	ins.RequestRPSLimit = 1.5
	ins.RequestBurst = 3
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.RequestRPSLimit != 1.5 || ins.RequestBurst != 3 {
		t.Fatalf("request limiter = %v/%d, want 1.5/3", ins.RequestRPSLimit, ins.RequestBurst)
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
			ins := validNewAPIConfigInstance(t)
			tt.mutate(ins)
			if err := ins.Init(); err == nil {
				t.Fatal("Init() error = nil, want invalid limiter configuration")
			}
		})
	}
}

func TestInstanceInitRejectsRemovedUsagePagingFields(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "page size", field: "usage_page_size = 50"},
		{name: "page limit", field: "usage_max_pages = 5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var plugin NewAPI
			err := cfg.LoadSingleConfig(cfg.ConfigWithFormat{Format: cfg.TomlFormat, Config: `
[[instances]]
site = "supplier-newapi"
base_url = "https://supplier.example.com"
user_id = 1001
access_token = "system-access-token"
state_file = "/tmp/newapi-state.json"
` + tt.field}, &plugin)
			if err != nil {
				t.Fatalf("LoadSingleConfig() error = %v", err)
			}
			err = plugin.Instances[0].Init()
			if err == nil || !strings.Contains(err.Error(), "was removed") {
				t.Fatalf("Init() error = %v, want removed-field error", err)
			}
		})
	}
}

func TestInstanceInitRestoresUsageState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	want := persistedState{
		Version:             stateVersion,
		UsageInitialized:    true,
		LastUsageTimestamp:  1234,
		LastUsageRequestIDs: []string{"req-a", "req-b"},
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

	ins := validNewAPIConfigInstance(t)
	ins.StateFile = stateFile
	if err := ins.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if ins.state.LastUsageTimestamp != 1234 || ins.state.UsageSeries["series"].Requests != 7 {
		t.Fatalf("restored state = %+v", ins.state)
	}
}

func TestInstanceInitRejectsUnknownStateVersion(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(stateFile, []byte(`{"version":99}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	ins := validNewAPIConfigInstance(t)
	ins.StateFile = stateFile

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "unsupported state version") {
		t.Fatalf("Init() error = %v, want version validation", err)
	}
}

func TestInstanceInitRejectsNullUsageSeries(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	data := []byte(`{"version":1,"usage_initialized":true,"last_usage_timestamp":100,"usage_series":{"broken":null}}`)
	if err := os.WriteFile(stateFile, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	ins := validNewAPIConfigInstance(t)
	ins.StateFile = stateFile

	err := ins.Init()
	if err == nil || !strings.Contains(err.Error(), "nil usage series") {
		t.Fatalf("Init() error = %v, want nil series validation", err)
	}
}

func TestWriteStateFileUsesPrivatePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "newapi")
	ins := &Instance{StateFile: filepath.Join(dir, "state.json")}
	state := persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}

	if err := ins.writeStateFile(state); err != nil {
		t.Fatalf("writeStateFile() error = %v", err)
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat directory: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("directory permissions = %o, want 700", got)
	}
	fileInfo, err := os.Stat(ins.StateFile)
	if err != nil {
		t.Fatalf("stat state file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("state permissions = %o, want 600", got)
	}
}

func validNewAPIConfigInstance(t *testing.T) *Instance {
	t.Helper()
	ins := &Instance{
		Site:        "supplier-a",
		BaseURL:     "https://supplier.example.com",
		UserID:      1001,
		AccessToken: config.NewSecret([]byte("system-access-token")),
		StateFile:   filepath.Join(t.TempDir(), "state.json"),
	}
	t.Cleanup(func() {
		ins.releaseRequestLimiter()
		ins.AccessToken.Destroy()
	})
	return ins
}
