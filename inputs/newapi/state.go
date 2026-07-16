package newapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const stateVersion = 1

type usageSeriesState struct {
	Group               string         `json:"group"`
	Model               string         `json:"model"`
	RequestType         string         `json:"request_type"`
	Requests            uint64         `json:"requests"`
	InputTokens         uint64         `json:"input_tokens"`
	OutputTokens        uint64         `json:"output_tokens"`
	CacheReadTokens     uint64         `json:"cache_read_tokens"`
	CacheCreationTokens uint64         `json:"cache_creation_tokens"`
	TotalTokens         uint64         `json:"total_tokens"`
	ActualCostUSD       float64        `json:"actual_cost_usd"`
	Duration            histogramState `json:"duration"`
	FirstToken          histogramState `json:"first_token"`
}

type histogramState struct {
	Buckets []uint64 `json:"buckets"`
	Sum     float64  `json:"sum"`
	Count   uint64   `json:"count"`
}

type persistedState struct {
	Version             int                          `json:"version"`
	UsageInitialized    bool                         `json:"usage_initialized,omitempty"`
	LastUsageTimestamp  int64                        `json:"last_usage_timestamp,omitempty"`
	LastUsageRequestIDs []string                     `json:"last_usage_request_ids,omitempty"`
	UsageSeries         map[string]*usageSeriesState `json:"usage_series,omitempty"`
}

func (ins *Instance) loadState() error {
	data, err := os.ReadFile(ins.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		ins.state = persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if err := json.Unmarshal(data, &ins.state); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	if ins.state.Version != stateVersion {
		return fmt.Errorf("unsupported state version %d", ins.state.Version)
	}
	if ins.state.UsageInitialized && ins.state.LastUsageTimestamp <= 0 {
		return fmt.Errorf("initialized usage state has invalid timestamp %d", ins.state.LastUsageTimestamp)
	}
	if ins.state.UsageSeries == nil {
		ins.state.UsageSeries = make(map[string]*usageSeriesState)
	}
	for key, series := range ins.state.UsageSeries {
		if series == nil {
			return fmt.Errorf("nil usage series %q", key)
		}
	}
	return nil
}

func (ins *Instance) writeStateFile(state persistedState) error {
	if ins.StateFile == "" {
		return fmt.Errorf("state_file is required")
	}
	dir := filepath.Dir(ins.StateFile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".newapi-state-*")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("set state permissions: %w", err)
	}
	state.Version = stateVersion
	if state.UsageSeries == nil {
		state.UsageSeries = make(map[string]*usageSeriesState)
	}
	if err := json.NewEncoder(tmp).Encode(&state); err != nil {
		tmp.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close state: %w", err)
	}
	if err := os.Rename(tmpName, ins.StateFile); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	return os.Chmod(ins.StateFile, 0o600)
}
