package sub2api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

const (
	tokenStateVersion = 1
	stateVersion      = 2
)

type tokenState struct {
	Version      int    `json:"version"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type histogramState struct {
	Buckets []uint64 `json:"buckets"`
	Sum     float64  `json:"sum"`
	Count   uint64   `json:"count"`
}

type usageSeriesState struct {
	Group               string         `json:"group"`
	Platform            string         `json:"platform"`
	Model               string         `json:"model"`
	RequestType         string         `json:"request_type"`
	Requests            uint64         `json:"requests"`
	InputTokens         uint64         `json:"input_tokens"`
	OutputTokens        uint64         `json:"output_tokens"`
	CacheCreationTokens uint64         `json:"cache_creation_tokens"`
	CacheReadTokens     uint64         `json:"cache_read_tokens"`
	TotalTokens         uint64         `json:"total_tokens"`
	StandardCostUSD     float64        `json:"standard_cost_usd"`
	ActualCostUSD       float64        `json:"actual_cost_usd"`
	Duration            histogramState `json:"duration"`
	FirstToken          histogramState `json:"first_token"`
}

type persistedState struct {
	Version            int                          `json:"version"`
	AccessToken        string                       `json:"access_token,omitempty"`
	RefreshToken       string                       `json:"refresh_token,omitempty"`
	LastUsageID        int64                        `json:"last_usage_id,omitempty"`
	LastUsageCreatedAt *time.Time                   `json:"last_usage_created_at,omitempty"`
	UsageInitialized   bool                         `json:"usage_initialized,omitempty"`
	UsageSeries        map[string]*usageSeriesState `json:"usage_series,omitempty"`
}

type refreshData struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func (ins *Instance) ensureAccessToken(ctx context.Context) error {
	ins.authMu.Lock()
	defer ins.authMu.Unlock()

	if ins.tokenStateDirty {
		if err := ins.persistStateLocked(); err != nil {
			return fmt.Errorf("persist pending token state: %w", err)
		}
		ins.tokenStateDirty = false
	}
	if ins.accessToken != "" {
		return nil
	}
	if ins.refreshToken == "" {
		return fmt.Errorf("refresh token is required")
	}
	if err := ins.refreshLocked(ctx); err != nil {
		return fmt.Errorf("refresh access token: %w", err)
	}
	return nil
}

func (ins *Instance) doUserJSON(ctx context.Context, method, endpoint string, query url.Values, body, out any) error {
	ins.authMu.Lock()
	if ins.tokenStateDirty {
		if err := ins.persistStateLocked(); err != nil {
			ins.authMu.Unlock()
			return fmt.Errorf("persist pending token state: %w", err)
		}
		ins.tokenStateDirty = false
	}
	usedToken := ins.accessToken
	ins.authMu.Unlock()

	err := ins.client.DoJSON(ctx, method, endpoint, query, map[string]string{
		"Authorization": "Bearer " + usedToken,
	}, body, out)
	var statusErr *llmupstream.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusUnauthorized {
		return err
	}

	ins.authMu.Lock()
	defer ins.authMu.Unlock()
	if ins.accessToken == usedToken {
		if ins.refreshToken == "" {
			return err
		}
		if refreshErr := ins.refreshLocked(ctx); refreshErr != nil {
			return fmt.Errorf("refresh access token: %w", refreshErr)
		}
	}

	return ins.client.DoJSON(ctx, method, endpoint, query, map[string]string{
		"Authorization": "Bearer " + ins.accessToken,
	}, body, out)
}

func (ins *Instance) refreshLocked(ctx context.Context) error {
	var response envelope[refreshData]
	if err := ins.client.DoJSON(ctx, http.MethodPost, "/api/v1/auth/refresh", nil, nil, map[string]string{
		"refresh_token": ins.refreshToken,
	}, &response); err != nil {
		return err
	}
	if response.Code != 0 {
		return fmt.Errorf("refresh API returned code %d: %s", response.Code, ins.sanitizeErrorField(response.Message))
	}
	if response.Data.AccessToken == "" || response.Data.RefreshToken == "" {
		return fmt.Errorf("refresh API returned empty tokens")
	}

	ins.accessToken = response.Data.AccessToken
	ins.refreshToken = response.Data.RefreshToken
	ins.updateRedactionSecrets()
	if err := ins.persistStateLocked(); err != nil {
		ins.tokenStateDirty = true
		return err
	}
	ins.tokenStateDirty = false
	return nil
}

func (ins *Instance) statePath() string {
	return ins.StateFile
}

func (ins *Instance) loadState() error {
	path := ins.statePath()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		ins.state = persistedState{Version: stateVersion, UsageSeries: make(map[string]*usageSeriesState)}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	if version.Version == tokenStateVersion {
		var legacy tokenState
		if err := json.Unmarshal(data, &legacy); err != nil {
			return fmt.Errorf("decode state: %w", err)
		}
		if legacy.AccessToken == "" || legacy.RefreshToken == "" {
			return fmt.Errorf("invalid state")
		}
		ins.state = persistedState{
			Version: stateVersion, AccessToken: legacy.AccessToken, RefreshToken: legacy.RefreshToken,
			UsageSeries: make(map[string]*usageSeriesState),
		}
	} else if version.Version == stateVersion {
		if err := json.Unmarshal(data, &ins.state); err != nil {
			return fmt.Errorf("decode state: %w", err)
		}
		if ins.state.UsageSeries == nil {
			ins.state.UsageSeries = make(map[string]*usageSeriesState)
		}
	} else {
		return fmt.Errorf("unsupported state version %d", version.Version)
	}
	if ins.state.LastUsageID < 0 {
		return fmt.Errorf("invalid negative usage cursor %d", ins.state.LastUsageID)
	}
	if ins.state.LastUsageID > 0 {
		ins.state.UsageInitialized = true
	}
	if ins.state.UsageInitialized && ins.state.LastUsageCreatedAt == nil {
		return fmt.Errorf("usage state has no baseline timestamp")
	}
	if ins.state.AccessToken != "" {
		ins.accessToken = ins.state.AccessToken
	}
	if ins.state.RefreshToken != "" {
		ins.refreshToken = ins.state.RefreshToken
	}
	return nil
}

func (ins *Instance) persistStateLocked() error {
	candidate := clonePersistedState(ins.state)
	candidate.AccessToken = ins.accessToken
	candidate.RefreshToken = ins.refreshToken
	if err := ins.writeStateFile(candidate); err != nil {
		return err
	}
	ins.state = candidate
	return nil
}

func (ins *Instance) writeStateFile(state persistedState) error {
	path := ins.statePath()
	if path == "" {
		return fmt.Errorf("state_file is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".sub2api-state-*")
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
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	return os.Chmod(path, 0o600)
}
