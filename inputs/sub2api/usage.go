package sub2api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type usageResponse struct {
	Mode      string   `json:"mode"`
	IsValid   bool     `json:"isValid"`
	Status    string   `json:"status,omitempty"`
	PlanName  string   `json:"planName,omitempty"`
	Unit      string   `json:"unit,omitempty"`
	Remaining *float64 `json:"remaining,omitempty"`
	Balance   *float64 `json:"balance,omitempty"`

	Quota        *quotaInfo        `json:"quota,omitempty"`
	RateLimits   []rateLimitEntry  `json:"rate_limits,omitempty"`
	Subscription *subscriptionInfo `json:"subscription,omitempty"`

	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	DaysUntilExpiry *int       `json:"days_until_expiry,omitempty"`

	Usage      *usageSummary     `json:"usage,omitempty"`
	DailyUsage []dailyUsagePoint `json:"daily_usage,omitempty"`
	ModelStats []modelStat       `json:"model_stats,omitempty"`
}

type quotaInfo struct {
	Limit     float64 `json:"limit"`
	Used      float64 `json:"used"`
	Remaining float64 `json:"remaining"`
	Unit      string  `json:"unit"`
}

type rateLimitEntry struct {
	Window      string     `json:"window"`
	Limit       float64    `json:"limit"`
	Used        float64    `json:"used"`
	Remaining   float64    `json:"remaining"`
	WindowStart *time.Time `json:"window_start,omitempty"`
	ResetAt     *time.Time `json:"reset_at,omitempty"`
}

type subscriptionInfo struct {
	DailyUsageUSD     float64    `json:"daily_usage_usd"`
	WeeklyUsageUSD    float64    `json:"weekly_usage_usd"`
	MonthlyUsageUSD   float64    `json:"monthly_usage_usd"`
	DailyLimitUSD     *float64   `json:"daily_limit_usd,omitempty"`
	WeeklyLimitUSD    *float64   `json:"weekly_limit_usd,omitempty"`
	MonthlyLimitUSD   *float64   `json:"monthly_limit_usd,omitempty"`
	WeeklyWindowStart *time.Time `json:"weekly_window_start,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
}

type usageSummary struct {
	Today             usageSlice `json:"today"`
	Total             usageSlice `json:"total"`
	AverageDurationMs float64    `json:"average_duration_ms"`
	RPM               float64    `json:"rpm"`
	TPM               float64    `json:"tpm"`
}

type usageSlice struct {
	Requests            int64   `json:"requests"`
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	TotalTokens         int64   `json:"total_tokens"`
	Cost                float64 `json:"cost"`
	ActualCost          float64 `json:"actual_cost"`
}

type dailyUsagePoint struct {
	Date             string  `json:"date"`
	Requests         int64   `json:"requests"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	CacheReadTokens  int64   `json:"cache_read_tokens"`
	CacheWriteTokens int64   `json:"cache_write_tokens"`
	TotalTokens      int64   `json:"total_tokens"`
	Cost             float64 `json:"cost"`
	ActualCost       float64 `json:"actual_cost"`
}

type modelStat struct {
	Model               string  `json:"model"`
	Requests            int64   `json:"requests"`
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	TotalTokens         int64   `json:"total_tokens"`
	Cost                float64 `json:"cost"`
	ActualCost          float64 `json:"actual_cost"`
	AccountCost         float64 `json:"account_cost"`
}

type billingResponse struct {
	Object                  string   `json:"object"`
	SchemaVersion           int      `json:"schema_version"`
	BillingScope            string   `json:"billing_scope"`
	GroupRateMultiplier     float64  `json:"group_rate_multiplier"`
	UserRateMultiplier      *float64 `json:"user_rate_multiplier,omitempty"`
	ResolvedRateMultiplier  float64  `json:"resolved_rate_multiplier"`
	PeakRateEnabled         bool     `json:"peak_rate_enabled"`
	PeakRateMultiplier      *float64 `json:"peak_rate_multiplier,omitempty"`
	AppliedPeakMultiplier   *float64 `json:"applied_peak_multiplier,omitempty"`
	EffectiveRateMultiplier float64  `json:"effective_rate_multiplier"`
}

type modelsListResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func (ins *Instance) fetchUsage(ctx context.Context) (*usageResponse, error) {
	query := url.Values{}
	if tz := ins.UsageTimezone; tz != "" {
		query.Set("timezone", tz)
	}
	var response usageResponse
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/v1/usage", query, ins.authHeaders(), nil, &response); err != nil {
		return nil, fmt.Errorf("fetch usage: %w", ins.wrapClientError(err))
	}
	if response.Mode == "" {
		return nil, fmt.Errorf("usage response missing mode")
	}
	return &response, nil
}

func (ins *Instance) fetchBilling(ctx context.Context) (*billingResponse, error) {
	var response billingResponse
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/v1/sub2api/billing", nil, ins.authHeaders(), nil, &response); err != nil {
		return nil, fmt.Errorf("fetch billing: %w", ins.wrapClientError(err))
	}
	return &response, nil
}

func (ins *Instance) fetchModels(ctx context.Context) (*modelsListResponse, error) {
	var response modelsListResponse
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/v1/models", nil, ins.authHeaders(), nil, &response); err != nil {
		return nil, fmt.Errorf("fetch models: %w", ins.wrapClientError(err))
	}
	return &response, nil
}
