package sub2api

import (
	"sort"

	"flashcat.cloud/categraf/types"
)

const modelStatsWindow = "30d"

// emitUsage maps a /v1/usage response onto the sample list. It always emits
// sub2api_key_remaining_usd with a mode label, then adds mode-specific detail
// metrics, lifetime usage counters, and per-model gauges.
func (ins *Instance) emitUsage(slist *types.SampleList, response *usageResponse) {
	base := ins.baseLabels()
	modeLabel := normalizeMode(response)

	remaining, ok := computeRemainingUSD(response, modeLabel)
	if ok {
		slist.PushSample(inputName, "key_remaining_usd", remaining, mergeLabels(base, map[string]string{"mode": modeLabel}))
	}

	switch modeLabel {
	case "quota_limited":
		emitQuotaMetrics(slist, base, response)
	case "subscription":
		emitSubscriptionMetrics(slist, base, response)
	case "wallet":
		emitWalletMetrics(slist, base, response)
	}

	if response.ExpiresAt != nil {
		slist.PushSample(inputName, "key_expires_timestamp", response.ExpiresAt.Unix(), base)
	}
	if response.DaysUntilExpiry != nil {
		slist.PushSample(inputName, "key_days_until_expiry", *response.DaysUntilExpiry, base)
	}

	if response.Usage != nil {
		emitUsageTotals(slist, base, response.Usage)
	}
	if len(response.ModelStats) > 0 {
		emitModelStats(slist, base, response.ModelStats)
	}
}

func normalizeMode(response *usageResponse) string {
	if response.Mode == "quota_limited" {
		return "quota_limited"
	}
	if response.Subscription != nil {
		return "subscription"
	}
	return "wallet"
}

func computeRemainingUSD(response *usageResponse, mode string) (float64, bool) {
	switch mode {
	case "quota_limited":
		if response.Quota != nil {
			return response.Quota.Remaining, true
		}
		if response.Remaining != nil {
			return *response.Remaining, true
		}
		if len(response.RateLimits) > 0 {
			min := response.RateLimits[0].Remaining
			for _, entry := range response.RateLimits[1:] {
				if entry.Remaining < min {
					min = entry.Remaining
				}
			}
			return min, true
		}
	case "subscription":
		if response.Remaining != nil {
			return *response.Remaining, true
		}
	case "wallet":
		if response.Balance != nil {
			return *response.Balance, true
		}
		if response.Remaining != nil {
			return *response.Remaining, true
		}
	}
	return 0, false
}

func emitQuotaMetrics(slist *types.SampleList, base map[string]string, response *usageResponse) {
	if response.Quota != nil {
		slist.PushSample(inputName, "key_quota_limit_usd", response.Quota.Limit, base)
		slist.PushSample(inputName, "key_quota_used_usd", response.Quota.Used, base)
	}
	for _, entry := range response.RateLimits {
		labels := mergeLabels(base, map[string]string{"window": entry.Window})
		slist.PushSample(inputName, "key_rate_limit_limit", entry.Limit, labels)
		slist.PushSample(inputName, "key_rate_limit_used", entry.Used, labels)
		slist.PushSample(inputName, "key_rate_limit_remaining", entry.Remaining, labels)
		if entry.ResetAt != nil {
			slist.PushSample(inputName, "key_rate_limit_reset_timestamp", entry.ResetAt.Unix(), labels)
		}
	}
}

func emitSubscriptionMetrics(slist *types.SampleList, base map[string]string, response *usageResponse) {
	sub := response.Subscription
	if sub == nil {
		return
	}
	windows := []struct {
		name  string
		usage float64
		limit *float64
	}{
		{"daily", sub.DailyUsageUSD, sub.DailyLimitUSD},
		{"weekly", sub.WeeklyUsageUSD, sub.WeeklyLimitUSD},
		{"monthly", sub.MonthlyUsageUSD, sub.MonthlyLimitUSD},
	}
	for _, window := range windows {
		labels := mergeLabels(base, map[string]string{"window": window.name})
		slist.PushSample(inputName, "key_subscription_usage_usd", window.usage, labels)
		if window.limit != nil {
			slist.PushSample(inputName, "key_subscription_limit_usd", *window.limit, labels)
		}
	}
}

func emitWalletMetrics(slist *types.SampleList, base map[string]string, response *usageResponse) {
	if response.Balance != nil {
		slist.PushSample(inputName, "key_wallet_balance_usd", *response.Balance, base)
	}
}

func emitUsageTotals(slist *types.SampleList, base map[string]string, summary *usageSummary) {
	slist.PushSample(inputName, "key_requests_total", summary.Total.Requests, base)
	emitTokenTotals(slist, base, "key_tokens_total", summary.Total)
	slist.PushSample(inputName, "key_cost_usd_total", summary.Total.Cost, mergeLabels(base, map[string]string{"kind": "standard"}))
	slist.PushSample(inputName, "key_cost_usd_total", summary.Total.ActualCost, mergeLabels(base, map[string]string{"kind": "actual"}))
	slist.PushSample(inputName, "key_rpm", summary.RPM, base)
	slist.PushSample(inputName, "key_tpm", summary.TPM, base)
	slist.PushSample(inputName, "key_average_duration_seconds", summary.AverageDurationMs/1000, base)
}

func emitTokenTotals(slist *types.SampleList, base map[string]string, metric string, slice usageSlice) {
	values := []struct {
		kind  string
		value int64
	}{
		{"input", slice.InputTokens},
		{"output", slice.OutputTokens},
		{"cache_creation", slice.CacheCreationTokens},
		{"cache_read", slice.CacheReadTokens},
		{"total", slice.TotalTokens},
	}
	for _, item := range values {
		slist.PushSample(inputName, metric, item.value, mergeLabels(base, map[string]string{"kind": item.kind}))
	}
}

func emitModelStats(slist *types.SampleList, base map[string]string, stats []modelStat) {
	sorted := append([]modelStat(nil), stats...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Model < sorted[j].Model })
	for _, stat := range sorted {
		labels := mergeLabels(base, map[string]string{"model": stat.Model, "window": modelStatsWindow})
		slist.PushSample(inputName, "key_model_requests", stat.Requests, labels)
		emitTokenTotals(slist, labels, "key_model_tokens", usageSlice{
			InputTokens:         stat.InputTokens,
			OutputTokens:        stat.OutputTokens,
			CacheCreationTokens: stat.CacheCreationTokens,
			CacheReadTokens:     stat.CacheReadTokens,
			TotalTokens:         stat.TotalTokens,
		})
		slist.PushSample(inputName, "key_model_cost_usd", stat.Cost, mergeLabels(labels, map[string]string{"kind": "standard"}))
		slist.PushSample(inputName, "key_model_cost_usd", stat.ActualCost, mergeLabels(labels, map[string]string{"kind": "actual"}))
	}
}

func emitBilling(slist *types.SampleList, base map[string]string, response *billingResponse) {
	slist.PushSample(inputName, "key_rate_multiplier", response.GroupRateMultiplier, mergeLabels(base, map[string]string{"kind": "group"}))
	if response.UserRateMultiplier != nil {
		slist.PushSample(inputName, "key_rate_multiplier", *response.UserRateMultiplier, mergeLabels(base, map[string]string{"kind": "user"}))
	}
	slist.PushSample(inputName, "key_rate_multiplier", response.ResolvedRateMultiplier, mergeLabels(base, map[string]string{"kind": "resolved"}))
	if response.PeakRateMultiplier != nil {
		slist.PushSample(inputName, "key_rate_multiplier", *response.PeakRateMultiplier, mergeLabels(base, map[string]string{"kind": "peak_configured"}))
	}
	if response.AppliedPeakMultiplier != nil {
		slist.PushSample(inputName, "key_rate_multiplier", *response.AppliedPeakMultiplier, mergeLabels(base, map[string]string{"kind": "peak_applied"}))
	}
	slist.PushSample(inputName, "key_rate_multiplier", response.EffectiveRateMultiplier, mergeLabels(base, map[string]string{"kind": "effective"}))
	peakEnabled := 0
	if response.PeakRateEnabled {
		peakEnabled = 1
	}
	slist.PushSample(inputName, "key_peak_rate_enabled", peakEnabled, base)
}

func emitModels(slist *types.SampleList, base map[string]string, response *modelsListResponse) {
	seen := make(map[string]struct{}, len(response.Data))
	models := make([]string, 0, len(response.Data))
	for _, entry := range response.Data {
		if entry.ID == "" {
			continue
		}
		if _, dup := seen[entry.ID]; dup {
			continue
		}
		seen[entry.ID] = struct{}{}
		models = append(models, entry.ID)
	}
	sort.Strings(models)
	slist.PushSample(inputName, "key_model_count", len(models), base)
	for _, model := range models {
		slist.PushSample(inputName, "key_model_available", 1, mergeLabels(base, map[string]string{"model": model}))
	}
}
