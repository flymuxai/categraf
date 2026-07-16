package newapi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"flashcat.cloud/categraf/types"
)

var (
	firstTokenBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 30}
	durationBuckets   = []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300}
)

type usageOther struct {
	FirstResponseMS     *float64 `json:"frt"`
	CacheReadTokens     int64    `json:"cache_tokens"`
	CacheCreationTokens int64    `json:"cache_creation_tokens"`
	CacheCreation5m     int64    `json:"cache_creation_tokens_5m"`
	CacheCreation1h     int64    `json:"cache_creation_tokens_1h"`
	IsTask              bool     `json:"is_task"`
}

func (ins *Instance) gatherUsage(ctx context.Context, slist *types.SampleList, quotaPerUnit float64) error {
	ins.stateMu.Lock()
	candidate := clonePersistedState(ins.state)
	ins.stateMu.Unlock()

	scan, err := ins.scanUsageWithHandler(ctx, func(record usageRecord) error {
		return accumulateUsageRecord(&candidate, record, quotaPerUnit)
	})
	if err != nil {
		ins.emitUsageFailureMetrics(slist, scan)
		return err
	}
	if err := ctx.Err(); err != nil {
		ins.emitUsageFailureMetrics(slist, scan)
		return err
	}
	if scan.Deferred {
		ins.emitUsageMetrics(slist, scan)
		return nil
	}

	candidate.UsageInitialized = true
	candidate.LastUsageTimestamp = scan.HighestTimestamp
	candidate.LastUsageRequestIDs = append([]string(nil), scan.HighestRequestIDs...)
	if err := ins.commitUsageState(ctx, candidate); err != nil {
		ins.emitUsageFailureMetrics(slist, scan)
		return err
	}
	ins.emitUsageMetrics(slist, scan)
	return nil
}

func (ins *Instance) commitUsageState(ctx context.Context, candidate persistedState) error {
	// Serialize the final state commit with Drop so cancellation cannot race the
	// last context check and advance the persisted watermark during shutdown.
	ins.lifecycleMu.Lock()
	defer ins.lifecycleMu.Unlock()
	if ins.dropped {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	ins.stateMu.Lock()
	defer ins.stateMu.Unlock()
	if err := ins.writeStateFile(candidate); err != nil {
		return err
	}
	ins.state = candidate
	return nil
}

func clonePersistedState(source persistedState) persistedState {
	clone := source
	clone.LastUsageRequestIDs = append([]string(nil), source.LastUsageRequestIDs...)
	clone.UsageSeries = make(map[string]*usageSeriesState, len(source.UsageSeries))
	for key, series := range source.UsageSeries {
		if series == nil {
			continue
		}
		seriesClone := *series
		seriesClone.Duration.Buckets = append([]uint64(nil), series.Duration.Buckets...)
		seriesClone.FirstToken.Buckets = append([]uint64(nil), series.FirstToken.Buckets...)
		clone.UsageSeries[key] = &seriesClone
	}
	return clone
}

func usageSeriesKey(group, model, requestType string) string {
	return strings.Join([]string{group, model, requestType}, "\x00")
}

func accumulateUsageRecords(state *persistedState, records []usageRecord, quotaPerUnit float64) error {
	for _, record := range records {
		if err := accumulateUsageRecord(state, record, quotaPerUnit); err != nil {
			return err
		}
	}
	return nil
}

func accumulateUsageRecord(state *persistedState, record usageRecord, quotaPerUnit float64) error {
	if quotaPerUnit <= 0 || math.IsNaN(quotaPerUnit) || math.IsInf(quotaPerUnit, 0) {
		return fmt.Errorf("quota_per_unit must be a finite positive number")
	}
	if state.UsageSeries == nil {
		state.UsageSeries = make(map[string]*usageSeriesState)
	}
	if record.PromptTokens < 0 || record.CompletionTokens < 0 {
		return fmt.Errorf("usage record has negative tokens")
	}
	if record.PromptTokens > math.MaxInt64-record.CompletionTokens {
		return fmt.Errorf("usage record token total overflows")
	}
	if record.Quota < 0 {
		return fmt.Errorf("usage record has negative quota")
	}

	other := usageOther{}
	if strings.TrimSpace(record.Other) != "" {
		if err := json.Unmarshal([]byte(record.Other), &other); err != nil {
			return fmt.Errorf("usage record has invalid other: %w", err)
		}
	}
	if other.CacheReadTokens < 0 || other.CacheCreationTokens < 0 || other.CacheCreation5m < 0 || other.CacheCreation1h < 0 {
		return fmt.Errorf("usage record has negative cache tokens")
	}
	if other.CacheCreation5m > math.MaxInt64-other.CacheCreation1h {
		return fmt.Errorf("usage record cache creation total overflows")
	}
	if other.FirstResponseMS != nil && (math.IsNaN(*other.FirstResponseMS) || math.IsInf(*other.FirstResponseMS, 0)) {
		return fmt.Errorf("usage record has invalid first response latency")
	}

	group := strings.TrimSpace(record.Group)
	if group == "" {
		group = "unknown"
	}
	model := strings.TrimSpace(record.ModelName)
	if model == "" {
		model = "unknown"
	}
	requestType := "non_stream"
	if record.IsStream {
		requestType = "stream"
	}

	key := usageSeriesKey(group, model, requestType)
	series := state.UsageSeries[key]
	if series == nil {
		series = &usageSeriesState{
			Group: group, Model: model, RequestType: requestType,
			Duration: newHistogramState(durationBuckets), FirstToken: newHistogramState(firstTokenBuckets),
		}
		state.UsageSeries[key] = series
	} else {
		normalizeHistogram(&series.Duration, durationBuckets)
		normalizeHistogram(&series.FirstToken, firstTokenBuckets)
	}

	cacheCreation := other.CacheCreation5m + other.CacheCreation1h
	if other.CacheCreationTokens > cacheCreation {
		cacheCreation = other.CacheCreationTokens
	}
	series.Requests++
	series.InputTokens += uint64(record.PromptTokens)
	series.OutputTokens += uint64(record.CompletionTokens)
	series.CacheReadTokens += uint64(other.CacheReadTokens)
	series.CacheCreationTokens += uint64(cacheCreation)
	series.TotalTokens += uint64(record.PromptTokens + record.CompletionTokens)
	series.ActualCostUSD += float64(record.Quota) / quotaPerUnit

	if record.UseTime != nil && !other.IsTask {
		if *record.UseTime < 0 {
			return fmt.Errorf("usage record has negative use_time")
		}
		observeHistogram(&series.Duration, durationBuckets, float64(*record.UseTime))
	}
	if record.IsStream && other.FirstResponseMS != nil && *other.FirstResponseMS > 0 {
		observeHistogram(&series.FirstToken, firstTokenBuckets, *other.FirstResponseMS/1000)
	}
	return nil
}

func newHistogramState(bounds []float64) histogramState {
	return histogramState{Buckets: make([]uint64, len(bounds)+1)}
}

func normalizeHistogram(histogram *histogramState, bounds []float64) {
	if len(histogram.Buckets) == len(bounds)+1 {
		return
	}
	buckets := make([]uint64, len(bounds)+1)
	copy(buckets, histogram.Buckets)
	histogram.Buckets = buckets
}

func observeHistogram(histogram *histogramState, bounds []float64, value float64) {
	for i, bound := range bounds {
		if value <= bound {
			histogram.Buckets[i]++
		}
	}
	histogram.Buckets[len(bounds)]++
	histogram.Sum += value
	histogram.Count++
}

func (ins *Instance) emitUsageFailureMetrics(slist *types.SampleList, scan usageScanResult) {
	base := ins.baseLabels()
	initialized := 0
	if ins.state.UsageInitialized {
		initialized = 1
	}
	slist.PushSample(inputName, "usage_initialized", initialized, base)
	slist.PushSample(inputName, "usage_last_processed_timestamp_seconds", ins.state.LastUsageTimestamp, base)
	slist.PushSample(inputName, "usage_new_records", 0, base)
	slist.PushSample(inputName, "usage_scan_pages", scan.Pages, base)
	slist.PushSample(inputName, "usage_scan_records", scan.RecordsFetched, base)
	slist.PushSample(inputName, "usage_scan_complete", 0, base)
}

func (ins *Instance) emitUsageMetrics(slist *types.SampleList, scan usageScanResult) {
	base := ins.baseLabels()
	initialized := 0
	if ins.state.UsageInitialized {
		initialized = 1
	}
	complete := 0
	if scan.Complete {
		complete = 1
	}
	slist.PushSample(inputName, "usage_initialized", initialized, base)
	slist.PushSample(inputName, "usage_last_processed_timestamp_seconds", ins.state.LastUsageTimestamp, base)
	slist.PushSample(inputName, "usage_new_records", scan.NewRecords, base)
	slist.PushSample(inputName, "usage_scan_pages", scan.Pages, base)
	slist.PushSample(inputName, "usage_scan_records", scan.RecordsFetched, base)
	slist.PushSample(inputName, "usage_scan_complete", complete, base)

	keys := make([]string, 0, len(ins.state.UsageSeries))
	for key := range ins.state.UsageSeries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		series := ins.state.UsageSeries[key]
		labels := mergeLabels(base, map[string]string{
			"group": series.Group, "platform": "newapi", "model": series.Model, "request_type": series.RequestType,
		})
		slist.PushSample(inputName, "usage_model_requests_total", series.Requests, labels)
		emitUsageTokens(slist, labels, series)
		slist.PushSample(inputName, "usage_model_cost_usd_total", series.ActualCostUSD, mergeLabels(labels, map[string]string{"kind": "actual"}))
		emitHistogram(slist, "usage_model_duration_seconds", labels, durationBuckets, series.Duration)
		emitHistogram(slist, "usage_model_first_token_latency_seconds", labels, firstTokenBuckets, series.FirstToken)
	}
}

func emitUsageTokens(slist *types.SampleList, labels map[string]string, series *usageSeriesState) {
	values := []struct {
		kind  string
		value uint64
	}{
		{"input", series.InputTokens}, {"output", series.OutputTokens},
		{"cache_read", series.CacheReadTokens}, {"cache_creation", series.CacheCreationTokens},
		{"total", series.TotalTokens},
	}
	for _, item := range values {
		slist.PushSample(inputName, "usage_model_tokens_total", item.value, mergeLabels(labels, map[string]string{"kind": item.kind}))
	}
}

func emitHistogram(slist *types.SampleList, metric string, labels map[string]string, bounds []float64, histogram histogramState) {
	normalizeHistogram(&histogram, bounds)
	for i, bound := range bounds {
		le := strconv.FormatFloat(bound, 'f', -1, 64)
		slist.PushSample(inputName, metric+"_bucket", histogram.Buckets[i], mergeLabels(labels, map[string]string{"le": le}))
	}
	slist.PushSample(inputName, metric+"_bucket", histogram.Buckets[len(bounds)], mergeLabels(labels, map[string]string{"le": "+Inf"}))
	slist.PushSample(inputName, metric+"_sum", histogram.Sum, labels)
	slist.PushSample(inputName, metric+"_count", histogram.Count, labels)
}
