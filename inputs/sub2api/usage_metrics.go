package sub2api

import (
	"context"
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

func (ins *Instance) gatherUsage(ctx context.Context, slist *types.SampleList) error {
	ins.authMu.Lock()
	candidate := clonePersistedState(ins.state)
	ins.authMu.Unlock()

	scan, err := ins.scanUsageWithHandler(ctx, func(record usageRecord) error {
		return accumulateUsageRecord(&candidate, record)
	})
	if err != nil {
		ins.emitUsageFailureMetrics(slist, scan)
		return err
	}

	ins.authMu.Lock()
	candidate.UsageInitialized = true
	if scan.HighestID > candidate.LastUsageID {
		if scan.HighestCreatedAt.IsZero() {
			ins.authMu.Unlock()
			ins.emitUsageFailureMetrics(slist, scan)
			return fmt.Errorf("usage record %d has no created_at", scan.HighestID)
		}
		createdAt := scan.HighestCreatedAt
		candidate.LastUsageID = scan.HighestID
		candidate.LastUsageCreatedAt = &createdAt
	} else if candidate.LastUsageCreatedAt == nil {
		baseline := ins.startedAt
		candidate.LastUsageCreatedAt = &baseline
	}
	candidate.AccessToken = ins.accessToken
	candidate.RefreshToken = ins.refreshToken
	if err := ins.writeStateFile(candidate); err != nil {
		ins.authMu.Unlock()
		ins.emitUsageFailureMetrics(slist, scan)
		return err
	}
	ins.state = candidate
	ins.authMu.Unlock()

	ins.emitUsageMetrics(slist, scan)
	return nil
}

func (ins *Instance) emitUsageFailureMetrics(slist *types.SampleList, scan usageScanResult) {
	base := ins.baseLabels()
	initialized := 0
	if ins.state.UsageInitialized {
		initialized = 1
	}
	slist.PushSample(inputName, "usage_initialized", initialized, base)
	slist.PushSample(inputName, "usage_last_processed_id", ins.state.LastUsageID, base)
	slist.PushSample(inputName, "usage_new_records", 0, base)
	slist.PushSample(inputName, "usage_scan_pages", scan.Pages, base)
	slist.PushSample(inputName, "usage_scan_records", scan.RecordsFetched, base)
	slist.PushSample(inputName, "usage_scan_complete", 0, base)
}

func clonePersistedState(source persistedState) persistedState {
	clone := source
	if source.LastUsageCreatedAt != nil {
		createdAt := *source.LastUsageCreatedAt
		clone.LastUsageCreatedAt = &createdAt
	}
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

func usageSeriesKey(group, platform, model, requestType string) string {
	return strings.Join([]string{group, platform, model, requestType}, "\x00")
}

func accumulateUsageRecords(state *persistedState, records []usageRecord) error {
	for _, record := range records {
		if err := accumulateUsageRecord(state, record); err != nil {
			return err
		}
	}
	return nil
}

func accumulateUsageRecord(state *persistedState, record usageRecord) error {
	if state.UsageSeries == nil {
		state.UsageSeries = make(map[string]*usageSeriesState)
	}
	if record.InputTokens < 0 || record.OutputTokens < 0 || record.CacheCreationTokens < 0 || record.CacheReadTokens < 0 {
		return fmt.Errorf("usage record %d has negative tokens", record.ID)
	}
	if record.TotalCost < 0 || record.ActualCost < 0 || math.IsNaN(record.TotalCost) || math.IsNaN(record.ActualCost) {
		return fmt.Errorf("usage record %d has invalid cost", record.ID)
	}

	group, platform := "unknown", "unknown"
	if record.Group != nil {
		if strings.TrimSpace(record.Group.Name) != "" {
			group = strings.TrimSpace(record.Group.Name)
		}
		if strings.TrimSpace(record.Group.Platform) != "" {
			platform = strings.TrimSpace(record.Group.Platform)
		}
	}
	model := strings.TrimSpace(record.Model)
	if model == "" {
		model = "unknown"
	}
	requestType := strings.TrimSpace(record.RequestType)
	if requestType == "" {
		requestType = "unknown"
	}

	key := usageSeriesKey(group, platform, model, requestType)
	series := state.UsageSeries[key]
	if series == nil {
		series = &usageSeriesState{
			Group: group, Platform: platform, Model: model, RequestType: requestType,
			Duration:   newHistogramState(durationBuckets),
			FirstToken: newHistogramState(firstTokenBuckets),
		}
		state.UsageSeries[key] = series
	} else {
		normalizeHistogram(&series.Duration, durationBuckets)
		normalizeHistogram(&series.FirstToken, firstTokenBuckets)
	}

	series.Requests++
	series.InputTokens += uint64(record.InputTokens)
	series.OutputTokens += uint64(record.OutputTokens)
	series.CacheCreationTokens += uint64(record.CacheCreationTokens)
	series.CacheReadTokens += uint64(record.CacheReadTokens)
	series.TotalTokens += uint64(record.InputTokens + record.OutputTokens + record.CacheCreationTokens + record.CacheReadTokens)
	series.StandardCostUSD += record.TotalCost
	series.ActualCostUSD += record.ActualCost

	if record.DurationMS != nil {
		if *record.DurationMS < 0 {
			return fmt.Errorf("usage record %d has negative duration_ms", record.ID)
		}
		observeHistogram(&series.Duration, durationBuckets, float64(*record.DurationMS)/1000)
	}
	if record.FirstTokenMS != nil {
		if *record.FirstTokenMS < 0 {
			return fmt.Errorf("usage record %d has negative first_token_ms", record.ID)
		}
		observeHistogram(&series.FirstToken, firstTokenBuckets, float64(*record.FirstTokenMS)/1000)
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
	resized := make([]uint64, len(bounds)+1)
	copy(resized, histogram.Buckets)
	histogram.Buckets = resized
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

func (ins *Instance) emitUsageMetrics(slist *types.SampleList, scan usageScanResult) {
	base := ins.baseLabels()
	initialized := 0
	if ins.state.UsageInitialized || ins.state.LastUsageID > 0 {
		initialized = 1
	}
	complete := 0
	if scan.Complete {
		complete = 1
	}
	slist.PushSample(inputName, "usage_initialized", initialized, base)
	slist.PushSample(inputName, "usage_last_processed_id", ins.state.LastUsageID, base)
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
			"group": series.Group, "platform": series.Platform, "model": series.Model, "request_type": series.RequestType,
		})
		slist.PushSample(inputName, "usage_model_requests_total", series.Requests, labels)
		emitUsageTokens(slist, labels, series)
		slist.PushSample(inputName, "usage_model_cost_usd_total", series.StandardCostUSD, mergeLabels(labels, map[string]string{"kind": "standard"}))
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
		{"cache_creation", series.CacheCreationTokens}, {"cache_read", series.CacheReadTokens},
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
