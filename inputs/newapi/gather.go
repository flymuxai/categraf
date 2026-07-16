package newapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"

	"flashcat.cloud/categraf/types"
)

type response[T any] struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

type statusData struct {
	QuotaPerUnit float64 `json:"quota_per_unit"`
}

type selfData struct {
	Quota float64 `json:"quota"`
}

type groupData struct {
	Ratio json.RawMessage `json:"ratio"`
}

type namedGroup struct {
	Name  string
	Ratio *float64
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx, gatherID, ok := ins.beginGather()
	if !ok {
		return
	}
	defer ins.endGather(gatherID)

	allOK := true

	start := time.Now()
	quotaPerUnit, statusErr := ins.fetchQuotaPerUnit(ctx)
	balanceErr := statusErr
	if balanceErr == nil {
		balanceErr = ins.gatherBalance(ctx, slist, quotaPerUnit)
	}
	ins.emitResourceStatus(slist, "balance", start, balanceErr)
	allOK = allOK && balanceErr == nil

	start = time.Now()
	groups, groupsErr := ins.gatherGroups(ctx, slist)
	ins.emitResourceStatus(slist, "groups", start, groupsErr)
	allOK = allOK && groupsErr == nil

	start = time.Now()
	modelsErr := groupsErr
	if modelsErr != nil {
		groups, modelsErr = ins.fetchGroups(ctx)
	}
	if modelsErr == nil {
		modelsErr = ins.gatherModels(ctx, slist, groups)
	}
	ins.emitResourceStatus(slist, "models", start, modelsErr)
	allOK = allOK && modelsErr == nil

	start = time.Now()
	usageErr := statusErr
	if usageErr == nil {
		usageErr = ins.gatherUsage(ctx, slist, quotaPerUnit)
	} else {
		ins.emitUsageFailureMetrics(slist, usageScanResult{})
	}
	ins.emitResourceStatus(slist, "usage", start, usageErr)
	allOK = allOK && usageErr == nil

	value := 0
	if allOK {
		value = 1
	}
	slist.PushSample(inputName, "up", value, ins.baseLabels())
}

func (ins *Instance) fetchQuotaPerUnit(ctx context.Context) (float64, error) {
	var statusResponse response[statusData]
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/api/status", nil, ins.authHeaders(), nil, &statusResponse); err != nil {
		return 0, err
	}
	if !statusResponse.Success {
		return 0, fmt.Errorf("status API failed: %s", ins.sanitizeErrorField(statusResponse.Message))
	}
	if statusResponse.Data.QuotaPerUnit <= 0 {
		return 0, fmt.Errorf("quota_per_unit must be positive")
	}
	return statusResponse.Data.QuotaPerUnit, nil
}

func (ins *Instance) gatherBalance(ctx context.Context, slist *types.SampleList, quotaPerUnit float64) error {
	var selfResponse response[selfData]
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/api/user/self", nil, ins.authHeaders(), nil, &selfResponse); err != nil {
		return err
	}
	if !selfResponse.Success {
		return fmt.Errorf("self API failed: %s", ins.sanitizeErrorField(selfResponse.Message))
	}
	slist.PushSample(inputName, "balance_usd", selfResponse.Data.Quota/quotaPerUnit, ins.baseLabels())
	return nil
}

func (ins *Instance) gatherGroups(ctx context.Context, slist *types.SampleList) ([]namedGroup, error) {
	groups, err := ins.fetchGroups(ctx)
	if err != nil {
		return nil, err
	}
	slist.PushSample(inputName, "group_count", len(groups), ins.baseLabels())
	for _, group := range groups {
		labels := ins.groupLabels(group.Name)
		slist.PushSample(inputName, "group_available", 1, labels)
		if group.Ratio != nil {
			slist.PushSample(inputName, "group_ratio", *group.Ratio, mergeLabels(labels, map[string]string{"kind": "effective"}))
		}
	}
	return groups, nil
}

func (ins *Instance) fetchGroups(ctx context.Context) ([]namedGroup, error) {
	var groupsResponse response[map[string]groupData]
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/api/user/self/groups", nil, ins.authHeaders(), nil, &groupsResponse); err != nil {
		return nil, err
	}
	if !groupsResponse.Success {
		return nil, fmt.Errorf("groups API failed: %s", ins.sanitizeErrorField(groupsResponse.Message))
	}

	names := make([]string, 0, len(groupsResponse.Data))
	for name := range groupsResponse.Data {
		names = append(names, name)
	}
	sort.Strings(names)
	groups := make([]namedGroup, 0, len(names))
	for _, name := range names {
		group := namedGroup{Name: name}
		var ratio float64
		if err := json.Unmarshal(groupsResponse.Data[name].Ratio, &ratio); err == nil {
			group.Ratio = &ratio
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func (ins *Instance) gatherModels(ctx context.Context, slist *types.SampleList, groups []namedGroup) error {
	var firstErr error
	for _, group := range groups {
		labels := ins.groupLabels(group.Name)
		var modelsResponse response[[]string]
		err := ins.client.DoJSON(ctx, http.MethodGet, "/api/user/models", url.Values{"group": []string{group.Name}}, ins.authHeaders(), nil, &modelsResponse)
		if err == nil && !modelsResponse.Success {
			err = fmt.Errorf("models API failed: %s", ins.sanitizeErrorField(modelsResponse.Message))
		}
		if err != nil {
			slist.PushSample(inputName, "model_scrape_success", 0, labels)
			if firstErr == nil {
				firstErr = fmt.Errorf("models for group %s: %w", group.Name, err)
			}
			continue
		}

		seen := make(map[string]struct{}, len(modelsResponse.Data))
		models := make([]string, 0, len(modelsResponse.Data))
		for _, model := range modelsResponse.Data {
			if model == "" {
				continue
			}
			if _, ok := seen[model]; ok {
				continue
			}
			seen[model] = struct{}{}
			models = append(models, model)
		}
		sort.Strings(models)
		slist.PushSample(inputName, "model_count", len(models), labels)
		for _, model := range models {
			slist.PushSample(inputName, "model_available", 1, mergeLabels(labels, map[string]string{"model": model}))
		}
		slist.PushSample(inputName, "model_scrape_success", 1, labels)
	}
	return firstErr
}

func (ins *Instance) emitResourceStatus(slist *types.SampleList, resource string, start time.Time, err error) {
	labels := mergeLabels(ins.baseLabels(), map[string]string{"resource": resource})
	value := 1
	if err != nil {
		value = 0
		log.Printf("E! newapi site=%s resource=%s: %v", ins.Site, resource, err)
	}
	slist.PushSample(inputName, "scrape_success", value, labels)
	slist.PushSample(inputName, "scrape_duration_seconds", time.Since(start).Seconds(), labels)
	if err == nil {
		slist.PushSample(inputName, "last_success_timestamp_seconds", time.Now().Unix(), labels)
	}
}

func (ins *Instance) baseLabels() map[string]string {
	return map[string]string{"site": ins.Site}
}

func (ins *Instance) groupLabels(group string) map[string]string {
	return map[string]string{"site": ins.Site, "group": group, "platform": "newapi"}
}

func mergeLabels(base map[string]string, extra map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}
