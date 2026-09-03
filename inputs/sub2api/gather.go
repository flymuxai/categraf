package sub2api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"flashcat.cloud/categraf/types"
)

type envelope[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

type profileData struct {
	Balance       float64 `json:"balance"`
	FrozenBalance float64 `json:"frozen_balance"`
}

type groupData struct {
	ID                 int64   `json:"id"`
	Name               string  `json:"name"`
	Platform           string  `json:"platform"`
	RateMultiplier     float64 `json:"rate_multiplier"`
	PeakRateEnabled    bool    `json:"peak_rate_enabled"`
	PeakRateMultiplier float64 `json:"peak_rate_multiplier"`
}

type modelList struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx, gatherID, ok := ins.beginGather()
	if !ok {
		return
	}
	defer ins.endGather(gatherID)

	if err := ins.ensureAccessToken(ctx); err != nil {
		start := time.Now()
		for _, resource := range []string{"balance", "groups", "models", "usage"} {
			ins.emitResourceStatus(slist, resource, start, err)
		}
		slist.PushSample(inputName, "up", 0, ins.baseLabels())
		return
	}

	allOK := true

	start := time.Now()
	err := ins.gatherBalance(ctx, slist)
	ins.emitResourceStatus(slist, "balance", start, err)
	allOK = allOK && err == nil

	start = time.Now()
	groups, groupsErr := ins.gatherGroups(ctx, slist)
	ins.emitResourceStatus(slist, "groups", start, groupsErr)
	allOK = allOK && groupsErr == nil

	start = time.Now()
	if groupsErr != nil {
		groups, err = ins.fetchAvailableGroups(ctx)
	} else {
		err = nil
	}
	if err == nil {
		err = ins.gatherModels(ctx, slist, groups)
	}
	ins.emitResourceStatus(slist, "models", start, err)
	allOK = allOK && err == nil

	start = time.Now()
	err = ins.gatherUsage(ctx, slist)
	ins.emitResourceStatus(slist, "usage", start, err)
	allOK = allOK && err == nil

	value := 0
	if allOK {
		value = 1
	}
	slist.PushSample(inputName, "up", value, ins.baseLabels())
}

func (ins *Instance) gatherBalance(ctx context.Context, slist *types.SampleList) error {
	var response envelope[profileData]
	if err := ins.doUserJSON(ctx, http.MethodGet, "/api/v1/user/profile", nil, nil, &response); err != nil {
		return err
	}
	if response.Code != 0 {
		return fmt.Errorf("profile API returned code %d: %s", response.Code, ins.sanitizeErrorField(response.Message))
	}
	labels := ins.baseLabels()
	slist.PushSample(inputName, "balance_usd", response.Data.Balance, labels)
	slist.PushSample(inputName, "frozen_balance_usd", response.Data.FrozenBalance, labels)
	return nil
}

func (ins *Instance) gatherGroups(ctx context.Context, slist *types.SampleList) ([]groupData, error) {
	groups, err := ins.fetchAvailableGroups(ctx)
	if err != nil {
		return nil, err
	}
	var ratesResponse envelope[map[string]float64]
	if err := ins.doUserJSON(ctx, http.MethodGet, "/api/v1/groups/rates", nil, nil, &ratesResponse); err != nil {
		return nil, err
	}
	if ratesResponse.Code != 0 {
		return nil, fmt.Errorf("group rates API returned code %d: %s", ratesResponse.Code, ins.sanitizeErrorField(ratesResponse.Message))
	}

	slist.PushSample(inputName, "group_count", len(groups), ins.baseLabels())
	for _, group := range groups {
		labels := ins.groupLabels(group.Name, group.Platform)
		slist.PushSample(inputName, "group_available", 1, labels)
		slist.PushSample(inputName, "group_ratio", group.RateMultiplier, mergeLabels(labels, map[string]string{"kind": "default"}))
		effective := group.RateMultiplier
		if rate, ok := ratesResponse.Data[strconv.FormatInt(group.ID, 10)]; ok {
			effective = rate
		}
		slist.PushSample(inputName, "group_ratio", effective, mergeLabels(labels, map[string]string{"kind": "effective"}))
		if group.PeakRateEnabled {
			slist.PushSample(inputName, "group_ratio", group.PeakRateMultiplier, mergeLabels(labels, map[string]string{"kind": "peak"}))
		}
	}
	return groups, nil
}

func (ins *Instance) fetchAvailableGroups(ctx context.Context) ([]groupData, error) {
	var response envelope[[]groupData]
	if err := ins.doUserJSON(ctx, http.MethodGet, "/api/v1/groups/available", nil, nil, &response); err != nil {
		return nil, err
	}
	if response.Code != 0 {
		return nil, fmt.Errorf("groups API returned code %d: %s", response.Code, ins.sanitizeErrorField(response.Message))
	}
	return response.Data, nil
}

func (ins *Instance) gatherModels(ctx context.Context, slist *types.SampleList, groups []groupData) error {
	targets, firstErr := ins.discoverModelTargets(ctx, groups)
	for _, target := range targets {
		labels := ins.groupLabels(target.Group.Name, target.Group.Platform)
		if target.Err != nil {
			slist.PushSample(inputName, "model_scrape_success", 0, labels)
			continue
		}
		var response modelList
		err := ins.client.DoJSON(ctx, http.MethodGet, "/v1/models", nil, map[string]string{
			"Authorization": "Bearer " + target.APIKey,
		}, nil, &response)
		if err != nil {
			slist.PushSample(inputName, "model_scrape_success", 0, labels)
			if firstErr == nil {
				firstErr = fmt.Errorf("models for %s/%s: %w", target.Group.Platform, target.Group.Name, err)
			}
			continue
		}
		modelIDs := make([]string, 0, len(response.Data))
		seen := make(map[string]struct{}, len(response.Data))
		for _, model := range response.Data {
			if model.ID == "" {
				continue
			}
			if _, ok := seen[model.ID]; ok {
				continue
			}
			seen[model.ID] = struct{}{}
			modelIDs = append(modelIDs, model.ID)
		}
		sort.Strings(modelIDs)
		slist.PushSample(inputName, "model_count", len(modelIDs), labels)
		for _, modelID := range modelIDs {
			slist.PushSample(inputName, "model_available", 1, mergeLabels(labels, map[string]string{"model": modelID}))
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
		log.Printf("E! sub2api site=%s resource=%s: %v", ins.Site, resource, err)
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

func (ins *Instance) groupLabels(group, platform string) map[string]string {
	return map[string]string{"site": ins.Site, "group": group, "platform": platform}
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
