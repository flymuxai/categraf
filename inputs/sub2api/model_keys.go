package sub2api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const modelKeyPageSize = 50

type apiKeyPage struct {
	Items    []apiKeyData `json:"items"`
	Page     int          `json:"page"`
	PageSize int          `json:"page_size"`
	Pages    int          `json:"pages"`
}

type apiKeyData struct {
	Key     string     `json:"key"`
	GroupID *int64     `json:"group_id"`
	Status  string     `json:"status"`
	Group   *groupData `json:"group"`
}

type modelTarget struct {
	Group  groupData
	APIKey string
	Err    error
}

func (ins *Instance) discoverModelTargets(ctx context.Context, available []groupData) ([]modelTarget, error) {
	wanted := make(map[string]int, len(ins.modelAPIKeys))
	for i, configured := range ins.modelAPIKeys {
		wanted[configured.APIKey] = i
	}
	found := make([]*apiKeyData, len(ins.modelAPIKeys))
	remaining := len(ins.modelAPIKeys)

	for page := 1; remaining > 0; page++ {
		query := url.Values{
			"page":      {strconv.Itoa(page)},
			"page_size": {strconv.Itoa(modelKeyPageSize)},
		}
		var response envelope[apiKeyPage]
		if err := ins.doUserJSON(ctx, http.MethodGet, "/api/v1/keys", query, nil, &response); err != nil {
			return nil, fmt.Errorf("list model API keys: %w", err)
		}
		if response.Code != 0 {
			return nil, fmt.Errorf("model API key list returned code %d: %s", response.Code, ins.sanitizeErrorField(response.Message))
		}

		for i := range response.Data.Items {
			item := &response.Data.Items[i]
			index, ok := wanted[item.Key]
			if !ok || found[index] != nil {
				continue
			}
			copy := *item
			found[index] = &copy
			remaining--
		}
		if remaining == 0 {
			break
		}
		if len(response.Data.Items) == 0 || len(response.Data.Items) < modelKeyPageSize || (response.Data.Pages > 0 && page >= response.Data.Pages) {
			break
		}
	}

	availableByID := make(map[int64]groupData, len(available))
	for _, group := range available {
		availableByID[group.ID] = group
	}
	targets := make([]modelTarget, 0, len(ins.modelAPIKeys))
	seenGroups := make(map[int64]struct{}, len(ins.modelAPIKeys))
	var firstErr error
	for index, item := range found {
		if item == nil {
			firstErr = firstModelError(firstErr, fmt.Errorf("model_api_keys[%d] was not found in the user key list", index))
			continue
		}
		if item.GroupID == nil || *item.GroupID <= 0 {
			log.Printf("W! sub2api site=%s model_api_keys[%d] is not assigned to a group; skipping model collection", ins.Site, index)
			continue
		}
		groupID := *item.GroupID
		if _, duplicate := seenGroups[groupID]; duplicate {
			firstErr = firstModelError(firstErr, fmt.Errorf("multiple model API keys resolve to group id %d", groupID))
			continue
		}
		seenGroups[groupID] = struct{}{}

		group, availableNow := availableByID[groupID]
		if !availableNow && item.Group != nil {
			group = *item.Group
		}
		if strings.TrimSpace(group.Name) == "" || strings.TrimSpace(group.Platform) == "" {
			firstErr = firstModelError(firstErr, fmt.Errorf("model_api_keys[%d] group id %d has no group metadata", index, groupID))
			continue
		}
		target := modelTarget{Group: group, APIKey: ins.modelAPIKeys[index].APIKey}
		if item.Status != "active" {
			target.Err = fmt.Errorf("configured model API key for group id %d has status %q", groupID, item.Status)
		} else if !availableNow {
			target.Err = fmt.Errorf("configured model API key group id %d is not currently available", groupID)
		}
		firstErr = firstModelError(firstErr, target.Err)
		targets = append(targets, target)
	}
	return targets, firstErr
}

func firstModelError(current, candidate error) error {
	if current != nil {
		return current
	}
	return candidate
}
