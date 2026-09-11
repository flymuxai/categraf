package sub2api

import (
	"log"
	"time"

	"flashcat.cloud/categraf/types"
)

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx, gatherID, ok := ins.beginGather()
	if !ok {
		return
	}
	defer ins.endGather(gatherID)

	allOK := true

	start := time.Now()
	usage, err := ins.fetchUsage(ctx)
	ins.emitResourceStatus(slist, "usage", start, err)
	if err == nil {
		ins.emitUsage(slist, usage)
	} else {
		allOK = false
	}

	if ins.CollectBilling {
		start = time.Now()
		billing, err := ins.fetchBilling(ctx)
		ins.emitResourceStatus(slist, "billing", start, err)
		if err == nil {
			emitBilling(slist, ins.baseLabels(), billing)
		} else {
			allOK = false
		}
	}

	if ins.CollectModels {
		start = time.Now()
		models, err := ins.fetchModels(ctx)
		ins.emitResourceStatus(slist, "models", start, err)
		if err == nil {
			emitModels(slist, ins.baseLabels(), models)
		} else {
			allOK = false
		}
	}

	value := 0
	if allOK {
		value = 1
	}
	slist.PushSample(inputName, "up", value, ins.baseLabels())
}

func (ins *Instance) emitResourceStatus(slist *types.SampleList, resource string, start time.Time, err error) {
	labels := mergeLabels(ins.baseLabels(), map[string]string{"resource": resource})
	value := 1
	if err != nil {
		value = 0
		log.Printf("E! sub2api key=%s resource=%s: %v", ins.Name, resource, err)
	}
	slist.PushSample(inputName, "scrape_success", value, labels)
	slist.PushSample(inputName, "scrape_duration_seconds", time.Since(start).Seconds(), labels)
	if err == nil {
		slist.PushSample(inputName, "last_success_timestamp_seconds", time.Now().Unix(), labels)
	}
}

func (ins *Instance) baseLabels() map[string]string {
	return map[string]string{"key": ins.Name}
}

func mergeLabels(base, extra map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

