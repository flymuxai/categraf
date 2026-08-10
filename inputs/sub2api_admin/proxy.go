package sub2api_admin

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/types"
)

type envelope[T any] struct {
	Code int `json:"code"`
	Data T   `json:"data"`
}

type proxyData struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Protocol     string `json:"protocol"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Status       string `json:"status"`
	AccountCount int64  `json:"account_count"`
}

type proxyTestData struct {
	Success   bool  `json:"success"`
	LatencyMs int64 `json:"latency_ms"`
}

type proxyProbeResult struct {
	proxy     proxyData
	data      proxyTestData
	completed bool
	err       error
}

func (ins *Instance) gatherProxies(parent context.Context, slist *types.SampleList) error {
	if ins.client == nil {
		return fmt.Errorf("admin client is not initialized")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(ins.GatherTimeout))
	defer cancel()

	proxies, err := ins.fetchActiveProxies(ctx)
	if err != nil {
		return err
	}
	slist.PushSample(inputName, "proxy_discovered_count", len(proxies), ins.baseLabels())
	if len(proxies) > ins.MaxProxies {
		return fmt.Errorf("discovered %d active proxies, exceeding max_proxies=%d", len(proxies), ins.MaxProxies)
	}
	for _, proxy := range proxies {
		slist.PushSample(inputName, "proxy_account_count", proxy.AccountCount, ins.proxyLabels(proxy))
	}
	if len(proxies) == 0 {
		return nil
	}

	workerCount := ins.Concurrency
	if workerCount > len(proxies) {
		workerCount = len(proxies)
	}
	jobs := make(chan proxyData, len(proxies))
	results := make(chan proxyProbeResult, len(proxies))
	for _, proxy := range proxies {
		jobs <- proxy
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case proxy, ok := <-jobs:
					if !ok {
						return
					}
					result := ins.probeProxy(ctx, proxy)
					if ctx.Err() != nil && result.err != nil {
						return
					}
					results <- result
				}
			}
		}()
	}
	workers.Wait()
	close(results)

	resultCount := 0
	failureCount := 0
	var firstErr error
	for result := range results {
		resultCount++
		ins.emitProxyResult(slist, result)
		if result.err != nil {
			failureCount++
			if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if resultCount != len(proxies) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("proxy gather incomplete: completed %d of %d: %w", resultCount, len(proxies), err)
		}
		return fmt.Errorf("proxy gather incomplete: completed %d of %d", resultCount, len(proxies))
	}
	if failureCount > 0 {
		return fmt.Errorf("%d proxy requests failed; first error: %w", failureCount, firstErr)
	}
	return nil
}

func (ins *Instance) fetchActiveProxies(ctx context.Context) ([]proxyData, error) {
	var response envelope[[]proxyData]
	query := url.Values{"with_count": {"true"}}
	if err := ins.client.DoJSON(ctx, http.MethodGet, "/api/v1/admin/proxies/all", query, ins.adminHeaders(), nil, &response); err != nil {
		return nil, fmt.Errorf("list active proxies: %w", err)
	}
	if response.Code != 0 {
		return nil, fmt.Errorf("proxy list API returned code %d", response.Code)
	}
	active := make([]proxyData, 0, len(response.Data))
	for _, proxy := range response.Data {
		if strings.EqualFold(strings.TrimSpace(proxy.Status), "active") {
			active = append(active, proxy)
		}
	}
	return active, nil
}

func (ins *Instance) probeProxy(ctx context.Context, proxy proxyData) proxyProbeResult {
	var response envelope[proxyTestData]
	endpoint := fmt.Sprintf("/api/v1/admin/proxies/%d/test", proxy.ID)
	if err := ins.client.DoJSON(ctx, http.MethodPost, endpoint, nil, ins.adminHeaders(), nil, &response); err != nil {
		return proxyProbeResult{proxy: proxy, err: fmt.Errorf("probe proxy %d: %w", proxy.ID, err)}
	}
	if response.Code != 0 {
		return proxyProbeResult{proxy: proxy, err: fmt.Errorf("proxy %d test API returned code %d", proxy.ID, response.Code)}
	}
	return proxyProbeResult{proxy: proxy, data: response.Data, completed: true}
}

func (ins *Instance) emitProxyResult(slist *types.SampleList, result proxyProbeResult) {
	labels := ins.proxyLabels(result.proxy)
	up, completed := 0, 0
	if result.completed {
		completed = 1
		if result.data.Success {
			up = 1
			slist.PushSample(inputName, "proxy_probe_latency_seconds", float64(result.data.LatencyMs)/1000, labels)
		}
	}
	slist.PushSample(inputName, "proxy_up", up, labels)
	slist.PushSample(inputName, "proxy_probe_completed", completed, labels)
	slist.PushSample(inputName, "proxy_last_probe_timestamp_seconds", time.Now().Unix(), labels)
}

func (ins *Instance) adminHeaders() map[string]string {
	return map[string]string{"x-api-key": ins.adminAPIKey}
}

func (ins *Instance) proxyLabels(proxy proxyData) map[string]string {
	return map[string]string{
		"site": ins.Site, "proxy_id": strconv.FormatInt(proxy.ID, 10), "proxy_name": proxy.Name,
		"protocol": proxy.Protocol, "proxy_host": proxy.Host, "proxy_port": strconv.Itoa(proxy.Port),
	}
}
