package sub2api_admin

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/inputs/internal/llmupstream"
	"flashcat.cloud/categraf/types"
)

const inputName = "sub2api_admin"

const (
	defaultConcurrency   = 10
	defaultTimeout       = 25 * time.Second
	defaultGatherTimeout = 4 * time.Minute
	defaultMaxProxies    = 100
	maxConcurrency       = 10
)

type Instance struct {
	config.InstanceConfig
	config.HTTPCommonConfig

	Site          string          `toml:"site"`
	BaseURL       string          `toml:"base_url"`
	AdminAPIKey   config.Secret   `toml:"admin_api_key"`
	Concurrency   int             `toml:"concurrency"`
	GatherTimeout config.Duration `toml:"gather_timeout"`
	MaxProxies    int             `toml:"max_proxies"`
	ResponseLimit int64           `toml:"response_limit_bytes"`

	client      *llmupstream.Client
	adminAPIKey string

	lifecycleMu         sync.Mutex
	dropped             bool
	nextGatherID        uint64
	activeGatherCancels map[uint64]context.CancelFunc
	activeGathersDone   chan struct{}
}

func (ins *Instance) Init() error {
	if strings.TrimSpace(ins.Site) == "" {
		return fmt.Errorf("sub2api_admin: site is required")
	}
	if strings.TrimSpace(ins.BaseURL) == "" {
		return fmt.Errorf("sub2api_admin: base_url is required")
	}
	key, err := secretString(&ins.AdminAPIKey)
	if err != nil {
		return fmt.Errorf("sub2api_admin: resolve admin_api_key: %w", err)
	}
	ins.adminAPIKey = strings.TrimSpace(key)
	if ins.adminAPIKey == "" {
		return fmt.Errorf("sub2api_admin: admin_api_key is required")
	}
	if ins.Concurrency == 0 {
		ins.Concurrency = defaultConcurrency
	}
	if ins.Concurrency < 1 || ins.Concurrency > maxConcurrency {
		return fmt.Errorf("sub2api_admin: concurrency must be between 1 and %d", maxConcurrency)
	}
	if ins.Timeout == 0 {
		ins.Timeout = config.Duration(defaultTimeout)
	}
	if ins.Timeout < config.Duration(time.Second) {
		return fmt.Errorf("sub2api_admin: timeout must be at least 1s")
	}
	if ins.GatherTimeout == 0 {
		ins.GatherTimeout = config.Duration(defaultGatherTimeout)
	}
	if ins.GatherTimeout < ins.Timeout {
		return fmt.Errorf("sub2api_admin: gather_timeout must be greater than or equal to timeout")
	}
	if ins.MaxProxies == 0 {
		ins.MaxProxies = defaultMaxProxies
	}
	if ins.MaxProxies < 1 {
		return fmt.Errorf("sub2api_admin: max_proxies must be greater than zero")
	}

	httpClient, err := llmupstream.NewHTTPClient(&ins.HTTPCommonConfig)
	if err != nil {
		return fmt.Errorf("sub2api_admin: create HTTP client: %w", err)
	}
	ins.client, err = llmupstream.NewClient(ins.BaseURL, httpClient, ins.ResponseLimit)
	if err != nil {
		return fmt.Errorf("sub2api_admin: create client: %w", err)
	}
	ins.lifecycleMu.Lock()
	ins.dropped = false
	ins.lifecycleMu.Unlock()
	return nil
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ctx, gatherID, ok := ins.beginGather()
	if !ok {
		return
	}
	defer ins.endGather(gatherID)

	start := time.Now()
	err := ins.gatherProxies(ctx, slist)
	up := 0
	if err == nil {
		up = 1
		slist.PushSample(inputName, "last_success_timestamp_seconds", time.Now().Unix(), ins.baseLabels())
	} else if ctx.Err() == nil {
		log.Printf("E! sub2api_admin site=%s gather failed: %s", ins.Site, ins.sanitize(err.Error()))
	}
	slist.PushSample(inputName, "up", up, ins.baseLabels())
	slist.PushSample(inputName, "scrape_duration_seconds", time.Since(start).Seconds(), ins.baseLabels())
}

func (ins *Instance) beginGather() (context.Context, uint64, bool) {
	ins.lifecycleMu.Lock()
	defer ins.lifecycleMu.Unlock()
	if ins.dropped {
		return nil, 0, false
	}
	if ins.activeGatherCancels == nil {
		ins.activeGatherCancels = make(map[uint64]context.CancelFunc)
	}
	if len(ins.activeGatherCancels) == 0 {
		ins.activeGathersDone = make(chan struct{})
	}
	ins.nextGatherID++
	ctx, cancel := context.WithCancel(context.Background())
	ins.activeGatherCancels[ins.nextGatherID] = cancel
	return ctx, ins.nextGatherID, true
}

func (ins *Instance) endGather(id uint64) {
	ins.lifecycleMu.Lock()
	defer ins.lifecycleMu.Unlock()
	if cancel, ok := ins.activeGatherCancels[id]; ok {
		cancel()
		delete(ins.activeGatherCancels, id)
	}
	if len(ins.activeGatherCancels) == 0 && ins.activeGathersDone != nil {
		close(ins.activeGathersDone)
		ins.activeGathersDone = nil
	}
}

func (ins *Instance) beginDrop() <-chan struct{} {
	ins.lifecycleMu.Lock()
	ins.dropped = true
	cancels := make([]context.CancelFunc, 0, len(ins.activeGatherCancels))
	for _, cancel := range ins.activeGatherCancels {
		cancels = append(cancels, cancel)
	}
	done := ins.activeGathersDone
	ins.lifecycleMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return done
}

func (ins *Instance) drop() {
	if done := ins.beginDrop(); done != nil {
		<-done
	}
}

func (ins *Instance) baseLabels() map[string]string {
	return map[string]string{"site": ins.Site}
}

func (ins *Instance) sanitize(value string) string {
	if ins.adminAPIKey != "" {
		value = strings.ReplaceAll(value, ins.adminAPIKey, "[redacted]")
	}
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if len(runes) > 256 {
		value = string(runes[:256]) + "..."
	}
	return value
}

func secretString(secret *config.Secret) (string, error) {
	if secret.Empty() {
		return "", nil
	}
	buffer, err := secret.Get()
	if err != nil {
		return "", err
	}
	defer buffer.Destroy()
	return buffer.String(), nil
}

type Sub2APIAdmin struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func (p *Sub2APIAdmin) Init() error {
	if p.Interval <= 0 {
		p.Interval = config.Duration(5 * time.Minute)
	}
	return nil
}

func init() {
	inputs.Add(inputName, func() inputs.Input { return &Sub2APIAdmin{} })
}

func (p *Sub2APIAdmin) Clone() inputs.Input { return &Sub2APIAdmin{} }
func (p *Sub2APIAdmin) Name() string        { return inputName }

func (p *Sub2APIAdmin) GetInstances() []inputs.Instance {
	result := make([]inputs.Instance, len(p.Instances))
	for i := range p.Instances {
		result[i] = p.Instances[i]
	}
	return result
}

func (p *Sub2APIAdmin) Drop() {
	pending := make([]<-chan struct{}, len(p.Instances))
	for i, instance := range p.Instances {
		if instance != nil {
			pending[i] = instance.beginDrop()
		}
	}
	for _, done := range pending {
		if done != nil {
			<-done
		}
	}
}

var _ inputs.Input = (*Sub2APIAdmin)(nil)
var _ inputs.Dropper = (*Sub2APIAdmin)(nil)
var _ inputs.Instance = (*Instance)(nil)
