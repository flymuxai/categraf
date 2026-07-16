package sub2api

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
)

const inputName = "sub2api"

type GroupAPIKey struct {
	Group    string        `toml:"group"`
	Platform string        `toml:"platform"`
	APIKey   config.Secret `toml:"api_key"`
}

type ModelAPIKey struct {
	APIKey config.Secret `toml:"api_key"`
}

type resolvedModelAPIKey struct {
	APIKey string
}

type Instance struct {
	config.InstanceConfig
	config.HTTPCommonConfig

	Site            string        `toml:"site"`
	BaseURL         string        `toml:"base_url"`
	AccessToken     config.Secret `toml:"access_token"`
	RefreshToken    config.Secret `toml:"refresh_token"`
	StateFile       string        `toml:"state_file"`
	UsageTimezone   string        `toml:"usage_timezone"`
	RequestRPSLimit float64       `toml:"request_rps_limit"`
	RequestBurst    int           `toml:"request_burst"`
	ModelAPIKeys    []ModelAPIKey `toml:"model_api_keys"`
	ResponseLimit   int64         `toml:"response_limit_bytes"`

	LegacyUsagePageSize   *int             `toml:"usage_page_size"`
	LegacyUsageMaxPages   *int             `toml:"usage_max_pages"`
	LegacyUsageMaxCount   *int             `toml:"usage_max_count"`
	LegacyRequestInterval *config.Duration `toml:"request_interval"`
	LegacyGroupAPIKeys    []GroupAPIKey    `toml:"group_api_keys"`

	client              *llmupstream.Client
	limiterRegistration *llmupstream.RateLimitRegistration
	accessToken         string
	refreshToken        string
	modelAPIKeys        []resolvedModelAPIKey
	tokenStateDirty     bool
	authMu              sync.Mutex
	redactionMu         sync.RWMutex
	redactionSecrets    []string
	lifecycleMu         sync.Mutex
	dropped             bool
	nextGatherID        uint64
	activeGatherCancels map[uint64]context.CancelFunc
	activeGathersDone   chan struct{}
	state               persistedState
	usageLocation       *time.Location
	startedAt           time.Time
}

func (ins *Instance) Init() error {
	if strings.TrimSpace(ins.Site) == "" {
		return fmt.Errorf("sub2api: site is required")
	}
	if strings.TrimSpace(ins.BaseURL) == "" {
		return fmt.Errorf("sub2api: base_url is required")
	}
	if strings.TrimSpace(ins.StateFile) == "" {
		return fmt.Errorf("sub2api: state_file is required")
	}
	if strings.TrimSpace(ins.UsageTimezone) == "" {
		return fmt.Errorf("sub2api: usage_timezone is required")
	}
	location, err := time.LoadLocation(ins.UsageTimezone)
	if err != nil {
		return fmt.Errorf("sub2api: invalid usage_timezone %q: %w", ins.UsageTimezone, err)
	}
	ins.usageLocation = location
	ins.startedAt = time.Now()
	if err := ins.rejectLegacyConfiguration(); err != nil {
		return err
	}
	if ins.RequestRPSLimit == 0 {
		ins.RequestRPSLimit = 1
	}
	if ins.RequestRPSLimit < 0 {
		return fmt.Errorf("sub2api: request_rps_limit must be greater than zero")
	}
	if ins.RequestBurst == 0 {
		ins.RequestBurst = 1
	}
	if ins.RequestBurst < 0 {
		return fmt.Errorf("sub2api: request_burst must be greater than zero")
	}
	if ins.RequestRPSLimit > 2 || ins.RequestBurst > 5 {
		log.Printf("W! sub2api site=%s request limiter %.3g rps burst=%d may trigger upstream rate limits", ins.Site, ins.RequestRPSLimit, ins.RequestBurst)
	}
	accessToken, err := secretString(&ins.AccessToken)
	if err != nil {
		return fmt.Errorf("sub2api: resolve access_token: %w", err)
	}
	refreshToken, err := secretString(&ins.RefreshToken)
	if err != nil {
		return fmt.Errorf("sub2api: resolve refresh_token: %w", err)
	}
	ins.accessToken = accessToken
	ins.refreshToken = refreshToken
	if err := ins.loadState(); err != nil {
		return fmt.Errorf("sub2api: %w", err)
	}
	if ins.accessToken == "" {
		return fmt.Errorf("sub2api: access_token is required")
	}

	if len(ins.ModelAPIKeys) == 0 {
		return fmt.Errorf("sub2api: model_api_keys is required")
	}
	seen := make(map[string]struct{}, len(ins.ModelAPIKeys))
	for i := range ins.ModelAPIKeys {
		entry := &ins.ModelAPIKeys[i]
		key, err := secretString(&entry.APIKey)
		if err != nil {
			return fmt.Errorf("sub2api: resolve model_api_keys[%d]: %w", i, err)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("sub2api: api_key is required for model_api_keys[%d]", i)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("sub2api: duplicate model API key at index %d", i)
		}
		seen[key] = struct{}{}
		ins.modelAPIKeys = append(ins.modelAPIKeys, resolvedModelAPIKey{APIKey: key})
	}
	ins.updateRedactionSecrets()

	httpClient, err := llmupstream.NewHTTPClient(&ins.HTTPCommonConfig)
	if err != nil {
		return fmt.Errorf("sub2api: create HTTP client: %w", err)
	}
	ins.releaseRequestLimiter()
	registration, conflict, err := llmupstream.RegisterRateLimitedClient(httpClient, ins.BaseURL, llmupstream.RateLimitSettings{
		RPS: ins.RequestRPSLimit, Burst: ins.RequestBurst,
	})
	if err != nil {
		return err
	}
	if conflict != nil {
		log.Printf("W! sub2api origin=%s limiter conflict requested=%.3g/%d existing=%.3g/%d effective=%.3g/%d",
			conflict.Origin, conflict.Requested.RPS, conflict.Requested.Burst,
			conflict.Existing.RPS, conflict.Existing.Burst, conflict.Effective.RPS, conflict.Effective.Burst)
	}
	ins.client, err = llmupstream.NewClient(ins.BaseURL, httpClient, ins.ResponseLimit)
	if err != nil {
		registration.Release()
		return err
	}
	ins.limiterRegistration = registration
	ins.lifecycleMu.Lock()
	ins.dropped = false
	ins.lifecycleMu.Unlock()
	return nil
}

func (ins *Instance) releaseRequestLimiter() {
	if ins.limiterRegistration != nil {
		ins.limiterRegistration.Release()
		ins.limiterRegistration = nil
	}
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

func (ins *Instance) drop() {
	done := ins.beginDrop()
	ins.finishDrop(done)
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

func (ins *Instance) finishDrop(done <-chan struct{}) {
	if done != nil {
		<-done
	}
	ins.releaseRequestLimiter()
}

func (ins *Instance) rejectLegacyConfiguration() error {
	switch {
	case ins.LegacyUsagePageSize != nil:
		return fmt.Errorf("sub2api: usage_page_size was removed; page size is fixed at 20")
	case ins.LegacyUsageMaxPages != nil:
		return fmt.Errorf("sub2api: usage_max_pages was removed; usage scans until the cursor or data end")
	case ins.LegacyUsageMaxCount != nil:
		return fmt.Errorf("sub2api: usage_max_count was removed; usage scans until the cursor or data end")
	case ins.LegacyRequestInterval != nil:
		return fmt.Errorf("sub2api: request_interval was replaced by request_rps_limit and request_burst")
	case len(ins.LegacyGroupAPIKeys) > 0:
		return fmt.Errorf("sub2api: group_api_keys was replaced by key-only model_api_keys")
	default:
		return nil
	}
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

type Sub2API struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func (p *Sub2API) Init() error {
	if p.Interval <= 0 {
		p.Interval = config.Duration(5 * time.Minute)
	}
	if time.Duration(p.Interval) < time.Minute {
		log.Printf("W! sub2api interval=%s may trigger upstream rate limits", time.Duration(p.Interval))
	}
	return nil
}

func init() {
	inputs.Add(inputName, func() inputs.Input { return &Sub2API{} })
}

func (p *Sub2API) Clone() inputs.Input { return &Sub2API{} }
func (p *Sub2API) Name() string        { return inputName }

func (p *Sub2API) GetInstances() []inputs.Instance {
	result := make([]inputs.Instance, len(p.Instances))
	for i := range p.Instances {
		result[i] = p.Instances[i]
	}
	return result
}

func (p *Sub2API) Drop() {
	type pendingDrop struct {
		instance *Instance
		done     <-chan struct{}
	}
	pending := make([]pendingDrop, 0, len(p.Instances))
	for _, instance := range p.Instances {
		if instance != nil {
			pending = append(pending, pendingDrop{instance: instance, done: instance.beginDrop()})
		}
	}
	for _, item := range pending {
		item.instance.finishDrop(item.done)
	}
}

var _ inputs.Input = (*Sub2API)(nil)
var _ inputs.Dropper = (*Sub2API)(nil)
var _ inputs.Instance = (*Instance)(nil)
