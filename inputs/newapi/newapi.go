package newapi

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

const inputName = "newapi"

type Instance struct {
	config.InstanceConfig
	config.HTTPCommonConfig

	Site            string        `toml:"site"`
	BaseURL         string        `toml:"base_url"`
	UserID          int           `toml:"user_id"`
	AccessToken     config.Secret `toml:"access_token"`
	StateFile       string        `toml:"state_file"`
	RequestRPSLimit float64       `toml:"request_rps_limit"`
	RequestBurst    int           `toml:"request_burst"`
	ResponseLimit   int64         `toml:"response_limit_bytes"`

	LegacyUsagePageSize *int `toml:"usage_page_size"`
	LegacyUsageMaxPages *int `toml:"usage_max_pages"`

	client              *llmupstream.Client
	limiterRegistration *llmupstream.RateLimitRegistration
	accessToken         string
	stateMu             sync.Mutex
	state               persistedState
	startedAt           time.Time
	now                 func() time.Time
	lifecycleMu         sync.Mutex
	dropped             bool
	nextGatherID        uint64
	activeGatherCancels map[uint64]context.CancelFunc
	activeGathersDone   chan struct{}
}

func (ins *Instance) Init() error {
	if strings.TrimSpace(ins.Site) == "" {
		return fmt.Errorf("newapi: site is required")
	}
	if strings.TrimSpace(ins.BaseURL) == "" {
		return fmt.Errorf("newapi: base_url is required")
	}
	if ins.UserID <= 0 {
		return fmt.Errorf("newapi: user_id must be positive")
	}
	if strings.TrimSpace(ins.StateFile) == "" {
		return fmt.Errorf("newapi: state_file is required")
	}
	if err := ins.rejectLegacyConfiguration(); err != nil {
		return err
	}
	if ins.RequestRPSLimit == 0 {
		ins.RequestRPSLimit = 1
	}
	if ins.RequestRPSLimit < 0 {
		return fmt.Errorf("newapi: request_rps_limit must be greater than zero")
	}
	if ins.RequestBurst == 0 {
		ins.RequestBurst = 1
	}
	if ins.RequestBurst < 0 {
		return fmt.Errorf("newapi: request_burst must be greater than zero")
	}
	if ins.RequestRPSLimit > 2 || ins.RequestBurst > 5 {
		log.Printf("W! newapi site=%s request limiter %.3g rps burst=%d may trigger upstream rate limits", ins.Site, ins.RequestRPSLimit, ins.RequestBurst)
	}
	accessToken, err := secretString(&ins.AccessToken)
	if err != nil {
		return fmt.Errorf("newapi: resolve access_token: %w", err)
	}
	if accessToken == "" {
		return fmt.Errorf("newapi: access_token is required")
	}
	ins.accessToken = accessToken
	ins.startedAt = time.Now()
	ins.now = time.Now
	if err := ins.loadState(); err != nil {
		return fmt.Errorf("newapi: %w", err)
	}

	httpClient, err := llmupstream.NewHTTPClient(&ins.HTTPCommonConfig)
	if err != nil {
		return fmt.Errorf("newapi: create HTTP client: %w", err)
	}
	ins.releaseRequestLimiter()
	registration, conflict, err := llmupstream.RegisterRateLimitedClient(httpClient, ins.BaseURL, llmupstream.RateLimitSettings{
		RPS: ins.RequestRPSLimit, Burst: ins.RequestBurst,
	})
	if err != nil {
		return err
	}
	if conflict != nil {
		log.Printf("W! newapi origin=%s limiter conflict requested=%.3g/%d existing=%.3g/%d effective=%.3g/%d",
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

func (ins *Instance) rejectLegacyConfiguration() error {
	switch {
	case ins.LegacyUsagePageSize != nil:
		return fmt.Errorf("newapi: usage_page_size was removed; page size is fixed at 100")
	case ins.LegacyUsageMaxPages != nil:
		return fmt.Errorf("newapi: usage_max_pages was removed; usage scans until the complete result")
	default:
		return nil
	}
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

func secretString(secret *config.Secret) (string, error) {
	if secret.Empty() {
		return "", nil
	}
	if err := secret.LinkEnv(); err != nil {
		return "", err
	}
	buffer, err := secret.Get()
	if err != nil {
		return "", err
	}
	defer buffer.Destroy()
	return buffer.String(), nil
}

func (ins *Instance) authHeaders() map[string]string {
	return map[string]string{
		"Authorization": ins.accessToken,
		"New-Api-User":  strconv.Itoa(ins.UserID),
	}
}

type NewAPI struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func (p *NewAPI) Init() error {
	if p.Interval <= 0 {
		p.Interval = config.Duration(5 * time.Minute)
	}
	if time.Duration(p.Interval) < time.Minute {
		log.Printf("W! newapi interval=%s may trigger upstream rate limits", time.Duration(p.Interval))
	}
	return nil
}

func init() {
	inputs.Add(inputName, func() inputs.Input { return &NewAPI{} })
}

func (p *NewAPI) Clone() inputs.Input { return &NewAPI{} }
func (p *NewAPI) Name() string        { return inputName }

func (p *NewAPI) GetInstances() []inputs.Instance {
	result := make([]inputs.Instance, len(p.Instances))
	for i := range p.Instances {
		result[i] = p.Instances[i]
	}
	return result
}

func (p *NewAPI) Drop() {
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

var _ inputs.Input = (*NewAPI)(nil)
var _ inputs.Dropper = (*NewAPI)(nil)
var _ inputs.Instance = (*Instance)(nil)
