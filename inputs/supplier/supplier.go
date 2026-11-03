package supplier

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

const inputName = "supplier"

const (
	providerLynxton = "lynxton"
	providerEpay    = "epay"
)

const (
	defaultLynxtonQuotaPerUnit = 500000
	defaultEpayUnit            = "CNY"
)

// Instance is one supplier account. The provider selects which upstream API
// shape is queried; every provider reports the same supplier_balance metric.
type Instance struct {
	config.InstanceConfig
	config.HTTPCommonConfig

	Supplier string        `toml:"supplier"`
	Provider string        `toml:"provider"`
	BaseURL  string        `toml:"base_url"`
	APIKey   config.Secret `toml:"api_key"`

	// lynxton only.
	ExpectScope  string  `toml:"expect_scope"`
	QuotaPerUnit float64 `toml:"quota_per_unit"`

	// epay only. Unit also acts as a fallback for lynxton when the response
	// carries no display_unit.
	PID  flexString `toml:"pid"`
	Unit string     `toml:"unit"`

	RequestRPSLimit float64 `toml:"request_rps_limit"`
	RequestBurst    int     `toml:"request_burst"`
	ResponseLimit   int64   `toml:"response_limit_bytes"`

	client              *llmupstream.Client
	limiterRegistration *llmupstream.RateLimitRegistration
	apiKey              string
	lifecycleMu         sync.Mutex
	dropped             bool
	nextGatherID        uint64
	activeGatherCancels map[uint64]context.CancelFunc
	activeGathersDone   chan struct{}
}

// flexString accepts both `pid = 3407` and `pid = "3407"` in TOML.
type flexString string

func (s *flexString) UnmarshalTOML(value any) error {
	switch v := value.(type) {
	case string:
		*s = flexString(v)
	case int64:
		*s = flexString(strconv.FormatInt(v, 10))
	default:
		return fmt.Errorf("expected a string or integer, got %T", value)
	}
	return nil
}

func (ins *Instance) Init() error {
	ins.Supplier = strings.TrimSpace(ins.Supplier)
	ins.Provider = strings.ToLower(strings.TrimSpace(ins.Provider))
	ins.BaseURL = strings.TrimSpace(ins.BaseURL)
	ins.PID = flexString(strings.TrimSpace(string(ins.PID)))
	ins.Unit = strings.TrimSpace(ins.Unit)
	ins.ExpectScope = strings.TrimSpace(ins.ExpectScope)

	if ins.Supplier == "" {
		return fmt.Errorf("supplier: supplier is required")
	}
	if ins.BaseURL == "" {
		return fmt.Errorf("supplier %s: base_url is required", ins.Supplier)
	}
	if err := ins.validateProvider(); err != nil {
		return fmt.Errorf("supplier %s: %w", ins.Supplier, err)
	}

	if ins.RequestRPSLimit == 0 {
		ins.RequestRPSLimit = 1
	}
	if ins.RequestRPSLimit < 0 {
		return fmt.Errorf("supplier %s: request_rps_limit must be greater than zero", ins.Supplier)
	}
	if ins.RequestBurst == 0 {
		ins.RequestBurst = 1
	}
	if ins.RequestBurst < 0 {
		return fmt.Errorf("supplier %s: request_burst must be greater than zero", ins.Supplier)
	}

	apiKey, err := secretString(&ins.APIKey)
	if err != nil {
		return fmt.Errorf("supplier %s: resolve api_key: %w", ins.Supplier, err)
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return fmt.Errorf("supplier %s: api_key is required", ins.Supplier)
	}
	ins.apiKey = apiKey

	httpClient, err := llmupstream.NewHTTPClient(&ins.HTTPCommonConfig)
	if err != nil {
		return fmt.Errorf("supplier %s: create HTTP client: %w", ins.Supplier, err)
	}
	ins.releaseRequestLimiter()
	registration, conflict, err := llmupstream.RegisterRateLimitedClient(httpClient, ins.BaseURL, llmupstream.RateLimitSettings{
		RPS: ins.RequestRPSLimit, Burst: ins.RequestBurst,
	})
	if err != nil {
		return fmt.Errorf("supplier %s: %w", ins.Supplier, err)
	}
	if conflict != nil {
		log.Printf("W! supplier origin=%s limiter conflict requested=%.3g/%d existing=%.3g/%d effective=%.3g/%d",
			conflict.Origin, conflict.Requested.RPS, conflict.Requested.Burst,
			conflict.Existing.RPS, conflict.Existing.Burst, conflict.Effective.RPS, conflict.Effective.Burst)
	}
	ins.client, err = llmupstream.NewClient(ins.BaseURL, httpClient, ins.ResponseLimit)
	if err != nil {
		registration.Release()
		return fmt.Errorf("supplier %s: %w", ins.Supplier, err)
	}
	ins.limiterRegistration = registration
	ins.lifecycleMu.Lock()
	ins.dropped = false
	ins.lifecycleMu.Unlock()
	return nil
}

// validateProvider checks provider-specific settings and rejects settings that
// belong to the other provider, so a copy-pasted block fails loudly.
func (ins *Instance) validateProvider() error {
	baseURL, err := url.Parse(ins.BaseURL)
	if err != nil {
		return fmt.Errorf("parse base_url: %w", err)
	}
	if baseURL.RawQuery != "" {
		return fmt.Errorf("base_url must not contain a query string; put credentials in api_key/pid")
	}
	basePath := strings.TrimRight(baseURL.Path, "/")

	switch ins.Provider {
	case providerLynxton:
		if strings.HasSuffix(basePath, "/v1") || strings.HasSuffix(basePath, "/account/balance") {
			return fmt.Errorf("base_url should be the site root; /v1/account/balance is added automatically")
		}
		if ins.PID != "" {
			return fmt.Errorf("pid only applies to provider %q", providerEpay)
		}
		switch ins.ExpectScope {
		case "", "personal", "org_pool":
		default:
			return fmt.Errorf("expect_scope must be \"personal\" or \"org_pool\", got %q", ins.ExpectScope)
		}
		if ins.QuotaPerUnit == 0 {
			ins.QuotaPerUnit = defaultLynxtonQuotaPerUnit
		}
		if ins.QuotaPerUnit < 0 || math.IsNaN(ins.QuotaPerUnit) || math.IsInf(ins.QuotaPerUnit, 0) {
			return fmt.Errorf("quota_per_unit must be a positive number")
		}
	case providerEpay:
		if strings.HasSuffix(basePath, "/api.php") {
			return fmt.Errorf("base_url should be the site root; /api.php is added automatically")
		}
		if ins.PID == "" {
			return fmt.Errorf("pid is required for provider %q", providerEpay)
		}
		if ins.ExpectScope != "" {
			return fmt.Errorf("expect_scope only applies to provider %q", providerLynxton)
		}
		if ins.QuotaPerUnit != 0 {
			return fmt.Errorf("quota_per_unit only applies to provider %q", providerLynxton)
		}
		if ins.Unit == "" {
			ins.Unit = defaultEpayUnit
		}
	case "":
		return fmt.Errorf("provider is required (%q or %q)", providerLynxton, providerEpay)
	default:
		return fmt.Errorf("unknown provider %q (want %q or %q)", ins.Provider, providerLynxton, providerEpay)
	}
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
	ins.finishDrop(ins.beginDrop())
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

type Supplier struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

func (p *Supplier) Init() error {
	if p.Interval <= 0 {
		p.Interval = config.Duration(5 * time.Minute)
	}
	if time.Duration(p.Interval) < time.Minute {
		log.Printf("W! supplier interval=%s may trigger upstream rate limits", time.Duration(p.Interval))
	}
	return nil
}

func init() {
	inputs.Add(inputName, func() inputs.Input { return &Supplier{} })
}

func (p *Supplier) Clone() inputs.Input { return &Supplier{} }
func (p *Supplier) Name() string        { return inputName }

func (p *Supplier) GetInstances() []inputs.Instance {
	result := make([]inputs.Instance, len(p.Instances))
	for i := range p.Instances {
		result[i] = p.Instances[i]
	}
	return result
}

func (p *Supplier) Drop() {
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

var _ inputs.Input = (*Supplier)(nil)
var _ inputs.Dropper = (*Supplier)(nil)
var _ inputs.Instance = (*Instance)(nil)
