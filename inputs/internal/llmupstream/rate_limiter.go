package llmupstream

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const maxRequestJitter = 250 * time.Millisecond

var defaultRateLimiterRegistry = newRateLimiterRegistry()

type RateLimitSettings struct {
	RPS   float64
	Burst int
}

type RateLimitConflict struct {
	Origin    string
	Existing  RateLimitSettings
	Requested RateLimitSettings
	Effective RateLimitSettings
}

type RateLimitRegistration struct {
	registry *rateLimiterRegistry
	origin   string
	id       uint64
	once     sync.Once
}

type limiterRuntime struct {
	now    func() time.Time
	wait   func(context.Context, time.Duration) error
	jitter func() time.Duration
}

type rateLimiterRegistry struct {
	mu             sync.Mutex
	entries        map[string]*rateLimiterEntry
	nextRegisterID uint64
	runtime        limiterRuntime
}

type rateLimiterEntry struct {
	limiter       *sharedRateLimiter
	registrations map[uint64]RateLimitSettings
}

type sharedRateLimiter struct {
	bucket   *rate.Limiter
	inFlight chan struct{}
	runtime  limiterRuntime

	settingsMu sync.Mutex
	config     RateLimitSettings
}

type rateLimitedRoundTripper struct {
	base           http.RoundTripper
	limiter        *sharedRateLimiter
	requestTimeout time.Duration
}

type releaseOnCloseBody struct {
	httpBody io.ReadCloser
	release  func()
	once     sync.Once
}

func RegisterRateLimitedClient(client *http.Client, baseURL string, settings RateLimitSettings) (*RateLimitRegistration, *RateLimitConflict, error) {
	return registerRateLimitedClient(defaultRateLimiterRegistry, client, baseURL, settings)
}

func registerRateLimitedClient(registry *rateLimiterRegistry, client *http.Client, baseURL string, settings RateLimitSettings) (*RateLimitRegistration, *RateLimitConflict, error) {
	if client == nil {
		return nil, nil, fmt.Errorf("llmupstream: HTTP client is required")
	}
	limiter, registration, conflict, err := registry.register(baseURL, settings)
	if err != nil {
		return nil, nil, err
	}
	client.Transport = &rateLimitedRoundTripper{
		base:           client.Transport,
		limiter:        limiter,
		requestTimeout: client.Timeout,
	}
	client.Timeout = 0
	return registration, conflict, nil
}

func (t *rateLimitedRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	release, err := t.limiter.acquire(request.Context())
	if err != nil {
		return nil, err
	}

	requestContext := request.Context()
	cancel := func() {}
	if t.requestTimeout > 0 {
		requestContext, cancel = context.WithTimeout(requestContext, t.requestTimeout)
		request = request.Clone(requestContext)
	}
	releaseRequest := func() {
		cancel()
		release()
	}

	response, err := base.RoundTrip(request)
	if err != nil {
		releaseRequest()
		return nil, err
	}
	if response == nil {
		releaseRequest()
		return nil, fmt.Errorf("llmupstream: HTTP transport returned a nil response")
	}
	if response.Body == nil {
		releaseRequest()
		return response, nil
	}
	response.Body = &releaseOnCloseBody{httpBody: response.Body, release: releaseRequest}
	return response, nil
}

func (b *releaseOnCloseBody) Read(data []byte) (int, error) {
	return b.httpBody.Read(data)
}

func (b *releaseOnCloseBody) Close() error {
	err := b.httpBody.Close()
	b.once.Do(b.release)
	return err
}

func newRateLimiterRegistry() *rateLimiterRegistry {
	return newRateLimiterRegistryWithRuntime(limiterRuntime{})
}

func newRateLimiterRegistryWithRuntime(runtime limiterRuntime) *rateLimiterRegistry {
	if runtime.now == nil {
		runtime.now = time.Now
	}
	if runtime.wait == nil {
		runtime.wait = waitForLimiter
	}
	if runtime.jitter == nil {
		runtime.jitter = randomRequestJitter
	}
	return &rateLimiterRegistry{entries: make(map[string]*rateLimiterEntry), runtime: runtime}
}

func (r *rateLimiterRegistry) register(baseURL string, requested RateLimitSettings) (*sharedRateLimiter, *RateLimitRegistration, *RateLimitConflict, error) {
	origin, err := normalizeUpstreamOrigin(baseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := requested.validate(); err != nil {
		return nil, nil, nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextRegisterID++
	registration := &RateLimitRegistration{registry: r, origin: origin, id: r.nextRegisterID}
	if entry, ok := r.entries[origin]; ok {
		current := entry.limiter.settings()
		entry.registrations[registration.id] = requested
		effective := effectiveRateSettings(entry.registrations)
		entry.limiter.setSettings(effective)
		if current == requested {
			return entry.limiter, registration, nil, nil
		}
		return entry.limiter, registration, &RateLimitConflict{
			Origin: origin, Existing: current, Requested: requested, Effective: effective,
		}, nil
	}

	limiter := &sharedRateLimiter{
		bucket: rate.NewLimiter(rate.Limit(requested.RPS), requested.Burst), inFlight: make(chan struct{}, 1),
		runtime: r.runtime, config: requested,
	}
	r.entries[origin] = &rateLimiterEntry{
		limiter: limiter, registrations: map[uint64]RateLimitSettings{registration.id: requested},
	}
	return limiter, registration, nil, nil
}

func (r *rateLimiterRegistry) release(origin string, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[origin]
	if !ok {
		return
	}
	delete(entry.registrations, id)
	if len(entry.registrations) == 0 {
		delete(r.entries, origin)
		return
	}
	entry.limiter.setSettings(effectiveRateSettings(entry.registrations))
}

func (r *RateLimitRegistration) Release() {
	if r == nil || r.registry == nil {
		return
	}
	r.once.Do(func() { r.registry.release(r.origin, r.id) })
}

func effectiveRateSettings(registrations map[uint64]RateLimitSettings) RateLimitSettings {
	var effective RateLimitSettings
	for _, settings := range registrations {
		if effective.RPS == 0 || settings.RPS < effective.RPS {
			effective.RPS = settings.RPS
		}
		if effective.Burst == 0 || settings.Burst < effective.Burst {
			effective.Burst = settings.Burst
		}
	}
	return effective
}

func (l *sharedRateLimiter) acquire(ctx context.Context) (func(), error) {
	select {
	case l.inFlight <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	var once sync.Once
	release := func() { once.Do(func() { <-l.inFlight }) }
	fail := func(reservation *rate.Reservation, err error) (func(), error) {
		if reservation != nil {
			reservation.CancelAt(l.runtime.now())
		}
		release()
		return nil, err
	}

	now := l.runtime.now()
	reservation := l.bucket.ReserveN(now, 1)
	if !reservation.OK() {
		return fail(nil, fmt.Errorf("llmupstream: request rate limiter cannot reserve a token"))
	}
	if delay := reservation.DelayFrom(now); delay > 0 {
		if err := l.runtime.wait(ctx, delay); err != nil {
			return fail(reservation, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(reservation, err)
	}
	if jitter := l.runtime.jitter(); jitter > 0 {
		if err := l.runtime.wait(ctx, jitter); err != nil {
			return fail(reservation, err)
		}
	}
	return release, nil
}

func (l *sharedRateLimiter) settings() RateLimitSettings {
	l.settingsMu.Lock()
	defer l.settingsMu.Unlock()
	return l.config
}

func (l *sharedRateLimiter) setSettings(settings RateLimitSettings) {
	l.settingsMu.Lock()
	defer l.settingsMu.Unlock()
	now := l.runtime.now()
	l.bucket.SetLimitAt(now, rate.Limit(settings.RPS))
	l.bucket.SetBurstAt(now, settings.Burst)
	l.config = settings
}

func (s RateLimitSettings) validate() error {
	if s.RPS <= 0 || math.IsNaN(s.RPS) || math.IsInf(s.RPS, 0) {
		return fmt.Errorf("llmupstream: request_rps_limit must be a finite value greater than zero")
	}
	if s.Burst <= 0 {
		return fmt.Errorf("llmupstream: request_burst must be greater than zero")
	}
	return nil
}

func normalizeUpstreamOrigin(baseURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("llmupstream: invalid base_url: %w", err)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("llmupstream: base_url scheme must be http or https")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", fmt.Errorf("llmupstream: base_url host is required")
	}
	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("llmupstream: invalid base_url port %q", port)
		}
	}
	return scheme + "://" + net.JoinHostPort(host, port), nil
}

func waitForLimiter(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func randomRequestJitter() time.Duration {
	return time.Duration(rand.Int64N(int64(maxRequestJitter) + 1))
}
