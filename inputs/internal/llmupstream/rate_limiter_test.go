package llmupstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type limiterRoundTripFunc func(*http.Request) (*http.Response, error)

func (f limiterRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRegisterRateLimitedClientExcludesTokenWaitFromHTTPTimeout(t *testing.T) {
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{
		now: time.Now, wait: waitForLimiter, jitter: func() time.Duration { return 0 },
	})
	requests := 0
	client := &http.Client{
		Timeout: 20 * time.Millisecond,
		Transport: limiterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		}),
	}

	registration, conflict, err := registerRateLimitedClient(registry, client, "https://supplier.example.com", RateLimitSettings{RPS: 20, Burst: 1})
	if err != nil {
		t.Fatalf("RegisterRateLimitedClient() error = %v", err)
	}
	defer registration.Release()
	if conflict != nil {
		t.Fatalf("RegisterRateLimitedClient() conflict = %#v, want nil", conflict)
	}

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodGet, "https://supplier.example.com/status", nil)
		if err != nil {
			t.Fatalf("NewRequest() error = %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d error = %v; limiter wait must not consume the 20ms HTTP timeout", i+1, err)
		}
		resp.Body.Close()
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestRateLimitedRoundTripperHoldsSharedSlotUntilBodyCloses(t *testing.T) {
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{now: time.Now, wait: noLimiterWait, jitter: func() time.Duration { return 0 }})
	limiter, registration, _, err := registry.register("https://example.com", RateLimitSettings{RPS: 1000, Burst: 2})
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}
	defer registration.Release()
	requests := 0
	transport := &rateLimitedRoundTripper{
		base: limiterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
		}),
		limiter: limiter,
	}

	firstRequest, _ := http.NewRequest(http.MethodGet, "https://example.com/first", nil)
	firstResponse, err := transport.RoundTrip(firstRequest)
	if err != nil {
		t.Fatalf("first RoundTrip() error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	secondRequest, _ := http.NewRequestWithContext(canceled, http.MethodGet, "https://example.com/second", nil)
	if _, err := transport.RoundTrip(secondRequest); !errors.Is(err, context.Canceled) {
		t.Fatalf("second RoundTrip() error = %v, want context.Canceled", err)
	}
	firstResponse.Body.Close()
	thirdRequest, _ := http.NewRequest(http.MethodGet, "https://example.com/third", nil)
	thirdResponse, err := transport.RoundTrip(thirdRequest)
	if err != nil {
		t.Fatalf("third RoundTrip() error = %v", err)
	}
	thirdResponse.Body.Close()
	if requests != 2 {
		t.Fatalf("base requests = %d, want 2", requests)
	}
}

func TestRateLimitedRoundTripperReleasesSlotOnTransportFailure(t *testing.T) {
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{now: time.Now, wait: noLimiterWait, jitter: func() time.Duration { return 0 }})
	limiter, registration, _, err := registry.register("https://example.com", RateLimitSettings{RPS: 1000, Burst: 3})
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}
	defer registration.Release()

	calls := 0
	transport := &rateLimitedRoundTripper{
		base: limiterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			switch calls {
			case 1:
				return nil, errors.New("transport failed")
			case 2:
				return nil, nil
			default:
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
			}
		}),
		limiter: limiter,
	}

	request := func() *http.Request {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		t.Cleanup(cancel)
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/status", nil)
		if requestErr != nil {
			t.Fatalf("NewRequestWithContext() error = %v", requestErr)
		}
		return req
	}
	if _, err := transport.RoundTrip(request()); err == nil {
		t.Fatal("first RoundTrip() error = nil, want transport error")
	}
	if _, err := transport.RoundTrip(request()); err == nil {
		t.Fatal("second RoundTrip() error = nil, want nil-response error")
	}
	response, err := transport.RoundTrip(request())
	if err != nil {
		t.Fatalf("third RoundTrip() error = %v; previous failures leaked the concurrency slot", err)
	}
	response.Body.Close()
}

func TestRateLimiterRegistrySharesOriginAndRelaxesAfterRelease(t *testing.T) {
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{now: time.Now, wait: noLimiterWait, jitter: func() time.Duration { return 0 }})
	first, firstRegistration, conflict, err := registry.register("HTTPS://Example.COM/api", RateLimitSettings{RPS: 5, Burst: 4})
	if err != nil || conflict != nil {
		t.Fatalf("first register = conflict %#v, error %v", conflict, err)
	}
	second, secondRegistration, conflict, err := registry.register("https://example.com:443/other", RateLimitSettings{RPS: 2, Burst: 6})
	if err != nil {
		t.Fatalf("second register() error = %v", err)
	}
	if first != second || conflict == nil || conflict.Effective != (RateLimitSettings{RPS: 2, Burst: 4}) {
		t.Fatalf("shared/conflict = %t/%#v, want effective 2/4", first == second, conflict)
	}
	secondRegistration.Release()
	if got := first.settings(); got != (RateLimitSettings{RPS: 5, Burst: 4}) {
		t.Fatalf("settings after strict release = %#v, want 5/4", got)
	}
	firstRegistration.Release()
	if len(registry.entries) != 0 {
		t.Fatalf("registry entries = %d, want zero", len(registry.entries))
	}
}

func TestRateLimiterConflictClampsTokensToEffectiveBurst(t *testing.T) {
	now := time.Unix(100, 0)
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{
		now: func() time.Time { return now }, wait: noLimiterWait, jitter: func() time.Duration { return 0 },
	})
	limiter, firstRegistration, _, err := registry.register("https://example.com", RateLimitSettings{RPS: 10, Burst: 5})
	if err != nil {
		t.Fatalf("first register() error = %v", err)
	}
	defer firstRegistration.Release()
	_, secondRegistration, _, err := registry.register("https://example.com", RateLimitSettings{RPS: 1, Burst: 1})
	if err != nil {
		t.Fatalf("second register() error = %v", err)
	}
	defer secondRegistration.Release()

	if tokens := limiter.bucket.TokensAt(now); tokens > 1 {
		t.Fatalf("tokens after restrictive registration = %v, want at most effective burst 1", tokens)
	}
}

func TestRegisterRateLimitedClientKeepsActualRequestTimeout(t *testing.T) {
	client := &http.Client{
		Timeout: 20 * time.Millisecond,
		Transport: limiterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}
	registration, _, err := RegisterRateLimitedClient(client, "https://timeout.example.com", RateLimitSettings{RPS: 1000, Burst: 1})
	if err != nil {
		t.Fatalf("RegisterRateLimitedClient() error = %v", err)
	}
	defer registration.Release()
	req, _ := http.NewRequest(http.MethodGet, "https://timeout.example.com/status", nil)
	_, err = client.Do(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("client.Do() error = %v, want context deadline", err)
	}
}

func TestDifferentOriginsUseIndependentConcurrencyGates(t *testing.T) {
	registry := newRateLimiterRegistryWithRuntime(limiterRuntime{now: time.Now, wait: noLimiterWait, jitter: func() time.Duration { return 0 }})
	first, firstRegistration, _, err := registry.register("https://first.example.com", RateLimitSettings{RPS: 1000, Burst: 1})
	if err != nil {
		t.Fatalf("first register() error = %v", err)
	}
	defer firstRegistration.Release()
	second, secondRegistration, _, err := registry.register("https://second.example.com", RateLimitSettings{RPS: 1000, Burst: 1})
	if err != nil {
		t.Fatalf("second register() error = %v", err)
	}
	defer secondRegistration.Release()

	firstRelease, err := first.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire() error = %v", err)
	}
	defer firstRelease()
	secondRelease, err := second.acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire() blocked by other origin: %v", err)
	}
	secondRelease()
}

func TestRandomRequestJitterIsBounded(t *testing.T) {
	for range 1000 {
		jitter := randomRequestJitter()
		if jitter < 0 || jitter > maxRequestJitter {
			t.Fatalf("randomRequestJitter() = %v, want 0..%v", jitter, maxRequestJitter)
		}
	}
}

func TestRateLimiterRegistrationReleaseIsIdempotent(t *testing.T) {
	registry := newRateLimiterRegistry()
	_, registration, _, err := registry.register("https://example.com", RateLimitSettings{RPS: 1, Burst: 1})
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}
	var group sync.WaitGroup
	for range 10 {
		group.Add(1)
		go func() {
			defer group.Done()
			registration.Release()
		}()
	}
	group.Wait()
	if len(registry.entries) != 0 {
		t.Fatalf("registry entries = %d, want zero", len(registry.entries))
	}
}

func noLimiterWait(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}
