package llmupstream

import (
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
)

func TestNewHTTPClientAppliesCommonConfig(t *testing.T) {
	t.Parallel()

	followRedirects := false
	disableKeepAlives := false
	cfg := &config.HTTPCommonConfig{
		Timeout:           config.Duration(2 * time.Second),
		FollowRedirects:   &followRedirects,
		DisableKeepAlives: &disableKeepAlives,
	}

	client, err := NewHTTPClient(cfg)
	if err != nil {
		t.Fatalf("NewHTTPClient() error = %v", err)
	}
	if got, want := client.Timeout, 2*time.Second; got != want {
		t.Fatalf("client timeout = %v, want %v", got, want)
	}
	if err := client.CheckRedirect(nil, nil); err == nil {
		t.Fatal("CheckRedirect() error = nil, want redirects disabled")
	}
}

func TestNewHTTPClientRejectsInvalidProxy(t *testing.T) {
	t.Parallel()

	cfg := &config.HTTPCommonConfig{}
	cfg.HTTPProxyURL = "://bad proxy"
	_, err := NewHTTPClient(cfg)
	if err == nil {
		t.Fatal("NewHTTPClient() error = nil, want invalid proxy error")
	}
}
