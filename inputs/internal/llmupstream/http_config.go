package llmupstream

import (
	"net"
	"net/http"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/pkg/httpx"
)

func NewHTTPClient(cfg *config.HTTPCommonConfig) (*http.Client, error) {
	if cfg == nil {
		cfg = &config.HTTPCommonConfig{}
	}
	cfg.InitHTTPClientConfig()

	tlsConfig, err := cfg.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	proxy, err := cfg.Proxy()
	if err != nil {
		return nil, err
	}

	client := httpx.CreateHTTPClient(
		httpx.TlsConfig(tlsConfig),
		httpx.NetDialer(&net.Dialer{}),
		httpx.Proxy(proxy),
		httpx.DisableKeepAlives(*cfg.DisableKeepAlives),
		httpx.Timeout(time.Duration(cfg.Timeout)),
		httpx.FollowRedirects(*cfg.FollowRedirects),
	)
	return client, nil
}
