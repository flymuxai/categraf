package llmupstream

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestClientDoJSONBuildsURLAndDecodesResponse(t *testing.T) {
	t.Parallel()

	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got, want := req.URL.String(), "https://example.com/root/api/status?group=pro"; got != want {
			t.Fatalf("request URL = %q, want %q", got, want)
		}
		if got, want := req.Header.Get("Authorization"), "secret"; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}

	client, err := NewClient("https://example.com/root/", httpClient, 1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var response struct {
		OK bool `json:"ok"`
	}
	err = client.DoJSON(
		context.Background(),
		http.MethodGet,
		"api/status",
		url.Values{"group": []string{"pro"}},
		map[string]string{"Authorization": "secret"},
		nil,
		&response,
	)
	if err != nil {
		t.Fatalf("DoJSON() error = %v", err)
	}
	if !response.OK {
		t.Fatal("DoJSON() did not decode response")
	}
}

func TestClientDoJSONRejectsNonSuccessStatus(t *testing.T) {
	t.Parallel()

	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"message":"bad token"}`)),
			Request:    req,
		}, nil
	})}
	client, err := NewClient("https://example.com", httpClient, 1024)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.DoJSON(context.Background(), http.MethodGet, "/api/status", nil, nil, nil, &struct{}{})
	statusErr, ok := err.(*HTTPStatusError)
	if !ok {
		t.Fatalf("DoJSON() error = %T %v, want *HTTPStatusError", err, err)
	}
	if statusErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusUnauthorized)
	}
	if strings.Contains(err.Error(), "bad token") {
		t.Fatalf("DoJSON() error leaks response body: %v", err)
	}
}

func TestClientDoJSONRejectsOversizedResponse(t *testing.T) {
	t.Parallel()

	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"value":"too large"}`)),
			Request:    req,
		}, nil
	})}
	client, err := NewClient("https://example.com", httpClient, 8)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	err = client.DoJSON(context.Background(), http.MethodGet, "/api/status", nil, nil, nil, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("DoJSON() error = %v, want response size error", err)
	}
}

func TestNewClientRejectsUnsupportedScheme(t *testing.T) {
	t.Parallel()

	_, err := NewClient("file:///tmp/data", http.DefaultClient, 1024)
	if err == nil || !strings.Contains(err.Error(), "http or https") {
		t.Fatalf("NewClient() error = %v, want scheme validation error", err)
	}
}
