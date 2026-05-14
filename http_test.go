package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*broker, *httptest.Server) {
	t.Helper()
	b := newBroker()
	srv := httptest.NewServer(newAppHandler(b))
	t.Cleanup(func() {
		srv.Close()
		b.close()
	})
	return b, srv
}

func do(t *testing.T, method, url string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(""))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(body)
}

func TestHTTPPutGetRoundtrip(t *testing.T) {
	_, srv := newTestServer(t)

	resp, _ := do(t, http.MethodPut, srv.URL+"/foo?v=bar")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	resp, body := do(t, http.MethodGet, srv.URL+"/foo")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", resp.StatusCode)
	}
	if body != "bar" {
		t.Fatalf("body: got %q, want %q", body, "bar")
	}
}

func TestHTTPGetEmptyReturns404(t *testing.T) {
	_, srv := newTestServer(t)
	resp, _ := do(t, http.MethodGet, srv.URL+"/foo")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestHTTPPutWithoutVReturns400(t *testing.T) {
	_, srv := newTestServer(t)
	resp, _ := do(t, http.MethodPut, srv.URL+"/foo")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestHTTPPutEmptyValueOK(t *testing.T) {
	_, srv := newTestServer(t)
	resp, _ := do(t, http.MethodPut, srv.URL+"/foo?v=")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	resp, body := do(t, http.MethodGet, srv.URL+"/foo")
	if resp.StatusCode != http.StatusOK || body != "" {
		t.Fatalf("got %d body %q, want 200 with empty body", resp.StatusCode, body)
	}
}

func TestHTTPGetTimeoutWaits(t *testing.T) {
	_, srv := newTestServer(t)

	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = do(t, http.MethodPut, srv.URL+"/test?v=delayed")
	}()

	start := time.Now()
	resp, body := do(t, http.MethodGet, srv.URL+"/test?timeout=5")
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if body != "delayed" {
		t.Fatalf("body: got %q, want %q", body, "delayed")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took too long: %v", elapsed)
	}
}

func TestHTTPGetTimeoutExpires(t *testing.T) {
	_, srv := newTestServer(t)

	start := time.Now()
	resp, _ := do(t, http.MethodGet, srv.URL+"/empty?timeout=1")
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("returned before timeout: %v", elapsed)
	}
}

func TestHTTPInvalidTimeoutReturns400(t *testing.T) {
	_, srv := newTestServer(t)
	tests := []string{"abc", "-1"}
	for _, ts := range tests {
		resp, _ := do(t, http.MethodGet, srv.URL+"/foo?timeout="+ts)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("timeout=%q: got %d, want 400", ts, resp.StatusCode)
		}
	}
}

func TestHTTPInvalidPathReturns404(t *testing.T) {
	_, srv := newTestServer(t)
	tests := []string{"/", "/foo/bar"}
	for _, path := range tests {
		resp, _ := do(t, http.MethodGet, srv.URL+path)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s: got %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestHTTPMethodNotAllowed(t *testing.T) {
	_, srv := newTestServer(t)
	resp, _ := do(t, http.MethodPost, srv.URL+"/foo?v=x")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405", resp.StatusCode)
	}
}

func TestHTTPShutdownReturns503(t *testing.T) {
	b, srv := newTestServer(t)
	b.close()
	time.Sleep(30 * time.Millisecond)

	resp, _ := do(t, http.MethodPut, srv.URL+"/foo?v=x")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", resp.StatusCode)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	_, srv := newTestServer(t)
	resp, body := do(t, http.MethodGet, srv.URL+"/metrics")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: got %d, want 200", resp.StatusCode)
	}
	want := []string{
		"queuebroker_http_requests_total",
		"queuebroker_longpoll_wait_seconds",
		"queuebroker_messages_enqueued_total",
		"queuebroker_messages_delivered_total",
	}
	for _, s := range want {
		if !strings.Contains(body, s) {
			t.Fatalf("metrics body missing %q", s)
		}
	}
}
