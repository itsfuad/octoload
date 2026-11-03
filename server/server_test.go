package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const URL = "http://example.com"

func TestNewServer(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	server, err := NewServer(URL, logger, Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if server.URL != URL {
		t.Errorf("expected URL to be 'http://example.com', got %s", server.URL)
	}
	if !server.Healthy.Load() {
		t.Errorf("expected server to be healthy")
	}
	if server.logger != logger {
		t.Errorf("expected logger to be set")
	}
}

func TestNewServerAcceptsNilLogger(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()

	server, err := NewServer(dead.URL, nil, Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	for range unhealthyAfter {
		server.CheckHealth(context.Background()) // Logs the state change.
	}
	if server.Healthy.Load() {
		t.Error("expected dead backend to be marked down")
	}
}

func TestNewServerRejectsInvalidURLs(t *testing.T) {
	for _, rawURL := range []string{"", "not a url", "localhost:8001", "ftp://host", "http://", "http://a b", "http://a?x=1"} {
		if _, err := NewServer(rawURL, nil, Options{}); err == nil {
			t.Errorf("NewServer(%q): expected error", rawURL)
		}
	}
}

func TestHandleRequest(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()

	server := newTestServer(t, dead.URL, Options{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	if err := server.HandleRequest(w, req); err == nil || errors.Is(err, ErrUnhealthy) {
		t.Errorf("expected connection error, got %v", err)
	}
	if server.Load.Load() != 0 {
		t.Errorf("expected load to return to 0, got %d", server.Load.Load())
	}

	server.Healthy.Store(false)
	if err := server.HandleRequest(w, req); !errors.Is(err, ErrUnhealthy) {
		t.Errorf("expected ErrUnhealthy, got %v", err)
	}
}

func newTestServer(t *testing.T, url string, options Options) *Server {
	t.Helper()
	server, err := NewServer(url, slog.New(slog.DiscardHandler), options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server
}

func TestCheckHealthStatusCodes(t *testing.T) {
	for status, wantHealthy := range map[int]bool{
		http.StatusOK:                  true,
		http.StatusNoContent:           true,
		http.StatusNotFound:            false,
		http.StatusInternalServerError: false,
		http.StatusServiceUnavailable:  false,
	} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		t.Cleanup(backend.Close)
		server := newTestServer(t, backend.URL, Options{})

		for range unhealthyAfter {
			server.CheckHealth(context.Background())
		}
		if got := server.Healthy.Load(); got != wantHealthy {
			t.Errorf("status %d: healthy = %v, want %v", status, got, wantHealthy)
		}
	}
}

func TestCheckHealthFollowsRedirects(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/login", http.StatusFound)
		}
	}))
	t.Cleanup(backend.Close)
	server := newTestServer(t, backend.URL, Options{})

	for range unhealthyAfter {
		server.CheckHealth(context.Background())
	}
	if !server.Healthy.Load() {
		t.Error("redirect to a healthy page should count as healthy")
	}
}

func TestCheckHealthUsesConfiguredPath(t *testing.T) {
	uris := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { uris <- r.RequestURI }))
	t.Cleanup(backend.Close)

	for base, path := range map[string]string{
		backend.URL:       "/healthz?full=1",
		backend.URL + "/": "/healthz?full=1",
	} {
		newTestServer(t, base, Options{HealthCheckPath: path}).CheckHealth(context.Background())
		if got := <-uris; got != path {
			t.Errorf("base %q: requested %q, want %q", base, got, path)
		}
	}

	newTestServer(t, backend.URL, Options{}).CheckHealth(context.Background())
	if got := <-uris; got != "/" {
		t.Errorf("default path: requested %q, want /", got)
	}
}

func TestNewServerRejectsInvalidHealthCheckPath(t *testing.T) {
	if _, err := NewServer(URL, nil, Options{HealthCheckPath: "healthz"}); err == nil {
		t.Fatal("expected error")
	}
}

// Regression: a backend that accepted connections and never answered blocked its
// checker forever and stayed healthy.
func TestCheckHealthMarksHungBackendDown(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(backend.Close)
	server := newTestServer(t, backend.URL, Options{HealthCheckTimeout: 20 * time.Millisecond})

	// The outer deadline only keeps this test from hanging if the check is unbounded.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range unhealthyAfter {
		server.CheckHealth(ctx)
	}
	if server.Healthy.Load() {
		t.Error("hung backend should be marked down")
	}
}

func TestCheckHealthFailureThresholdAndRecovery(t *testing.T) {
	var failing atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(backend.Close)
	server := newTestServer(t, backend.URL, Options{})
	check := func(want bool, when string) {
		t.Helper()
		server.CheckHealth(context.Background())
		if got := server.Healthy.Load(); got != want {
			t.Errorf("%s: healthy = %v, want %v", when, got, want)
		}
	}

	failing.Store(true)
	check(true, "one failure is not enough")
	failing.Store(false)
	check(true, "success resets the count")
	failing.Store(true)
	check(true, "first failure after reset")
	check(false, "second failure in a row")
	check(false, "still failing")
	failing.Store(false)
	check(true, "one success recovers")
}

// Regression: every failed check logged a line.
func TestCheckHealthLogsOnlyStateChanges(t *testing.T) {
	var failing atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(backend.Close)

	var logs bytes.Buffer
	server, err := NewServer(backend.URL, slog.New(slog.NewTextHandler(&logs, nil)), Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	check := func(times int) {
		for range times {
			server.CheckHealth(context.Background())
		}
	}

	check(3)
	if logs.Len() != 0 {
		t.Errorf("healthy checks logged: %s", logs.String())
	}

	failing.Store(true)
	check(5)
	if got := strings.Count(logs.String(), "server unhealthy"); got != 1 {
		t.Errorf("logged unhealthy %d times, want 1:\n%s", got, logs.String())
	}

	failing.Store(false)
	check(3)
	if got := strings.Count(logs.String(), "server recovered"); got != 1 {
		t.Errorf("logged recovery %d times, want 1:\n%s", got, logs.String())
	}
}

func TestCheckHealthRestoresPassivelyEjectedBackend(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)
	server := newTestServer(t, backend.URL, Options{})

	server.Healthy.Store(false)
	server.CheckHealth(context.Background())
	if !server.Healthy.Load() {
		t.Error("successful check should restore backend")
	}
}

func TestCheckHealthIgnoresCancellation(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	server := newTestServer(t, dead.URL, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range unhealthyAfter {
		server.CheckHealth(ctx)
	}
	if !server.Healthy.Load() {
		t.Error("cancelled checks must not mark backend down")
	}
}
