package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/itsfuad/octoload/balancer"
	"github.com/itsfuad/octoload/config"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestLB(t *testing.T) *balancer.LoadBalancer {
	t.Helper()
	lb, err := balancer.NewLoadBalancer(nil, "round_robin")
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	return lb
}

// setupMockServers starts test backends that are closed when the test ends.
func setupMockServers(t *testing.T, count int) []*httptest.Server {
	servers := make([]*httptest.Server, count)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Mock Server %d", i+1)
		}))
		t.Cleanup(servers[i].Close)
	}
	return servers
}

// createTestConfig writes a config pointing at servers into a temp file.
func createTestConfig(t *testing.T, servers []*httptest.Server) string {
	var cfg config.Config
	cfg.LoadBalancer.Port = 8080
	cfg.LoadBalancer.HealthCheckIntervalSeconds = 5
	cfg.LoadBalancer.Strategy = "round_robin"
	cfg.LoadBalancer.ShutdownTimeoutSeconds = 30
	for _, s := range servers {
		cfg.Servers.URLs = append(cfg.Servers.URLs, s.URL)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to encode config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	return path
}

func TestLoadBalancerInitialization(t *testing.T) {
	configFile := createTestConfig(t, setupMockServers(t, 2))

	// Test configuration loading
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if len(cfg.Servers.URLs) != 2 {
		t.Errorf("Expected 2 servers in config, got %d", len(cfg.Servers.URLs))
	}

	// Test load balancer creation
	if _, err := balancer.NewLoadBalancer(nil, cfg.LoadBalancer.Strategy); err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}
}

func TestAdminEndpoints(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)
	lb := newTestLB(t)
	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	var draining atomic.Bool
	admin := httptest.NewServer(newAdminMux(lb, &draining))
	t.Cleanup(admin.Close)

	status := func(path string) int {
		t.Helper()
		resp, err := http.Get(admin.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	expect := func(when string, livez, readyz int) {
		t.Helper()
		if got := status("/livez"); got != livez {
			t.Errorf("%s: /livez = %d, want %d", when, got, livez)
		}
		if got := status("/readyz"); got != readyz {
			t.Errorf("%s: /readyz = %d, want %d", when, got, readyz)
		}
	}

	expect("healthy", http.StatusOK, http.StatusOK)

	lb.Servers[0].Healthy.Store(false)
	expect("backend outage must not restart the balancer", http.StatusOK, http.StatusServiceUnavailable)

	lb.Servers[0].Healthy.Store(true)
	draining.Store(true)
	expect("draining", http.StatusOK, http.StatusServiceUnavailable)

	if got := status("/"); got != http.StatusNotFound {
		t.Errorf("admin / = %d, want 404: admin port must not proxy", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)
	lb := newTestLB(t)
	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	var draining atomic.Bool
	admin := httptest.NewServer(newAdminMux(lb, &draining))
	t.Cleanup(admin.Close)

	lb.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	resp, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to get metrics: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	var metrics balancer.Metrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("Failed to decode metrics: %v", err)
	}
	if metrics.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", metrics.TotalRequests)
	}
	want := balancer.BackendMetrics{URL: backend.URL, Healthy: true}
	if len(metrics.Backends) != 1 || metrics.Backends[0] != want {
		t.Errorf("Backends = %+v, want [%+v]", metrics.Backends, want)
	}
}

// Regression: /metrics on the traffic port hid any backend path of the same name.
func TestTrafficPortProxiesEveryPath(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "backend ", r.URL.Path)
	}))
	t.Cleanup(backend.Close)
	lb := newTestLB(t)
	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url, _ := startServe(t, ctx, lb, 0, time.Second)

	for _, path := range []string{"/metrics", "/livez", "/readyz"} {
		resp, err := http.Get(url + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "backend "+path {
			t.Errorf("%s answered %q, want it proxied", path, body)
		}
	}
}

// startServe runs serve on an ephemeral port and returns its address and result.
func startServe(t *testing.T, ctx context.Context, handler http.Handler, delay, timeout time.Duration) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- serve(ctx, listener, handler, delay, timeout) }()
	return "http://" + listener.Addr().String(), result
}

func waitForServe(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return")
		return nil
	}
}

// newBlockingLB returns a balancer whose backend writes "partial", signals started,
// then finishes with "done" only once release is called. Cleanup releases it first
// so a failing test cannot hang closing the backend.
func newBlockingLB(t *testing.T) (lb *balancer.LoadBalancer, started chan struct{}, release func()) {
	t.Helper()
	started = make(chan struct{}, 8)
	unblock := make(chan struct{})
	release = sync.OnceFunc(func() { close(unblock) })
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		started <- struct{}{}
		<-unblock
		w.Write([]byte("done"))
	}))
	t.Cleanup(backend.Close)
	t.Cleanup(release)
	lb = newTestLB(t)
	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	return lb, started, release
}

func waitUntilRefused(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener still accepting new connections")
}

// Regression: a stop signal called os.Exit(0), killing every in-flight request.
func TestServeDrainsInFlightRequests(t *testing.T) {
	lb, started, release := newBlockingLB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url, result := startServe(t, ctx, lb, 0, 10*time.Second)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	<-started

	cancel()
	waitUntilRefused(t, url)
	select {
	case err := <-result:
		t.Fatalf("serve returned with a request in flight: %v", err)
	default:
	}

	release()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "partialdone" {
		t.Fatalf("body = %q, err = %v, want complete response", body, err)
	}
	if err := waitForServe(t, result); err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestServeKeepsAcceptingDuringShutdownDelay(t *testing.T) {
	const delay = 500 * time.Millisecond
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(backend.Close)
	lb := newTestLB(t)
	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url, result := startServe(t, ctx, lb, delay, 5*time.Second)

	// Prove the server is up before starting the delay.
	first, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET before cancel: %v", err)
	}
	first.Body.Close()

	cancel()
	// A fresh connection: a kept-alive one would not prove the listener is open.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET during delay: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status during delay = %d, want 200", resp.StatusCode)
	}

	if err := waitForServe(t, result); err != nil {
		t.Fatalf("serve: %v", err)
	}
	waitUntilRefused(t, url)
}

func TestServeReportsDrainTimeout(t *testing.T) {
	lb, started, _ := newBlockingLB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url, result := startServe(t, ctx, lb, 0, 50*time.Millisecond)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	<-started

	cancel()
	if err := waitForServe(t, result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("serve = %v, want drain deadline exceeded", err)
	}
}

func TestServeReportsListenerFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	listener.Close()

	result := make(chan error, 1)
	go func() { result <- serve(context.Background(), listener, http.NotFoundHandler(), 0, time.Second) }()
	if err := waitForServe(t, result); err == nil {
		t.Fatal("expected error from closed listener")
	}
}

// Regression: http.ListenAndServe has no timeouts, so slow clients held connections forever.
func TestServeDropsClientsThatStallSendingHeaders(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the real 10s ReadHeaderTimeout")
	}
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	url, _ := startServe(t, ctx, http.NotFoundHandler(), 0, time.Second)

	conn, err := net.Dial("tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("connection was not closed by the server: %v", err)
	}
}

func TestLoadBalancerRequestHandling(t *testing.T) {
	mockServers := setupMockServers(t, 2)

	// Create load balancer
	lb := newTestLB(t)
	for _, s := range mockServers {
		if err := lb.AddServer(s.URL); err != nil {
			t.Fatalf("Failed to add server: %v", err)
		}
	}

	// Create test server with the load balancer
	server := httptest.NewServer(lb)
	defer server.Close()

	// Test request handling
	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if len(body) == 0 {
		t.Error("Expected non-empty response body")
	}
}

func TestRetryPreservesRequestBody(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	}))
	t.Cleanup(echo.Close)

	lb := newTestLB(t)
	for _, url := range []string{dead.URL, echo.URL} {
		if err := lb.AddServer(url); err != nil {
			t.Fatalf("Failed to add server: %v", err)
		}
	}

	rw := httptest.NewRecorder()
	lb.ServeHTTP(rw, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")))
	if rw.Code != http.StatusOK || rw.Body.String() != "x" {
		t.Fatalf("Expected 200 with echoed body after retry, got %d %q", rw.Code, rw.Body.String())
	}
}
