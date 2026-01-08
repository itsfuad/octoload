package main

import (
	"encoding/json"
	"fmt"
	"io"
	"loadbalancer/balancer"
	"loadbalancer/config"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
	lb := balancer.NewLoadBalancer(nil, cfg.LoadBalancer.Strategy)
	if lb == nil {
		t.Fatal("Failed to create load balancer")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	// No backends: /metrics must answer locally, everything else reaches lb (503).
	server := httptest.NewServer(newMux(balancer.NewLoadBalancer(nil, "round_robin")))
	defer server.Close()

	proxied, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("Failed to get /: %v", err)
	}
	proxied.Body.Close()
	if proxied.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected / to reach load balancer (503), got %v", proxied.Status)
	}

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	var metrics balancer.Metrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("Failed to decode metrics: %v", err)
	}
}

func TestGracefulShutdown(t *testing.T) {

	// Create test load balancer
	lb := balancer.NewLoadBalancer(nil, "round_robin")

	// Create shutdown channel
	shutdown := make(chan struct{})
	done := make(chan struct{})

	// Start test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-shutdown:
			// Simulate graceful shutdown
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Add test server to load balancer
	err := lb.AddServer(server.URL)
	if err != nil {
		t.Fatalf("Failed to add server: %v", err)
	}

	// Start shutdown process in goroutine
	go func() {
		close(shutdown)
		lb.GracefulShutdown()
		close(done)
	}()

	// Wait for shutdown with timeout
	select {
	case <-done:
		// Shutdown completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown timeout")
	}

	// Verify server is not accepting new requests
	resp, err := http.Get(server.URL)
	if err == nil && resp.StatusCode != http.StatusServiceUnavailable {
		t.Error("Expected server to reject requests after shutdown")
	}
}

func TestLoadBalancerRequestHandling(t *testing.T) {
	mockServers := setupMockServers(t, 2)

	// Create load balancer
	lb := balancer.NewLoadBalancer(nil, "round_robin")
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

	lb := balancer.NewLoadBalancer(nil, "round_robin")
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
