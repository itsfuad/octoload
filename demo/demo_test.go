package main

import (
	"context"
	"fmt"
	"github.com/itsfuad/octoload/balancer"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type demoServer struct{ id, port int }

// buildDemo compiles demo_server.go into bin and removes it when the test ends.
func buildDemo(t *testing.T) string {
	t.Log("Building demo server...")
	bin := filepath.Join(t.TempDir(), "demo_server")
	if err := exec.Command("go", "build", "-o", bin, "demo_server.go").Run(); err != nil {
		t.Fatalf("Failed to build demo server: %v", err)
	}
	return bin
}

// freePort lets tests run in parallel without fighting over fixed ports.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find a free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// TestLoadBalancerWithDemoServers spawns real demo servers and tests load balancing
func TestLoadBalancerWithDemoServers(t *testing.T) {
	bin := buildDemo(t)

	serverConfigs := []demoServer{
		{id: 1, port: freePort(t)},
		{id: 2, port: freePort(t)},
		{id: 3, port: freePort(t)},
	}

	// Start demo servers
	var processes []*exec.Cmd
	for _, cfg := range serverConfigs {
		cmd := exec.Command(bin,
			"-id", fmt.Sprintf("%d", cfg.id),
			"-port", fmt.Sprintf("%d", cfg.port))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start demo server %d: %v", cfg.id, err)
		}
		processes = append(processes, cmd)
		t.Logf("Started demo server %d on port %d", cfg.id, cfg.port)
	}

	// Cleanup: kill all demo servers when test completes
	defer func() {
		for i, cmd := range processes {
			if cmd.Process != nil {
				t.Logf("Stopping demo server %d", i+1)
				cmd.Process.Kill()
			}
		}
	}()

	// Wait for servers to be ready
	time.Sleep(2 * time.Second)

	// Verify servers are running
	for _, cfg := range serverConfigs {
		url := fmt.Sprintf("http://localhost:%d/health", cfg.port)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Server %d is not responding: %v", cfg.id, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Server %d health check failed: %v", cfg.id, resp.Status)
		}
		t.Logf("Server %d health check passed", cfg.id)
	}

	// Test Round Robin Strategy
	t.Run("RoundRobinStrategy", func(t *testing.T) {
		testStrategy(t, "round_robin", serverConfigs)
	})

	// Test Least Active Strategy
	t.Run("LeastActiveStrategy", func(t *testing.T) {
		testStrategy(t, "least_active", serverConfigs)
	})
}

func testStrategy(t *testing.T, strategy string, serverConfigs []demoServer) {
	t.Logf("Testing %s strategy", strategy)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("strategy", strategy)
	lb, err := balancer.NewLoadBalancer(logger, strategy)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}

	for _, cfg := range serverConfigs {
		url := fmt.Sprintf("http://localhost:%d", cfg.port)
		if err := lb.AddServer(url); err != nil {
			t.Fatalf("Failed to add server %d: %v", cfg.id, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	lb.StartHealthChecks(ctx, 5*time.Second)
	time.Sleep(2 * time.Second)

	numRequests := 10
	t.Logf("Making %d requests to test load distribution", numRequests)

	serverHits, successfulRequests := sendRequestsThroughBalancer(t, lb, numRequests)

	verifyLoadDistribution(t, strategy, serverConfigs, serverHits, successfulRequests, numRequests)
}

func sendRequestsThroughBalancer(t *testing.T, lb *balancer.LoadBalancer, numRequests int) (map[string]int, int) {
	serverHits := make(map[string]int)
	successfulRequests := 0

	for i := 0; i < numRequests; i++ {
		req, err := http.NewRequest("GET", "http://test.local", nil)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		rw := httptest.NewRecorder()
		lb.ServeHTTP(rw, req)

		if rw.Code != http.StatusOK {
			t.Logf("Request %d failed with status: %d", i+1, rw.Code)
			continue
		}

		response := rw.Body.String()
		serverHits[response]++
		successfulRequests++
		t.Logf("Request %d: %s", i+1, response)
	}
	return serverHits, successfulRequests
}

func verifyLoadDistribution(t *testing.T, strategy string, serverConfigs []demoServer, serverHits map[string]int, successfulRequests, numRequests int) {
	t.Logf("\n=== Load Distribution Summary for %s ===", strategy)
	for response, count := range serverHits {
		t.Logf("%s: %d requests", response, count)
	}
	t.Logf("Total successful requests: %d/%d", successfulRequests, numRequests)

	if successfulRequests == 0 {
		t.Error("No successful requests - load balancer may not be working")
	}

	if strategy == "round_robin" && len(serverHits) > 1 {
		expectedHitsPerServer := float64(successfulRequests) / float64(len(serverConfigs))
		tolerance := expectedHitsPerServer * 0.5

		for response, hits := range serverHits {
			diff := math.Abs(float64(hits) - expectedHitsPerServer)
			if diff > tolerance {
				t.Logf("Warning: Uneven distribution for %s: got %d hits, expected ~%.1f",
					response, hits, expectedHitsPerServer)
			}
		}
		t.Logf("Round robin distributed requests across %d servers", len(serverHits))
	}
}

// TestDemoServerDirectly tests demo servers directly without load balancer
func TestDemoServerDirectly(t *testing.T) {
	bin := buildDemo(t)
	port := freePort(t)

	// Start a single demo server
	cmd := exec.Command(bin, "-id", "99", "-port", fmt.Sprintf("%d", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start demo server: %v", err)
	}
	defer cmd.Process.Kill()

	// Wait for server to be ready
	time.Sleep(1 * time.Second)

	// Test server endpoint
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/", port))
	if err != nil {
		t.Fatalf("Failed to connect to demo server: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	response := string(body)
	t.Logf("Demo server response: %s", response)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	// Verify response contains server ID
	expectedSubstring := "SERVER 99"
	if !strings.Contains(response, expectedSubstring) {
		t.Errorf("Expected response to contain '%s', got: %s", expectedSubstring, response)
	}
}
