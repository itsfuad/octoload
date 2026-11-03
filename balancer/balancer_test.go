package balancer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sv "github.com/itsfuad/octoload/server"
)

func newTestBalancer(t *testing.T, strategy string, urls ...string) *LoadBalancer {
	t.Helper()
	lb := NewLoadBalancer(nil, strategy)
	for _, url := range urls {
		if err := lb.AddServer(url); err != nil {
			t.Fatalf("AddServer(%s): %v", url, err)
		}
	}
	return lb
}

func serverURLs(lb *LoadBalancer) []string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	urls := make([]string, 0, len(lb.Servers))
	for _, s := range lb.Servers {
		urls = append(urls, s.URL)
	}
	return urls
}

// Regression: the old code waited for drain, then removed a stale index, which
// removed the wrong server if the list changed during the wait.
func TestRemoveServerRemovesCorrectServerWhileDraining(t *testing.T) {
	lb := newTestBalancer(t, "round_robin", "http://a", "http://b", "http://c")
	b := lb.Servers[1]
	b.Load.Add(1)

	done := make(chan error, 1)
	go func() { done <- lb.RemoveServer("http://b") }()

	deadline := time.Now().Add(2 * time.Second)
	for slices.Contains(serverURLs(lb), "http://b") {
		if time.Now().After(deadline) {
			t.Fatal("server still listed while draining: it would keep receiving traffic")
		}
		time.Sleep(time.Millisecond)
	}

	if err := lb.RemoveServer("http://a"); err != nil {
		t.Fatalf("RemoveServer(a): %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("RemoveServer(b) returned before drain: %v", err)
	default:
	}

	b.Load.Add(-1)
	if err := <-done; err != nil {
		t.Fatalf("RemoveServer(b): %v", err)
	}
	if got := serverURLs(lb); !slices.Equal(got, []string{"http://c"}) {
		t.Fatalf("remaining servers = %v, want [http://c]", got)
	}
}

func TestRemoveServerReportsDrainTimeout(t *testing.T) {
	lb := newTestBalancer(t, "round_robin", "http://a")
	lb.drainTimeout = 10 * time.Millisecond
	lb.Servers[0].Load.Add(1)

	if err := lb.RemoveServer("http://a"); err == nil {
		t.Fatal("expected drain timeout error")
	}
	if got := serverURLs(lb); len(got) != 0 {
		t.Fatalf("server should be removed even on timeout, got %v", got)
	}
}

func TestRemoveServerNotFound(t *testing.T) {
	lb := newTestBalancer(t, "round_robin", "http://a")
	if err := lb.RemoveServer("http://missing"); err == nil {
		t.Fatal("expected not found error")
	}
}

// Run under -race: traffic, health checks and removal all touch routing state.
func TestRemoveServerReturnsUnderConstantTraffic(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(2 * time.Millisecond)
	}))
	t.Cleanup(backend.Close)
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(other.Close)

	lb := newTestBalancer(t, "least_active", backend.URL, other.URL)
	checked := lb.Servers[1]

	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				lb.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			checked.CheckHealth(context.Background())
		}
	}()
	t.Cleanup(func() {
		stop.Store(true)
		wg.Wait()
	})

	time.Sleep(20 * time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- lb.RemoveServer(backend.URL) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RemoveServer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RemoveServer did not return under constant traffic")
	}
}

func TestLeastActiveSpreadsTiesAcrossIdleServers(t *testing.T) {
	lb := newTestBalancer(t, "least_active", "http://a", "http://b", "http://c")

	hits := map[string]int{}
	for range 3 {
		hits[lb.GetServer().URL]++
	}
	if len(hits) != 3 {
		t.Fatalf("hits = %v, want each idle server once", hits)
	}
}

func TestLeastActivePicksLowestLoadAndSkipsUnhealthy(t *testing.T) {
	lb := newTestBalancer(t, "least_active", "http://a", "http://b", "http://c")
	lb.Servers[0].Load.Add(1)
	lb.Servers[1].Healthy.Store(false)

	for range 6 {
		if got := lb.GetServer().URL; got != "http://c" {
			t.Fatalf("selected %s, want http://c", got)
		}
	}
}

func TestRoundRobinOrderSkipsUnhealthy(t *testing.T) {
	lb := newTestBalancer(t, "round_robin", "http://a", "http://b", "http://c")

	urlsOf := func(n int) []string {
		var got []string
		for range n {
			got = append(got, lb.GetServer().URL)
		}
		return got
	}
	if got, want := urlsOf(4), []string{"http://a", "http://b", "http://c", "http://a"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}

	lb.Servers[1].Healthy.Store(false)
	for _, url := range urlsOf(4) {
		if url == "http://b" {
			t.Fatal("selected unhealthy server")
		}
	}
}

func TestHealthChecksStopWhenContextEnds(t *testing.T) {
	var hits atomic.Int32
	backend := newBackend(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) })
	lb := newTestBalancer(t, "round_robin", backend.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lb.StartHealthChecks(ctx, time.Millisecond)
	eventually(t, "checks to run", func() bool { return hits.Load() >= 2 })

	cancel()
	assertHitsStop(t, &hits)
	eventually(t, "checker goroutine to exit", func() bool {
		stacks := make([]byte, 1<<20)
		return !bytes.Contains(stacks[:runtime.Stack(stacks, true)], []byte("startHealthCheck"))
	})
}

func TestRemoveServerStopsItsHealthChecks(t *testing.T) {
	var removedHits, keptHits atomic.Int32
	removed := newBackend(t, func(http.ResponseWriter, *http.Request) { removedHits.Add(1) })
	kept := newBackend(t, func(http.ResponseWriter, *http.Request) { keptHits.Add(1) })
	lb := newTestBalancer(t, "round_robin", removed.URL, kept.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lb.StartHealthChecks(ctx, time.Millisecond)
	eventually(t, "checks to run", func() bool { return removedHits.Load() >= 2 })

	if err := lb.RemoveServer(removed.URL); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	assertHitsStop(t, &removedHits)

	before := keptHits.Load()
	eventually(t, "other server to stay checked", func() bool { return keptHits.Load() > before })
}

func TestServerAddedAfterStartIsChecked(t *testing.T) {
	backend := newBackend(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	lb := newTestBalancer(t, "round_robin")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lb.StartHealthChecks(ctx, time.Millisecond)

	if err := lb.AddServer(backend.URL); err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	eventually(t, "new server to be marked down", func() bool { return !lb.Servers[0].Healthy.Load() })
}

// assertHitsStop allows in-flight checks to land, then requires no new ones.
func assertHitsStop(t *testing.T, hits *atomic.Int32) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	settled := hits.Load()
	time.Sleep(50 * time.Millisecond)
	if got := hits.Load(); got != settled {
		t.Errorf("checks kept running: %d hits grew to %d", settled, got)
	}
}

func TestFailureClassification(t *testing.T) {
	dial := func(cause error) error { return fmt.Errorf("proxy: %w", &net.OpError{Op: "dial", Err: cause}) }
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}

	tests := []struct {
		name          string
		method        string
		upgrade       string
		err           error
		wantRetry     bool
		wantBackendDn bool
	}{
		{name: "refused POST", method: http.MethodPost, err: dial(syscall.ECONNREFUSED), wantRetry: true, wantBackendDn: true},
		{name: "refused upgrade", method: http.MethodGet, upgrade: "websocket", err: dial(syscall.ECONNREFUSED), wantRetry: true, wantBackendDn: true},
		{name: "dial timeout may be overload", method: http.MethodPost, err: dial(os.ErrDeadlineExceeded), wantRetry: true},
		{name: "no file descriptors is our problem", method: http.MethodPost, err: dial(syscall.EMFILE), wantRetry: true},
		{name: "no ports is our problem", method: http.MethodPost, err: dial(syscall.EADDRNOTAVAIL), wantRetry: true},
		{name: "reset GET", method: http.MethodGet, err: reset, wantRetry: true},
		{name: "reset POST", method: http.MethodPost, err: reset},
		{name: "failed upgrade may be hijacked", method: http.MethodGet, upgrade: "websocket", err: errors.New("response write: broken pipe")},
		{name: "unhealthy POST", method: http.MethodPost, err: sv.ErrUnhealthy, wantRetry: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, "/", nil)
			if tt.upgrade != "" {
				r.Header.Set("Upgrade", tt.upgrade)
			}
			if got := canRetry(r, tt.err); got != tt.wantRetry {
				t.Errorf("canRetry = %v, want %v", got, tt.wantRetry)
			}
			if got := provesBackendDown(tt.err); got != tt.wantBackendDn {
				t.Errorf("provesBackendDown = %v, want %v", got, tt.wantBackendDn)
			}
		})
	}
}

func TestEmptyBalancerSelectsNothing(t *testing.T) {
	for _, strategy := range []string{"round_robin", "least_active"} {
		lb := newTestBalancer(t, strategy)
		if got := lb.GetServer(); got != nil {
			t.Fatalf("%s: empty balancer selected %s", strategy, got.URL)
		}
	}
}
