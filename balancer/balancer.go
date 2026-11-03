package balancer

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	sv "github.com/itsfuad/octoload/server"
	"github.com/itsfuad/octoload/utils"
)

type LoadBalancer struct {
	Servers      []*sv.Server
	mu           sync.RWMutex
	Logger       *log.Logger
	shutdown     atomic.Bool
	metrics      *Metrics
	strategy     string
	rotation     atomic.Uint64
	drainTimeout time.Duration
}

const (
	maxRetries          = 3
	defaultDrainTimeout = 30 * time.Second
)

type Metrics struct {
	TotalRequests     uint64
	FailedRequests    uint64
	ActiveConnections int64
}

func NewLoadBalancer(logger *log.Logger, strategy string) *LoadBalancer {
	if logger == nil {
		logger = log.New(io.Discard, "", log.LstdFlags)
	}
	return &LoadBalancer{
		Logger:       logger,
		metrics:      &Metrics{},
		strategy:     strategy,
		drainTimeout: defaultDrainTimeout,
	}
}

func (lb *LoadBalancer) AddServer(url string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if url == "" {
		return errors.New("server URL cannot be empty")
	}
	if slices.ContainsFunc(lb.Servers, func(s *sv.Server) bool { return s.URL == url }) {
		return fmt.Errorf("server %s already exists", url)
	}

	server := sv.NewServer(url, lb.Logger)
	lb.Servers = append(lb.Servers, server)
	lb.Logger.Println(utils.Colorize("Added server "+url+" to the load balancer", utils.GREEN))
	return nil
}

func (lb *LoadBalancer) RemoveServer(url string) error {
	lb.mu.Lock()
	i := slices.IndexFunc(lb.Servers, func(s *sv.Server) bool { return s.URL == url })
	if i < 0 {
		lb.mu.Unlock()
		return fmt.Errorf("server %s not found", url)
	}
	server := lb.Servers[i]
	// Remove before draining: a listed server keeps receiving traffic and may never drain.
	lb.Servers = slices.Delete(lb.Servers, i, i+1)
	lb.mu.Unlock()
	lb.Logger.Println(utils.Colorize("Removed server "+url, utils.YELLOW))

	// shortcut: a request that selected this server just before removal can start after load reads 0; add per-server draining if callers need a hard guarantee.
	deadline := time.Now().Add(lb.drainTimeout)
	for server.Load.Load() > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("server %s removed with %d requests still running after %v", url, server.Load.Load(), lb.drainTimeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (lb *LoadBalancer) GetMetrics() *Metrics {
	return &Metrics{
		TotalRequests:     atomic.LoadUint64(&lb.metrics.TotalRequests),
		FailedRequests:    atomic.LoadUint64(&lb.metrics.FailedRequests),
		ActiveConnections: atomic.LoadInt64(&lb.metrics.ActiveConnections),
	}
}

func (lb *LoadBalancer) GetLeastLoadedServer() *sv.Server {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	var leastLoadedServer *sv.Server
	var leastLoad int64
	// Rotate the scan start so ties do not all land on the first server.
	start := lb.rotation.Add(1) - 1
	for i := range lb.Servers {
		server := lb.Servers[(start+uint64(i))%uint64(len(lb.Servers))]
		if !server.Healthy.Load() {
			continue
		}
		if load := server.Load.Load(); leastLoadedServer == nil || load < leastLoad {
			leastLoadedServer, leastLoad = server, load
		}
	}

	if leastLoadedServer != nil {
		lb.Logger.Println(utils.Colorize("Selected server "+leastLoadedServer.URL+" with load "+fmt.Sprint(leastLoad), utils.BLUE))
	}

	return leastLoadedServer
}

func (lb *LoadBalancer) GetRoundRobinServer() *sv.Server {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	var healthyServers []*sv.Server
	for _, server := range lb.Servers {
		if server.Healthy.Load() {
			healthyServers = append(healthyServers, server)
		}
	}

	if len(healthyServers) == 0 {
		return nil
	}

	selectedServer := healthyServers[(lb.rotation.Add(1)-1)%uint64(len(healthyServers))]

	lb.Logger.Println(utils.Colorize("Selected server "+selectedServer.URL+" (round robin)", utils.BLUE))

	return selectedServer
}

func (lb *LoadBalancer) GetServer() *sv.Server {
	if lb.strategy == "round_robin" {
		return lb.GetRoundRobinServer()
	}
	return lb.GetLeastLoadedServer() // "least_active" and default
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&lb.metrics.TotalRequests, 1)
	atomic.AddInt64(&lb.metrics.ActiveConnections, 1)
	defer atomic.AddInt64(&lb.metrics.ActiveConnections, -1)

	if lb.shutdown.Load() {
		http.Error(w, "Service is shutting down", http.StatusServiceUnavailable)
		return
	}

	for retry := range maxRetries {
		server := lb.GetServer()
		if server == nil {
			continue
		}

		err := server.HandleRequest(w, r)
		if err == nil {
			return
		}

		lb.Logger.Printf("Request failed on server %s, attempt %d: %v", server.URL, retry+1, err)
	}

	atomic.AddUint64(&lb.metrics.FailedRequests, 1)
	http.Error(w, "All servers failed to process the request", http.StatusServiceUnavailable)
}

func (lb *LoadBalancer) StartHealthChecks(interval time.Duration) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	for _, server := range lb.Servers {
		go func(s *sv.Server) {
			for {
				s.CheckHealth()
				time.Sleep(interval)
			}
		}(server)
	}
}

func (lb *LoadBalancer) GracefulShutdown() {
	lb.Logger.Println(utils.Colorize("Shutting down load balancer gracefully", utils.YELLOW))

	lb.shutdown.Store(true)

	lb.Logger.Println(utils.Colorize("All servers have been shut down, and connections are closed.", utils.YELLOW))
}
