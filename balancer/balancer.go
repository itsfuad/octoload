package balancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sv "github.com/itsfuad/octoload/server"
	"github.com/itsfuad/octoload/utils"
)

type LoadBalancer struct {
	Servers []*sv.Server
	mu      sync.RWMutex
	Logger  *log.Logger
	// ServerOptions applies to servers added after it is set.
	ServerOptions sv.Options
	metrics       *Metrics
	strategy      string
	rotation      atomic.Uint64
	drainTimeout  time.Duration
	// healthCtx is set by StartHealthChecks so servers added later join the checks.
	healthCtx        context.Context
	healthInterval   time.Duration
	stopHealthChecks map[string]context.CancelFunc
}

// Load balancing strategies. An empty strategy means StrategyLeastActive.
const (
	StrategyRoundRobin  = "round_robin"
	StrategyLeastActive = "least_active"
)

const (
	maxRetries          = 3
	defaultDrainTimeout = 30 * time.Second
)

type Metrics struct {
	TotalRequests     uint64
	FailedRequests    uint64
	ActiveConnections int64
}

// ValidateStrategy rejects strategies that would otherwise silently act as the default.
func ValidateStrategy(strategy string) error {
	switch strategy {
	case "", StrategyRoundRobin, StrategyLeastActive:
		return nil
	}
	return fmt.Errorf("unknown strategy %q: want %s or %s", strategy, StrategyRoundRobin, StrategyLeastActive)
}

func NewLoadBalancer(logger *log.Logger, strategy string) (*LoadBalancer, error) {
	if err := ValidateStrategy(strategy); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.New(io.Discard, "", log.LstdFlags)
	}
	return &LoadBalancer{
		Logger:           logger,
		metrics:          &Metrics{},
		strategy:         strategy,
		drainTimeout:     defaultDrainTimeout,
		stopHealthChecks: map[string]context.CancelFunc{},
	}, nil
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

	server, err := sv.NewServer(url, lb.Logger, lb.ServerOptions)
	if err != nil {
		return err
	}
	lb.Servers = append(lb.Servers, server)
	if lb.healthCtx != nil {
		lb.startHealthCheck(server)
	}
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
	if stop, ok := lb.stopHealthChecks[url]; ok {
		stop()
		delete(lb.stopHealthChecks, url)
	}
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
	if lb.strategy == StrategyRoundRobin {
		return lb.GetRoundRobinServer()
	}
	return lb.GetLeastLoadedServer()
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&lb.metrics.TotalRequests, 1)
	atomic.AddInt64(&lb.metrics.ActiveConnections, 1)
	defer atomic.AddInt64(&lb.metrics.ActiveConnections, -1)

	// A response that dies midway surfaces as a panic that net/http handles.
	defer func() {
		if p := recover(); p != nil {
			if r.Context().Err() == nil {
				atomic.AddUint64(&lb.metrics.FailedRequests, 1)
			}
			panic(p)
		}
	}()

	// A failed dial closes the body on Go 1.23, where the stdlib does not guard it. Keep it open for a retry.
	if r.Body != nil {
		r.Body = io.NopCloser(r.Body)
	}

	status := http.StatusServiceUnavailable
	for retry := range maxRetries {
		server := lb.GetServer()
		if server == nil {
			break
		}

		err := server.HandleRequest(w, r)
		if err == nil {
			return
		}
		if r.Context().Err() != nil {
			return // Client is gone: not a backend failure, and nobody to answer.
		}

		lb.Logger.Printf("Request failed on server %s, attempt %d: %v", server.URL, retry+1, err)
		status = http.StatusBadGateway
		if provesBackendDown(err) {
			server.Healthy.Store(false)
		}
		if !canRetry(r, err) {
			break
		}
	}

	atomic.AddUint64(&lb.metrics.FailedRequests, 1)
	http.Error(w, http.StatusText(status), status)
}

// canRetry reports whether another attempt is safe: it must never repeat a
// request that a backend may have acted on, or need a body already consumed.
func canRetry(r *http.Request, err error) bool {
	// The request never left, so any method is safe and the body is still unread.
	var opErr *net.OpError
	if errors.Is(err, sv.ErrUnhealthy) || (errors.As(err, &opErr) && opErr.Op == "dial") {
		return true
	}
	// A failed upgrade may have already hijacked the client connection.
	if r.Header.Get("Upgrade") != "" || r.ContentLength != 0 {
		return false
	}
	return r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions
}

// provesBackendDown reports whether err alone justifies ejecting a backend until
// its next health check. A refused connection means nothing is listening. Resets
// and dial timeouts can be overload, and running out of file descriptors or ports
// is the balancer's own problem: ejecting on those could cascade into an outage.
func provesBackendDown(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

// StartHealthChecks checks every server now and each interval until ctx is done,
// including servers added later. Call it once. Without it, nothing restores a
// backend that was ejected after refusing a connection.
func (lb *LoadBalancer) StartHealthChecks(ctx context.Context, interval time.Duration) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.healthCtx, lb.healthInterval = ctx, interval
	for _, server := range lb.Servers {
		lb.startHealthCheck(server)
	}
}

// startHealthCheck requires lb.mu held for writing.
func (lb *LoadBalancer) startHealthCheck(server *sv.Server) {
	ctx, stop := context.WithCancel(lb.healthCtx)
	lb.stopHealthChecks[server.URL] = stop
	interval := lb.healthInterval

	go func() {
		for {
			server.CheckHealth(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
}
