package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itsfuad/octoload/balancer"
	"github.com/itsfuad/octoload/config"
	sv "github.com/itsfuad/octoload/server"
	"github.com/itsfuad/octoload/utils"
)

func main() {
	logger := log.New(os.Stdout, "load-balancer: ", log.LstdFlags)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("Error loading configuration: %v\n", err)
	}

	lb := balancer.NewLoadBalancer(logger, cfg.LoadBalancer.Strategy)
	lb.ServerOptions = sv.Options{
		TrustForwardedHeaders: cfg.LoadBalancer.TrustForwardedHeaders,
		HealthCheckPath:       cfg.LoadBalancer.HealthCheckPath,
		HealthCheckTimeout:    time.Duration(cfg.LoadBalancer.HealthCheckTimeoutSeconds) * time.Second,
	}
	for _, url := range cfg.Servers.URLs {
		if err := lb.AddServer(url); err != nil {
			logger.Fatalf("Error adding server: %v\n", err)
		}
	}

	// Not tied to the stop signal: routing needs health data until the drain ends.
	lb.StartHealthChecks(context.Background(), time.Duration(cfg.LoadBalancer.HealthCheckIntervalSeconds)*time.Second)

	port := cfg.LoadBalancer.Port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("Load balancer failed: %v\n", err)
	}
	logger.Println(utils.Colorize(fmt.Sprintf("Load balancer is running on port %d", port), utils.GREEN))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, func() {
		stop() // Restore default handling so a second signal force quits a slow drain.
		logger.Println(utils.Colorize("Shutting down load balancer gracefully (signal again to force)", utils.YELLOW))
	})

	shutdownDelay := time.Duration(cfg.LoadBalancer.ShutdownDelaySeconds) * time.Second
	shutdownTimeout := time.Duration(cfg.LoadBalancer.ShutdownTimeoutSeconds) * time.Second
	if err := serve(ctx, listener, newMux(lb), shutdownDelay, shutdownTimeout); err != nil {
		logger.Fatalf("Load balancer failed: %v\n", err)
	}
	logger.Println(utils.Colorize("All connections are closed.", utils.YELLOW))
}

// serve handles requests until ctx is done, keeps serving for delay so external
// load balancers can stop routing here, then drains in-flight requests for at
// most timeout. Hijacked connections such as WebSockets are not waited for.
func serve(ctx context.Context, listener net.Listener, handler http.Handler, delay, timeout time.Duration) error {
	// No ReadTimeout or WriteTimeout: they would cut off long uploads and streaming responses.
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	defer srv.Close()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	time.Sleep(delay)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("draining connections: %w", err)
	}
	return nil
}

// newMux serves /metrics locally and proxies everything else through lb.
func newMux(lb *balancer.LoadBalancer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(lb.GetMetrics())
	})
	mux.Handle("/", lb)
	return mux
}
