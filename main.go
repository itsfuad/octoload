package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/itsfuad/octoload/balancer"
	"github.com/itsfuad/octoload/config"
	sv "github.com/itsfuad/octoload/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	configPath := flag.String("config", "servers.json", "path to the configuration file")
	flag.Parse()

	if err := run(*configPath, logger); err != nil {
		logger.Error("load balancer failed", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}

	lb, err := balancer.NewLoadBalancer(logger, cfg.LoadBalancer.Strategy)
	if err != nil {
		return err
	}
	lb.ServerOptions = sv.Options{
		TrustForwardedHeaders: cfg.LoadBalancer.TrustForwardedHeaders,
		HealthCheckPath:       cfg.LoadBalancer.HealthCheckPath,
		HealthCheckTimeout:    time.Duration(cfg.LoadBalancer.HealthCheckTimeoutSeconds) * time.Second,
	}
	for _, url := range cfg.Servers.URLs {
		if err := lb.AddServer(url); err != nil {
			return err
		}
	}

	// Not tied to the stop signal: routing needs health data until the drain ends.
	lb.StartHealthChecks(context.Background(), time.Duration(cfg.LoadBalancer.HealthCheckIntervalSeconds)*time.Second)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.LoadBalancer.Port))
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var draining atomic.Bool
	context.AfterFunc(ctx, func() {
		stop() // Restore default handling so a second signal force quits a slow drain.
		draining.Store(true)
		logger.Info("shutting down, signal again to force")
	})

	if cfg.LoadBalancer.AdminPort != 0 {
		adminListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.LoadBalancer.AdminPort))
		if err != nil {
			return err
		}
		// Outlives the drain so probes can see it. Stopped by the defers below.
		adminCtx, stopAdmin := context.WithCancel(context.Background())
		adminDone := make(chan struct{})
		go func() {
			defer close(adminDone)
			if err := serve(adminCtx, adminListener, newAdminMux(lb, &draining), 0, time.Second); err != nil {
				logger.Error("admin server failed", "error", err)
			}
		}()
		defer func() {
			stopAdmin()
			<-adminDone
		}()
	}

	logger.Info("listening", "port", cfg.LoadBalancer.Port, "admin_port", cfg.LoadBalancer.AdminPort, "strategy", cfg.LoadBalancer.Strategy)
	shutdownDelay := time.Duration(cfg.LoadBalancer.ShutdownDelaySeconds) * time.Second
	shutdownTimeout := time.Duration(cfg.LoadBalancer.ShutdownTimeoutSeconds) * time.Second
	if err := serve(ctx, listener, lb, shutdownDelay, shutdownTimeout); err != nil {
		return err
	}
	logger.Info("all connections closed")
	return nil
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

// newAdminMux serves operations endpoints, separate from proxied traffic so they
// neither shadow backend paths nor need to be reachable by clients.
func newAdminMux(lb *balancer.LoadBalancer, draining *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(lb.GetMetrics())
	})
	// Liveness ignores backends: an outage behind the balancer must not restart it.
	mux.HandleFunc("GET /livez", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		switch {
		case draining.Load():
			http.Error(w, "draining", http.StatusServiceUnavailable)
		case !slices.ContainsFunc(lb.GetMetrics().Backends, func(b balancer.BackendMetrics) bool { return b.Healthy }):
			http.Error(w, "no healthy backends", http.StatusServiceUnavailable)
		}
	})
	return mux
}
