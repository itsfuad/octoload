package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itsfuad/octoload/balancer"
	"github.com/itsfuad/octoload/config"
	"github.com/itsfuad/octoload/utils"
)

func main() {
	logger := log.New(os.Stdout, "load-balancer: ", log.LstdFlags)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("Error loading configuration: %v\n", err)
	}

	lb := balancer.NewLoadBalancer(logger, cfg.LoadBalancer.Strategy)
	lb.TrustForwardedHeaders = cfg.LoadBalancer.TrustForwardedHeaders
	for _, url := range cfg.Servers.URLs {
		if err := lb.AddServer(url); err != nil {
			logger.Fatalf("Error adding server: %v\n", err)
		}
	}

	lb.StartHealthChecks(time.Duration(cfg.LoadBalancer.HealthCheckIntervalSeconds) * time.Second)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		lb.GracefulShutdown()
		os.Exit(0)
	}()

	port := cfg.LoadBalancer.Port
	logger.Println(utils.Colorize(fmt.Sprintf("Load balancer is running on port %d", port), utils.GREEN))
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), newMux(lb)); err != nil {
		logger.Fatalf("Load balancer failed: %v\n", err)
	}
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
