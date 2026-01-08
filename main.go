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

	"loadbalancer/balancer"
	"loadbalancer/config"
	"loadbalancer/utils"
)

func main() {
	logger := log.New(os.Stdout, "load-balancer: ", log.LstdFlags)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("Error loading configuration: %v\n", err)
	}

	lb := balancer.NewLoadBalancer(logger, cfg.LoadBalancer.Strategy)
	for _, url := range cfg.Servers.URLs {
		lb.AddServer(url)
	}

	lb.StartHealthChecks(time.Duration(cfg.LoadBalancer.HealthCheckIntervalSeconds) * time.Second)

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics := lb.GetMetrics()
		json.NewEncoder(w).Encode(metrics)
	})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		lb.GracefulShutdown()
		os.Exit(0)
	}()

	port := cfg.LoadBalancer.Port
	logger.Println(utils.Colorize(fmt.Sprintf("Load balancer is running on port %d", port), utils.GREEN))
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), lb); err != nil {
		logger.Fatalf("Load balancer failed: %v\n", err)
	}
}
