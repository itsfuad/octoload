package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	LoadBalancer struct {
		Port                       int    `json:"port"`
		HealthCheckIntervalSeconds int    `json:"health_check_interval_seconds"`
		Strategy                   string `json:"strategy"` // "least_active" or "round_robin"
		// TrustForwardedHeaders keeps inbound X-Forwarded-*. Enable only behind a trusted edge.
		TrustForwardedHeaders bool `json:"trust_forwarded_headers"`
		// ShutdownDelaySeconds keeps serving after a stop signal so external load
		// balancers can stop routing here. ShutdownTimeoutSeconds then bounds the drain.
		ShutdownDelaySeconds   int `json:"shutdown_delay_seconds"`
		ShutdownTimeoutSeconds int `json:"shutdown_timeout_seconds"`
	} `json:"load_balancer"`
	Servers struct {
		URLs []string `json:"urls"`
	} `json:"servers"`
}

func LoadConfig(filenames ...string) (*Config, error) {

	filename := "servers.json"
	if len(filenames) > 0 {
		filename = filenames[0]
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{}
	// Set before decoding so an explicit 0 in the file stays distinct from a missing field.
	config.LoadBalancer.ShutdownDelaySeconds = 5
	config.LoadBalancer.ShutdownTimeoutSeconds = 30
	if err := json.NewDecoder(file).Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}
