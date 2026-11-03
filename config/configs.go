package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/itsfuad/octoload/balancer"
	sv "github.com/itsfuad/octoload/server"
)

type Config struct {
	LoadBalancer struct {
		Port                       int    `json:"port"`
		AdminPort                  int    `json:"admin_port"` // Serves /metrics, /livez and /readyz. 0 disables.
		HealthCheckIntervalSeconds int    `json:"health_check_interval_seconds"`
		HealthCheckPath            string `json:"health_check_path"`            // Default "/"
		HealthCheckTimeoutSeconds  int    `json:"health_check_timeout_seconds"` // Default 2
		Strategy                   string `json:"strategy"`                     // "least_active" or "round_robin"
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

// LoadConfig reads and validates the configuration file at path.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	defer file.Close()

	config := &Config{}
	// Set before decoding so an explicit 0 in the file stays distinct from a missing field.
	config.LoadBalancer.ShutdownDelaySeconds = 5
	config.LoadBalancer.ShutdownTimeoutSeconds = 30

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields() // A misspelled option must not silently keep its default.
	if err := decoder.Decode(config); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}
	// Decode stops after one value, so settings in leftover text would silently do nothing.
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("parsing config %s: unexpected data after the JSON object", path)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s:\n%w", path, err)
	}
	return config, nil
}

// Validate reports every problem at once, so fixing a config takes one pass.
func (c *Config) Validate() error {
	var errs []error
	require := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	lb := &c.LoadBalancer
	require(lb.Port >= 1 && lb.Port <= 65535, "port must be 1-65535, got %d", lb.Port)
	require(lb.AdminPort >= 0 && lb.AdminPort <= 65535, "admin_port must be 0-65535, got %d", lb.AdminPort)
	require(lb.AdminPort == 0 || lb.AdminPort != lb.Port, "admin_port must differ from port, both are %d", lb.Port)
	require(lb.HealthCheckIntervalSeconds > 0, "health_check_interval_seconds must be positive, got %d", lb.HealthCheckIntervalSeconds)
	require(lb.HealthCheckTimeoutSeconds >= 0, "health_check_timeout_seconds must not be negative, got %d", lb.HealthCheckTimeoutSeconds)
	require(lb.ShutdownDelaySeconds >= 0, "shutdown_delay_seconds must not be negative, got %d", lb.ShutdownDelaySeconds)
	require(lb.ShutdownTimeoutSeconds > 0, "shutdown_timeout_seconds must be positive, got %d", lb.ShutdownTimeoutSeconds)
	require(len(c.Servers.URLs) > 0, "servers.urls must not be empty")
	errs = append(errs, balancer.ValidateStrategy(lb.Strategy), sv.ValidateHealthCheckPath(lb.HealthCheckPath))

	seen := map[string]bool{}
	for _, url := range c.Servers.URLs {
		_, err := sv.ParseURL(url)
		errs = append(errs, err)
		require(err != nil || !seen[url], "duplicate server URL %q", url) // Only valid URLs are safe to print.
		seen[url] = true
	}
	return errors.Join(errs...)
}
