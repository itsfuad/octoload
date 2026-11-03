package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validConfig = `{
	"load_balancer": {"port": 8080, "health_check_interval_seconds": 5, "strategy": "round_robin"%s},
	"servers": {"urls": ["http://a:1", "https://b/api"]}
}`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "servers.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func validWith(extra string) string {
	return strings.Replace(validConfig, "%s", extra, 1)
}

func TestLoadConfigShutdownSettings(t *testing.T) {
	tests := []struct {
		name                string
		settings            string
		wantDelay, wantWait int
	}{
		{"defaults when missing", ``, 5, 30},
		{"explicit values", `, "shutdown_delay_seconds": 1, "shutdown_timeout_seconds": 2`, 1, 2},
		{"explicit zero delay is kept", `, "shutdown_delay_seconds": 0`, 0, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeConfig(t, validWith(tt.settings)))
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if got := cfg.LoadBalancer.ShutdownDelaySeconds; got != tt.wantDelay {
				t.Errorf("delay = %d, want %d", got, tt.wantDelay)
			}
			if got := cfg.LoadBalancer.ShutdownTimeoutSeconds; got != tt.wantWait {
				t.Errorf("timeout = %d, want %d", got, tt.wantWait)
			}
		})
	}
}

func TestLoadConfigRejectsBadFiles(t *testing.T) {
	tests := []struct {
		name, path, want string
	}{
		{"missing file", filepath.Join(t.TempDir(), "missing.json"), "reading config"},
		{"malformed JSON", writeConfig(t, `{`), "parsing config"},
		{"misspelled option", writeConfig(t, validWith(`, "stratgy": "x"`)), `unknown field "stratgy"`},
		{"invalid values", writeConfig(t, validWith(`, "port": 0`)), "port must be"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := LoadConfig(tt.path); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
		want   string // Empty means valid.
	}{
		{"unchanged", func(*Config) {}, ""},
		{"empty strategy means default", func(c *Config) { c.LoadBalancer.Strategy = "" }, ""},
		{"zero shutdown delay", func(c *Config) { c.LoadBalancer.ShutdownDelaySeconds = 0 }, ""},
		{"lowest port", func(c *Config) { c.LoadBalancer.Port = 1 }, ""},
		{"highest port", func(c *Config) { c.LoadBalancer.Port = 65535 }, ""},
		{"health check path", func(c *Config) { c.LoadBalancer.HealthCheckPath = "/healthz" }, ""},
		{"zero port", func(c *Config) { c.LoadBalancer.Port = 0 }, "port must be"},
		{"port too high", func(c *Config) { c.LoadBalancer.Port = 65536 }, "port must be"},
		{"admin port", func(c *Config) { c.LoadBalancer.AdminPort = 8081 }, ""},
		{"negative admin port", func(c *Config) { c.LoadBalancer.AdminPort = -1 }, "admin_port must be"},
		{"admin port too high", func(c *Config) { c.LoadBalancer.AdminPort = 65536 }, "admin_port must be"},
		{"admin port same as port", func(c *Config) { c.LoadBalancer.AdminPort = 8080 }, "admin_port must differ"},
		{"zero interval", func(c *Config) { c.LoadBalancer.HealthCheckIntervalSeconds = 0 }, "health_check_interval_seconds"},
		{"negative check timeout", func(c *Config) { c.LoadBalancer.HealthCheckTimeoutSeconds = -1 }, "health_check_timeout_seconds"},
		{"negative shutdown delay", func(c *Config) { c.LoadBalancer.ShutdownDelaySeconds = -1 }, "shutdown_delay_seconds"},
		{"zero shutdown timeout", func(c *Config) { c.LoadBalancer.ShutdownTimeoutSeconds = 0 }, "shutdown_timeout_seconds"},
		{"misspelled strategy", func(c *Config) { c.LoadBalancer.Strategy = "roundrobin" }, "unknown strategy"},
		{"relative health path", func(c *Config) { c.LoadBalancer.HealthCheckPath = "healthz" }, "health check path"},
		{"no servers", func(c *Config) { c.Servers.URLs = nil }, "servers.urls"},
		{"bad server URL", func(c *Config) { c.Servers.URLs = []string{"localhost:8001"} }, "invalid server URL"},
		{"duplicate server URL", func(c *Config) { c.Servers.URLs = []string{"http://a", "http://a"} }, "duplicate server URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeConfig(t, validWith("")))
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			tt.change(cfg)

			err = cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	var cfg Config
	cfg.LoadBalancer.Strategy = "x"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"port must be", "health_check_interval_seconds", "shutdown_timeout_seconds", "servers.urls", "unknown strategy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

func TestSampleConfigIsValid(t *testing.T) {
	if _, err := LoadConfig("../servers.json"); err != nil {
		t.Fatalf("servers.json: %v", err)
	}
}
