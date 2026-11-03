package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/itsfuad/octoload/utils"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// ErrUnhealthy means a request was refused before any backend I/O.
var ErrUnhealthy = errors.New("server is not healthy")

// unhealthyAfter is how many failed checks in a row mark a backend down, so one
// slow check does not eject it. One success restores it.
const unhealthyAfter = 2

// transport is immutable shared config; connection pools are kept per backend host.
var transport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil // Backend traffic must not leave through an HTTP_PROXY from the environment.
	t.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	t.ResponseHeaderTimeout = 30 * time.Second // Bounds hung backends without capping long responses.
	t.MaxIdleConns = 0                         // The default total of 100 would cap the per-host limit.
	t.MaxIdleConnsPerHost = 100                // The default of 2 churns connections under load.
	return t
}()

// Options configures a Server. Zero values select the defaults.
type Options struct {
	// TrustForwardedHeaders keeps inbound X-Forwarded-* values. Enable only behind
	// a trusted edge: otherwise clients can spoof their IP and scheme.
	TrustForwardedHeaders bool
	// HealthCheckPath is requested by CheckHealth. Default "/".
	HealthCheckPath string
	// HealthCheckTimeout bounds each check. Default 2s.
	HealthCheckTimeout time.Duration
}

type Server struct {
	URL          string
	Load         atomic.Int64
	Healthy      atomic.Bool
	options      Options
	healthURL    string
	healthClient *http.Client
	failedChecks atomic.Int32
	target       *url.URL
	logger       *log.Logger
}

// ParseURL parses and validates a backend URL.
func ParseURL(rawURL string) (*url.URL, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL %q: %w", rawURL, err)
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.RawQuery != "" {
		return nil, fmt.Errorf("invalid server URL %q: want http(s)://host[:port][/path]", rawURL)
	}
	return target, nil
}

// ValidateHealthCheckPath accepts "" (the default) or a path starting with "/".
func ValidateHealthCheckPath(path string) error {
	if path != "" && !strings.HasPrefix(path, "/") {
		return fmt.Errorf("invalid health check path %q: must start with /", path)
	}
	return nil
}

func NewServer(rawURL string, logger *log.Logger, options Options) (*Server, error) {
	target, err := ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	if err := ValidateHealthCheckPath(options.HealthCheckPath); err != nil {
		return nil, err
	}

	if options.HealthCheckPath == "" {
		options.HealthCheckPath = "/"
	}
	if options.HealthCheckTimeout == 0 {
		options.HealthCheckTimeout = 2 * time.Second
	}

	s := &Server{
		URL:          rawURL,
		options:      options,
		healthURL:    strings.TrimSuffix(rawURL, "/") + options.HealthCheckPath,
		healthClient: &http.Client{Transport: transport, Timeout: options.HealthCheckTimeout},
		target:       target,
		logger:       logger,
	}
	s.Healthy.Store(true)
	return s, nil
}

// HandleRequest proxies r to this backend. An error means no response has
// started, so the caller may write to w or retry. The exception is a failed
// protocol upgrade, which may have hijacked the connection: never retry those. A
// response that dies midway aborts the client connection by panicking with
// http.ErrAbortHandler, so per-request cleanup must use defer.
func (s *Server) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	if !s.Healthy.Load() {
		return fmt.Errorf("%w: %s", ErrUnhealthy, s.URL)
	}
	s.Load.Add(1)
	defer s.Load.Add(-1)

	// Without this, net/http drains the unread request body once the response
	// starts, which truncates uploads to backends that reply while still reading.
	// It is unsupported on HTTP/2, which is full duplex anyway.
	_ = http.NewResponseController(w).EnableFullDuplex()

	var proxyErr error
	proxy := httputil.ReverseProxy{
		Rewrite:      s.rewrite,
		Transport:    transport,
		ErrorLog:     s.logger,
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) { proxyErr = err },
	}
	proxy.ServeHTTP(w, r)
	return proxyErr
}

func (s *Server) rewrite(pr *httputil.ProxyRequest) {
	pr.SetURL(s.target)
	pr.Out.Host = pr.In.Host // Vhost routing and backend-built redirects need the client's Host.
	// ReverseProxy may re-encode the query, which breaks signed URLs. Nothing here
	// routes on the query, so forwarding it raw cannot cause a parsing mismatch.
	pr.Out.URL.RawQuery = pr.In.URL.RawQuery

	if !s.options.TrustForwardedHeaders {
		pr.SetXForwarded()
		return
	}
	pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"] // SetXForwarded appends the peer to this.
	pr.SetXForwarded()
	for _, name := range []string{"X-Forwarded-Host", "X-Forwarded-Proto"} {
		if value := pr.In.Header.Get(name); value != "" {
			pr.Out.Header.Set(name, value)
		}
	}
}

// CheckHealth probes the backend once and updates Healthy. A cancelled ctx says
// nothing about the backend, so it changes nothing.
func (s *Server) CheckHealth(ctx context.Context) {
	err := s.probe(ctx)
	if ctx.Err() != nil {
		return
	}

	if err == nil {
		s.failedChecks.Store(0)
		if !s.Healthy.Swap(true) {
			s.logger.Println(utils.Colorize(fmt.Sprintf("Server %s recovered", s.URL), utils.GREEN))
		}
		return
	}
	if s.failedChecks.Add(1) >= unhealthyAfter && s.Healthy.Swap(false) {
		s.logger.Println(utils.Colorize(fmt.Sprintf("Server %s is unhealthy: %v", s.URL, err), utils.RED))
	}
}

func (s *Server) probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := s.healthClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain a little so small responses can reuse the connection.
	io.CopyN(io.Discard, resp.Body, 64<<10)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("health check returned status %d", resp.StatusCode)
	}
	return nil
}
