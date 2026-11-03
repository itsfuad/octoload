package server

import (
	"errors"
	"fmt"
	"github.com/itsfuad/octoload/utils"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

// ErrUnhealthy means a request was refused before any backend I/O.
var ErrUnhealthy = errors.New("server is not healthy")

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

type Server struct {
	URL     string
	Load    atomic.Int64
	Healthy atomic.Bool
	// TrustForwardedHeaders keeps inbound X-Forwarded-* values. Enable only behind
	// a trusted edge: otherwise clients can spoof their IP and scheme.
	TrustForwardedHeaders bool
	target                *url.URL
	logger                *log.Logger
}

func NewServer(rawURL string, logger *log.Logger) (*Server, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL %q: %w", rawURL, err)
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.RawQuery != "" {
		return nil, fmt.Errorf("invalid server URL %q: want http(s)://host[:port][/path]", rawURL)
	}

	s := &Server{URL: rawURL, target: target, logger: logger}
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

	if !s.TrustForwardedHeaders {
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

func (s *Server) CheckHealth() {
	resp, err := http.Get(s.URL + "/")
	if err == nil {
		resp.Body.Close()
	}

	if err != nil || resp.StatusCode != http.StatusOK {
		s.Healthy.Store(false)
		s.logger.Println(utils.Colorize(fmt.Sprintf("Server %s is unhealthy\n", s.URL), utils.RED))
	} else {
		s.Healthy.Store(true)
	}
}
