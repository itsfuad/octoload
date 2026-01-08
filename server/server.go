package server

import (
	"bytes"
	"fmt"
	"io"
	"loadbalancer/utils"
	"log"
	"net/http"
	"sync"
	"time"
)

// client is immutable shared config; connection pooling comes from DefaultTransport.
var client = &http.Client{Timeout: 30 * time.Second}

type Server struct {
	URL     string
	Load    int
	Healthy bool
	mu      sync.RWMutex
	logger  *log.Logger
}

func NewServer(url string, logger *log.Logger) *Server {
	return &Server{
		URL:     url,
		Healthy: true,
		logger:  logger,
	}
}

func (s *Server) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	s.mu.Lock()
	if !s.Healthy {
		s.mu.Unlock()
		return fmt.Errorf("server %s is not healthy", s.URL)
	}
	s.Load++
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.Load--
		s.mu.Unlock()
	}()

	var bodyBytes []byte
	var err error
	if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("failed to read request body: %v", err)
		}
		// Restore body so the balancer can retry on another server.
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	req, err := http.NewRequest(r.Method, s.URL+r.RequestURI, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header = r.Header.Clone()

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	utils.CopyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	if _, err = io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("failed to copy response: %v", err)
	}

	return nil
}

func (s *Server) CheckHealth() {
	resp, err := http.Get(s.URL + "/")
	if err == nil {
		resp.Body.Close()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil || resp.StatusCode != http.StatusOK {
		s.Healthy = false
		s.logger.Println(utils.Colorize(fmt.Sprintf("Server %s is unhealthy\n", s.URL), utils.RED))
	} else {
		s.Healthy = true
	}
}
