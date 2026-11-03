package server

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

const URL = "http://example.com"

func TestNewServer(t *testing.T) {
	logger := log.New(io.Discard, "", log.LstdFlags)
	server := NewServer(URL, logger)

	if server.URL != URL {
		t.Errorf("expected URL to be 'http://example.com', got %s", server.URL)
	}
	if !server.Healthy.Load() {
		t.Errorf("expected server to be healthy")
	}
	if server.logger != logger {
		t.Errorf("expected logger to be set")
	}
}

func TestHandleRequest(t *testing.T) {
	logger := log.New(io.Discard, "", log.LstdFlags)
	server := NewServer(URL, logger)

	req := httptest.NewRequest(http.MethodGet, URL, nil)
	w := httptest.NewRecorder()

	err := server.HandleRequest(w, req)
	if err == nil {
		t.Errorf("expected error due to server being unreachable")
	}

	server.Healthy.Store(false)
	err = server.HandleRequest(w, req)
	if err == nil {
		t.Errorf("expected error due to server being unhealthy")
	}
}

func TestCheckHealth(t *testing.T) {
	logger := log.New(io.Discard, "", log.LstdFlags)
	server := NewServer("http://example.com", logger)

	server.CheckHealth()
	if !server.Healthy.Load() {
		t.Errorf("expected server to be healthy")
	}
}
