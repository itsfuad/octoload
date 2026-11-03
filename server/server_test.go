package server

import (
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

const URL = "http://example.com"

func TestNewServer(t *testing.T) {
	logger := log.New(io.Discard, "", log.LstdFlags)
	server, err := NewServer(URL, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

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

func TestNewServerRejectsInvalidURLs(t *testing.T) {
	for _, rawURL := range []string{"", "not a url", "localhost:8001", "ftp://host", "http://", "http://a b", "http://a?x=1"} {
		if _, err := NewServer(rawURL, nil); err == nil {
			t.Errorf("NewServer(%q): expected error", rawURL)
		}
	}
}

func TestHandleRequest(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()

	server, err := NewServer(dead.URL, log.New(io.Discard, "", log.LstdFlags))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	if err := server.HandleRequest(w, req); err == nil || errors.Is(err, ErrUnhealthy) {
		t.Errorf("expected connection error, got %v", err)
	}
	if server.Load.Load() != 0 {
		t.Errorf("expected load to return to 0, got %d", server.Load.Load())
	}

	server.Healthy.Store(false)
	if err := server.HandleRequest(w, req); !errors.Is(err, ErrUnhealthy) {
		t.Errorf("expected ErrUnhealthy, got %v", err)
	}
}

func TestCheckHealth(t *testing.T) {
	logger := log.New(io.Discard, "", log.LstdFlags)
	server, err := NewServer("http://example.com", logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	server.CheckHealth()
	if !server.Healthy.Load() {
		t.Errorf("expected server to be healthy")
	}
}
