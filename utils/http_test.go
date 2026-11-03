package utils

import (
	"net/http"
	"testing"
)

const CONTENT_TYPE_HEADER = "Content-Type"
const CUSTOM_HEADER = "X-Custom-Header"

func TestCopyHeaders(t *testing.T) {
	src := http.Header{}
	src.Add(CONTENT_TYPE_HEADER, "application/json")
	src.Add(CUSTOM_HEADER, "custom-value")

	dst := http.Header{}
	CopyHeaders(dst, src)

	if dst.Get(CONTENT_TYPE_HEADER) != "application/json" {
		t.Errorf("expected Content-Type to be 'application/json', got '%s'", dst.Get(CONTENT_TYPE_HEADER))
	}

	if dst.Get(CUSTOM_HEADER) != "custom-value" {
		t.Errorf("expected X-Custom-Header to be 'custom-value', got '%s'", dst.Get(CUSTOM_HEADER))
	}
}
