package utils

import "net/http"

// CopyHeaders copies HTTP headers from src to dst
func CopyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
