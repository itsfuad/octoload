package balancer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newFrontend serves lb on a real HTTP server: ReverseProxy only aborts
// connections, and the transport only sees real bodies, in that setup.
func newFrontend(t *testing.T, lb *LoadBalancer) *httptest.Server {
	t.Helper()
	frontend := httptest.NewServer(lb)
	t.Cleanup(frontend.Close)
	return frontend
}

func newBackend(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	return backend
}

func deadBackendURL(t *testing.T) string {
	t.Helper()
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	return dead.URL
}

// newResetBackend reads the request, then drops the connection without a response.
func newResetBackend(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		conn.Close()
	})
}

// echoHandler replies while still reading. It needs full duplex itself, or its own
// net/http server would drain part of the body the way the proxy must not.
func echoHandler(w http.ResponseWriter, r *http.Request) {
	http.NewResponseController(w).EnableFullDuplex()
	io.Copy(w, r.Body)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustDo(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func mustRequest(t *testing.T, method, url string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

// Regression: a backend dying midway made the proxy retry, sending the POST three
// times and giving the client "partialpartialpartialAll servers failed...".
func TestResponseDyingMidwayIsNotRetried(t *testing.T) {
	tests := []struct {
		name     string
		backend  http.HandlerFunc
		wantBody string
	}{
		{
			name: "short body stays in proxy buffer",
			backend: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "100")
				w.Write([]byte("partial"))
			},
		},
		{
			name: "flushed body reaches client",
			backend: func(w http.ResponseWriter, _ *http.Request) {
				w.Write([]byte("partial"))
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			},
			wantBody: "partial",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits atomic.Int32
			backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				tt.backend(w, r)
			})
			lb := newTestBalancer(t, "round_robin", backend.URL)
			frontend := newFrontend(t, lb)

			var body []byte
			resp, err := http.DefaultTransport.RoundTrip(mustRequest(t, http.MethodPost, frontend.URL+"/pay", strings.NewReader("x")))
			if err == nil {
				defer resp.Body.Close()
				body, err = io.ReadAll(resp.Body)
			}
			if err == nil {
				t.Error("expected client to see a broken connection")
			}
			if string(body) != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
			if got := hits.Load(); got != 1 {
				t.Errorf("backend hit %d times, want 1", got)
			}
			eventually(t, "failure to be counted", func() bool { return lb.GetMetrics().FailedRequests == 1 })
			eventually(t, "load to return to 0", func() bool { return lb.Servers[0].Load.Load() == 0 })
			eventually(t, "active connections to return to 0", func() bool { return lb.GetMetrics().ActiveConnections == 0 })
		})
	}
}

// Regression: the proxy followed redirects itself, so clients saw the final 200.
func TestRedirectPassesThrough(t *testing.T) {
	backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/new", http.StatusFound)
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	resp := mustDo(t, mustRequest(t, http.MethodGet, frontend.URL+"/old", nil))
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/new" {
		t.Fatalf("got %d Location=%q, want 302 /new", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// Regression: responses were held back until the backend finished. The backend
// here only finishes after the client has the first byte.
func TestResponseIsStreamed(t *testing.T) {
	release := make(chan struct{})
	backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("a"))
		w.(http.Flusher).Flush()
		<-release
		w.Write([]byte("b"))
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp := mustDo(t, mustRequest(t, http.MethodGet, frontend.URL, nil).WithContext(ctx))

	first := make([]byte, 1)
	_, err := io.ReadFull(resp.Body, first)
	close(release)
	if err != nil {
		t.Fatalf("reading first byte: %v", err)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(first)+string(rest) != "ab" {
		t.Fatalf("body = %q%q, err = %v, want ab", first, rest, err)
	}
}

// Regression: the request body was fully buffered before forwarding. The client
// here only finishes sending after the backend has the first byte.
func TestRequestBodyIsStreamed(t *testing.T) {
	gotFirst := make(chan struct{})
	backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, 1)
		if _, err := io.ReadFull(r.Body, first); err != nil {
			t.Errorf("reading first byte: %v", err)
			return
		}
		close(gotFirst)
		rest, _ := io.ReadAll(r.Body)
		w.Write(append(first, rest...))
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	reader, writer := io.Pipe()
	go func() {
		writer.Write([]byte("a"))
		select {
		case <-gotFirst:
			writer.Write([]byte("b"))
			writer.Close()
		case <-time.After(5 * time.Second):
			writer.CloseWithError(fmt.Errorf("backend did not get first byte: body was buffered"))
		}
	}()

	resp := mustDo(t, mustRequest(t, http.MethodPost, frontend.URL, reader))
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "ab" {
		t.Fatalf("body = %q, err = %v, want ab", body, err)
	}
}

// Regression: backend work kept running after the client left. A retry here
// would also hit the second backend.
func TestClientDisconnectCancelsBackendWithoutRetry(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	slow := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
	})
	var otherHits atomic.Int32
	other := newBackend(t, func(http.ResponseWriter, *http.Request) { otherHits.Add(1) })
	lb := newTestBalancer(t, "round_robin", slow.URL, other.URL)
	frontend := newFrontend(t, lb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go http.DefaultTransport.RoundTrip(mustRequest(t, http.MethodGet, frontend.URL, nil).WithContext(ctx))

	<-started
	cancel()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("backend request was not cancelled")
	}
	eventually(t, "request to finish", func() bool { return lb.GetMetrics().ActiveConnections == 0 })
	if got := otherHits.Load(); got != 0 {
		t.Errorf("request was retried %d times after client left", got)
	}
	if got := lb.GetMetrics().FailedRequests; got != 0 {
		t.Errorf("client disconnect counted as %d failures", got)
	}
}

// Regression: the Host header and client IP never reached the backend.
func TestForwardedHeaders(t *testing.T) {
	tests := []struct {
		name                 string
		trust                bool
		inbound              map[string]string
		wantFor, wantHostHdr string
		wantProto            string
	}{
		{
			name:        "untrusted client values are dropped",
			inbound:     map[string]string{"X-Forwarded-For": "6.6.6.6", "X-Forwarded-Proto": "https", "X-Forwarded-Host": "evil.example"},
			wantFor:     "127.0.0.1",
			wantHostHdr: "app.example.com",
			wantProto:   "http",
		},
		{
			name:        "trusted edge values are kept",
			trust:       true,
			inbound:     map[string]string{"X-Forwarded-For": "203.0.113.9", "X-Forwarded-Proto": "https", "X-Forwarded-Host": "edge.example"},
			wantFor:     "203.0.113.9, 127.0.0.1",
			wantHostHdr: "edge.example",
			wantProto:   "https",
		},
		{
			name:        "trusted edge that sets nothing gets defaults",
			trust:       true,
			wantFor:     "127.0.0.1",
			wantHostHdr: "app.example.com",
			wantProto:   "http",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			backend := newBackend(t, func(_ http.ResponseWriter, r *http.Request) { requests <- r.Clone(context.Background()) })
			lb := NewLoadBalancer(nil, "round_robin")
			lb.TrustForwardedHeaders = tt.trust
			if err := lb.AddServer(backend.URL); err != nil {
				t.Fatalf("AddServer: %v", err)
			}
			frontend := newFrontend(t, lb)

			req := mustRequest(t, http.MethodGet, frontend.URL, nil)
			req.Host = "app.example.com"
			for name, value := range tt.inbound {
				req.Header.Set(name, value)
			}
			mustDo(t, req)
			got := <-requests

			if got.Host != "app.example.com" {
				t.Errorf("Host = %q, want client's", got.Host)
			}
			if v := got.Header.Get("X-Forwarded-For"); v != tt.wantFor {
				t.Errorf("X-Forwarded-For = %q, want %q", v, tt.wantFor)
			}
			if v := got.Header.Get("X-Forwarded-Host"); v != tt.wantHostHdr {
				t.Errorf("X-Forwarded-Host = %q, want %q", v, tt.wantHostHdr)
			}
			if v := got.Header.Get("X-Forwarded-Proto"); v != tt.wantProto {
				t.Errorf("X-Forwarded-Proto = %q, want %q", v, tt.wantProto)
			}
		})
	}
}

func TestHopByHopHeadersAreStripped(t *testing.T) {
	headers := make(chan http.Header, 1)
	backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		w.Header().Set("Proxy-Authenticate", "secret")
		w.Header().Set("X-Kept", "yes")
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	req := mustRequest(t, http.MethodGet, frontend.URL, nil)
	req.Header.Set("Connection", "X-Internal")
	req.Header.Set("X-Internal", "secret")
	req.Header.Set("Proxy-Authorization", "secret")
	req.Header.Set("X-Kept", "yes")
	resp := mustDo(t, req)
	got := <-headers

	for _, name := range []string{"X-Internal", "Proxy-Authorization"} {
		if v := got.Get(name); v != "" {
			t.Errorf("request header %s reached backend: %q", name, v)
		}
	}
	if got.Get("X-Kept") != "yes" {
		t.Error("end-to-end request header was dropped")
	}
	if v := resp.Header.Get("Proxy-Authenticate"); v != "" {
		t.Errorf("response header Proxy-Authenticate reached client: %q", v)
	}
	if resp.Header.Get("X-Kept") != "yes" {
		t.Error("end-to-end response header was dropped")
	}
}

// The query must arrive byte for byte: re-encoding it breaks signed URLs.
func TestPathAndQueryAreForwarded(t *testing.T) {
	const uri = "/a%2Fb?z=1&a=%20&b=c;d&e=%zz"
	uris := make(chan string, 1)
	backend := newBackend(t, func(_ http.ResponseWriter, r *http.Request) { uris <- r.RequestURI })

	for base, want := range map[string]string{
		backend.URL:          uri,
		backend.URL + "/":    uri,
		backend.URL + "/api": "/api" + uri,
	} {
		frontend := newFrontend(t, newTestBalancer(t, "round_robin", base))
		mustDo(t, mustRequest(t, http.MethodGet, frontend.URL+uri, nil))
		if got := <-uris; got != want {
			t.Errorf("base %q: backend saw %q, want %q", base, got, want)
		}
	}
}

// Regression: without full duplex, net/http drained the request body once the
// response started, truncating uploads to backends that reply while reading.
func TestBackendRepliesWhileReadingLargeUpload(t *testing.T) {
	large := strings.Repeat("x", 1<<20)
	for name, body := range map[string]io.Reader{
		"known length": strings.NewReader(large),
		"chunked":      struct{ io.Reader }{strings.NewReader(large)},
	} {
		t.Run(name, func(t *testing.T) {
			echo := newBackend(t, echoHandler)
			frontend := newFrontend(t, newTestBalancer(t, "round_robin", echo.URL))

			resp := mustDo(t, mustRequest(t, http.MethodPost, frontend.URL, body))
			got, err := io.ReadAll(resp.Body)
			if err != nil || string(got) != large {
				t.Fatalf("body length = %d, err = %v, want %d", len(got), err, len(large))
			}
		})
	}
}

func TestWebSocketUpgrade(t *testing.T) {
	backend := newBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "test" {
			t.Errorf("Upgrade header lost: %q", r.Header.Get("Upgrade"))
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		rw.Flush()
		line, _ := rw.ReadString('\n')
		rw.WriteString("echo " + line)
		rw.Flush()
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	conn, err := net.Dial("tcp", frontend.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	fmt.Fprint(conn, "ping\n")
	if line, err := reader.ReadString('\n'); err != nil || line != "echo ping\n" {
		t.Fatalf("read %q, err = %v, want echo", line, err)
	}
}

// Pins two net/http behaviors the retry rule relies on: a failed dial leaves the
// body unread, and the proxy does not write to the client when RoundTrip fails.
func TestDialFailureRetriesAnyMethodWithIntactBody(t *testing.T) {
	large := strings.Repeat("x", 1<<20)
	bodies := map[string]func() io.Reader{
		"known length": func() io.Reader { return strings.NewReader(large) },
		"chunked":      func() io.Reader { return struct{ io.Reader }{strings.NewReader(large)} },
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			echo := newBackend(t, echoHandler)
			lb := newTestBalancer(t, "round_robin", deadBackendURL(t), echo.URL)
			frontend := newFrontend(t, lb)

			resp := mustDo(t, mustRequest(t, http.MethodPost, frontend.URL, body()))
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("reading body: %v", err)
			}
			if resp.StatusCode != http.StatusOK || string(got) != large {
				t.Fatalf("status = %d, body length = %d, want 200 and %d", resp.StatusCode, len(got), len(large))
			}
			if lb.Servers[0].Healthy.Load() {
				t.Error("dial failure should mark backend unhealthy")
			}
			if got := lb.GetMetrics().FailedRequests; got != 0 {
				t.Errorf("FailedRequests = %d, want 0 after successful failover", got)
			}
		})
	}
}

func TestConnectionResetRetriesOnlySafeMethods(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		upgrade    string
		body       io.Reader
		wantStatus int
		wantOther  int32
	}{
		{"GET retries", http.MethodGet, "", nil, http.StatusOK, 1},
		{"HEAD retries", http.MethodHead, "", nil, http.StatusOK, 1},
		{"OPTIONS retries", http.MethodOptions, "", nil, http.StatusOK, 1},
		{"POST fails", http.MethodPost, "", strings.NewReader("x"), http.StatusBadGateway, 0},
		{"bodyless POST fails", http.MethodPost, "", nil, http.StatusBadGateway, 0},
		{"PUT fails", http.MethodPut, "", strings.NewReader("x"), http.StatusBadGateway, 0},
		{"GET with body fails", http.MethodGet, "", strings.NewReader("x"), http.StatusBadGateway, 0},
		{"upgrade fails", http.MethodGet, "websocket", nil, http.StatusBadGateway, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resetHits, otherHits atomic.Int32
			reset := newResetBackend(t, &resetHits)
			other := newBackend(t, func(http.ResponseWriter, *http.Request) { otherHits.Add(1) })
			lb := newTestBalancer(t, "round_robin", reset.URL, other.URL)
			frontend := newFrontend(t, lb)

			req := mustRequest(t, tt.method, frontend.URL, tt.body)
			if tt.upgrade != "" {
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", tt.upgrade)
			}
			resp := mustDo(t, req)
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if got := resetHits.Load(); got != 1 {
				t.Errorf("reset backend hit %d times, want 1", got)
			}
			if got := otherHits.Load(); got != tt.wantOther {
				t.Errorf("other backend hit %d times, want %d", got, tt.wantOther)
			}
			if !lb.Servers[0].Healthy.Load() {
				t.Error("connection reset must not mark backend unhealthy")
			}
		})
	}
}

func TestBackendErrorStatusIsNotRetried(t *testing.T) {
	var hits atomic.Int32
	backend := newBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	frontend := newFrontend(t, newTestBalancer(t, "round_robin", backend.URL))

	resp := mustDo(t, mustRequest(t, http.MethodGet, frontend.URL, nil))
	if resp.StatusCode != http.StatusInternalServerError || hits.Load() != 1 {
		t.Fatalf("status = %d, hits = %d, want 500 once", resp.StatusCode, hits.Load())
	}
}

func TestFailureStatuses(t *testing.T) {
	lb := newTestBalancer(t, "round_robin", deadBackendURL(t))
	frontend := newFrontend(t, lb)

	// First request discovers the dead backend; the next finds none healthy.
	for _, want := range []int{http.StatusBadGateway, http.StatusServiceUnavailable} {
		resp := mustDo(t, mustRequest(t, http.MethodGet, frontend.URL, nil))
		if resp.StatusCode != want {
			t.Errorf("status = %d, want %d", resp.StatusCode, want)
		}
	}
	if got := lb.GetMetrics().FailedRequests; got != 2 {
		t.Errorf("FailedRequests = %d, want 2", got)
	}
}

func TestAddServerRejectsInvalidURL(t *testing.T) {
	lb := NewLoadBalancer(nil, "round_robin")
	if err := lb.AddServer("localhost:8001"); err == nil {
		t.Fatal("expected error")
	}
	if len(lb.Servers) != 0 {
		t.Fatalf("invalid server was added: %v", serverURLs(lb))
	}
}
