package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/itsfuad/octoload/balancer"
)

// These tests run the real binary with real signals, which the serve tests cannot cover.

func buildBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs the real binary")
	}
	bin := filepath.Join(t.TempDir(), "octoload")
	// Prefer the race detector, which makes the binary exit non-zero on a race.
	if exec.Command("go", "build", "-race", "-o", bin, ".").Run() != nil {
		if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
			t.Fatalf("go build: %v\n%s", err, out)
		}
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// restartableBackend can be killed and brought back on the same port.
type restartableBackend struct {
	t       *testing.T
	url     string
	addr    string
	handler http.Handler
	server  *http.Server
}

func newRestartableBackend(t *testing.T, handler http.Handler) *restartableBackend {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	b := &restartableBackend{t: t, url: "http://" + addr, addr: addr, handler: handler}
	b.start()
	t.Cleanup(b.stop)
	return b
}

func (b *restartableBackend) start() {
	b.t.Helper()
	listener, err := net.Listen("tcp", b.addr)
	if err != nil {
		b.t.Fatalf("Listen: %v", err)
	}
	b.server = &http.Server{Handler: b.handler}
	go b.server.Serve(listener)
}

func (b *restartableBackend) stop() { b.server.Close() }

type balancerProcess struct {
	t            *testing.T
	cmd          *exec.Cmd
	traffic      string
	admin        string
	logPath      string
	exited       chan struct{}
	waitErr      error
	exitDeadline time.Duration
}

func startBalancer(t *testing.T, bin string, delay, timeout int, urls ...string) *balancerProcess {
	t.Helper()
	dir := t.TempDir()
	port, adminPort := freePort(t), freePort(t)

	cfg := map[string]any{
		"load_balancer": map[string]any{
			"port":                          port,
			"admin_port":                    adminPort,
			"health_check_interval_seconds": 1,
			"health_check_timeout_seconds":  1,
			"strategy":                      "round_robin",
			"shutdown_delay_seconds":        delay,
			"shutdown_timeout_seconds":      timeout,
		},
		"servers": map[string]any{"urls": urls},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	configPath := filepath.Join(dir, "servers.json")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	logPath := filepath.Join(dir, "octoload.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { logFile.Close() })

	cmd := exec.Command(bin, "-config", configPath)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	p := &balancerProcess{
		t:            t,
		cmd:          cmd,
		traffic:      fmt.Sprintf("http://127.0.0.1:%d", port),
		admin:        fmt.Sprintf("http://127.0.0.1:%d", adminPort),
		logPath:      logPath,
		exited:       make(chan struct{}),
		exitDeadline: 20 * time.Second,
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		cmd.Process.Kill()
		<-p.exited
	})

	waitFor(t, "balancer to become ready", func() bool { return p.status("/readyz") == http.StatusOK })
	return p
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// status returns the admin endpoint's status code, or 0 if it cannot be reached.
func (p *balancerProcess) status(path string) int {
	resp, err := http.Get(p.admin + path)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (p *balancerProcess) backendHealthy(url string) bool {
	p.t.Helper()
	resp, err := http.Get(p.admin + "/metrics")
	if err != nil {
		p.t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	var metrics balancer.Metrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		p.t.Fatalf("Decode metrics: %v", err)
	}
	for _, backend := range metrics.Backends {
		if backend.URL == url {
			return backend.Healthy
		}
	}
	p.t.Fatalf("backend %s not in metrics: %+v", url, metrics.Backends)
	return false
}

func (p *balancerProcess) signal(sig syscall.Signal) {
	p.t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		p.t.Fatalf("Signal: %v", err)
	}
}

// waitExit returns the process exit code, or -1 if a signal killed it.
func (p *balancerProcess) waitExit() int {
	p.t.Helper()
	select {
	case <-p.exited:
	case <-time.After(p.exitDeadline):
		p.t.Fatal("balancer did not exit")
	}
	return p.cmd.ProcessState.ExitCode()
}

func (p *balancerProcess) assertLogsAreJSON() {
	p.t.Helper()
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		p.t.Fatalf("ReadFile: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if !json.Valid([]byte(line)) {
			p.t.Errorf("log line is not JSON: %q", line)
		}
	}
}

func TestBinaryFailsOverAndRecovers(t *testing.T) {
	bin := buildBinary(t)

	var hitsA, hitsB atomic.Int64
	backendFor := func(hits *atomic.Int64) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			io.Copy(w, r.Body)
		})
	}
	a := newRestartableBackend(t, backendFor(&hitsA))
	b := newRestartableBackend(t, backendFor(&hitsB))
	// Above 5s: Shutdown waits that long for client connections that never sent a request.
	p := startBalancer(t, bin, 0, 10, a.url, b.url)

	const workers = 8
	var ok, badStatus, broken atomic.Int64
	var stopLoad atomic.Bool
	var load sync.WaitGroup
	client := &http.Client{Timeout: 10 * time.Second}
	for range workers {
		load.Add(1)
		go func() {
			defer load.Done()
			for !stopLoad.Load() {
				resp, err := client.Get(p.traffic)
				if err != nil {
					broken.Add(1)
					continue
				}
				_, err = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch {
				case err != nil:
					broken.Add(1)
				case resp.StatusCode != http.StatusOK:
					badStatus.Add(1)
				default:
					ok.Add(1)
				}
			}
		}()
	}
	stopWorkers := func() {
		stopLoad.Store(true)
		load.Wait()
	}
	defer stopWorkers()

	waitFor(t, "traffic on both backends", func() bool { return hitsA.Load() > 50 && hitsB.Load() > 50 })
	if bad, errs := badStatus.Load(), broken.Load(); bad != 0 || errs != 0 {
		t.Fatalf("with both backends up: %d bad statuses, %d broken requests", bad, errs)
	}

	a.stop()
	waitFor(t, "killed backend to be marked down", func() bool { return !p.backendHealthy(a.url) })
	before := ok.Load()
	waitFor(t, "traffic to continue", func() bool { return ok.Load() > before+200 })

	// Unsafe requests keep working and arrive intact on the remaining backend.
	for i := range 20 {
		body := fmt.Sprintf("payment-%d", i)
		resp, err := client.Post(p.traffic, "text/plain", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST during outage: %v", err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(got) != body {
			t.Fatalf("POST during outage: status %d, body %q, want 200 %q", resp.StatusCode, got, body)
		}
	}

	if bad := badStatus.Load(); bad != 0 {
		t.Errorf("GETs should fail over: %d bad statuses during outage", bad)
	}
	// Only a response cut off by the kill can break, at most one per worker.
	if errs := broken.Load(); errs > workers {
		t.Errorf("%d broken requests during outage, want at most %d in flight at the kill", errs, workers)
	}

	a.start()
	waitFor(t, "restarted backend to rejoin", func() bool { return p.backendHealthy(a.url) })
	hitsBefore := hitsA.Load()
	waitFor(t, "traffic to return to restarted backend", func() bool { return hitsA.Load() > hitsBefore+50 })

	stopWorkers()
	p.signal(syscall.SIGTERM)
	if code := p.waitExit(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	p.assertLogsAreJSON()
}

// newSlowBackend answers /slow only after release is called, which cleanup also does
// so a failing test cannot hang closing the backend.
func newSlowBackend(t *testing.T) (backend *restartableBackend, started chan struct{}, release func()) {
	t.Helper()
	started = make(chan struct{}, 8)
	unblock := make(chan struct{})
	release = sync.OnceFunc(func() { close(unblock) })
	backend = newRestartableBackend(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			started <- struct{}{}
			<-unblock
		}
		fmt.Fprint(w, "finished")
	}))
	t.Cleanup(release)
	return backend, started, release
}

func TestBinaryDrainsOnStopSignal(t *testing.T) {
	bin := buildBinary(t)
	backend, started, release := newSlowBackend(t)
	const delay = 2
	p := startBalancer(t, bin, delay, 10, backend.url)

	type result struct {
		status int
		body   string
		err    error
	}
	inFlight := make(chan result, 1)
	go func() {
		resp, err := http.Get(p.traffic + "/slow")
		if err != nil {
			inFlight <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		inFlight <- result{resp.StatusCode, string(body), err}
	}()
	<-started

	p.signal(syscall.SIGTERM)
	waitFor(t, "readiness to fail while draining", func() bool { return p.status("/readyz") == http.StatusServiceUnavailable })
	if got := p.status("/livez"); got != http.StatusOK {
		t.Errorf("/livez while draining = %d, want 200", got)
	}

	// New connections are still served during the delay, so external load balancers
	// have time to stop routing here.
	fresh := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := fresh.Get(p.traffic)
	if err != nil {
		t.Fatalf("GET during shutdown delay: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status during shutdown delay = %d, want 200", resp.StatusCode)
	}

	waitFor(t, "traffic listener to close after the delay", func() bool {
		conn, err := net.Dial("tcp", strings.TrimPrefix(p.traffic, "http://"))
		if err != nil {
			return true
		}
		conn.Close()
		return false
	})
	select {
	case <-p.exited:
		t.Fatal("balancer exited with a request in flight")
	default:
	}

	release()
	got := <-inFlight
	if got.err != nil || got.status != http.StatusOK || got.body != "finished" {
		t.Errorf("in-flight request: %+v, want complete 200", got)
	}
	if code := p.waitExit(); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	p.assertLogsAreJSON()
}

func TestBinaryExitsNonZeroWhenDrainTimesOut(t *testing.T) {
	bin := buildBinary(t)
	backend, started, _ := newSlowBackend(t)
	p := startBalancer(t, bin, 0, 1, backend.url)

	go func() {
		if resp, err := http.Get(p.traffic + "/slow"); err == nil {
			resp.Body.Close()
		}
	}()
	<-started

	p.signal(syscall.SIGTERM)
	if code := p.waitExit(); code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestBinarySecondSignalForcesExit(t *testing.T) {
	bin := buildBinary(t)
	backend, started, _ := newSlowBackend(t)
	p := startBalancer(t, bin, 0, 60, backend.url)
	p.exitDeadline = 5 * time.Second // Far below the 60s drain timeout.

	go func() {
		if resp, err := http.Get(p.traffic + "/slow"); err == nil {
			resp.Body.Close()
		}
	}()
	<-started

	p.signal(syscall.SIGTERM)
	waitFor(t, "first signal to be handled", func() bool { return p.status("/readyz") == http.StatusServiceUnavailable })
	p.signal(syscall.SIGTERM)
	if code := p.waitExit(); code == 0 {
		t.Error("forced exit should not report success")
	}
}

func TestBinaryStartupFailures(t *testing.T) {
	bin := buildBinary(t)

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer busy.Close()
	busyPort := busy.Addr().(*net.TCPAddr).Port

	write := func(content string) string {
		path := filepath.Join(t.TempDir(), "servers.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		return path
	}
	settings := `"health_check_interval_seconds": 1, "strategy": "round_robin"`
	servers := `"servers": {"urls": ["http://127.0.0.1:1"]}`

	tests := []struct {
		name, config, want string
	}{
		{"missing file", filepath.Join(t.TempDir(), "missing.json"), "no such file"},
		{"invalid config", write(`{"load_balancer": {"port": 0, ` + settings + `}, ` + servers + `}`), "port must be"},
		{"port in use", write(fmt.Sprintf(`{"load_balancer": {"port": %d, %s}, %s}`, busyPort, settings, servers)), "address already in use"},
		{"admin port in use", write(fmt.Sprintf(`{"load_balancer": {"port": %d, "admin_port": %d, %s}, %s}`, freePort(t), busyPort, settings, servers)), "address already in use"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := exec.Command(bin, "-config", tt.config).CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Errorf("err = %v, want exit code 1", err)
			}
			if !strings.Contains(string(out), tt.want) {
				t.Errorf("output %q does not contain %q", out, tt.want)
			}
		})
	}
}

func TestBinaryPrintsVersion(t *testing.T) {
	out, err := exec.Command(buildBinary(t), "-version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "dev" {
		t.Fatalf("output %q, err = %v, want dev", out, err)
	}
}
