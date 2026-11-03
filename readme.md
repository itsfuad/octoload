# Octoload

An HTTP load balancer in Go, using only the standard library. It distributes requests across backend servers, routes around failures, and drains in-flight requests on shutdown.

![logo](cover.png)

## Features

- **Strategies:** round robin, or least active connections
- **Health checks:** timeouts, configurable path, and thresholds so one slow check does not eject a backend
- **Safe retries:** failover that never replays a request a backend may have acted on
- **Streaming:** request and response bodies are streamed, including WebSocket upgrades and server-sent events
- **Graceful shutdown:** a delay for external load balancers to stop routing here, then a bounded drain
- **Operations:** JSON logs, `/metrics`, `/livez` and `/readyz` on a separate admin port
- **Strict config:** every mistake is reported at startup, including misspelled options

## Installation

**Docker:**
```sh
docker build --build-arg VERSION=$(git describe --tags --always) -t octoload .
docker run -p 8080:8080 -p 8081:8081 -v "$PWD/servers.json:/etc/octoload/servers.json:ro" octoload
```
Inside a container, `localhost` is the container itself, so point `servers.urls` at addresses it can reach.

**Binary** (Go 1.26 or later):
```sh
go install github.com/itsfuad/octoload@latest
octoload -config /path/to/servers.json
```
`-config` defaults to `servers.json` in the working directory. `-version` prints the version.

**As a library:**
```sh
go get github.com/itsfuad/octoload@latest
```
```go
lb, err := balancer.NewLoadBalancer(nil, balancer.StrategyRoundRobin)
if err != nil {
	log.Fatal(err)
}
if err := lb.AddServer("http://localhost:8001"); err != nil {
	log.Fatal(err)
}
lb.StartHealthChecks(context.Background(), 5*time.Second)
log.Fatal(http.ListenAndServe(":8080", lb))
```
Always call `StartHealthChecks`: it is the only thing that restores a backend ejected after refusing a connection. In production, use an `http.Server` with timeouts and `Shutdown`, as `serve` in `main.go` does.

## Configuration

```json
{
    "load_balancer": {
        "port": 8080,
        "admin_port": 8081,
        "strategy": "round_robin",
        "health_check_interval_seconds": 5,
        "health_check_path": "/healthz",
        "health_check_timeout_seconds": 2,
        "shutdown_delay_seconds": 5,
        "shutdown_timeout_seconds": 30,
        "trust_forwarded_headers": false
    },
    "servers": {
        "urls": [
            "http://localhost:8001",
            "http://localhost:8002"
        ]
    }
}
```

| Option | Default | Description |
|---|---|---|
| `port` | required | Traffic port. Every path is proxied. |
| `admin_port` | `0` | Port for `/metrics`, `/livez` and `/readyz`. `0` disables it. |
| `strategy` | `least_active` | `round_robin` or `least_active`. |
| `health_check_interval_seconds` | required | Time between checks of each backend. |
| `health_check_path` | `/` | Path to check. Use a cheap endpoint. Any 2xx is healthy, and redirects are followed. |
| `health_check_timeout_seconds` | `2` | Limit for each check. |
| `shutdown_delay_seconds` | `5` | How long to keep serving after a stop signal. Use `0` for local development. |
| `shutdown_timeout_seconds` | `30` | Limit for draining in-flight requests. Keep this above 5: Go waits up to 5s for client connections that have not sent a request, so a lower value can report a failed drain with nothing in flight. |
| `trust_forwarded_headers` | `false` | Keep inbound `X-Forwarded-*` values. See [Deployment](#deployment). |

`servers.urls` needs at least one `http` or `https` URL, with no duplicates. Each may include a base path. Unknown options are errors.

## How failures are handled

A backend is marked down after 2 failed health checks in a row, and restored by 1 success. A refused connection also ejects it immediately until its next check.

Failed requests follow this rule, which never repeats a request a backend may have acted on:

| Failure | Retried on another backend? | Ejects the backend? |
|---|---|---|
| Connection refused | Yes, any method | Yes |
| Other connection failure (timeout, balancer out of file descriptors or ports) | Yes, any method | No, this can be overload or a local problem |
| Reset or timeout before response headers | Only `GET`, `HEAD` and `OPTIONS` with no body, and not upgrades | No |
| Response already started | Never. The client connection is aborted, so the client sees a truncated response. | No |

A backend's 5xx response is passed through and not retried. After up to 3 attempts, the client gets `502`. With no healthy backend it gets `503`.

Timeouts are 5s to connect and 30s for backend response headers. Once a response starts there is no limit, so downloads and streams can run as long as they need. Clients get 10s to send request headers, and idle connections close after 120s.

Requests reach backends with the client's `Host`, `X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto`. Paths and query strings are forwarded unchanged, so signed URLs keep working.

## Operations

**Admin endpoints** on `admin_port`. They have no authentication, so do not expose this port to clients.

| Endpoint | Purpose |
|---|---|
| `GET /livez` | Always `200` while the process runs. Use for liveness: a backend outage must not restart the balancer. |
| `GET /readyz` | `503` while draining or with no healthy backend. Use for readiness. |
| `GET /metrics` | JSON counters and per-backend state. |

```json
{
    "TotalRequests": 150,
    "FailedRequests": 2,
    "ActiveConnections": 3,
    "Backends": [
        {"URL": "http://localhost:8001", "Healthy": true, "Load": 1}
    ]
}
```

**Logs** are JSON on stdout, with one line per state change or failed attempt. There is no per-request access log.

**Stopping:** on `SIGINT` or `SIGTERM`, `/readyz` turns `503`, requests are served for `shutdown_delay_seconds`, then the listener closes and in-flight requests get up to `shutdown_timeout_seconds` to finish. The exit code is `0` if they all finish, and `1` if the drain times out. A second signal force quits. WebSockets are not waited for, and streams still open at the timeout are cut off.

## Deployment

Run it behind something that terminates TLS, since Octoload does not. Then set `trust_forwarded_headers: true`, or backends see `http` as the scheme and the edge's address as the client. Enable it only if clients cannot reach Octoload directly, because otherwise they can spoof these headers.

**Kubernetes:** the grace period must exceed the shutdown delay plus timeout.
```yaml
spec:
  terminationGracePeriodSeconds: 40
  containers:
    - name: octoload
      image: octoload
      args: ["-config", "/etc/octoload/servers.json"]
      livenessProbe:
        httpGet: { path: /livez, port: 8081 }
      readinessProbe:
        httpGet: { path: /readyz, port: 8081 }
```

**systemd:**
```ini
[Service]
ExecStart=/usr/local/bin/octoload -config /etc/octoload/servers.json
Restart=on-failure
TimeoutStopSec=40
DynamicUser=yes
```

## Limitations

- No TLS termination. Terminate at an edge proxy.
- No limit on concurrent connections or request rate.
- Metrics are JSON only, with no Prometheus format. The admin port has no authentication.
- No access log, and no tracing.
- Configuration is read at startup. Restart to change it.
- No weighted, sticky or IP hash strategies.
- Unsafe requests are not retried after a connection reset, so clients must handle `502`.
- On Windows, a refused connection probably does not eject a backend immediately, so it waits for health checks. Retries still work. Windows is compiled for releases but not tested.

## Quick start with demo servers

```sh
# Three backends, each in its own terminal
go run demo/demo_server.go -id 1 -port 8001
go run demo/demo_server.go -id 2 -port 8002
go run demo/demo_server.go -id 3 -port 8003

# The load balancer, using the sample servers.json
go run .

# Traffic, and state
curl http://localhost:8080
curl http://localhost:8081/metrics
```

On Windows, `cd demo` and run `.\start.ps1` to start the backends.

## Testing

```sh
go test -race ./...          # Everything, including the real binary with real signals
go test -race -short ./...   # Skips the real binary and a test that waits 10s
```

## Project structure

```
octoload/
├── main.go              # Startup, shutdown and admin endpoints
├── e2e_test.go          # Tests of the real binary
├── servers.json         # Sample configuration
├── Dockerfile
├── balancer/            # Strategies, retries and health check scheduling
├── server/              # Proxying and health checks for one backend
├── config/              # Loading and validation
└── demo/                # Demo backend server
```

## Contributing

See [contributing.md](contributing.md).

## License

This project is licensed under the GNU General Public License v3. See the LICENSE file for more information.
