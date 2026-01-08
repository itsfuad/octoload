# Octoload

## Overview
This project is a Go-based load balancer designed to distribute incoming HTTP requests to a pool of backend servers. It supports multiple load balancing strategies, health monitoring, and graceful shutdown capabilities, making it robust and scalable for production use.

![logo](cover.png)

## Features
- **Multiple Load Balancing Strategies:**
  - **Round Robin:** Distributes requests evenly across all healthy servers in a circular fashion
  - **Least Active:** Routes requests to the server with the fewest active connections
- **Health Checks:** Regularly checks the health of each server and only routes traffic to healthy servers
- **Graceful Shutdown:** Ensures that ongoing requests are handled before shutting down the load balancer
- **Configuration via JSON:** Server details and settings are loaded from a JSON configuration file
- **Detailed Logging:** Provides comprehensive logging for monitoring and debugging
- **Metrics Endpoint:** Exposes metrics for monitoring total requests, failed requests, and active connections
- **Demo Servers:** Includes demo servers for easy testing and development

## Prerequisites
- Go 1.23 or later

## Installation

**Install the binary:**
```sh
go install github.com/itsfuad/octoload@latest
```
Run `octoload` from a directory containing `servers.json`.

**Use as a library:**
```sh
go get github.com/itsfuad/octoload@latest
```
```go
import "github.com/itsfuad/octoload/balancer"

lb := balancer.NewLoadBalancer(nil, "round_robin")
lb.AddServer("http://localhost:8001")
lb.StartHealthChecks(30 * time.Second)
http.ListenAndServe(":8080", lb)
```

**From source:**

1. **Clone the Repository:**
```sh
git clone https://github.com/itsfuad/octoload.git
cd octoload
```
2. **Build the Load Balancer:**
```sh
go build -o octoload main.go
```

## Configuration
**1. Create a JSON Configuration File:**
Create a `servers.json` file in the root directory with the following structure:
```json
{
    "load_balancer": {
        "port": 8080,
        "health_check_interval_seconds": 30,
        "strategy": "round_robin"
    },
    "servers": {
        "urls": [
            "http://localhost:8001",
            "http://localhost:8002",
            "http://localhost:8003"
        ]
    }
}
```

**Configuration Options:**
- `port`: The port on which the load balancer will listen (default: 8080)
- `health_check_interval_seconds`: Interval in seconds for health checks (default: 30)
- `strategy`: Load balancing strategy - `"round_robin"` or `"least_active"` (default: "least_active")
- `urls`: List of backend server URLs

### Load Balancing Strategies

#### Round Robin
Distributes requests evenly across all healthy servers in a circular fashion. Each request goes to the next server in sequence, ensuring even load distribution regardless of current server load.

**When to use:**
- All servers have similar capacity
- You want predictable, even request distribution
- Same client should get different servers on subsequent requests

**Example log output:**
```
load-balancer: Selected server http://localhost:8001 (round robin)
load-balancer: Selected server http://localhost:8002 (round robin)
load-balancer: Selected server http://localhost:8003 (round robin)
```

#### Least Active
Routes requests to the server with the fewest active connections. Automatically adapts to server performance and varying request processing times.

**When to use:**
- Servers have different capacities
- Request processing times vary significantly
- You want automatic adaptation to server performance

**Example log output:**
```
load-balancer: Selected server http://localhost:8001 with load 0
load-balancer: Selected server http://localhost:8002 with load 1
```

See [LOAD_BALANCING_STRATEGIES.md](LOAD_BALANCING_STRATEGIES.md) for detailed information.

**2. Running the Load Balancer:**
Start the load balancer by executing:
```sh
./octoload
```
or
```sh
go run main.go
```
The load balancer will start on the specified port and begin routing traffic using the configured strategy.

## Quick Start with Demo Servers

For testing and development, we provide demo servers that can be easily spawned:

**Option 1: Using PowerShell Script**
```powershell
.\start_demo_servers.ps1
```
This starts 3 demo servers on ports 8001, 8002, and 8003.

**Option 2: Manual Start**
```sh
# Terminal 1
go run demo/demo_server.go -id 1 -port 8001

# Terminal 2
go run demo/demo_server.go -id 2 -port 8002

# Terminal 3
go run demo/demo_server.go -id 3 -port 8003
```

**Option 3: Build and Run**
```sh
# Build demo server
go build -o demo_server.exe ./demo/demo_server.go

# Start servers
./demo_server.exe -id 1 -port 8001
./demo_server.exe -id 2 -port 8002
./demo_server.exe -id 3 -port 8003
```

Then start the load balancer and test it:
```sh
# In another terminal
go run main.go

# Test with curl
curl http://localhost:8080
curl http://localhost:8080
curl http://localhost:8080
```

See [DEMO_SETUP.md](DEMO_SETUP.md) for detailed demo server documentation.

## Usage

**Sending Requests:**
Send HTTP requests to the load balancer's address (e.g., http://localhost:8080). The load balancer will forward the request to a backend server based on the configured strategy.

**Viewing Metrics:**
Access the metrics endpoint to monitor load balancer performance:
```sh
curl http://localhost:8080/metrics
```

Response:
```json
{
    "TotalRequests": 150,
    "FailedRequests": 2,
    "ActiveConnections": 3
}
```

**Server Health Checks:**
The load balancer periodically checks each server's health by sending a `GET` request to the root endpoint. If a server does not respond with a status code of 200, it is marked as unhealthy and temporarily removed from the load balancer's pool.

> **Note:** For production use, it's recommended to implement a dedicated `/health` endpoint that returns a lightweight response instead of using the root path.

**Graceful Shutdown:**
The load balancer handles OS signals (SIGINT, SIGTERM) to perform a graceful shutdown, allowing in-progress requests to complete before terminating. Simply press `Ctrl+C` to initiate graceful shutdown.

## Testing

**Run All Tests:**
```sh
go test -v ./...
```

**Test with Demo Servers:**
```sh
go test -v -run TestLoadBalancerWithDemoServers -timeout 60s
```

This automated test will:
1. Build and spawn 3 demo servers
2. Test both round robin and least active strategies
3. Verify load distribution
4. Display detailed logs showing which server handles each request
5. Automatically clean up all servers

**Run Specific Tests:**
```sh
# Test only the demo server
go test -v -run TestDemoServerDirectly

# Test load balancer initialization
go test -v -run TestLoadBalancerInitialization

# Test metrics endpoint
go test -v -run TestMetricsEndpoint
```

## Project Structure

```
octoload/
├── main.go                          # Entry point
├── servers.json                     # Configuration file
├── balancer/
│   ├── balancer.go                  # Load balancer implementation
│   └── balancer_test.go             # Load balancer tests
├── config/
│   └── configs.go                   # Configuration management
├── server/
│   ├── server.go                    # Server handling logic
│   └── server_test.go               # Server tests
├── utils/
│   ├── http.go                      # HTTP utilities
│   ├── colors.go                    # Terminal color utilities
│   └── http_test.go                 # Utility tests
├── demo/
│   └── demo_server.go               # Demo backend server
├── demo_test.go                     # Automated demo tests
├── start_demo_servers.ps1           # Script to start demo servers
├── LOAD_BALANCING_STRATEGIES.md     # Strategy documentation
├── DEMO_SETUP.md                    # Demo server guide
└── readme.md                        # This file
```

## Extending the Load Balancer

You can extend this load balancer by:

- **Adding Custom Strategies:** Implement new load balancing algorithms (e.g., weighted round robin, IP hash)
- **Server Auto-scaling:** Add support for dynamically adding/removing servers based on load
- **HTTPS Support:** Implement SSL/TLS termination for secure connections
- **WebSocket Support:** Add support for WebSocket connections
- **Connection Pooling:** Implement connection pooling for better performance
- **Rate Limiting:** Add rate limiting per client or globally
- **Monitoring Dashboard:** Create a web UI for real-time monitoring
- **Session Persistence:** Implement sticky sessions for stateful applications
- **Circuit Breaker:** Add circuit breaker pattern for failing servers

## Contributing

Feel free to contribute by opening issues or submitting pull requests. Make sure to:
- Follow the existing coding style
- Include relevant tests for any new features or bug fixes
- Update documentation as needed
- Run all tests before submitting: `go test -v ./...`

## Example Output

### Round Robin Strategy
```
load-balancer: 2025/09/30 18:53:45 Added server http://localhost:8001 to the load balancer
load-balancer: 2025/09/30 18:53:45 Added server http://localhost:8002 to the load balancer
load-balancer: 2025/09/30 18:53:45 Added server http://localhost:8003 to the load balancer
load-balancer: 2025/09/30 18:53:45 Load balancer is running on port 8080
load-balancer: 2025/09/30 18:53:47 Selected server http://localhost:8001 (round robin)
load-balancer: 2025/09/30 18:53:47 Selected server http://localhost:8002 (round robin)
load-balancer: 2025/09/30 18:53:47 Selected server http://localhost:8003 (round robin)
load-balancer: 2025/09/30 18:53:47 Selected server http://localhost:8001 (round robin)
```

### Least Active Strategy
```
load-balancer: 2025/09/30 18:53:45 Selected server http://localhost:8001 with load 0
load-balancer: 2025/09/30 18:53:45 Selected server http://localhost:8002 with load 0
load-balancer: 2025/09/30 18:53:45 Selected server http://localhost:8001 with load 1
load-balancer: 2025/09/30 18:53:45 Selected server http://localhost:8003 with load 0
```

## Troubleshooting

### Port Already in Use
```
Error: listen tcp :8080: bind: Only one usage of each socket address is normally permitted
```
**Solution:** Change the port in `servers.json` or stop the process using the port:
```powershell
# Find process using port 8080
netstat -ano | findstr :8080

# Kill the process
taskkill /F /PID <process_id>
```

### Servers Showing as Unhealthy
**Possible causes:**
- Backend servers are not running
- Firewall blocking connections
- Wrong URLs in configuration

**Solution:** 
1. Verify servers are running: `curl http://localhost:8001`
2. Check server logs for errors
3. Wait for health check interval to pass
4. Verify URLs in `servers.json` are correct

### No Requests Being Routed
**Check:**
1. Load balancer is running and listening on correct port
2. At least one server is healthy
3. Requests are being sent to correct load balancer address

## Performance Tips

1. **Adjust Health Check Interval:** Balance between responsiveness and overhead
   ```json
   "health_check_interval_seconds": 30
   ```

2. **Choose Right Strategy:** 
   - Use round robin for uniform load distribution
   - Use least active when servers have different capacities

3. **Monitor Metrics:** Regularly check the `/metrics` endpoint to identify bottlenecks

4. **Server Capacity:** Ensure backend servers can handle the expected load

## License
This project is licensed under the GNU License. See the LICENSE file for more information.

## Acknowledgments

- Built with Go's standard library
- Inspired by production load balancers like NGINX and HAProxy
- Designed for learning and production use
