# Running the Load Balancer Benchmark in Windows PowerShell

This guide explains how to run load-balancer benchmarks using Windows PowerShell, including tests without fault tolerance, tests with fault tolerance, server failure simulation, and collecting evidence for your report.

## 1. Start the Cluster

Open PowerShell in your project directory and run:

```powershell
docker compose up -d --build
docker compose ps
```

Wait until the servers show as healthy.

Open two PowerShell terminals in the same project directory:

- **Terminal A:** Runs the benchmark.
- **Terminal B:** Configures the load balancer and stops or restarts `server2`.

Watch the elapsed time printed by the benchmark in Terminal A. Stop `server2` at approximately 30 seconds and restart it at approximately 60 seconds.

---

## 2. Item 9 — Without Fault Tolerance

### Test A: Least Connections

#### Terminal B — Configure the Load Balancer

```powershell
docker start server1 server2 server3

$env:HEALTH_CHECK = "off"
$env:LB_ALGORITHM = "least_connections"

docker compose up -d --force-recreate load-balancer
```

#### Terminal A — Run the Benchmark

```powershell
docker compose exec -e CONFIG_LABEL=nofc_lc -e STOP_AT=30 -e START_AT=60 client python timeline_benchmark.py
```

#### Terminal B — Simulate the Server Failure

At approximately 30 seconds:

```powershell
docker stop server2
```

At approximately 60 seconds:

```powershell
docker start server2
```

### Test B: Least Response Time

#### Terminal B — Configure the Load Balancer

```powershell
docker start server1 server2 server3

$env:HEALTH_CHECK = "off"
$env:LB_ALGORITHM = "least_response_time"

docker compose up -d --force-recreate load-balancer
```

#### Terminal A — Run the Benchmark

```powershell
docker compose exec -e CONFIG_LABEL=nofc_lrt -e STOP_AT=30 -e START_AT=60 client python timeline_benchmark.py
```

#### Terminal B — Simulate the Server Failure

At approximately 30 seconds:

```powershell
docker stop server2
```

At approximately 60 seconds:

```powershell
docker start server2
```

**Expected result:** Connection errors or `EOFError` failures may occur while the server is down because fault tolerance is disabled.

Least connections may direct excessive traffic to the dead server because it has zero active connections. Least response time may continue sending traffic according to its stale exponentially weighted moving average (EWMA).

---

## 3. Items 10 and 11 — With Fault Tolerance

### Test C: Least Connections

#### Terminal B — Configure the Load Balancer

```powershell
docker start server1 server2 server3

$env:HEALTH_CHECK = "on"
$env:LB_ALGORITHM = "least_connections"

docker compose up -d --force-recreate load-balancer
```

#### Terminal A — Run the Benchmark

```powershell
docker compose exec -e CONFIG_LABEL=ft_lc -e STOP_AT=30 -e START_AT=60 client python timeline_benchmark.py
```

#### Terminal B — Simulate the Server Failure

At approximately 30 seconds:

```powershell
docker stop server2
```

At approximately 60 seconds:

```powershell
docker start server2
```

### Test D: Least Response Time

#### Terminal B — Configure the Load Balancer

```powershell
docker start server1 server2 server3

$env:HEALTH_CHECK = "on"
$env:LB_ALGORITHM = "least_response_time"

docker compose up -d --force-recreate load-balancer
```

#### Terminal A — Run the Benchmark

```powershell
docker compose exec -e CONFIG_LABEL=ft_lrt -e STOP_AT=30 -e START_AT=60 client python timeline_benchmark.py
```

#### Terminal B — Simulate the Server Failure

At approximately 30 seconds:

```powershell
docker stop server2
```

At approximately 60 seconds:

```powershell
docker start server2
```

**Expected result:** The load balancer should detect that `server2` is down, stop routing new requests to it, and resume sending traffic after the server recovers.

---

## 4. Collect Evidence for the Report

Run these commands in PowerShell after the tests.

### Check Server Status

```powershell
docker ps --format "table {{.Names}}`t{{.Status}}"
```

To include stopped containers:

```powershell
docker ps -a --format "table {{.Names}}`t{{.Status}}"
```

Check that `server2` appears as stopped or exited during the outage and becomes healthy again after restarting, if health checks are configured correctly.

### Show Health-Check Events

```powershell
docker compose logs load-balancer | Select-String "HEALTH"
```

Look for `DOWN` and `UP` events and their timestamps.

### Show Load-Balancer Statistics

```powershell
docker compose logs load-balancer | Select-String "LB-STATS"
```

Look for per-backend request counters and server status changes.

### Check Generated Benchmark Results

```powershell
Get-ChildItem results/phase4 -Recurse
```

Each benchmark run should generate files similar to these:

```text
results/
└── phase4/
    ├── nofc_lc/
    │   ├── timeline.png
    │   └── timeline.csv
    ├── nofc_lrt/
    │   ├── timeline.png
    │   └── timeline.csv
    ├── ft_lc/
    │   ├── timeline.png
    │   └── timeline.csv
    └── ft_lrt/
        ├── timeline.png
        └── timeline.csv
```

Use the generated PNG graphs and CSV data as evidence for your report.

---

## 5. Expected Results

For the `ft_lc` and `ft_lrt` tests, look for the following behavior:

- **Server failure:** Traffic to `server2` drops to zero after the load balancer detects the failure.
- **Load redistribution:** The other two servers handle most of the requests.
- **Few failed requests:** There should be very few failed requests, apart from requests that were already in flight when the server stopped.
- **Server recovery:** Traffic returns to `server2` a few seconds after it restarts and passes its health checks.
- **Health-check logs:** The load-balancer logs show `DOWN` and `UP` events in the expected order.

At an overall load of approximately 100 requests per second, the two surviving servers may handle roughly 50 requests per second each, depending on the benchmark and scheduling behavior.

---

## 6. Important PowerShell Notes

### Environment Variables

The following commands set environment variables in the current PowerShell session:

```powershell
$env:HEALTH_CHECK = "off"
$env:LB_ALGORITHM = "least_connections"
```

These variables configure the load balancer only if your `docker-compose.yml` passes them into the load-balancer service.

The `-e` flags in the benchmark command set environment variables inside the client container for that specific command:

```powershell
docker compose exec -e CONFIG_LABEL=ft_lc -e STOP_AT=30 -e START_AT=60 client python timeline_benchmark.py
```

### Recreate the Load Balancer Between Tests

After changing the algorithm or health-check configuration, recreate the load-balancer container:

```powershell
docker compose up -d --force-recreate load-balancer
```

Ensure all three servers are running before starting each new benchmark.

### Timing the Failure

Use the elapsed time printed by the benchmark in Terminal A as your reference:

- **Around 30 seconds:** Stop `server2`.
- **Around 60 seconds:** Restart `server2`.

The commands must be entered manually at the appropriate times unless you implement an automated schedule.

---

## 7. Benchmark Checklist

Before finishing, verify that you have completed all four runs:

- [ ] `nofc_lc` — Least connections without fault tolerance.
- [ ] `nofc_lrt` — Least response time without fault tolerance.
- [ ] `ft_lc` — Least connections with fault tolerance.
- [ ] `ft_lrt` — Least response time with fault tolerance.

For each run, check that:

- [ ] `timeline.png` was generated.
- [ ] `timeline.csv` was generated.
- [ ] The server stop and restart occurred at approximately the intended times.
- [ ] Relevant load-balancer logs were collected.
- [ ] The results are saved for inclusion in the report.

**Final reminder:** Confirm that your Docker Compose configuration passes `HEALTH_CHECK` and `LB_ALGORITHM` to the load-balancer service. Otherwise, changing these PowerShell variables will not change the configuration inside the container.