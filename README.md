# ADS
AIDS

# Distributed Word Count Service

A Docker-based distributed word-count service using **Python, RPyC, and Redis**. The client sends keyword-count requests to the server through RPC, while Redis caches results to reduce repeated computation.

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose
- Git

## Getting Started

1. Clone the repository:

2. Build the image and start the containers:

   ```bash
   docker compose up --build -d
   ```

3. Run the client:

   ```bash
   docker compose exec client python client.py
   ```

4. Enter a keyword when prompted to see its occurrence count and request latency.

## Benchmarking

Run the benchmark to measure average and 99th-percentile (P99) request latency at different request rates:

```bash
docker compose exec client python benchmark.py
```

The results are saved in the `results/` directory:

- `request_latencies.csv` — individual request measurements
- `summary.csv` — summary statistics for each request rate
- `average_latency.png` — average latency plot
- `p99_latency.png` — P99 latency plot

Verify Redis caching:
```
docker compose exec redis redis-cli KEYS "wordcount:*"
```

## Project Structure

```text
.
├── Dockerfile
├── docker-compose.yaml
├── server.py
├── client.py
├── benchmark.py
├── texts/
│   └── sample.txt
└── results/
```

## Stopping the Services

```bash
docker compose down
```

To also remove the Redis data volume:

```bash
docker compose down -v
```
