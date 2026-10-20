"""Open-loop benchmark: one RPyC connection per request.

Run the SAME script with the SAME rates/duration/workers in three setups:
  1 server : SERVER_HOST=server1  CONFIG_LABEL=single
  LB (LC)  : SERVER_HOST=load-balancer  CONFIG_LABEL=least_connections
  LB (LRT) : SERVER_HOST=load-balancer  CONFIG_LABEL=least_response_time
Results go to results/<CONFIG_LABEL>/. Then run compare.py.
"""
import csv
import json
import math
import os
import queue
import statistics
import threading
import time
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import redis
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "load-balancer")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RATES = [int(x) for x in os.getenv("RATES", "50,100,150,200,250").split(",")]
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "30"))
WORKERS = int(os.getenv("WORKERS", "16"))
TEXT_NAME = os.getenv("TEXT_NAME", "") or None
CONFIG_LABEL = os.getenv("CONFIG_LABEL", "run")
KEYWORDS = [k.strip() for k in os.getenv(
    "KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]
WARM_CACHE = os.getenv("WARM_CACHE", "1") == "1"

OUT_DIR = os.path.join("results", CONFIG_LABEL)
os.makedirs(OUT_DIR, exist_ok=True)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def prepare_cache():
    redis_client.flushall()
    if WARM_CACHE:
        with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
            for kw in KEYWORDS:
                c.root.count_keyword(kw, TEXT_NAME)
        redis_client.delete("hot_keywords")


def run_benchmark(rate):
    print(f"\n[{CONFIG_LABEL}] Testing target rate: {rate} requests/second", flush=True)
    prepare_cache()

    jobs = queue.Queue()
    records = []
    records_lock = threading.Lock()
    total_requests = rate * DURATION_SECONDS
    interval = 1.0 / rate
    benchmark_start = [None]
    ready = threading.Barrier(WORKERS + 1)

    def worker():
        ready.wait()
        while True:
            job = jobs.get()
            try:
                if job is None:
                    return
                request_number, keyword, scheduled_at = job
                actual_start = time.perf_counter()
                info = {}
                try:
                    # Fresh connection per request so the LB makes a new dynamic
                    # decision each time.
                    with rpyc.connect(
                        SERVER_HOST,
                        SERVER_PORT,
                        config={"sync_request_timeout": 30},
                    ) as connection:
                        raw = connection.root.count_keyword(keyword, TEXT_NAME)
                    received_at = time.perf_counter()
                    info = json.loads(raw)
                    error = ""
                except Exception as exc:
                    received_at = time.perf_counter()
                    error = repr(exc)
                record = {
                    "target_rate": rate,
                    "request_number": request_number,
                    "keyword": keyword,
                    "result": info.get("count", ""),
                    "server_id": info.get("server_id", ""),
                    "cache": info.get("cache", ""),
                    "scheduled_offset_ms": (scheduled_at - benchmark_start[0]) * 1000,
                    "queue_delay_ms": (actual_start - scheduled_at) * 1000,
                    "latency_ms": (received_at - actual_start) * 1000,
                    "total_latency_ms": (received_at - scheduled_at) * 1000,
                    "success": not bool(error),
                    "error": error,
                }
                with records_lock:
                    records.append(record)
            finally:
                jobs.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
    for t in threads:
        t.start()
    ready.wait()
    benchmark_start[0] = time.perf_counter()

    for i in range(total_requests):
        scheduled_at = benchmark_start[0] + i * interval
        remaining = scheduled_at - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        jobs.put((i + 1, KEYWORDS[i % len(KEYWORDS)], scheduled_at))

    for _ in threads:
        jobs.put(None)
    jobs.join()
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - benchmark_start[0]
    records.sort(key=lambda r: r["request_number"])
    successful = [r for r in records if r["success"]]
    failed_count = len(records) - len(successful)
    if not successful:
        raise RuntimeError(f"No successful requests at target rate {rate}")

    latencies = [r["latency_ms"] for r in successful]
    totals = [r["total_latency_ms"] for r in successful]
    distribution = Counter(r["server_id"] for r in successful)
    achieved_rate = len(successful) / elapsed
    mean_queue = statistics.mean(r["queue_delay_ms"] for r in records)

    print(f"Scheduled: {total_requests}; successful: {len(successful)}; failed: {failed_count}")
    print(f"Achieved rate: {achieved_rate:.1f} rps")
    print(f"Average latency: {statistics.mean(latencies):.3f} ms; "
          f"P99: {percentile(latencies, 0.99):.3f} ms")
    print(f"Average client queue delay: {mean_queue:.3f} ms; "
          f"total-latency avg: {statistics.mean(totals):.3f} ms, "
          f"P99: {percentile(totals, 0.99):.3f} ms")
    print(f"Requests by server: {dict(distribution)}")
    if achieved_rate < 0.95 * rate:
        print(f"WARNING: achieved rate is below 95% of target {rate}; the client "
              f"or the system is saturated. Report this or lower the rates.")

    summary_row = {
        "config": CONFIG_LABEL,
        "target_rate": rate,
        "achieved_rate": achieved_rate,
        "scheduled_requests": total_requests,
        "successful_requests": len(successful),
        "failed_requests": failed_count,
        "mean_latency_ms": statistics.mean(latencies),
        "p99_latency_ms": percentile(latencies, 0.99),
        "mean_total_latency_ms": statistics.mean(totals),
        "p99_total_latency_ms": percentile(totals, 0.99),
        "average_queue_delay_ms": mean_queue,
        "elapsed_seconds": elapsed,
        "warm_cache": WARM_CACHE,
        "server_distribution": json.dumps(dict(distribution)),
    }
    return records, summary_row


def save_plot(rates, values, ylabel, title, filename):
    plt.figure()
    plt.plot(rates, values, marker="o")
    plt.xlabel("Target keyword request rate (requests/second)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, filename), dpi=200)
    plt.close()


all_results, summary = [], []
for rate in RATES:
    records, row = run_benchmark(rate)
    all_results.extend(records)
    summary.append(row)

with open(os.path.join(OUT_DIR, "request_latencies.csv"), "w", newline="", encoding="utf-8") as f:
    fields = ["target_rate", "request_number", "keyword", "result", "server_id", "cache",
              "scheduled_offset_ms", "queue_delay_ms", "latency_ms", "total_latency_ms",
              "success", "error"]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(all_results)

with open(os.path.join(OUT_DIR, "summary.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
    writer.writeheader()
    writer.writerows(summary)

rates = [r["target_rate"] for r in summary]
save_plot(rates, [r["mean_latency_ms"] for r in summary],
          "Average request latency (ms)", f"Average Latency ({CONFIG_LABEL})", "average_latency.png")
save_plot(rates, [r["p99_latency_ms"] for r in summary],
          "99th-percentile latency (ms)", f"P99 Latency ({CONFIG_LABEL})", "p99_latency.png")

print(f"\nBenchmark complete. Results saved in {OUT_DIR}/.")