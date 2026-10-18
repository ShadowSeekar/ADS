import csv
import math
import os
import queue
import statistics
import threading
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import redis
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

RATES = [int(x) for x in os.getenv("RATES", "300,400,500,600,700").split(",")]
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "30"))
WORKERS = int(os.getenv("WORKERS", "8"))
TEXT_NAME = os.getenv("TEXT_NAME", "") or None
KEYWORDS = [k.strip() for k in os.getenv(
    "KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]
# WARM_CACHE=1: flush Redis, then pre-populate every keyword (measures cache hits).
# WARM_CACHE=0: flush Redis only (first request per keyword is a miss).
WARM_CACHE = os.getenv("WARM_CACHE", "1") == "1"

os.makedirs("results", exist_ok=True)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def prepare_cache():
    """Make every run reproducible: empty Redis, optionally warm it deliberately."""
    redis_client.flushall()
    if WARM_CACHE:
        with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
            for kw in KEYWORDS:
                c.root.count_keyword(kw, TEXT_NAME)
        # Warm-up requests should not count towards hot-keyword statistics.
        redis_client.delete("hot_keywords")


def run_benchmark(rate):
    print(f"\nTesting target rate: {rate} requests/second", flush=True)
    prepare_cache()

    jobs = queue.Queue()
    records = []
    records_lock = threading.Lock()
    total_requests = rate * DURATION_SECONDS
    interval = 1.0 / rate
    benchmark_start = [None]

    # Open all persistent connections up front (one per worker).
    connections = [
        rpyc.connect(SERVER_HOST, SERVER_PORT, config={"sync_request_timeout": 30})
        for _ in range(WORKERS)
    ]

    def worker(connection):
        while True:
            job = jobs.get()
            try:
                if job is None:
                    return
                request_number, keyword, scheduled_at = job
                actual_start = time.perf_counter()
                try:
                    result = connection.root.count_keyword(keyword, TEXT_NAME)
                    error = ""
                except Exception as exc:
                    result = ""
                    error = repr(exc)
                received_at = time.perf_counter()
                record = {
                    "target_rate": rate,
                    "request_number": request_number,
                    "keyword": keyword,
                    "result": result,
                    "scheduled_offset_ms": (scheduled_at - benchmark_start[0]) * 1000,
                    "queue_delay_ms": (actual_start - scheduled_at) * 1000,
                    # Execution latency as defined in the assignment: send -> response.
                    "latency_ms": (received_at - actual_start) * 1000,
                    # Latency including time waiting in the client's queue.
                    "total_latency_ms": (received_at - scheduled_at) * 1000,
                    "success": not bool(error),
                    "error": error,
                }
                with records_lock:
                    records.append(record)
            finally:
                jobs.task_done()

    threads = [threading.Thread(target=worker, args=(c,), daemon=True) for c in connections]
    for t in threads:
        t.start()

    benchmark_start[0] = time.perf_counter()

    # Open-loop scheduler: offer requests at fixed intervals regardless of responses.
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
    for c in connections:
        c.close()

    elapsed = time.perf_counter() - benchmark_start[0]
    records.sort(key=lambda r: r["request_number"])
    successful = [r for r in records if r["success"]]
    failed_count = len(records) - len(successful)
    if not successful:
        raise RuntimeError(f"No successful requests at target rate {rate}")

    latencies = [r["latency_ms"] for r in successful]
    totals = [r["total_latency_ms"] for r in successful]
    achieved_rate = len(successful) / elapsed
    # Rate at which requests were actually offered (scheduler speed), independent of drain time.
    offered_rate = total_requests / max(records[-1]["scheduled_offset_ms"] / 1000, 1e-9)
    mean_queue = statistics.mean(r["queue_delay_ms"] for r in records)

    print(f"Scheduled: {total_requests}; successful: {len(successful)}; failed: {failed_count}")
    print(f"Offered rate: {offered_rate:.1f} rps; achieved (completed) rate: {achieved_rate:.1f} rps")
    print(f"Average latency: {statistics.mean(latencies):.3f} ms; "
          f"P99: {percentile(latencies, 0.99):.3f} ms")
    print(f"Average client queue delay: {mean_queue:.3f} ms; "
          f"total-latency avg: {statistics.mean(totals):.3f} ms, "
          f"P99: {percentile(totals, 0.99):.3f} ms")
    if achieved_rate < 0.95 * rate:
        print(f"WARNING: achieved rate is below 95% of target {rate}. The client "
              f"(Python threads) or the server is saturated; report this or lower the rates.")

    summary_row = {
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
    plt.savefig(f"results/{filename}", dpi=200)
    plt.close()


all_results, summary = [], []
for rate in RATES:
    records, row = run_benchmark(rate)
    all_results.extend(records)
    summary.append(row)

with open("results/request_latencies.csv", "w", newline="", encoding="utf-8") as f:
    fields = ["target_rate", "request_number", "keyword", "result", "scheduled_offset_ms",
              "queue_delay_ms", "latency_ms", "total_latency_ms", "success", "error"]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(all_results)

with open("results/summary.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
    writer.writeheader()
    writer.writerows(summary)

rates = [r["target_rate"] for r in summary]
# The two figures required by the assignment:
save_plot(rates, [r["mean_latency_ms"] for r in summary],
          "Average request latency (ms)", "Average Word-Count Request Latency", "average_latency.png")
save_plot(rates, [r["p99_latency_ms"] for r in summary],
          "99th-percentile request latency (ms)", "P99 Word-Count Request Latency", "p99_latency.png")
# Extra figures including client-side queueing (useful to show saturation):
save_plot(rates, [r["mean_total_latency_ms"] for r in summary],
          "Average latency incl. queueing (ms)", "Average Latency Including Queueing", "average_total_latency.png")
save_plot(rates, [r["p99_total_latency_ms"] for r in summary],
          "P99 latency incl. queueing (ms)", "P99 Latency Including Queueing", "p99_total_latency.png")

print("\nBenchmark complete. Results saved in results/.")