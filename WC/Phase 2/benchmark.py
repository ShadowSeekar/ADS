import csv
import json
import math
import os
import queue
import random
import re
import statistics
import threading
import time
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import redis
import rpyc

# Configuration
SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TEXT_DIR = os.getenv("TEXT_DIR", "/app/script")
DEFAULT_TEXT = os.getenv("DEFAULT_TEXT", "Deadpool")
TEXT_NAME = os.getenv("TEXT_NAME", "").strip() or None

RATES = [int(x.strip()) for x in os.getenv("RATES", "50,100,150,200,250").split(",") if x.strip()]
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "30"))
WORKERS = int(os.getenv("WORKERS", "8"))
SEED = int(os.getenv("SEED", "42"))
HIT_KEYWORDS = [k.strip() for k in os.getenv("KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]

# Cache modes -> fraction of requests that are *designed* to be cache hits.
#   cold    : every request uses a never-seen keyword          -> 0 % hits
#   mixed50 : half the requests use pre-cached keywords        -> 50 % hits
#   mixed90 : 90 % of the requests use pre-cached keywords     -> 90 % hits
#   warm    : every request uses a pre-cached keyword          -> 100 % hits
MODE_HIT_TARGET = {"cold": 0.0, "mixed50": 0.5, "warm": 1.0}
MODES = [m.strip() for m in os.getenv("MODES", "cold,mixed50,warm").split(",") if m.strip()]
for m in MODES:
    if m not in MODE_HIT_TARGET:
        raise SystemExit(f"Unknown mode '{m}'. Choose from {list(MODE_HIT_TARGET)}")

os.makedirs("results", exist_ok=True)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


# Helpers
def percentile(values, p):
    """Nearest-rank percentile."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def mean(values):
    values = list(values)
    return statistics.mean(values) if values else float("nan")


def load_vocabulary():
    """Load distinct words from the selected text for unique cache-miss keywords."""
    path = os.path.join(TEXT_DIR, f"{TEXT_NAME or DEFAULT_TEXT}.txt")
    if not os.path.exists(path):
        print(f"WARNING: vocabulary file not found: {path}. Synthetic miss keywords will be used.")
        return []
    with open(path, "r", encoding="utf-8") as file:
        words = set(re.findall(r"[a-z]+", file.read().lower()))
    words -= {keyword.lower() for keyword in HIT_KEYWORDS}
    vocabulary = sorted(words)
    random.Random(SEED).shuffle(vocabulary)
    return vocabulary


VOCAB = load_vocabulary()


def miss_keyword(n):
    """Return a unique keyword that has not been used earlier in this run."""
    return VOCAB[n] if n < len(VOCAB) else f"zzmiss{n}"


def build_workload(rate, mode):
    total_requests = rate * DURATION_SECONDS
    number_of_hits = round(total_requests * MODE_HIT_TARGET[mode])
    plan = [True] * number_of_hits + [False] * (total_requests - number_of_hits)
    random.Random(SEED + rate).shuffle(plan)
    workload = []
    miss_number = 0
    for index, is_hit in enumerate(plan):
        if is_hit:
            workload.append((HIT_KEYWORDS[index % len(HIT_KEYWORDS)], True))
        else:
            workload.append((miss_keyword(miss_number), False))
            miss_number += 1
    return workload


def prepare_cache(mode):
    """Reset Redis, warm intended hit keys if required, then reset measurement counters."""
    redis_client.flushall()
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as connection:
        if MODE_HIT_TARGET[mode] > 0:
            for keyword in HIT_KEYWORDS:
                connection.root.count_keyword(keyword, TEXT_NAME)
        connection.root.reset_stats()
    redis_client.delete("hot_keywords")


def read_cache_stats():
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as connection:
        return json.loads(connection.root.cache_stats())


# Benchmark execution
def run_benchmark(rate, mode):
    designed_hit_ratio = MODE_HIT_TARGET[mode]
    print(f"\n[{mode}] target={rate} req/s, duration={DURATION_SECONDS}s, designed hits={designed_hit_ratio:.0%}", flush=True)
    workload = build_workload(rate, mode)
    prepare_cache(mode)

    jobs = queue.Queue()
    records = []
    records_lock = threading.Lock()
    total_requests = len(workload)
    interval = 1.0 / rate
    benchmark_start = [None]

    # Each worker uses its own persistent RPyC connection.
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
                request_number, keyword, expected_hit, scheduled_at = job
                actual_start = time.perf_counter()
                try:
                    result = connection.root.count_keyword(keyword, TEXT_NAME)
                    error = ""
                except Exception as exc:
                    result = ""
                    error = repr(exc)
                received_at = time.perf_counter()
                record = {
                    "mode": mode,
                    "target_rate": rate,
                    "request_number": request_number,
                    "keyword": keyword,
                    "expected_hit": expected_hit,
                    "result": result,
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

    threads = [threading.Thread(target=worker, args=(connection,), daemon=True) for connection in connections]
    for thread in threads:
        thread.start()

    benchmark_start[0] = time.perf_counter()
    # Open-loop scheduling: offer requests at fixed intervals regardless of response times.
    for index, (keyword, expected_hit) in enumerate(workload):
        scheduled_at = benchmark_start[0] + index * interval
        remaining = scheduled_at - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        jobs.put((index + 1, keyword, expected_hit, scheduled_at))

    for _ in threads:
        jobs.put(None)
    jobs.join()
    for thread in threads:
        thread.join()
    for connection in connections:
        connection.close()

    elapsed = time.perf_counter() - benchmark_start[0]
    stats = read_cache_stats()
    records.sort(key=lambda record: record["request_number"])
    successful = [record for record in records if record["success"]]
    failed_count = len(records) - len(successful)
    if not successful:
        raise RuntimeError(f"No successful requests at mode={mode}, rate={rate}. Check server logs.")

    latencies = [record["latency_ms"] for record in successful]
    total_latencies = [record["total_latency_ms"] for record in successful]
    hit_latencies = [record["latency_ms"] for record in successful if record["expected_hit"]]
    miss_latencies = [record["latency_ms"] for record in successful if not record["expected_hit"]]
    average_queue_delay = mean(record["queue_delay_ms"] for record in records)
    achieved_rate = len(successful) / elapsed

    cache_hits = stats["hits"]
    cache_misses = stats["misses"]
    actual_hit_ratio = stats["hit_ratio"]

    print(f"  Requests: scheduled={total_requests}, successful={len(successful)}, failed={failed_count}")
    print(f"  Throughput: target={rate} req/s, achieved={achieved_rate:.2f} req/s")
    print(f"  Cache: hits={cache_hits}, misses={cache_misses}, designed ratio={designed_hit_ratio:.1%}, actual ratio={actual_hit_ratio:.1%}")
    print(f"  RPC latency: average={mean(latencies):.3f} ms, P99={percentile(latencies, 0.99):.3f} ms")
    print(f"  Total latency incl. queueing: average={mean(total_latencies):.3f} ms, P99={percentile(total_latencies, 0.99):.3f} ms")
    print(f"  Average queue delay={average_queue_delay:.3f} ms; intended-hit avg={mean(hit_latencies):.3f} ms; intended-miss avg={mean(miss_latencies):.3f} ms")
    print(f"  Elapsed time={elapsed:.2f} s")

    if failed_count:
        print("  WARNING: one or more requests failed.")
    if achieved_rate < 0.95 * rate:
        print(f"  WARNING: achieved throughput is below 95% of target ({rate} req/s); client or server may be saturated.")
    if abs(actual_hit_ratio - designed_hit_ratio) > 0.02:
        print("  WARNING: actual cache-hit ratio differs from designed ratio by more than 2 percentage points.")

    summary_row = {
        "mode": mode,
        "target_rate": rate,
        "achieved_rate": achieved_rate,
        "scheduled_requests": total_requests,
        "successful_requests": len(successful),
        "failed_requests": failed_count,
        "designed_hit_ratio": designed_hit_ratio,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "actual_hit_ratio": actual_hit_ratio,
        "mean_latency_ms": mean(latencies),
        "p99_latency_ms": percentile(latencies, 0.99),
        "mean_hit_latency_ms": mean(hit_latencies),
        "mean_miss_latency_ms": mean(miss_latencies),
        "mean_total_latency_ms": mean(total_latencies),
        "p99_total_latency_ms": percentile(total_latencies, 0.99),
        "average_queue_delay_ms": average_queue_delay,
        "elapsed_seconds": elapsed,
    }
    return records, summary_row


# Plotting
def plot_by_mode(summary, key, ylabel, title, filename):
    fig, ax = plt.subplots(figsize=(8, 5))
    for mode in MODES:
        rows = sorted((row for row in summary if row["mode"] == mode), key=lambda row: row["target_rate"])
        if rows:
            ax.plot(
                [row["target_rate"] for row in rows],
                [row[key] for row in rows],
                marker="o",
                label=f"{mode} ({MODE_HIT_TARGET[mode]:.0%} designed hits)",
            )
    ax.set_xlabel("Target request rate (requests/second)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join("results", filename), dpi=200)
    plt.close(fig)


def plot_mode_bars(summary, filename):
    labels = [mode for mode in MODES if any(row["mode"] == mode for row in summary)]
    average_latency = [mean(row["mean_latency_ms"] for row in summary if row["mode"] == mode) for mode in labels]
    average_hit_ratio = [mean(row["actual_hit_ratio"] for row in summary if row["mode"] == mode) * 100 for mode in labels]

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.bar(labels, average_latency)
    ax1.set_ylabel("Average RPC latency across rates (ms)")
    ax1.set_xlabel("Cache mode")
    ax2 = ax1.twinx()
    ax2.plot(labels, average_hit_ratio, marker="o")
    ax2.set_ylabel("Average measured cache-hit ratio (%)")
    ax2.set_ylim(0, 105)
    ax1.set_title("Average Latency vs. Measured Cache-Hit Ratio")
    fig.tight_layout()
    fig.savefig(os.path.join("results", filename), dpi=200)
    plt.close(fig)


# Terminal summary
def print_table(summary):
    print("\n" + "=" * 100)
    print("FINAL BENCHMARK SUMMARY")
    print("=" * 100)
    for row in summary:
        print(f"\nMode: {row['mode']} | Target rate: {row['target_rate']} req/s | Designed hit ratio: {row['designed_hit_ratio']:.0%}")
        print("-" * 90)
        print(f"Requests: scheduled={row['scheduled_requests']}, successful={row['successful_requests']}, failed={row['failed_requests']}")
        print(f"Throughput: target={row['target_rate']} req/s, achieved={row['achieved_rate']:.2f} req/s")
        print(f"Cache: hits={row['cache_hits']}, misses={row['cache_misses']}, actual hit ratio={row['actual_hit_ratio']:.2%}")
        print(f"RPC latency: average={row['mean_latency_ms']:.3f} ms, P99={row['p99_latency_ms']:.3f} ms")
        print(f"Total latency incl. queueing: average={row['mean_total_latency_ms']:.3f} ms, P99={row['p99_total_latency_ms']:.3f} ms")
        print(f"Average queue delay={row['average_queue_delay_ms']:.3f} ms; intended-hit avg={row['mean_hit_latency_ms']:.3f} ms; intended-miss avg={row['mean_miss_latency_ms']:.3f} ms")
        print(f"Elapsed time: {row['elapsed_seconds']:.2f} seconds")
        if row["failed_requests"] > 0:
            print("WARNING: Some requests failed.")
        if row["achieved_rate"] < 0.95 * row["target_rate"]:
            print("WARNING: Achieved throughput is below 95% of target.")
        if abs(row["actual_hit_ratio"] - row["designed_hit_ratio"]) > 0.02:
            print("WARNING: Actual hit ratio differs from the designed ratio.")
    print("\n" + "=" * 100)
    print("Per-request data: results/request_latencies.csv")
    print("Aggregated data:  results/summary.csv")
    print("Figures:          results/*.png")
    print("=" * 100)


# Main
if __name__ == "__main__":
    print("Word-count service benchmark")
    print(f"Modes: {MODES}")
    print(f"Target rates: {RATES} requests/second")
    print(f"Duration per rate: {DURATION_SECONDS} seconds")
    print(f"Workers: {WORKERS}")
    print(f"Hit keywords: {HIT_KEYWORDS}")
    print(f"Real-word miss vocabulary size: {len(VOCAB)}")

    all_results = []
    summary = []
    for mode in MODES:
        for rate in RATES:
            records, summary_row = run_benchmark(rate, mode)
            all_results.extend(records)
            summary.append(summary_row)

    request_fields = [
        "mode", "target_rate", "request_number", "keyword", "expected_hit",
        "result", "scheduled_offset_ms", "queue_delay_ms", "latency_ms",
        "total_latency_ms", "success", "error",
    ]
    with open("results/request_latencies.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=request_fields)
        writer.writeheader()
        writer.writerows(all_results)

    with open("results/summary.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(summary[0].keys()))
        writer.writeheader()
        writer.writerows(summary)

    # Required figures
    plot_by_mode(summary, "mean_latency_ms", "Average RPC latency (ms)", "Average RPC Latency by Cache Mode", "average_latency.png")
    plot_by_mode(summary, "p99_latency_ms", "P99 RPC latency (ms)", "P99 RPC Latency by Cache Mode", "p99_latency.png")

    # Additional figures
    plot_by_mode(summary, "achieved_rate", "Completed throughput (requests/second)", "Achieved Throughput by Cache Mode", "throughput.png")
    plot_by_mode(summary, "actual_hit_ratio", "Measured cache-hit ratio", "Measured Cache-Hit Ratio by Cache Mode", "hit_ratio.png")
    plot_by_mode(summary, "mean_total_latency_ms", "Average scheduled-to-response latency (ms)", "Average Total Latency Including Queueing", "average_total_latency.png")
    plot_by_mode(summary, "p99_total_latency_ms", "P99 scheduled-to-response latency (ms)", "P99 Total Latency Including Queueing", "p99_total_latency.png")
    plot_mode_bars(summary, "latency_vs_hit_ratio.png")

    print_table(summary)
    print("\nBenchmark complete. Results saved in results/.")