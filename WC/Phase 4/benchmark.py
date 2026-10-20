"""Open-loop benchmark with cache modes: one RPyC connection per request.

Run the SAME script with the SAME rates/duration/workers/modes in three setups:
  1 server : SERVER_HOST=server1        CONFIG_LABEL=single
  LB (LC)  : SERVER_HOST=load-balancer  CONFIG_LABEL=least_connections
  LB (LRT) : SERVER_HOST=load-balancer  CONFIG_LABEL=least_response_time
Results go to results/<CONFIG_LABEL>/. Then run compare.py.

Cache modes (MODES env, comma separated; default runs all four):
  cold    : every request uses a never-seen keyword      -> designed 0 % hits
  mixed50 : half the requests use pre-cached keywords    -> designed 50 % hits
  mixed90 : 90 % of the requests use pre-cached keywords -> designed 90 % hits
  warm    : every request uses a pre-cached keyword      -> designed 100 % hits

IMPORTANT: the servers must run with BYPASS_CACHE=0 (the default), otherwise nothing
is cached and every mode degenerates into 'cold'.
"""
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
TEXT_DIR = os.getenv("TEXT_DIR", "/app/script")
DEFAULT_TEXT = os.getenv("DEFAULT_TEXT", "sample")
RATES = [int(x) for x in os.getenv("RATES", "50,100,150,200,250").split(",")]
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "30"))
WORKERS = int(os.getenv("WORKERS", "16"))
TEXT_NAME = os.getenv("TEXT_NAME", "") or None
CONFIG_LABEL = os.getenv("CONFIG_LABEL", "run")
SEED = int(os.getenv("SEED", "42"))

# Keywords pre-populated in Redis and used for every deliberate cache hit.
HIT_KEYWORDS = [k.strip() for k in os.getenv(
    "KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]

MODE_HIT_TARGET = {"cold": 0.0, "mixed50": 0.5, "mixed90": 0.9, "warm": 1.0}
MODES = [m.strip() for m in os.getenv("MODES", "cold,mixed50,mixed90,warm").split(",") if m.strip()]
for m in MODES:
    if m not in MODE_HIT_TARGET:
        raise SystemExit(f"Unknown mode '{m}'. Choose from {list(MODE_HIT_TARGET)}")

OUT_DIR = os.path.join("results", CONFIG_LABEL)
os.makedirs(OUT_DIR, exist_ok=True)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


# ---------------------------------------------------------------- helpers
def percentile(values, p):
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def mean(values):
    values = list(values)
    return statistics.mean(values) if values else float("nan")


def load_vocabulary():
    """Distinct words of the benchmark text (minus the hit keywords), shuffled
    deterministically; used as realistic never-seen keywords for cache misses."""
    path = os.path.join(TEXT_DIR, f"{TEXT_NAME or DEFAULT_TEXT}.txt")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        words = set(re.findall(r"[a-z]+", f.read().lower()))
    words -= {k.lower() for k in HIT_KEYWORDS}
    vocab = sorted(words)
    random.Random(SEED).shuffle(vocab)
    return vocab


VOCAB = load_vocabulary()


def miss_keyword(n):
    """n-th never-seen keyword: real words first, synthetic ones once they run out."""
    return VOCAB[n] if n < len(VOCAB) else f"zzmiss{n}"


def build_workload(rate, mode):
    """Deterministic request plan: list of (keyword, designed_to_hit)."""
    total = rate * DURATION_SECONDS
    n_hits = round(total * MODE_HIT_TARGET[mode])
    plan = [True] * n_hits + [False] * (total - n_hits)
    random.Random(SEED + rate).shuffle(plan)  # spread hits/misses over time
    workload, miss_n = [], 0
    for i, is_hit in enumerate(plan):
        if is_hit:
            workload.append((HIT_KEYWORDS[i % len(HIT_KEYWORDS)], True))
        else:
            workload.append((miss_keyword(miss_n), False))
            miss_n += 1
    return workload


def prepare_cache(mode):
    """Reproducible start: empty Redis, pre-cache the hit keywords if the mode has hits,
    then reset counters and hot-keyword scores so warm-up does not pollute results."""
    redis_client.flushall()
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
        if MODE_HIT_TARGET[mode] > 0:
            for kw in HIT_KEYWORDS:
                c.root.count_keyword(kw, TEXT_NAME)
        c.root.reset_stats()
    redis_client.delete("hot_keywords")


def read_cache_stats():
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
        return json.loads(c.root.cache_stats())  # shared counters in Redis


# ---------------------------------------------------------------- benchmark
def run_benchmark(rate, mode):
    target_hit = MODE_HIT_TARGET[mode]
    print(f"\n[{CONFIG_LABEL}/{mode}] target rate {rate} req/s, "
          f"designed hit ratio {target_hit:.0%}", flush=True)
    workload = build_workload(rate, mode)
    prepare_cache(mode)

    jobs = queue.Queue()
    records = []
    records_lock = threading.Lock()
    total_requests = len(workload)
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
                request_number, keyword, expected_hit, scheduled_at = job
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
                    "config": CONFIG_LABEL,
                    "mode": mode,
                    "target_rate": rate,
                    "request_number": request_number,
                    "keyword": keyword,
                    "expected_hit": expected_hit,
                    "result": info.get("count", ""),
                    "server_id": info.get("server_id", ""),
                    "cache": info.get("cache", ""),  # HIT / MISS as reported by the server
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

    for i, (keyword, expected_hit) in enumerate(workload):
        scheduled_at = benchmark_start[0] + i * interval
        remaining = scheduled_at - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        jobs.put((i + 1, keyword, expected_hit, scheduled_at))

    for _ in threads:
        jobs.put(None)
    jobs.join()
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - benchmark_start[0]
    stats = read_cache_stats()

    records.sort(key=lambda r: r["request_number"])
    successful = [r for r in records if r["success"]]
    failed_count = len(records) - len(successful)
    if not successful:
        raise RuntimeError(f"No successful requests at mode={mode}, rate={rate}")

    latencies = [r["latency_ms"] for r in successful]
    totals = [r["total_latency_ms"] for r in successful]
    # Hit/miss latency uses the status the server actually reported per request.
    hit_lat = [r["latency_ms"] for r in successful if r["cache"] == "HIT"]
    miss_lat = [r["latency_ms"] for r in successful if r["cache"] == "MISS"]
    distribution = Counter(r["server_id"] for r in successful)
    achieved_rate = len(successful) / elapsed
    mean_queue = mean(r["queue_delay_ms"] for r in records)

    hits, misses = stats["hits"], stats["misses"]
    actual_ratio = stats["hit_ratio"]

    print(f"  successful {len(successful)}/{total_requests} (failed {failed_count}); "
          f"throughput {achieved_rate:.1f} req/s")
    print(f"  cache hits {hits}, misses {misses}, actual hit ratio {actual_ratio:.1%} "
          f"(designed {target_hit:.0%})")
    print(f"  avg latency {mean(latencies):.3f} ms; P99 {percentile(latencies, 0.99):.3f} ms; "
          f"avg hit {mean(hit_lat):.3f} ms; avg miss {mean(miss_lat):.3f} ms")
    print(f"  requests by server: {dict(distribution)}")
    if hits + misses == 0:
        print("  WARNING: no cache activity recorded. Are the servers running with "
              "BYPASS_CACHE=1? Cache modes are meaningless in that case.")
    elif abs(actual_ratio - target_hit) > 0.02:
        print("  WARNING: actual hit ratio deviates >2 points from design "
              "(failed requests or a leftover cache?).")
    if achieved_rate < 0.95 * rate:
        print(f"  WARNING: throughput below 95% of target {rate}; the client "
              f"or the system is saturated. Report this or lower the rates.")

    summary_row = {
        "config": CONFIG_LABEL,
        "mode": mode,
        "target_rate": rate,
        "achieved_rate": achieved_rate,
        "scheduled_requests": total_requests,
        "successful_requests": len(successful),
        "failed_requests": failed_count,
        "designed_hit_ratio": target_hit,
        "cache_hits": hits,
        "cache_misses": misses,
        "actual_hit_ratio": actual_ratio,
        "mean_latency_ms": mean(latencies),
        "p99_latency_ms": percentile(latencies, 0.99),
        "mean_hit_latency_ms": mean(hit_lat),
        "mean_miss_latency_ms": mean(miss_lat),
        "mean_total_latency_ms": mean(totals),
        "p99_total_latency_ms": percentile(totals, 0.99),
        "average_queue_delay_ms": mean_queue,
        "elapsed_seconds": elapsed,
        "server_distribution": json.dumps(dict(distribution)),
    }
    return records, summary_row


# ---------------------------------------------------------------- plotting
def plot_by_mode(summary, key, ylabel, title, filename):
    plt.figure()
    for mode in MODES:
        rows = [r for r in summary if r["mode"] == mode]
        plt.plot([r["target_rate"] for r in rows], [r[key] for r in rows],
                 marker="o", label=f"{mode} ({MODE_HIT_TARGET[mode]:.0%} hits)")
    plt.xlabel("Target keyword request rate (requests/second)")
    plt.ylabel(ylabel)
    plt.title(f"{title} ({CONFIG_LABEL})")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, filename), dpi=200)
    plt.close()


def plot_mode_bars(summary, filename):
    avg = [mean(r["mean_latency_ms"] for r in summary if r["mode"] == m) for m in MODES]
    ratio = [mean(r["actual_hit_ratio"] for r in summary if r["mode"] == m) * 100 for m in MODES]
    fig, ax1 = plt.subplots()
    ax1.bar(MODES, avg, color="tab:blue")
    ax1.set_ylabel("Mean latency, averaged over rates (ms)")
    ax2 = ax1.twinx()
    ax2.plot(MODES, ratio, color="tab:red", marker="o")
    ax2.set_ylabel("Measured cache hit ratio (%)")
    ax2.set_ylim(0, 105)
    plt.title(f"Latency vs. measured hit ratio ({CONFIG_LABEL})")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, filename), dpi=200)
    plt.close(fig)


def print_table(summary):
    print(f"\n=== Cache mode comparison ({CONFIG_LABEL}) ===")
    print(f"{'mode':8} {'rate':>5} {'thrpt':>8} {'hits':>7} {'miss':>7} {'hit%':>6} "
          f"{'avg ms':>8} {'p99 ms':>8} {'hit ms':>8} {'miss ms':>8}")
    for r in summary:
        print(f"{r['mode']:8} {r['target_rate']:>5} {r['achieved_rate']:>8.1f} "
              f"{r['cache_hits']:>7} {r['cache_misses']:>7} {r['actual_hit_ratio']*100:>5.1f}% "
              f"{r['mean_latency_ms']:>8.3f} {r['p99_latency_ms']:>8.3f} "
              f"{r['mean_hit_latency_ms']:>8.3f} {r['mean_miss_latency_ms']:>8.3f}")


# ---------------------------------------------------------------- main
if __name__ == "__main__":
    print(f"Config: {CONFIG_LABEL}; target: {SERVER_HOST}:{SERVER_PORT}; modes: {MODES}; "
          f"rates: {RATES}; {DURATION_SECONDS}s each; "
          f"miss vocabulary: {len(VOCAB)} real words (synthetic beyond that)")
    all_results, summary = [], []
    for mode in MODES:
        for rate in RATES:
            records, row = run_benchmark(rate, mode)
            all_results.extend(records)
            summary.append(row)

    with open(os.path.join(OUT_DIR, "request_latencies.csv"), "w", newline="",
              encoding="utf-8") as f:
        fields = ["config", "mode", "target_rate", "request_number", "keyword", "expected_hit",
                  "result", "server_id", "cache", "scheduled_offset_ms", "queue_delay_ms",
                  "latency_ms", "total_latency_ms", "success", "error"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_results)

    with open(os.path.join(OUT_DIR, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        writer.writeheader()
        writer.writerows(summary)

    plot_by_mode(summary, "mean_latency_ms", "Average request latency (ms)",
                 "Average Latency by Cache Mode", "average_latency.png")
    plot_by_mode(summary, "p99_latency_ms", "99th-percentile latency (ms)",
                 "P99 Latency by Cache Mode", "p99_latency.png")
    plot_by_mode(summary, "achieved_rate", "Achieved throughput (requests/second)",
                 "Throughput by Cache Mode", "throughput.png")
    plot_by_mode(summary, "actual_hit_ratio", "Measured cache hit ratio",
                 "Measured Hit Ratio by Cache Mode", "hit_ratio.png")
    plot_mode_bars(summary, "latency_vs_hit_ratio.png")

    print_table(summary)
    print(f"\nBenchmark complete. Results saved in {OUT_DIR}/.")