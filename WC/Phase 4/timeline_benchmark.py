"""Constant-rate load for a fixed duration, one RPyC connection per request.

While it runs, stop and restart a server from another terminal, e.g.
    docker stop server2        (about 30 s in)
    docker start server2       (about 60 s in)
Set STOP_AT / START_AT (seconds after the run started) to the moments you do this.
They draw event lines on the figure and switch on the evidence summary:
  - requests per second per server before the failure, during the outage and after
    the restart (with each server's share of the traffic),
  - how long failed requests continued after the stop (failover time),
  - how long the restarted server needed to receive traffic again (rejoin time).
Per second it records: successful requests per server, failed requests, cache hits
and misses, and mean latency. Requests are bucketed by the second they were SENT.
Results go to results/phase4/<CONFIG_LABEL>/.

Cache modes (MODE env): cold (0 % hits), mixed50, mixed90, warm (100 % hits; default).
The cache is flushed and, if the mode has hits, pre-warmed before the run starts.
"""
import csv
import json
import os
import queue
import random
import re
import statistics
import threading
import time
from collections import Counter, defaultdict

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
RATE = int(os.getenv("RATE", "100"))
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "90"))
WORKERS = int(os.getenv("WORKERS", "32"))
TEXT_NAME = os.getenv("TEXT_NAME", "") or None
CONFIG_LABEL = os.getenv("CONFIG_LABEL", "run")
STOP_AT = os.getenv("STOP_AT")
START_AT = os.getenv("START_AT")
SETTLE_SECONDS = int(float(os.getenv("SETTLE_SECONDS", "6")))  # ignore detection/rejoin transients
SEED = int(os.getenv("SEED", "42"))
MODE = os.getenv("MODE", "warm")
HIT_KEYWORDS = [k.strip() for k in os.getenv(
    "KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]

MODE_HIT_TARGET = {"cold": 0.0, "mixed50": 0.5, "mixed90": 0.9, "warm": 1.0}
if MODE not in MODE_HIT_TARGET:
    raise SystemExit(f"Unknown MODE '{MODE}'. Choose from {list(MODE_HIT_TARGET)}")

OUT_DIR = os.path.join("results", "phase4", CONFIG_LABEL)
os.makedirs(OUT_DIR, exist_ok=True)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

lock = threading.Lock()
ok_by_sec = defaultdict(Counter)       # second -> Counter(server_id)
fail_by_sec = Counter()                # second -> failed count
lat_by_sec = defaultdict(list)         # second -> [latency_ms]
hit_by_sec = Counter()                 # second -> cache hits reported by the servers
miss_by_sec = Counter()                # second -> cache misses reported by the servers
errors = Counter()
server_ids = set()
jobs = queue.Queue()
t0 = [None]


# ---------------------------------------------------------------- workload
def load_vocabulary():
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
    return VOCAB[n] if n < len(VOCAB) else f"zzmiss{n}"


def build_workload(total, mode):
    """Deterministic plan: list of keywords; never-seen keywords guarantee misses."""
    n_hits = round(total * MODE_HIT_TARGET[mode])
    plan = [True] * n_hits + [False] * (total - n_hits)
    random.Random(SEED).shuffle(plan)
    workload, miss_n = [], 0
    for i, is_hit in enumerate(plan):
        if is_hit:
            workload.append(HIT_KEYWORDS[i % len(HIT_KEYWORDS)])
        else:
            workload.append(miss_keyword(miss_n))
            miss_n += 1
    return workload


def prepare_cache():
    redis_client.flushall()
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
        if MODE_HIT_TARGET[MODE] > 0:
            for kw in HIT_KEYWORDS:
                c.root.count_keyword(kw, TEXT_NAME)
        c.root.reset_stats()
    redis_client.delete("hot_keywords")


# ---------------------------------------------------------------- load
def worker():
    while True:
        job = jobs.get()
        try:
            if job is None:
                return
            keyword, scheduled_at = job
            sec = int(scheduled_at - t0[0])
            start = time.perf_counter()
            try:
                with rpyc.connect(SERVER_HOST, SERVER_PORT,
                                  config={"sync_request_timeout": 10}) as conn:
                    raw = conn.root.count_keyword(keyword, TEXT_NAME)
                latency = (time.perf_counter() - start) * 1000
                info = json.loads(raw)
                sid = info["server_id"]
                with lock:
                    ok_by_sec[sec][sid] += 1
                    lat_by_sec[sec].append(latency)
                    server_ids.add(sid)
                    if info.get("cache") == "HIT":
                        hit_by_sec[sec] += 1
                    elif info.get("cache") == "MISS":
                        miss_by_sec[sec] += 1
            except Exception as exc:
                with lock:
                    fail_by_sec[sec] += 1
                    errors[type(exc).__name__] += 1
        finally:
            jobs.task_done()


def reporter():
    """Print the bucket from 2 s ago every second so you can time your docker commands."""
    printed = -1
    while True:
        time.sleep(1)
        if t0[0] is None:
            continue
        sec = int(time.perf_counter() - t0[0]) - 2
        if sec > printed and sec >= 0:
            printed = sec
            with lock:
                dist = dict(sorted(ok_by_sec[sec].items()))
                lats = lat_by_sec[sec]
                avg = f"{statistics.mean(lats):.1f}" if lats else "-"
                print(f"t={sec:3d}s ok={sum(dist.values()):4d} failed={fail_by_sec[sec]:4d} "
                      f"avg_ms={avg:>7} {dist}", flush=True)


total = RATE * DURATION_SECONDS
workload = build_workload(total, MODE)
prepare_cache()

threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
for t in threads:
    t.start()
threading.Thread(target=reporter, daemon=True).start()

print(f"[{CONFIG_LABEL}] {RATE} req/s for {DURATION_SECONDS}s via {SERVER_HOST}:{SERVER_PORT}; "
      f"cache mode {MODE} ({MODE_HIT_TARGET[MODE]:.0%} designed hits). "
      f"Stop/start a server from another terminal now.", flush=True)

interval = 1.0 / RATE
t0[0] = time.perf_counter()
for i, keyword in enumerate(workload):
    scheduled_at = t0[0] + i * interval
    remaining = scheduled_at - time.perf_counter()
    if remaining > 0:
        time.sleep(remaining)
    jobs.put((keyword, scheduled_at))

for _ in threads:
    jobs.put(None)
jobs.join()
for t in threads:
    t.join()
time.sleep(1.2)  # let the reporter flush the last buckets

seconds = list(range(DURATION_SECONDS))
sids = sorted(server_ids)

with open(os.path.join(OUT_DIR, "timeline.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["second", "ok", "failed", "mean_latency_ms", "cache_hits", "cache_misses"] + sids)
    for s in seconds:
        lats = lat_by_sec[s]
        w.writerow([s, sum(ok_by_sec[s].values()), fail_by_sec[s],
                    f"{statistics.mean(lats):.3f}" if lats else "",
                    hit_by_sec[s], miss_by_sec[s]]
                   + [ok_by_sec[s][sid] for sid in sids])

total_ok = sum(sum(c.values()) for c in ok_by_sec.values())
total_fail = sum(fail_by_sec.values())
print(f"\nTotal ok={total_ok} failed={total_fail} "
      f"({100 * total_fail / max(1, total_ok + total_fail):.1f}% failed)")
print(f"Error types: {dict(errors)}")
print(f"Per-server totals: { {sid: sum(ok_by_sec[s][sid] for s in seconds) for sid in sids} }")

obs_hits, obs_misses = sum(hit_by_sec.values()), sum(miss_by_sec.values())
print(f"Cache (as reported per request): hits={obs_hits} misses={obs_misses} "
      f"hit ratio={obs_hits / max(1, obs_hits + obs_misses):.1%} (designed {MODE_HIT_TARGET[MODE]:.0%})")
try:
    with rpyc.connect(SERVER_HOST, SERVER_PORT) as c:
        st = json.loads(c.root.cache_stats())
    print(f"Cache (server counters, all replicas): hits={st['hits']} misses={st['misses']} "
          f"hit ratio={st['hit_ratio']:.1%}")
except Exception as exc:
    print(f"Could not read server cache counters: {exc!r}")

# ---------------------------------------------------------------- evidence summary
stop = int(float(STOP_AT)) if STOP_AT else None
start_s = int(float(START_AT)) if START_AT else None

if stop is not None:
    out_end = start_s if start_s is not None else DURATION_SECONDS
    windows = [("before failure", range(0, stop)),
               (f"outage (+{SETTLE_SECONDS}s settle)",
                range(min(stop + SETTLE_SECONDS, out_end), out_end))]
    if start_s is not None:
        windows.append((f"after restart (+{SETTLE_SECONDS}s settle)",
                        range(min(start_s + SETTLE_SECONDS, DURATION_SECONDS), DURATION_SECONDS)))

    print("\n=== Phase summary (requests/second per server, share of traffic) ===")
    rows = []
    for name, rng in windows:
        n = len(rng)
        if n == 0:
            continue
        rps = {sid: sum(ok_by_sec[s][sid] for s in rng) / n for sid in sids}
        failed = sum(fail_by_sec[s] for s in rng)
        tot = sum(rps.values()) or 1.0
        share = {sid: rps[sid] / tot * 100 for sid in sids}
        print(f"{name:32} seconds={n:3d} failed={failed:5d}  "
              + "  ".join(f"{sid}={rps[sid]:6.1f} ({share[sid]:4.1f}%)" for sid in sids))
        rows.append([name, n, failed] + [f"{rps[sid]:.2f}" for sid in sids]
                    + [f"{share[sid]:.1f}" for sid in sids])
    with open(os.path.join(OUT_DIR, "phase_summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phase", "seconds", "failed"] + [f"{s}_rps" for s in sids]
                   + [f"{s}_share_pct" for s in sids])
        w.writerows(rows)

    fail_secs = [s for s in range(stop, out_end) if fail_by_sec[s] > 0]
    failed_in_outage = sum(fail_by_sec[s] for s in range(stop, out_end))
    failover = (max(fail_secs) - stop + 1) if fail_secs else 0
    print(f"\nFailover: failed requests continued for {failover}s after the stop "
          f"({failed_in_outage} failed requests in total during the outage).")

    if start_s is not None and sids:
        out_rng = range(min(stop + SETTLE_SECONDS, out_end), out_end)
        if len(out_rng) > 0:
            out_rps = {sid: sum(ok_by_sec[s][sid] for s in out_rng) / len(out_rng) for sid in sids}
            restarted = min(sids, key=lambda sid: out_rps[sid])
            if out_rps[restarted] < 1:
                rejoin = next((s - start_s for s in range(start_s, DURATION_SECONDS)
                               if ok_by_sec[s][restarted] > 0), None)
                if rejoin is None:
                    print(f"Rejoin: {restarted} did NOT receive traffic again before the run ended.")
                else:
                    print(f"Rejoin: {restarted} received traffic again {rejoin}s after the restart.")

# ---------------------------------------------------------------- figure
fig, ax = plt.subplots(figsize=(9, 4.5))
for sid in sids:
    ax.plot(seconds, [ok_by_sec[s][sid] for s in seconds], label=sid)
ax.plot(seconds, [fail_by_sec[s] for s in seconds], "k--", label="failed requests")
if STOP_AT:
    ax.axvline(float(STOP_AT), color="red", linestyle=":", label="server stopped")
if START_AT:
    ax.axvline(float(START_AT), color="green", linestyle=":", label="server restarted")
ax.set_xlabel("Time (s)")
ax.set_ylabel("Requests per second")
ax.set_title(f"Requests per server over time ({CONFIG_LABEL}, {MODE} cache)")
ax.grid(True)
ax.legend()
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "timeline.png"), dpi=200)
plt.close(fig)
print(f"Saved {OUT_DIR}/timeline.csv, timeline.png" + (" and phase_summary.csv" if stop is not None else ""))