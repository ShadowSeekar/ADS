"""Constant-rate load for a fixed duration, one RPyC connection per request.

While it runs, stop and restart a server from another terminal, e.g.
    docker stop server2        (about 30 s in)
    docker start server2       (about 60 s in)
It records, per second of the run: successful requests per server, failed
requests, and mean latency. Results go to results/phase4/<CONFIG_LABEL>/.

Requests are bucketed by the second in which they were SENT.
Set STOP_AT / START_AT (seconds) to draw event lines on the figure.
"""
import csv
import json
import os
import queue
import statistics
import threading
import time
from collections import Counter, defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "load-balancer")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))
RATE = int(os.getenv("RATE", "100"))
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "90"))
WORKERS = int(os.getenv("WORKERS", "32"))
TEXT_NAME = os.getenv("TEXT_NAME", "") or None
CONFIG_LABEL = os.getenv("CONFIG_LABEL", "run")
STOP_AT = os.getenv("STOP_AT")
START_AT = os.getenv("START_AT")
KEYWORDS = [k.strip() for k in os.getenv(
    "KEYWORDS", "Deadpool,blood,knows,feels,slaps").split(",") if k.strip()]

OUT_DIR = os.path.join("results", "phase4", CONFIG_LABEL)
os.makedirs(OUT_DIR, exist_ok=True)

lock = threading.Lock()
ok_by_sec = defaultdict(Counter)       # second -> Counter(server_id)
fail_by_sec = Counter()                # second -> failed count
lat_by_sec = defaultdict(list)         # second -> [latency_ms]
errors = Counter()
server_ids = set()
jobs = queue.Queue()
t0 = [None]


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
                sid = json.loads(raw)["server_id"]
                with lock:
                    ok_by_sec[sec][sid] += 1
                    lat_by_sec[sec].append(latency)
                    server_ids.add(sid)
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


threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
for t in threads:
    t.start()
threading.Thread(target=reporter, daemon=True).start()

print(f"[{CONFIG_LABEL}] {RATE} req/s for {DURATION_SECONDS}s via {SERVER_HOST}:{SERVER_PORT}. "
      f"Stop/start a server from another terminal now.", flush=True)

total = RATE * DURATION_SECONDS
interval = 1.0 / RATE
t0[0] = time.perf_counter()
for i in range(total):
    scheduled_at = t0[0] + i * interval
    remaining = scheduled_at - time.perf_counter()
    if remaining > 0:
        time.sleep(remaining)
    jobs.put((KEYWORDS[i % len(KEYWORDS)], scheduled_at))

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
    w.writerow(["second", "ok", "failed", "mean_latency_ms"] + sids)
    for s in seconds:
        lats = lat_by_sec[s]
        w.writerow([s, sum(ok_by_sec[s].values()), fail_by_sec[s],
                    f"{statistics.mean(lats):.3f}" if lats else ""]
                   + [ok_by_sec[s][sid] for sid in sids])

total_ok = sum(sum(c.values()) for c in ok_by_sec.values())
total_fail = sum(fail_by_sec.values())
print(f"\nTotal ok={total_ok} failed={total_fail} ({100 * total_fail / max(1, total_ok + total_fail):.1f}% failed)")
print(f"Error types: {dict(errors)}")
print(f"Per-server totals: { {sid: sum(ok_by_sec[s][sid] for s in seconds) for sid in sids} }")

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
ax.set_title(f"Requests per server over time ({CONFIG_LABEL})")
ax.grid(True)
ax.legend()
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "timeline.png"), dpi=200)
plt.close(fig)
print(f"Saved {OUT_DIR}/timeline.csv and timeline.png")