import csv
import math
import os
import statistics
import sys
from collections import defaultdict

def p99(values):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.99 * len(ordered)) - 1)]

labels = sys.argv[1:] or sorted(
    d for d in os.listdir("results")
    if os.path.exists(os.path.join("results", d, "request_latencies.csv")))

for label in labels:
    path = os.path.join("results", label, "request_latencies.csv")
    groups = defaultdict(list)
    totals = defaultdict(int)
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["success"] != "True":
                continue
            key = (r.get("mode", ""), int(r["target_rate"]))
            groups[key + (r["server_id"],)].append(float(r["latency_ms"]))
            totals[key] += 1
    print(f"\n=== {label} ===")
    print(f"{'mode':8} {'rate':>5} {'server':<9} {'requests':>8} {'share':>7} {'mean ms':>9} {'p99 ms':>9}")
    for (mode, rate, server), lats in sorted(groups.items()):
        share = 100 * len(lats) / totals[(mode, rate)]
        print(f"{mode:8} {rate:>5} {server:<9} {len(lats):>8} {share:>6.1f}% "
              f"{statistics.mean(lats):>9.2f} {p99(lats):>9.2f}")