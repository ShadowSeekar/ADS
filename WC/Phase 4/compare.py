"""Combine results/<config>/summary.csv into comparison figures.

1. For every cache mode: 1 server vs 3 servers (both algorithms) - average latency,
   P99 latency and throughput (one line per configuration).
2. Across cache modes: grouped bars of mean latency / throughput per configuration.
The two figures required by the assignment keep their old names
(compare_average_latency.png, compare_p99_latency.png) and use COMPARE_MODE
(default: warm, else the first mode found).
"""
import csv
import glob
import os
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Preferred order and display names; any other folder is plotted under its folder name.
ORDER = ["single", "least_connections", "least_response_time"]
NAMES = {
    "single": "1 server (no LB)",
    "least_connections": "3 servers, least connections",
    "least_response_time": "3 servers, least response time",
}
MODE_ORDER = ["cold", "mixed50", "mixed90", "warm"]

data = {}
for path in glob.glob("results/*/summary.csv"):
    label = os.path.basename(os.path.dirname(path))
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        if not r.get("mode"):  # results from the older, mode-less benchmark
            r["mode"] = "default"
    data[label] = rows

if not data:
    raise SystemExit("No results/*/summary.csv found. Run benchmark.py first.")

labels = [l for l in ORDER if l in data] + sorted(l for l in data if l not in ORDER)
found_modes = {r["mode"] for rows in data.values() for r in rows}
modes = [m for m in MODE_ORDER if m in found_modes] + sorted(found_modes - set(MODE_ORDER))
COMPARE_MODE = os.getenv("COMPARE_MODE") or ("warm" if "warm" in modes else modes[0])


def series(label, mode):
    rows = [r for r in data[label] if r["mode"] == mode]
    return sorted(rows, key=lambda r: float(r["target_rate"]))


def num(row, key):
    try:
        return float(row[key])
    except (KeyError, ValueError, TypeError):
        return float("nan")


def plot_configs(mode, key, ylabel, title, filename):
    """One line per configuration for a single cache mode."""
    plt.figure()
    drew = False
    for label in labels:
        rows = series(label, mode)
        if not rows:
            continue
        plt.plot([num(r, "target_rate") for r in rows], [num(r, key) for r in rows],
                 marker="o", label=NAMES.get(label, label))
        drew = True
    if drew:
        plt.xlabel("Target keyword request rate (requests/second)")
        plt.ylabel(ylabel)
        plt.title(f"{title} [{mode} cache]")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"results/{filename}", dpi=200)
    plt.close()


def plot_mode_bars(key, ylabel, title, filename):
    """Grouped bars: x = cache mode, one bar per configuration (mean over all rates)."""
    plt.figure()
    width = 0.8 / len(labels)
    for i, label in enumerate(labels):
        vals = []
        for m in modes:
            rows = series(label, m)
            vals.append(statistics.mean(num(r, key) for r in rows) if rows else float("nan"))
        xs = [j + i * width for j in range(len(modes))]
        plt.bar(xs, vals, width=width, label=NAMES.get(label, label))
    plt.xticks([j + 0.4 - width / 2 for j in range(len(modes))], modes)
    plt.xlabel("Cache mode")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, axis="y")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"results/{filename}", dpi=200)
    plt.close()


saved = []
for mode in modes:
    plot_configs(mode, "mean_latency_ms", "Average request latency (ms)",
                 "Average Latency: 1 Server vs 3 Servers", f"compare_average_latency_{mode}.png")
    plot_configs(mode, "p99_latency_ms", "99th-percentile latency (ms)",
                 "P99 Latency: 1 Server vs 3 Servers", f"compare_p99_latency_{mode}.png")
    plot_configs(mode, "achieved_rate", "Achieved throughput (requests/second)",
                 "Throughput: 1 Server vs 3 Servers", f"compare_throughput_{mode}.png")
    saved.append(mode)

# Figures with the original names, for the cache mode chosen above.
plot_configs(COMPARE_MODE, "mean_latency_ms", "Average request latency (ms)",
             "Average Latency: 1 Server vs 3 Servers", "compare_average_latency.png")
plot_configs(COMPARE_MODE, "p99_latency_ms", "99th-percentile latency (ms)",
             "P99 Latency: 1 Server vs 3 Servers", "compare_p99_latency.png")

if len(modes) > 1:
    plot_mode_bars("mean_latency_ms", "Mean latency, averaged over rates (ms)",
                   "Latency by Cache Mode and Configuration", "compare_cache_modes_latency.png")
    plot_mode_bars("achieved_rate", "Mean achieved throughput (requests/second)",
                   "Throughput by Cache Mode and Configuration", "compare_cache_modes_throughput.png")

print(f"Saved per-mode figures for {saved}, plus compare_average_latency.png and "
      f"compare_p99_latency.png (mode: {COMPARE_MODE})"
      + (", compare_cache_modes_latency.png and compare_cache_modes_throughput.png"
         if len(modes) > 1 else "") + " in results/")

for mode in modes:
    print(f"\n##### cache mode: {mode} #####")
    for label in labels:
        rows = series(label, mode)
        if not rows:
            continue
        print(f"\n{NAMES.get(label, label)}")
        for r in rows:
            print(f"  rate={r['target_rate']:>4}  avg={num(r, 'mean_latency_ms'):8.2f} ms  "
                  f"p99={num(r, 'p99_latency_ms'):8.2f} ms  "
                  f"achieved={num(r, 'achieved_rate'):7.1f} rps  "
                  f"hit={num(r, 'actual_hit_ratio') * 100:5.1f}%  "
                  f"dist={r.get('server_distribution', '')}")