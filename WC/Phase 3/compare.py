"""Combine results/<config>/summary.csv into two comparison figures
(average and P99 latency, one line per configuration)."""
import csv
import glob
import os

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

data = {}
for path in glob.glob("results/*/summary.csv"):
    label = os.path.basename(os.path.dirname(path))
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    data[label] = rows

if not data:
    raise SystemExit("No results/*/summary.csv found. Run benchmark_phase3.py first.")

labels = [l for l in ORDER if l in data] + sorted(l for l in data if l not in ORDER)


def plot(key, ylabel, title, filename):
    plt.figure()
    for label in labels:
        rows = data[label]
        plt.plot([float(r["target_rate"]) for r in rows],
                 [float(r[key]) for r in rows],
                 marker="o", label=NAMES.get(label, label))
    plt.xlabel("Target keyword request rate (requests/second)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"results/{filename}", dpi=200)
    plt.close()


plot("mean_latency_ms", "Average request latency (ms)",
     "Average Latency: 1 Server vs 3 Servers", "compare_average_latency.png")
plot("p99_latency_ms", "99th-percentile latency (ms)",
     "P99 Latency: 1 Server vs 3 Servers", "compare_p99_latency.png")

print("Saved results/compare_average_latency.png and results/compare_p99_latency.png")
for label in labels:
    print(f"\n{NAMES.get(label, label)}")
    for r in data[label]:
        print(f"  rate={r['target_rate']:>4}  avg={float(r['mean_latency_ms']):8.2f} ms  "
              f"p99={float(r['p99_latency_ms']):8.2f} ms  "
              f"achieved={float(r['achieved_rate']):7.1f} rps  dist={r['server_distribution']}")