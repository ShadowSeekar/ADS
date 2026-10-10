import csv
import math
import os
import statistics
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = 18812

RATES = [10, 20, 30, 40, 50]
DURATION_SECONDS = 20
KEYWORDS = ["Docker", "containers", "applications", "services", "distributed"]

os.makedirs("results", exist_ok=True)

all_results = []
summary = []


def percentile(values, p):
    ordered = sorted(values)
    index = max(0, math.ceil(p * len(ordered)) - 1)
    return ordered[index]


for rate in RATES:
    print(f"\nTesting target rate: {rate} requests/second")

    latencies = []
    request_records = []
    interval = 1.0 / rate
    start = time.perf_counter()
    next_request = start
    deadline = start + DURATION_SECONDS
    request_number = 0

    with rpyc.connect(
        SERVER_HOST,
        SERVER_PORT,
        config={"sync_request_timeout": 30},
    ) as connection:
        while next_request < deadline:
            now = time.perf_counter()

            if now < next_request:
                time.sleep(next_request - now)

            sent_at = time.perf_counter()

            keyword = KEYWORDS[request_number % len(KEYWORDS)]
            try:
                result = connection.root.count_keyword(keyword)
            except Exception as error:
                print(f"Request failed: {error}")
                break

            received_at = time.perf_counter()
            latency_ms = (received_at - sent_at) * 1000
            latencies.append(latency_ms)

            request_records.append({
                "target_rate": rate,
                "request_number": request_number + 1,
                "keyword": keyword,
                "result": result,
                "latency_ms": latency_ms,
            })

            request_number += 1
            next_request += interval

    elapsed = time.perf_counter() - start

    if not latencies:
        raise RuntimeError(f"No successful requests at rate {rate}")

    mean_latency = statistics.mean(latencies)
    p99_latency = percentile(latencies, 0.99)
    achieved_rate = len(latencies) / elapsed

    summary.append({
        "target_rate": rate,
        "achieved_rate": achieved_rate,
        "successful_requests": len(latencies),
        "mean_latency_ms": mean_latency,
        "p99_latency_ms": p99_latency,
    })
    all_results.extend(request_records)

    print(f"Successful requests: {len(latencies)}")
    print(f"Achieved rate: {achieved_rate:.2f} requests/second")
    print(f"Average latency: {mean_latency:.3f} ms")
    print(f"P99 latency: {p99_latency:.3f} ms")


with open("results/request_latencies.csv", "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=all_results[0].keys())
    writer.writeheader()
    writer.writerows(all_results)

with open("results/summary.csv", "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=summary[0].keys())
    writer.writeheader()
    writer.writerows(summary)


rates = [row["target_rate"] for row in summary]
means = [row["mean_latency_ms"] for row in summary]
p99s = [row["p99_latency_ms"] for row in summary]

plt.figure()
plt.plot(rates, means, marker="o")
plt.xlabel("Target keyword request rate (requests/second)")
plt.ylabel("Average request latency (ms)")
plt.title("Average Word-Count Request Latency")
plt.grid(True)
plt.tight_layout()
plt.savefig("results/average_latency.png", dpi=200)
plt.close()

plt.figure()
plt.plot(rates, p99s, marker="o")
plt.xlabel("Target keyword request rate (requests/second)")
plt.ylabel("99th-percentile request latency (ms)")
plt.title("P99 Word-Count Request Latency")
plt.grid(True)
plt.tight_layout()
plt.savefig("results/p99_latency.png", dpi=200)
plt.close()

print("\nBenchmark complete. Results saved in the results/ directory.")
