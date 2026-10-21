import json
import os
import re
import time
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "load-balancer")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))

scripts = {
    "1": "Alien",
    "2": "Deadpool",
    "3": "Jurassic Park",
    "4": "X-Men"
}

print("Which script do you want to search?")
print("1. Alien")
print("2. Deadpool")
print("3. Jurassic Park")
print("4. X-Men")

script_choice = input("Enter your choice (1-4): ").strip()

while script_choice not in scripts:
    script_choice = input("Invalid choice. Please enter 1, 2, 3, or 4: ").strip()

text_name = scripts[script_choice]

raw_input_words = input("Enter one or more words to count (separated by spaces or commas): ")
words = []
for w in re.split(r"[,\s]+", raw_input_words.strip()):
    if w and w.lower() not in (x.lower() for x in words):
        words.append(w)

while not words:
    raw_input_words = input("Please enter at least one word: ")
    words = [w for w in re.split(r"[,\s]+", raw_input_words.strip()) if w]
results = []
batch_start = time.perf_counter()
for word in words:
    start = time.perf_counter()
    with rpyc.connect(
        SERVER_HOST,
        SERVER_PORT,
        config={"sync_request_timeout": 30},
    ) as connection:
        raw = connection.root.count_keyword(word, text_name)
    elapsed_ms = (time.perf_counter() - start) * 1000
    response = json.loads(raw)
    results.append((word, response["count"], response["server_id"],
                    response["cache"], elapsed_ms))
total_ms = (time.perf_counter() - batch_start) * 1000

print(f"\nText: {text_name}")
print(f"{'Word':<18} {'Occurrences':>11}  {'Served by':<10} {'Cache':<7} {'Latency (ms)':>12}")
for word, count, server_id, cache_status, ms in results:
    print(f"{word:<18} {count:>11}  {server_id:<10} {cache_status:<7} {ms:>12.3f}")
print(f"\nTotal for {len(results)} word(s): {total_ms:.3f} ms "
      f"(latency includes connection setup)")