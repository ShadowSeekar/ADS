import json
import os
import re
import time
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
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

raw = input("Enter one or more words to count (separated by spaces or commas): ")
words = []
for w in re.split(r"[,\s]+", raw.strip()):
    if w and w.lower() not in (x.lower() for x in words):  # skip empty and repeated words
        words.append(w)

while not words:
    raw = input("Please enter at least one word: ")
    words = [w for w in re.split(r"[,\s]+", raw.strip()) if w]

results = []
with rpyc.connect(
    SERVER_HOST,
    SERVER_PORT,
    config={"sync_request_timeout": 30},
) as connection:
    batch_start = time.perf_counter()
    for word in words:
        start = time.perf_counter()
        count = connection.root.count_keyword(word, text_name)
        elapsed_ms = (time.perf_counter() - start) * 1000
        results.append((word, count, elapsed_ms))
    total_ms = (time.perf_counter() - batch_start) * 1000
    hot = json.loads(connection.root.hot_keywords(5))

print(f"\nText: {text_name or '(default)'}")
print(f"{'Word':<20} {'Occurrences':>12} {'Latency (ms)':>14}")
for word, count, ms in results:
    print(f"{word:<20} {count:>12} {ms:>14.3f}")
print(f"\nTotal for {len(results)} word(s): {total_ms:.3f} ms")

print("Hot keywords (top 5):")
for kw, n in hot:
    print(f"  {kw}: {n} requests")