import json
import os
import sys
import time

import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))

# Usage: python client.py [keyword] [text_name]
#keyword = sys.argv[1] if len(sys.argv) > 1 else input("Enter a keyword to count: ").strip()
#text_name = sys.argv[2] if len(sys.argv) > 2 else input("Enter script name to search in (blank = default): ").strip() or None

scripts = {
    "1": "Alien",
    "2": "Deadpool",
    "3": "Jurassic Park",
    "4": "X-Men"
}

# Ask which script to search
print("Which script do you want to search?")
print("1. Alien")
print("2. Deadpool")
print("3. Jurassic Park")
print("4. X-Men")

script_choice = input("Enter your choice (1-4): ").strip()

while script_choice not in scripts:
    script_choice = input("Invalid choice. Please enter 1, 2, 3, or 4: ").strip()

text_name = scripts[script_choice]

# Ask for the keyword to count
keyword = input("Enter a keyword to count: ").strip()


with rpyc.connect(
    SERVER_HOST,
    SERVER_PORT,
    config={"sync_request_timeout": 30},
) as connection:
    start = time.perf_counter()
    result = connection.root.count_keyword(keyword, text_name)
    elapsed_ms = (time.perf_counter() - start) * 1000
    hot = json.loads(connection.root.hot_keywords(5))

print(f"Keyword: {keyword}")
print(f"Text: {text_name or '(default)'}")
print(f"Occurrences: {result}")
print(f"Execution latency: {elapsed_ms:.3f} ms")
print("Hot keywords (top 5):")
for kw, n in hot:
    print(f"  {kw}: {n} requests")