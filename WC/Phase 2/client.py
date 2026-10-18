import json
import os
import sys
import time

import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))

# Usage: python client.py [keyword] [text_name]
keyword = sys.argv[1] if len(sys.argv) > 1 else input("Enter a keyword: ").strip()
text_name = sys.argv[2] if len(sys.argv) > 2 else input("Enter text name (blank = default): ").strip() or None

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