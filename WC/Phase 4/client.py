import json
import os
import sys
import time

import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "load-balancer")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18812"))

# Usage: python client.py [keyword] [text_name]
keyword = sys.argv[1] if len(sys.argv) > 1 else input("Enter a keyword: ").strip()
text_name = sys.argv[2] if len(sys.argv) > 2 else (input("Enter text name (blank = default): ").strip() or None)

start = time.perf_counter()
with rpyc.connect(
    SERVER_HOST,
    SERVER_PORT,
    config={"sync_request_timeout": 30},
) as connection:
    raw = connection.root.count_keyword(keyword, text_name)
elapsed_ms = (time.perf_counter() - start) * 1000

# raw is a plain str, so it is safe to use after the connection has closed.
response = json.loads(raw)

print(f"Keyword: {keyword}")
print(f"Occurrences: {response['count']}")
print(f"Served by: {response['server_id']}")
print(f"Redis cache: {response['cache']}")
print(f"Execution latency (includes connection setup): {elapsed_ms:.3f} ms")