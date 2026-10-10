import time
import rpyc

SERVER_HOST = "server"
SERVER_PORT = 18812

keyword = input("Enter a keyword: ").strip()

start = time.perf_counter()

with rpyc.connect(
    SERVER_HOST,
    SERVER_PORT,
    config={"sync_request_timeout": 30},
) as connection:
    result = connection.root.count_keyword(keyword)

elapsed_ms = (time.perf_counter() - start) * 1000

print(f"Keyword: {keyword}")
print(f"Occurrences: {result}")
print(f"Execution latency: {elapsed_ms:.3f} ms")
