import hashlib
import json
import os
import re
import time

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TEXT_DIR = os.getenv("TEXT_DIR", "/app/script")
DEFAULT_TEXT = os.getenv("DEFAULT_TEXT", "sample")
SERVER_ID = os.getenv("SERVER_ID", os.getenv("HOSTNAME", "wordcount-server"))
VERBOSE = os.getenv("SERVER_VERBOSE", "0") == "1"
# BYPASS_CACHE=1 recomputes the count on every request (never serves from Redis),
# which makes the servers do real work so scaling becomes visible.
BYPASS_CACHE = os.getenv("BYPASS_CACHE", "0") == "1"
# Simulated extra processing time, to make one replica slower than the others.
ARTIFICIAL_DELAY_S = float(os.getenv("ARTIFICIAL_DELAY_MS", "0")) / 1000.0
HOT_KEY = "hot_keywords"

cache = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

TEXTS = {}
for filename in sorted(os.listdir(TEXT_DIR)):
    if filename.lower().endswith(".txt"):
        name = os.path.splitext(filename)[0]
        with open(os.path.join(TEXT_DIR, filename), "r", encoding="utf-8") as f:
            content = f.read()
        TEXTS[name] = (content, hashlib.sha256(content.encode("utf-8")).hexdigest())

if not TEXTS:
    raise RuntimeError(f"No .txt files found in {TEXT_DIR}")
if DEFAULT_TEXT not in TEXTS:
    raise RuntimeError(f"DEFAULT_TEXT '{DEFAULT_TEXT}' not found. Available: {sorted(TEXTS)}")


class WordCountService(rpyc.Service):
    def exposed_count_keyword(self, keyword, text=None):
        """Return a JSON string: {"count": int, "server_id": str, "cache": str}.

        A JSON string (primitive) is returned instead of a dict. A dict would
        be passed as an RPyC netref and fail once the connection is closed.
        """
        keyword = str(keyword).strip().lower()
        if not keyword:
            raise ValueError("Keyword cannot be empty")

        text_name = str(text).strip() if text else DEFAULT_TEXT
        if text_name not in TEXTS:
            raise ValueError(f"Unknown text '{text_name}'. Available: {sorted(TEXTS)}")
        content, text_hash = TEXTS[text_name]

        cache_key = f"wordcount:{text_hash}:{keyword}"

        # One Redis round trip: lookup + hot-keyword counter.
        pipe = cache.pipeline(transaction=False)
        pipe.get(cache_key)
        pipe.zincrby(HOT_KEY, 1, keyword)
        cached_result, _ = pipe.execute()

        if cached_result is not None and not BYPASS_CACHE:
            count = int(cached_result)
            status = "HIT"
        else:
            pattern = rf"\b{re.escape(keyword)}\b"
            count = len(re.findall(pattern, content, flags=re.IGNORECASE))
            if BYPASS_CACHE:
                status = "BYPASS"
            else:
                cache.set(cache_key, count)
                status = "MISS"

        if ARTIFICIAL_DELAY_S > 0:
            time.sleep(ARTIFICIAL_DELAY_S)

        if VERBOSE:
            print(
                f"server={SERVER_ID} text={text_name} keyword={keyword!r} "
                f"count={count} cache={status}",
                flush=True,
            )
        return json.dumps({"count": count, "server_id": SERVER_ID, "cache": status})

    def exposed_hot_keywords(self, n=5):
        n = max(1, int(n))
        top = cache.zrevrange(HOT_KEY, 0, n - 1, withscores=True)
        return json.dumps([[kw, int(score)] for kw, score in top])

    def exposed_list_texts(self):
        return json.dumps(sorted(TEXTS))


if __name__ == "__main__":
    server = ThreadedServer(
        WordCountService,
        hostname="0.0.0.0",
        port=18812,
        protocol_config={"allow_public_attrs": False},
    )
    print(
        f"Word-count server {SERVER_ID} listening on port 18812; texts={sorted(TEXTS)}; "
        f"bypass_cache={BYPASS_CACHE}; delay_ms={ARTIFICIAL_DELAY_S * 1000:.0f}",
        flush=True,
    )
    server.start()