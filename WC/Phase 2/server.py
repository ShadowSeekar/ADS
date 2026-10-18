import hashlib
import json
import os
import re

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TEXT_DIR = os.getenv("TEXT_DIR", "/app/script")
DEFAULT_TEXT = os.getenv("DEFAULT_TEXT", "sample")
VERBOSE = os.getenv("SERVER_VERBOSE", "0") == "1"
HOT_KEY = "hot_keywords"

cache = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

# Load every *.txt file in TEXT_DIR. Clients refer to a text by its name
# (file name without extension), e.g. "sample" -> /app/script/sample.txt.
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
        """Return the number of occurrences of `keyword` in the named text (int)."""
        keyword = str(keyword).strip().lower()
        if not keyword:
            raise ValueError("Keyword cannot be empty")

        text_name = str(text).strip() if text else DEFAULT_TEXT
        if text_name not in TEXTS:
            raise ValueError(f"Unknown text '{text_name}'. Available: {sorted(TEXTS)}")
        content, text_hash = TEXTS[text_name]

        cache_key = f"wordcount:{text_hash}:{keyword}"

        # One Redis round trip: read the cached value and bump the hot-keyword score.
        pipe = cache.pipeline(transaction=False)
        pipe.get(cache_key)
        pipe.zincrby(HOT_KEY, 1, keyword)
        cached_result, _ = pipe.execute()

        if cached_result is not None:
            count = int(cached_result)
            status = "HIT"
        else:
            pattern = rf"\b{re.escape(keyword)}\b"
            count = len(re.findall(pattern, content, flags=re.IGNORECASE))
            cache.set(cache_key, count)
            status = "MISS"

        if VERBOSE:
            print(f"text={text_name} keyword={keyword!r} count={count} cache={status}", flush=True)
        return count

    def exposed_hot_keywords(self, n=5):
        """Top-n most requested keywords as a JSON string [[keyword, requests], ...].

        JSON (a primitive str) is returned because lists/dicts would be passed
        by reference as RPyC netrefs and break once the connection is closed.
        """
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
    print(f"Word-count server listening on port 18812; texts={sorted(TEXTS)}", flush=True)
    server.start()