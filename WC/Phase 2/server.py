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
STATS_KEY = "cache_stats"

cache = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

LOOKUP = cache.register_script(
    """
local v = redis.call('GET', KEYS[1])
redis.call('ZINCRBY', KEYS[2], 1, ARGV[1])
if v then
  redis.call('HINCRBY', KEYS[3], 'hits', 1)
else
  redis.call('HINCRBY', KEYS[3], 'misses', 1)
end
return v
"""
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


def normalise_keyword(keyword):
    return " ".join(str(keyword).lower().split())

def build_pattern(keyword):
    words = keyword.split(" ")
    separator = r"\s+"
    return r"\b" + separator.join(re.escape(w) for w in words) + r"\b"

class WordCountService(rpyc.Service):
    def exposed_count_keyword(self, keyword, text=None):
        keyword = normalise_keyword(keyword)
        if not keyword:
            raise ValueError("Keyword cannot be empty")

        text_name = str(text).strip() if text else DEFAULT_TEXT
        if text_name not in TEXTS:
            raise ValueError(f"Unknown text '{text_name}'. Available: {sorted(TEXTS)}")
        content, text_hash = TEXTS[text_name]
        cache_key = f"wordcount:{text_hash}:{keyword}"
        cached_result = LOOKUP(keys=[cache_key, HOT_KEY, STATS_KEY], args=[keyword])

        if cached_result is not None:
            count = int(cached_result)
            status = "HIT"
        else:
            count = len(re.findall(build_pattern(keyword), content, flags=re.IGNORECASE))
            cache.set(cache_key, count)
            status = "MISS"

        if VERBOSE:
            print(f"text={text_name} keyword={keyword!r} count={count} cache={status}", flush=True)
        return count

    def exposed_hot_keywords(self, n=5):
        n = max(1, int(n))
        top = cache.zrevrange(HOT_KEY, 0, n - 1, withscores=True)
        return json.dumps([[kw, int(score)] for kw, score in top])

    def exposed_cache_stats(self):
        raw = cache.hgetall(STATS_KEY)
        hits, misses = int(raw.get("hits", 0)), int(raw.get("misses", 0))
        total = hits + misses
        return json.dumps({"hits": hits, "misses": misses,
                           "hit_ratio": (hits / total) if total else 0.0})

    def exposed_reset_stats(self):
        cache.delete(STATS_KEY)
        return True

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