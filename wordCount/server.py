import os
import re
import hashlib

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TEXT_PATH = "/app/texts/sample.txt"

cache = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

with open(TEXT_PATH, "r", encoding="utf-8") as file:
    TEXT = file.read()

TEXT_HASH = hashlib.sha256(TEXT.encode("utf-8")).hexdigest()


class WordCountService(rpyc.Service):
    def exposed_count_keyword(self, keyword):
        keyword = keyword.strip().lower()

        if not keyword:
            raise ValueError("Keyword cannot be empty")

        cache_key = f"wordcount:{TEXT_HASH}:{keyword}"

        cached_result = cache.get(cache_key)
        if cached_result is not None:
            return int(cached_result)

        pattern = rf"\b{re.escape(keyword)}\b"
        count = len(re.findall(pattern, TEXT, flags=re.IGNORECASE))

        cache.set(cache_key, count)
        return count


if __name__ == "__main__":
    server = ThreadedServer(
        WordCountService,
        hostname="0.0.0.0",
        port=18812,
    )
    print("Word-count server listening on port 18812")
    server.start()
