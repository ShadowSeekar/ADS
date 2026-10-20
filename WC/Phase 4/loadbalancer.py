"""Raw TCP proxy for RPyC with fault tolerance.

Dynamic policies (unchanged from Phase 3):
  - least_connections
  - least_response_time: score = EWMA(connection duration) * (active + 1)

Fault tolerance (HEALTH_CHECK=on):
  1. Active health monitor: every HEALTH_INTERVAL seconds each backend gets a TCP
     probe (including backends marked down). FAIL_THRESHOLD consecutive failures
     mark a backend unhealthy; RISE_THRESHOLD consecutive successes mark it
     healthy again, so a restarted server rejoins automatically.
  2. Passive detection + retry: if connecting to the chosen backend fails, that
     backend is marked unhealthy immediately and the request is retried on
     another healthy backend, so the client does not see the failure.
  3. Failed connects never update the EWMA (they would look "fast").
  4. A recovered backend's EWMA is seeded with the average of its healthy peers,
     so least_response_time does not send it a burst of connections.

HEALTH_CHECK=off reproduces the Phase 3 behaviour (no probes, no retry, dead
backends keep being chosen). Use it for Phase 4 item 9.

Limit: the proxy works at byte-stream level, so it can only retry before the
backend connection is established. A request already in flight on a server that
dies cannot be replayed and fails.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import List, Optional

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.getenv("LB_PORT", "18812"))
ALGORITHM = os.getenv("LB_ALGORITHM", "least_connections").strip().lower()
BACKENDS_RAW = os.getenv("BACKENDS", "server1:18812,server2:18812,server3:18812")
VERBOSE = os.getenv("LB_VERBOSE", "0") == "1"
HEALTH_CHECK = os.getenv("HEALTH_CHECK", "on").strip().lower() in ("on", "1", "true", "yes")
HEALTH_INTERVAL = float(os.getenv("HEALTH_INTERVAL", "2"))
FAIL_THRESHOLD = int(os.getenv("FAIL_THRESHOLD", "2"))
RISE_THRESHOLD = int(os.getenv("RISE_THRESHOLD", "2"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "1"))
EWMA_ALPHA = 0.2


@dataclass
class Backend:
    host: str
    port: int
    active: int = 0
    ewma_seconds: Optional[float] = None
    completed: int = 0
    selected: int = 0
    connect_failures: int = 0
    healthy: bool = True
    probe_fails: int = 0
    probe_successes: int = 0

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"


backends: List[Backend] = []
for item in BACKENDS_RAW.split(","):
    host, port = item.strip().rsplit(":", 1)
    backends.append(Backend(host=host, port=int(port)))

if not backends:
    raise RuntimeError("Configure at least one backend in BACKENDS")
if ALGORITHM not in {"least_connections", "least_response_time"}:
    raise ValueError("LB_ALGORITHM must be least_connections or least_response_time")

selection_counter = 0


def log(msg: str) -> None:
    print(msg, flush=True)


def mark_down(b: Backend, reason: str) -> None:
    if b.healthy:
        b.healthy = False
        b.probe_successes = 0
        log(f"[HEALTH] {b.name} marked DOWN ({reason}); "
            f"healthy={[x.host for x in backends if x.healthy]}")


def mark_up(b: Backend) -> None:
    peers = [x.ewma_seconds for x in backends
             if x is not b and x.healthy and x.ewma_seconds is not None]
    b.ewma_seconds = sum(peers) / len(peers) if peers else None
#    b.active = 0
    b.healthy = True
    b.probe_fails = 0
    b.probe_successes = 0
    log(f"[HEALTH] {b.name} marked UP (recovered); "
        f"healthy={[x.host for x in backends if x.healthy]}")


def choose_backend(exclude: List[Backend]) -> Optional[Backend]:
    """Synchronous (no await), so selections cannot interleave."""
    global selection_counter

    pool = [b for b in backends
            if b not in exclude and (b.healthy or not HEALTH_CHECK)]
    if not pool:
        return None

    if ALGORITHM == "least_connections":
        minimum = min(b.active for b in pool)
        candidates = [b for b in pool if b.active == minimum]
    else:
        def score(b: Backend) -> float:
            ewma = b.ewma_seconds if b.ewma_seconds is not None else 0.0
            return ewma * (b.active + 1)

        best = min(score(b) for b in pool)
        candidates = [b for b in pool if score(b) == best]
        min_active = min(b.active for b in candidates)
        candidates = [b for b in candidates if b.active == min_active]

    chosen = candidates[selection_counter % len(candidates)]
    selection_counter += 1
    return chosen


async def connect_backend(b: Backend):
    return await asyncio.wait_for(
        asyncio.open_connection(b.host, b.port), timeout=CONNECT_TIMEOUT
    )


async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
                await writer.drain()
        except (ConnectionError, OSError):
            pass


async def close_writer(writer) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


async def handle_client(client_reader, client_writer):
    peer = client_writer.get_extra_info("peername")
    tried: List[Backend] = []
    backend: Optional[Backend] = None
    server_reader = server_writer = None
    started = 0.0

    # Without fault tolerance there is exactly one attempt (Phase 3 behaviour).
    max_attempts = len(backends) if HEALTH_CHECK else 1

    for _ in range(max_attempts):
        candidate = choose_backend(tried)
        if candidate is None:
            break
        candidate.active += 1
        started = time.perf_counter()
        try:
            server_reader, server_writer = await connect_backend(candidate)
            backend = candidate
            backend.selected += 1
            break
        except (OSError, asyncio.TimeoutError) as exc:
            candidate.active = max(0, candidate.active - 1)
            candidate.connect_failures += 1
            log(f"[LB] connect to {candidate.name} FAILED ({exc!r})"
                f"{'; retrying on another backend' if HEALTH_CHECK else ''}")
            if HEALTH_CHECK:
                mark_down(candidate, "connect failed")
                tried.append(candidate)

    if backend is None:
        log(f"[LB] no backend available for client={peer}; dropping connection")
        await close_writer(client_writer)
        return

    if VERBOSE:
        log(f"[LB] algorithm={ALGORITHM} client={peer} -> {backend.name} "
            f"active={[f'{b.host}={b.active}' for b in backends]}")

    try:
        await asyncio.gather(
            pipe(client_reader, server_writer),
            pipe(server_reader, client_writer),
        )
    except (OSError, ConnectionError) as exc:
        log(f"[LB] stream error on {backend.name}: {exc}")
    finally:
        duration = time.perf_counter() - started
        backend.active = max(0, backend.active - 1)
        backend.completed += 1
        if backend.ewma_seconds is None:
            backend.ewma_seconds = duration
        else:
            backend.ewma_seconds = (
                EWMA_ALPHA * duration + (1 - EWMA_ALPHA) * backend.ewma_seconds
            )
        if VERBOSE:
            log(f"[LB] completed backend={backend.host} duration_ms={duration * 1000:.3f} "
                f"ewma_ms={backend.ewma_seconds * 1000:.3f}")
        await close_writer(server_writer)
        await close_writer(client_writer)


async def probe(b: Backend) -> bool:
    """TCP-level liveness probe: can we open a connection to the backend?"""
    writer = None
    try:
        _, writer = await connect_backend(b)
        return True
    except (OSError, asyncio.TimeoutError):
        return False
    finally:
        await close_writer(writer)


async def health_monitor():
    log(f"[HEALTH] monitor started: interval={HEALTH_INTERVAL}s "
        f"fail_threshold={FAIL_THRESHOLD} rise_threshold={RISE_THRESHOLD}")
    while True:
        results = await asyncio.gather(*(probe(b) for b in backends))
        for b, ok in zip(backends, results):
            if ok:
                b.probe_fails = 0
                if not b.healthy:
                    b.probe_successes += 1
                    if b.probe_successes >= RISE_THRESHOLD:
                        mark_up(b)
            else:
                b.probe_successes = 0
                b.probe_fails += 1
                if b.healthy and b.probe_fails >= FAIL_THRESHOLD:
                    mark_down(b, f"{b.probe_fails} failed probes")
        await asyncio.sleep(HEALTH_INTERVAL)


async def print_stats():
    while True:
        await asyncio.sleep(5)
        summary = " ".join(
            f"{b.host}[{'UP' if b.healthy else 'DOWN'},sel={b.selected},act={b.active},"
            f"fail={b.connect_failures},ewma_ms={(b.ewma_seconds or 0) * 1000:.2f}]"
            for b in backends
        )
        log(f"[LB-STATS] {ALGORITHM} health_check={'on' if HEALTH_CHECK else 'off'}: {summary}")


async def main():
    log(f"[LB] listening on {LISTEN_HOST}:{LISTEN_PORT}; algorithm={ALGORITHM}; "
        f"health_check={'on' if HEALTH_CHECK else 'off'}; "
        f"backends={[b.name for b in backends]}; verbose={VERBOSE}")
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    asyncio.create_task(print_stats())
    if HEALTH_CHECK:
        asyncio.create_task(health_monitor())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
