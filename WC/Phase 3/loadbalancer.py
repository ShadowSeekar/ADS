from __future__ import annotations
import asyncio
import math
import os
import time
from dataclasses import dataclass
from typing import Optional

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.getenv("LB_PORT", "18812"))
ALGORITHM = os.getenv("LB_ALGORITHM", "least_connections").strip().lower()
BACKENDS_RAW = os.getenv("BACKENDS", "server1:18812,server2:18812,server3:18812")
VERBOSE = os.getenv("LB_VERBOSE", "0") == "1"
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.2"))
EWMA_TAU_SECONDS = float(os.getenv("EWMA_TAU_SECONDS", "3"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "3"))

@dataclass
class Backend:
    host: str
    port: int
    active: int = 0
    ewma_seconds: Optional[float] = None
    last_update: float = 0.0
    last_pick: int = 0
    completed: int = 0
    selected: int = 0

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"

backends = []
for item in BACKENDS_RAW.split(","):
    host, port = item.strip().rsplit(":", 1)
    backends.append(Backend(host=host, port=int(port)))

if not backends:
    raise RuntimeError("Configure at least one backend in BACKENDS")
if ALGORITHM not in {"least_connections", "least_response_time"}:
    raise ValueError("LB_ALGORITHM must be least_connections or least_response_time")

selection_counter = 0

def effective_ewma(b: Backend, now: float) -> Optional[float]:
    if b.ewma_seconds is None:
        return None
    peers = [x.ewma_seconds for x in backends if x is not b and x.ewma_seconds is not None]
    if not peers:
        return b.ewma_seconds
    floor = min(peers)
    if b.ewma_seconds <= floor:
        return b.ewma_seconds
    age = max(0.0, now - b.last_update)
    return floor + (b.ewma_seconds - floor) * math.exp(-age / EWMA_TAU_SECONDS)

def choose_backend() -> Backend:
    global selection_counter
    if ALGORITHM == "least_connections":
        minimum = min(b.active for b in backends)
        candidates = [b for b in backends if b.active == minimum]
    else:
        now = time.perf_counter()
        scored = []
        for b in backends:
            ewma = effective_ewma(b, now)
            # Unmeasured backends score 0 so every replica is probed early.
            scored.append(((ewma if ewma is not None else 0.0) * (b.active + 1), b))
        best = min(score for score, _ in scored)
        candidates = [b for score, b in scored if score == best]
        min_active = min(b.active for b in candidates)
        candidates = [b for b in candidates if b.active == min_active]
    selection_counter += 1
    chosen = min(candidates, key=lambda b: b.last_pick)
    chosen.last_pick = selection_counter
    chosen.selected += 1
    return chosen

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

async def handle_client(client_reader, client_writer):
    started = time.perf_counter()
    peer = client_writer.get_extra_info("peername")
    backend = choose_backend()
    backend.active += 1
    server_writer = None
    connected = False
    if VERBOSE:
        print(
            f"[LB] algorithm={ALGORITHM} client={peer} -> {backend.name} "
            f"active={[f'{b.host}={b.active}' for b in backends]}",
            flush=True,
        )
    try:
        server_reader, server_writer = await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port), timeout=CONNECT_TIMEOUT
        )
        connected = True
        await asyncio.gather(
            pipe(client_reader, server_writer),
            pipe(server_reader, client_writer),
        )
    except (OSError, ConnectionError, asyncio.TimeoutError) as exc:
        print(f"[LB] backend error for {backend.name}: {exc!r}", flush=True)
    finally:
        now = time.perf_counter()
        duration = now - started
        backend.active = max(0, backend.active - 1)
        # Only successful connections update the EWMA. A dead backend fails
        # instantly; counting that tiny duration would make it look fastest.
        if connected:
            backend.completed += 1
            current = effective_ewma(backend, now)
            if current is None:
                backend.ewma_seconds = duration
            else:
                backend.ewma_seconds = EWMA_ALPHA * duration + (1 - EWMA_ALPHA) * current
            backend.last_update = now
        if VERBOSE:
            ewma_ms = (backend.ewma_seconds or 0.0) * 1000
            print(
                f"[LB] completed backend={backend.host} duration_ms={duration * 1000:.3f} "
                f"ewma_ms={ewma_ms:.3f} completed={backend.completed}",
                flush=True,
            )
        if server_writer is not None:
            server_writer.close()
            try:
                await server_writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        client_writer.close()
        try:
            await client_writer.wait_closed()
        except (ConnectionError, OSError):
            pass

async def print_stats():
    while True:
        await asyncio.sleep(5)
        now = time.perf_counter()
        parts = []
        for b in backends:
            ewma = effective_ewma(b, now)
            parts.append(f"{b.host}[sel={b.selected},act={b.active},"
                         f"ewma_ms={(ewma or 0) * 1000:.2f}]")
        print(f"[LB-STATS] {ALGORITHM}: {' '.join(parts)}", flush=True)

async def main():
    print(
        f"[LB] listening on {LISTEN_HOST}:{LISTEN_PORT}; algorithm={ALGORITHM}; "
        f"backends={[b.name for b in backends]}; ewma_alpha={EWMA_ALPHA}; "
        f"ewma_tau={EWMA_TAU_SECONDS}s; connect_timeout={CONNECT_TIMEOUT}s; verbose={VERBOSE}",
        flush=True,
    )
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    asyncio.create_task(print_stats())
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())