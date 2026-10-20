"""Raw TCP proxy for RPyC with two dynamic policies:
  - least_connections: pick the backend with the fewest active connections.
  - least_response_time: pick the backend with the lowest
        score = EWMA(connection duration) * (active connections + 1).

The proxy never speaks RPyC. It chooses a backend per TCP connection and
forwards bytes unchanged. Connection duration is used as the observed response
time, which is meaningful when each connection carries one RPC (as the Phase 3
benchmark does).
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.getenv("LB_PORT", "18812"))
ALGORITHM = os.getenv("LB_ALGORITHM", "least_connections").strip().lower()
BACKENDS_RAW = os.getenv("BACKENDS", "server1:18812,server2:18812,server3:18812")
VERBOSE = os.getenv("LB_VERBOSE", "0") == "1"
EWMA_ALPHA = 0.2


@dataclass
class Backend:
    host: str
    port: int
    active: int = 0
    ewma_seconds: Optional[float] = None
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


def choose_backend() -> Backend:
    """Pick a backend. Synchronous with no await, so the event loop cannot
    interleave another selection before the caller increments `active`."""
    global selection_counter

    if ALGORITHM == "least_connections":
        minimum = min(b.active for b in backends)
        candidates = [b for b in backends if b.active == minimum]
    else:
        # Unmeasured backends score 0 so every replica is probed early.
        def score(b: Backend) -> float:
            ewma = b.ewma_seconds if b.ewma_seconds is not None else 0.0
            return ewma * (b.active + 1)

        best = min(score(b) for b in backends)
        candidates = [b for b in backends if score(b) == best]
        # Among equal scores prefer the least busy backend.
        min_active = min(b.active for b in candidates)
        candidates = [b for b in candidates if b.active == min_active]

    # Rotate among ties so equal-load requests are not all sent to server1.
    chosen = candidates[selection_counter % len(candidates)]
    selection_counter += 1
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
        server_reader, server_writer = await asyncio.open_connection(
            backend.host, backend.port
        )
        connected = True
        await asyncio.gather(
            pipe(client_reader, server_writer),
            pipe(server_reader, client_writer),
        )
    except (OSError, ConnectionError) as exc:
        print(f"[LB] backend error for {backend.name}: {exc}", flush=True)
    finally:
        duration = time.perf_counter() - started
        backend.active = max(0, backend.active - 1)
        # Only successful connections update the EWMA. A dead backend fails
        # instantly; counting that tiny duration would make it look fastest.
        if connected:
            backend.completed += 1
            if backend.ewma_seconds is None:
                backend.ewma_seconds = duration
            else:
                backend.ewma_seconds = (
                    EWMA_ALPHA * duration + (1 - EWMA_ALPHA) * backend.ewma_seconds
                )
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
    """Every 5 s print a one-line summary (always on; it is cheap)."""
    while True:
        await asyncio.sleep(5)
        summary = " ".join(
            f"{b.host}[sel={b.selected},act={b.active},ewma_ms={(b.ewma_seconds or 0) * 1000:.2f}]"
            for b in backends
        )
        print(f"[LB-STATS] {ALGORITHM}: {summary}", flush=True)


async def main():
    print(
        f"[LB] listening on {LISTEN_HOST}:{LISTEN_PORT}; algorithm={ALGORITHM}; "
        f"backends={[b.name for b in backends]}; verbose={VERBOSE}",
        flush=True,
    )
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    asyncio.create_task(print_stats())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())