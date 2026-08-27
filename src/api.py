#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config import StreamConfig, load_streams, save_streams, get_stream, upsert_stream, delete_stream
# Imported for the error message only, so that what the API tells the operator the default is cannot
# drift from what the rounding actually applies.
from .influx_writer import DEFAULT_VALUE_PRECISION
from .stream_manager import manager

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
_static_dir = os.path.join(BASE_DIR, "static")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start all enabled streams from config
    for stream in load_streams():
        if stream.enabled:
            await manager.start_stream(stream)
    yield
    await manager.stop_all()


app = FastAPI(title="mqtt2influx", lifespan=lifespan)


# ── REST: streams ──────────────────────────────────────────────────────────────

def _validate_value_precision(body: dict):
    """Refuse a `value_precision` that is not an integer, at the edge, before anything is stored.

    `StreamConfig` is a plain dataclass and `StreamConfig(**body)` checks no types at all, so
    without this a `{"value_precision": "8"}` was answered with 200, written to streams.json and
    echoed back by GET and by the UI — while the stream itself went on rounding to the default two
    decimals, because `resolve_value_precision()` rejects the string. That fallback is the right
    answer for a file that is ALREADY wrong (it keeps the stream writing rather than killing it),
    and the wrong answer for a request that can still be refused. A precision that is quietly
    ignored is exactly the bug this setting exists to fix, so the request is the place to say no.

    An absent key and an explicit null are both "not configured" and are both accepted — see
    `_serialize()` in config.py and `resolve_value_precision()` for why those two spellings must
    stay interchangeable.
    """
    if "value_precision" not in body:
        return
    value = body["value_precision"]
    if value is None:
        return
    # bool BEFORE int, because bool is a subclass of int: `isinstance(True, int)` is True, so
    # without this line `{"value_precision": true}` would be accepted as a request for one decimal.
    # Same ordering, and the same reason, as in resolve_value_precision().
    if isinstance(value, bool) or not isinstance(value, int):
        raise HTTPException(
            status_code=422,
            detail="value_precision must be an integer or null (omit it or send null for the "
                   "default {} decimals, -1 to store values as received); got {!r}".format(
                       DEFAULT_VALUE_PRECISION, value))


@app.get("/api/streams")
async def list_streams():
    streams = load_streams()
    result = []
    for s in streams:
        d = asdict(s)
        d["running"] = manager.is_running(s.id)
        result.append(d)
    return result


@app.post("/api/streams")
async def create_stream(body: dict):
    body.pop("id", None)
    _validate_value_precision(body)
    stream = StreamConfig(**{k: v for k, v in body.items() if hasattr(StreamConfig, k) or k in StreamConfig.__dataclass_fields__})
    upsert_stream(stream)
    if stream.enabled:
        await manager.start_stream(stream)
    return asdict(stream)


@app.put("/api/streams/{stream_id}")
async def update_stream(stream_id: str, body: dict):
    existing = get_stream(stream_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Stream not found")
    # Before upsert_stream() and before restart_stream(): a refused request must leave both the file
    # and the running processor exactly as they were.
    _validate_value_precision(body)
    body["id"] = stream_id
    fields = StreamConfig.__dataclass_fields__
    updated = StreamConfig(**{k: body.get(k, getattr(existing, k)) for k in fields})
    upsert_stream(updated)
    await manager.restart_stream(updated)
    return asdict(updated)


@app.delete("/api/streams/{stream_id}")
async def remove_stream(stream_id: str):
    if not get_stream(stream_id):
        raise HTTPException(status_code=404, detail="Stream not found")
    await manager.stop_stream(stream_id)
    delete_stream(stream_id)
    return {"ok": True}


@app.post("/api/streams/{stream_id}/start")
async def start_stream(stream_id: str):
    stream = get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")
    await manager.start_stream(stream)
    return {"ok": True}


@app.post("/api/streams/{stream_id}/stop")
async def stop_stream(stream_id: str):
    await manager.stop_stream(stream_id)
    return {"ok": True}


# ── REST: stats ────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def stats():
    return manager.get_all_stats()


# ── WebSocket ──────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    manager.add_ws_queue(q)

    # Send initial stats snapshot
    try:
        await ws.send_json({"type": "snapshot", "data": manager.get_all_stats()})
    except Exception:
        pass

    async def sender():
        while True:
            msg = await q.get()
            await ws.send_json(msg)

    send_task = asyncio.create_task(sender())

    # Periodic stats push every 2 s
    async def stats_pusher():
        while True:
            await asyncio.sleep(2)
            try:
                await ws.send_json({"type": "stats", "data": manager.get_all_stats()})
            except Exception:
                break

    stats_task = asyncio.create_task(stats_pusher())

    try:
        while True:
            await ws.receive_text()  # keep connection alive, ignore pings
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        send_task.cancel()
        stats_task.cancel()
        manager.remove_ws_queue(q)


# ── Static UI ──────────────────────────────────────────────────────────────────

if os.path.isdir(_static_dir):
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="static")
