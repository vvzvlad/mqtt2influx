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
