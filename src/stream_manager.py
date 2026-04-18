#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import logging
from typing import Dict, Set

from .config import StreamConfig
from .mqtt_processor import StreamProcessor

logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(self):
        self._processors: Dict[str, StreamProcessor] = {}
        self._ws_queues: Set[asyncio.Queue] = set()

    def add_ws_queue(self, q: asyncio.Queue):
        self._ws_queues.add(q)

    def remove_ws_queue(self, q: asyncio.Queue):
        self._ws_queues.discard(q)

    async def _broadcast(self, stream_id: str, event_type: str, data: dict):
        msg = {"stream_id": stream_id, "type": event_type, "data": data}
        dead = set()
        for q in self._ws_queues:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead.add(q)
        self._ws_queues -= dead

    async def start_stream(self, cfg: StreamConfig):
        if cfg.id in self._processors:
            await self.stop_stream(cfg.id)
        proc = StreamProcessor(cfg, on_event=self._broadcast)
        self._processors[cfg.id] = proc
        proc.start()
        logger.info("Started stream %s (%s)", cfg.id, cfg.name)

    async def stop_stream(self, stream_id: str):
        proc = self._processors.pop(stream_id, None)
        if proc:
            await proc.stop()
            logger.info("Stopped stream %s", stream_id)

    async def restart_stream(self, cfg: StreamConfig):
        await self.stop_stream(cfg.id)
        if cfg.enabled:
            await self.start_stream(cfg)

    async def stop_all(self):
        for sid in list(self._processors):
            await self.stop_stream(sid)

    def get_all_stats(self) -> list:
        return [p.get_stats() for p in self._processors.values()]

    def get_stats(self, stream_id: str) -> dict:
        proc = self._processors.get(stream_id)
        return proc.get_stats() if proc else {}

    def is_running(self, stream_id: str) -> bool:
        return stream_id in self._processors


manager = StreamManager()
