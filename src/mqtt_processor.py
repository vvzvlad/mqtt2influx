#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import json
import logging
import time
from typing import Callable, Optional

import aiomqtt

from .influx_writer import contains_excluded, flatten_payload, InfluxWriter
from .config import StreamConfig

logger = logging.getLogger(__name__)

BATCH_SIZE = 200
BATCH_INTERVAL = 2.0  # seconds


class StreamProcessor:
    def __init__(self, cfg: StreamConfig, on_event: Callable):
        self.cfg = cfg
        self.on_event = on_event  # async callback(stream_id, event_type, data)
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # stats
        self.msgs_received = 0
        self.msgs_filtered = 0
        self.points_sent = 0
        self.errors = 0
        self.last_topics: list = []  # rolling last 20 topics

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run(), name=f"stream-{self.cfg.id}")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        writer = InfluxWriter(
            host=self.cfg.influx_host,
            port=self.cfg.influx_port,
            user=self.cfg.influx_user,
            password=self.cfg.influx_password,
            database=self.cfg.influx_database,
            stream_id=self.cfg.id,
        )
        await writer.start()
        try:
            await self._connect_and_process(writer)
        finally:
            await writer.stop()

    async def _connect_and_process(self, writer: InfluxWriter):
        batch = []
        last_flush = time.monotonic()

        async def flush():
            nonlocal batch, last_flush
            if not batch:
                return
            ok = await writer.write_batch(batch)
            count = len(batch)
            if ok:
                self.points_sent += count
                await self.on_event(self.cfg.id, "flush", {"count": count, "status": "ok"})
            else:
                self.errors += 1
                await self.on_event(self.cfg.id, "flush", {"count": count, "status": "error"})
            batch = []
            last_flush = time.monotonic()

        while self._running:
            try:
                auth = None
                if self.cfg.mqtt_user:
                    auth = aiomqtt.Will  # placeholder; set below
                client_kwargs = dict(
                    hostname=self.cfg.mqtt_host,
                    port=self.cfg.mqtt_port,
                    timeout=10,
                )
                if self.cfg.mqtt_user:
                    client_kwargs["username"] = self.cfg.mqtt_user
                    client_kwargs["password"] = self.cfg.mqtt_password

                async with aiomqtt.Client(**client_kwargs) as client:
                    await client.subscribe(self.cfg.mqtt_topic)
                    await self.on_event(self.cfg.id, "connected", {})
                    logger.info("[%s] MQTT connected, subscribed to %s", self.cfg.id, self.cfg.mqtt_topic)

                    async for message in client.messages:
                        if not self._running:
                            break

                        topic = str(message.topic)
                        try:
                            payload_raw = message.payload.decode("utf-8", errors="replace")
                        except Exception:
                            payload_raw = str(message.payload)

                        self.msgs_received += 1

                        if contains_excluded(topic):
                            self.msgs_filtered += 1
                            continue

                        try:
                            payload = json.loads(payload_raw)
                        except Exception:
                            payload = payload_raw

                        prefix = self.cfg.topic_prefix
                        full_topic = f"{prefix}/{topic}" if prefix else topic

                        for flat_topic, value in flatten_payload(full_topic, payload):
                            ts_ms = int(time.time() * 1000)
                            batch.append((flat_topic, value, ts_ms))

                            self.last_topics.append({"topic": flat_topic, "value": value, "ts": ts_ms})
                            if len(self.last_topics) > 20:
                                self.last_topics.pop(0)

                            await self.on_event(self.cfg.id, "message", {
                                "topic": flat_topic,
                                "value": value,
                                "ts": ts_ms,
                            })

                        now = time.monotonic()
                        if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_INTERVAL:
                            await flush()

            except asyncio.CancelledError:
                await flush()
                raise
            except Exception as e:
                self.errors += 1
                logger.error("[%s] Error: %s", self.cfg.id, e)
                await self.on_event(self.cfg.id, "error", {"message": str(e)})
                if self._running:
                    await asyncio.sleep(5)

    def get_stats(self) -> dict:
        return {
            "id": self.cfg.id,
            "name": self.cfg.name,
            "msgs_received": self.msgs_received,
            "msgs_filtered": self.msgs_filtered,
            "points_sent": self.points_sent,
            "errors": self.errors,
            "last_topics": list(self.last_topics),
        }
