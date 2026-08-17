#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import json
import logging
import os
import time
from typing import Callable, Optional

import aiomqtt

from .influx_writer import contains_excluded, flatten_payload, InfluxWriter
from .config import StreamConfig

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
BATCH_INTERVAL = float(os.environ.get("BATCH_INTERVAL", "1.0"))

# Backoff ladder for a failed InfluxDB write. write_batch() returns False for any non-2xx answer and
# for any exception — an InfluxDB restart, a blipped network, a DNS hiccup — and before this the
# batch was dropped on the first such answer, up to 220 points at the production BATCH_SIZE.
RETRY_DELAYS = (0.5, 1.0, 2.0)

# Hard ceiling on the whole retry phase, and the reason there is one at all: a flush triggered from
# the message loop runs INSIDE `async for message in client.messages`, so for as long as it retries
# nothing is read from the broker, and MQTT does not buffer for a subscriber that is not reading.
# Unbounded, the worst case would be the ladder's own 3.5s of sleeps plus four aiohttp timeouts of
# 10s each — roughly 43s of unread MQTT to save 220 points, a cure far worse than the disease. With
# the ceiling the added blind window is at most RETRY_BUDGET on top of the first attempt's existing
# 10s timeout. It exceeds the sleeps (3.5s) so that a fast-failing InfluxDB still gets all three
# retries; a slow-failing one gets however many fit.
RETRY_BUDGET = 5.0


class StreamProcessor:
    def __init__(self, cfg: StreamConfig, on_event: Callable):
        self.cfg = cfg
        self.on_event = on_event  # async callback(stream_id, event_type, data)
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # The pending batch is shared between the message loop and the interval timer task, so every
        # read and write of it goes through the lock. Without it one task appends to the list while
        # the other swaps it out, and points are silently lost or written twice — a worse bug than
        # the missing interval flush the timer exists to fix.
        self._batch: list = []
        self._batch_lock = asyncio.Lock()

        # stats
        self.msgs_received = 0
        self.msgs_filtered = 0
        self.points_sent = 0
        self.errors = 0
        self.retries = 0  # writes that failed and then succeeded on a retry
        self.last_topics: list = []  # rolling last 20 topics
        self.batch_current = 0
        self.batches_sent = 0
        self.last_flush_time: float = 0.0
        self.last_flush_count: int = 0
        self.last_flush_ok: bool = True

    @property
    def is_alive(self) -> bool:
        # `_task` is None until start() is called, and done() covers the task that finished or died;
        # the manager keeps its dictionary entry either way, so this is the only honest answer to
        # "is this stream running".
        return self._task is not None and not self._task.done()

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

    async def _write_with_retries(self, writer: InfluxWriter, points: list) -> bool:
        if await writer.write_batch(points):
            return True
        # asyncio.timeout, not a plain elapsed-time check: the check can only run between attempts,
        # so an attempt that hangs in aiohttp's 10s timeout would blow the budget it is supposed to
        # respect. Cancelling mid-attempt is what makes RETRY_BUDGET an actual ceiling.
        try:
            async with asyncio.timeout(RETRY_BUDGET):
                for delay in RETRY_DELAYS:
                    await asyncio.sleep(delay)
                    if await writer.write_batch(points):
                        self.retries += 1
                        return True
        except TimeoutError:
            pass
        return False

    async def _flush(self, writer: InfluxWriter, *, retry: bool = True):
        # The lock is taken HERE rather than at the call sites, so that no caller can forget it. The
        # flip side: nothing reachable from this method may call it again, because asyncio.Lock is
        # not reentrant and that would deadlock the stream permanently.
        async with self._batch_lock:
            if not self._batch:
                return
            pending, self._batch = self._batch, []
            self.batch_current = 0

        # The write runs OUTSIDE the lock. It can take seconds with the retry ladder, and holding
        # the lock across it would block the message loop from appending for exactly that long —
        # which is the throughput problem the retries already have to be rationed against.
        count = len(pending)
        ok = await (self._write_with_retries(writer, pending) if retry else writer.write_batch(pending))
        self.last_flush_time = time.time()
        self.last_flush_count = count
        self.last_flush_ok = ok
        if ok:
            self.points_sent += count
            self.batches_sent += 1
            await self.on_event(self.cfg.id, "flush", {"count": count, "status": "ok"})
        else:
            # The batch is already out of self._batch and is dropped here. Keeping it would grow
            # memory without bound while InfluxDB is down; this is a deliberate ceiling on the loss,
            # not the silent single-attempt drop it replaces.
            self.errors += 1
            await self.on_event(self.cfg.id, "flush", {"count": count, "status": "error"})

    async def _flush_timer(self, writer: InfluxWriter):
        # The interval check used to live inside `async for message in client.messages`, where it
        # could only be evaluated when a new message arrived — so on a topic that went quiet a
        # part-filled batch sat in memory indefinitely, while README and docker-compose both promise
        # a flush "after BATCH_SIZE points or after BATCH_INTERVAL seconds, whichever comes first".
        # Only a task with a clock of its own can make the "or" true. BATCH_INTERVAL is read on
        # every pass so the module attribute stays the single source of truth.
        while True:
            await asyncio.sleep(BATCH_INTERVAL)
            try:
                await self._flush(writer)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Nothing awaits this task, so an escaping exception would kill the timer in silence
                # and quietly restore the very bug it exists to fix.
                self.errors += 1
                logger.error("[%s] Interval flush failed: %s", self.cfg.id, e)

    async def _connect_and_process(self, writer: InfluxWriter):
        # Created once, OUTSIDE the reconnect loop: started inside it, every reconnect would leave
        # the previous timer running, and each leaked timer would keep flushing the same shared
        # batch for the lifetime of the stream.
        timer = asyncio.create_task(self._flush_timer(writer), name=f"flush-timer-{self.cfg.id}")
        try:
            await self._process_forever(writer, timer)
        finally:
            timer.cancel()
            # Awaited, not just cancelled: _run()'s finally closes the writer's HTTP session the
            # moment this returns, and a timer still mid-flush would then write into a closed one.
            try:
                await timer
            except asyncio.CancelledError:
                pass

    async def _process_forever(self, writer: InfluxWriter, timer: asyncio.Task):
        while self._running:
            try:
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

                        points = [
                            (flat_topic, value, int(time.time() * 1000))
                            for flat_topic, value in flatten_payload(full_topic, payload)
                        ]
                        if not points:
                            continue

                        # One lock acquisition per message rather than one per point, and no `await
                        # self.on_event` inside it: on_event broadcasts to every websocket client,
                        # and holding the batch lock across that would stall the interval flush for
                        # as long as the slowest UI tab takes.
                        async with self._batch_lock:
                            self._batch.extend(points)
                            self.batch_current = len(self._batch)
                            batch_full = len(self._batch) >= BATCH_SIZE

                        for flat_topic, value, ts_ms in points:
                            self.last_topics.append({"topic": flat_topic, "value": value, "ts": ts_ms})
                            if len(self.last_topics) > 20:
                                self.last_topics.pop(0)

                            await self.on_event(self.cfg.id, "message", {
                                "topic": flat_topic,
                                "value": value,
                                "ts": ts_ms,
                            })

                        # The interval half of the condition is gone from here: it lives in
                        # _flush_timer, which can fire without a message having arrived.
                        if batch_full:
                            await self._flush(writer)

            except asyncio.CancelledError:
                # Order matters. The timer goes first, because a tick landing between here and the
                # final flush would race it for the same batch; then the last flush runs WITHOUT the
                # retry ladder, so a shutdown never waits out its sleeps against a dead InfluxDB.
                timer.cancel()
                await self._flush(writer, retry=False)
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
            "retries": self.retries,
            "last_topics": list(self.last_topics),
            "batch_current": self.batch_current,
            "batches_sent": self.batches_sent,
            "batch_max": BATCH_SIZE,
            "batch_interval": BATCH_INTERVAL,
            "last_flush_time": self.last_flush_time,
            "last_flush_count": self.last_flush_count,
            "last_flush_ok": self.last_flush_ok,
        }
