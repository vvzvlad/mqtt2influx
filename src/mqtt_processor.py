#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import json
import logging
import time
from contextlib import AsyncExitStack
from typing import Callable, Optional

import aiomqtt

from .influx_writer import contains_excluded, flatten_payload, resolve_value_precision, InfluxWriter
from .config import StreamConfig
from .config_errors import load_settings_or_exit
from .settings import Settings

logger = logging.getLogger(__name__)


# A fresh Settings() per call, not the module-level singleton: the `reimport` fixture in
# tests/test_mqtt_processor.py reloads this module under a different environment, and a reload does
# not re-execute src/settings.py.
def batch_size_from_env() -> int:
    return load_settings_or_exit(Settings).batch_size


def batch_interval_from_env() -> float:
    return load_settings_or_exit(Settings).batch_interval


# Read once, at import; through a function so a test can check that the value comes from the
# environment without reloading the module.
BATCH_SIZE = batch_size_from_env()
BATCH_INTERVAL = batch_interval_from_env()

# Backoff ladder for an InfluxDB write that failed in a way worth repeating (a restart, a network
# blip, a 5xx, a 429). Other 4xx never reach it: write_batch_detailed() reports them as not worth
# retrying.
RETRY_DELAYS = (0.5, 1.0, 2.0)

# Ceiling on the whole retry phase: a flush from the message loop blocks `async for message` while
# aiomqtt's queue fills; unbounded, four 10 s timeouts plus the sleeps would be ~43 s per flush.
# Above the 3.5 s of sleeps, so a fast-failing InfluxDB still gets all three retries.
RETRY_BUDGET = 5.0

# Ceiling on aiomqtt's incoming queue (unbounded by default): a stall ends in "Message queue is
# full" warnings instead of an OOM kill that drops the subscription. At tens of messages a second
# this is about an hour of InfluxDB outage, and near 50 MB at roughly 1 KB per queued message.
MAX_QUEUED_INCOMING_MESSAGES = 50_000

# The client's `timeout=` (CONNACK, SUBACK, the disconnect wait), and the same budget again for the
# connect before it: aiomqtt runs paho's blocking connect() in an executor before that timeout
# starts, and paho does not bound getaddrinfo, so a hung DNS lookup would block the stream forever.
MQTT_TIMEOUT = 10

# Pause between a failed attempt and the next one.
RECONNECT_DELAY = 5

# Consecutive failed attempts after which the retry line is logged at ERROR instead of WARNING: one
# minute at the 15s cadence (MQTT_TIMEOUT + RECONNECT_DELAY) of a broker that never answers.
FAILURES_BEFORE_ERROR = 4


class StreamProcessor:
    def __init__(self, cfg: StreamConfig, on_event: Callable):
        self.cfg = cfg
        self.on_event = on_event  # async callback(stream_id, event_type, data)
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # Resolved once per processor, not per message, so a bad value warns once;
        # manager.restart_stream() rebuilds the processor after every edit.
        self._value_precision = resolve_value_precision(cfg.value_precision, cfg.id)
        # bool(): streams.json is hand-editable and `StreamConfig(**record)` type-checks nothing.
        self._write_strings = bool(cfg.write_strings)

        # Shared by the message loop and the interval timer, so every access goes through the lock,
        # except the cancellation handler in _flush, which puts its batch back without it.
        self._batch: list = []
        self._batch_lock = asyncio.Lock()

        # When the batch was last emptied, by whichever flush. The interval is measured from here,
        # stamped when the batch is taken rather than when its write finishes, so a 10 s InfluxDB
        # timeout does not delay the next interval flush.
        self._last_flush_monotonic = time.monotonic()

        # Start time of the flush whose result the last_flush_* fields below are showing. Two
        # flushes can be in flight at once and can finish in the opposite order to the one they
        # started in; see _record_flush.
        self._last_stats_started = float("-inf")

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
        # The MQTT link, as opposed to the task: `is_alive` stays true through every failure the
        # reconnect loop survives.
        self.connected = False
        self.last_message_time: float = 0.0
        self.last_connect_time: float = 0.0
        self.reconnects = 0  # successful connects after the first one
        self.consecutive_failures = 0  # failed attempts since the last successful connect

    @property
    def is_alive(self) -> bool:
        # None until start(); done() covers a finished or dead task, which the manager still holds.
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
        ok, worth_retrying = await writer.write_batch_detailed(points)
        if ok:
            return True
        if not worth_retrying:
            # A 400, 401 or 404: the same bytes get the same answer, and retrying would spend
            # RETRY_BUDGET of a message loop that is not draining the broker's queue.
            return False
        # asyncio.timeout rather than an elapsed-time check between attempts: it cancels an attempt
        # hung in aiohttp's 10 s timeout, which makes RETRY_BUDGET a real ceiling.
        try:
            async with asyncio.timeout(RETRY_BUDGET):
                for delay in RETRY_DELAYS:
                    await asyncio.sleep(delay)
                    ok, worth_retrying = await writer.write_batch_detailed(points)
                    if ok:
                        self.retries += 1
                        return True
                    if not worth_retrying:
                        return False
        except TimeoutError:
            pass
        return False

    def _record_flush(self, started: float, count: int, ok: bool):
        # Two flushes can be inside their write at once (the lock is released before it) and finish
        # out of order. The one that started later wins: its answer describes the more recent state
        # of InfluxDB.
        if started < self._last_stats_started:
            return
        self._last_stats_started = started
        self.last_flush_time = time.time()
        self.last_flush_count = count
        self.last_flush_ok = ok

    async def _flush(self, writer: InfluxWriter, *, retry: bool = True):
        # The lock is taken here so no caller can forget it; asyncio.Lock is not reentrant, so
        # nothing reachable from this method may call it again.
        async with self._batch_lock:
            # Moved even when there is nothing to send, so an idle stream's next tick is a whole
            # interval away instead of a deadline permanently in the past.
            self._last_flush_monotonic = time.monotonic()
            if not self._batch:
                return
            pending, self._batch = self._batch, []
            self.batch_current = 0

        # The write runs outside the lock: with the retry ladder it can take seconds, and the
        # message loop must keep appending meanwhile.
        count = len(pending)
        started = time.monotonic()
        try:
            ok = await (self._write_with_retries(writer, pending) if retry else writer.write_batch(pending))
        except asyncio.CancelledError:
            # `pending` is the only reference to these points, and a cancellation mid-write is a
            # container stop: put them back at the front, ahead of newer appends, for the shutdown
            # flush. No lock: nothing here awaits, and a suspension point would let a second
            # cancellation lose them.
            self._batch[:0] = pending
            self.batch_current = len(self._batch)
            logger.warning(
                "[%s] Write cancelled mid-flight, %d points returned to the batch", self.cfg.id, count)
            raise
        self._record_flush(started, count, ok)
        if ok:
            self.points_sent += count
            self.batches_sent += 1
            await self.on_event(self.cfg.id, "flush", {"count": count, "status": "ok"})
        else:
            # Dropped: keeping failed batches would grow memory without bound during an outage.
            self.errors += 1
            await self.on_event(self.cfg.id, "flush", {"count": count, "status": "error"})

    async def _flush_timer(self, writer: InfluxWriter):
        # The BATCH_INTERVAL half of the flush rule: a task with its own clock, so a quiet topic's
        # part-filled batch still goes out. BATCH_INTERVAL is read on every pass, so the module
        # attribute stays the single source of truth.
        while True:
            # The remainder since the last flush, so a tick does not land just after a size-driven
            # flush. sleep() runs even when nothing remains: with BATCH_INTERVAL at 0 the loop
            # would otherwise never yield, since _flush takes a free lock without suspending.
            remaining = BATCH_INTERVAL - (time.monotonic() - self._last_flush_monotonic)
            await asyncio.sleep(remaining if remaining > 0 else 0)
            if time.monotonic() - self._last_flush_monotonic < BATCH_INTERVAL:
                continue
            try:
                await self._flush(writer)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Nothing awaits this task, so an escaping exception would stop interval flushes
                # silently.
                self.errors += 1
                logger.error("[%s] Interval flush failed: %s", self.cfg.id, e)

    async def _connect_and_process(self, writer: InfluxWriter):
        # Created once, outside the reconnect loop: inside it every reconnect would leak another
        # timer flushing the same batch.
        timer = asyncio.create_task(self._flush_timer(writer), name=f"flush-timer-{self.cfg.id}")
        try:
            await self._process_forever(writer)
        finally:
            # A finally, because the message loop can also exit without a cancellation (the `break`
            # in _process_forever, or a disconnect timeout raised as MqttError and absorbed). The
            # timer is cancelled and awaited first: mid-write, it hands its points back only once
            # the cancellation reaches it, and the writer's session must outlive it. No retry
            # ladder, so a shutdown never waits out its sleeps.
            timer.cancel()
            try:
                await timer
            except asyncio.CancelledError:
                pass
            # Anything else the timer died of must not skip the flush below.
            except Exception as e:  # noqa: BLE001
                logger.error("[%s] Flush timer died: %s", self.cfg.id, e)
            await self._flush(writer, retry=False)

    async def _process_forever(self, writer: InfluxWriter):
        while self._running:
            try:
                client_kwargs = dict(
                    hostname=self.cfg.mqtt_host,
                    port=self.cfg.mqtt_port,
                    timeout=MQTT_TIMEOUT,
                    # aiomqtt's default, passed so that it is a choice: keepalive is the only thing
                    # that detects a half-open socket.
                    keepalive=60,
                    max_queued_incoming_messages=MAX_QUEUED_INCOMING_MESSAGES,
                )
                if self.cfg.mqtt_user:
                    client_kwargs["username"] = self.cfg.mqtt_user
                    client_kwargs["password"] = self.cfg.mqtt_password

                # The stack stands in for `async with aiomqtt.Client(...)` so the connect can be
                # bounded. Two MQTT_TIMEOUTs (executor connect, then CONNACK) let aiomqtt's own
                # timeout fire first; cancelling its connect future instead makes a later disconnect
                # log a CancelledError traceback.
                connect_bound = 2 * MQTT_TIMEOUT
                async with AsyncExitStack() as stack:
                    try:
                        client = await asyncio.wait_for(
                            stack.enter_async_context(aiomqtt.Client(**client_kwargs)), connect_bound)
                    except TimeoutError:
                        raise aiomqtt.MqttError(f"Connect did not complete within {connect_bound}s") from None
                    await client.subscribe(self.cfg.mqtt_topic)
                    self.connected = True
                    self.consecutive_failures = 0
                    if self.last_connect_time:
                        self.reconnects += 1
                    self.last_connect_time = time.time()
                    await self.on_event(self.cfg.id, "connected", {})
                    logger.info("[%s] MQTT connected, subscribed to %s", self.cfg.id, self.cfg.mqtt_topic)

                    async for message in client.messages:
                        # Leaves without an exception; _connect_and_process's finally sends the
                        # batch. stop() does not get here: it sets _running and cancels together.
                        if not self._running:
                            break

                        topic = str(message.topic)
                        try:
                            payload_raw = message.payload.decode("utf-8", errors="replace")
                        except Exception:
                            payload_raw = str(message.payload)

                        self.msgs_received += 1
                        self.last_message_time = time.time()

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
                            for flat_topic, value in flatten_payload(
                                full_topic, payload, self._value_precision, self._write_strings)
                        ]
                        if not points:
                            continue

                        # One lock acquisition per message, and no `await self.on_event` under it:
                        # on_event broadcasts to every websocket client, and the interval flush
                        # would wait for the slowest tab.
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

                        # The size half of the flush rule; the interval half is _flush_timer.
                        if batch_full:
                            await self._flush(writer)

            except Exception as e:
                # Before any await: the RECONNECT_DELAY sleep below is link-down time too.
                self.connected = False
                self.errors += 1
                self.consecutive_failures += 1
                level = logging.ERROR if self.consecutive_failures >= FAILURES_BEFORE_ERROR else logging.WARNING
                logger.log(level, "[%s] Error: %s", self.cfg.id, e)
                await self.on_event(self.cfg.id, "error", {"message": str(e)})
                await self.on_event(self.cfg.id, "disconnected", {})
                if self._running:
                    await asyncio.sleep(RECONNECT_DELAY)
            finally:
                # The exits that are not an exception: the `break` above and a cancellation.
                self.connected = False

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
            "connected": self.connected,
            "last_message_time": self.last_message_time,
            "last_connect_time": self.last_connect_time,
            "reconnects": self.reconnects,
            "consecutive_failures": self.consecutive_failures,
        }
