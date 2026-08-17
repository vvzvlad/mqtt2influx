#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import asyncio
import json
import logging
import os
import time
from contextlib import suppress
from typing import Callable, Optional

import aiomqtt

from .influx_writer import contains_excluded, flatten_payload, InfluxWriter
from .config import StreamConfig

logger = logging.getLogger(__name__)


def batch_size_from_env() -> int:
    return int(os.environ.get("BATCH_SIZE", "100"))


def batch_interval_from_env() -> float:
    return float(os.environ.get("BATCH_INTERVAL", "1.0"))


# Read once, at import. Through a function rather than inline so that "this value comes from the
# environment" is something a test can state without reloading the module: production runs 220/3.0
# out of docker-compose, and a build that quietly stopped reading BATCH_SIZE would fall back to 100
# and multiply the number of write requests InfluxDB receives by 2.2 with nothing to show for it.
BATCH_SIZE = batch_size_from_env()
BATCH_INTERVAL = batch_interval_from_env()

# Backoff ladder for an InfluxDB write that failed in a way worth repeating — a restart, a blipped
# network, a DNS hiccup, a 5xx, a 429. Before this the batch was dropped on the first such answer,
# up to 220 points at the production BATCH_SIZE. A 4xx that is not 429 never reaches the ladder;
# write_batch_detailed() reports it as not worth retrying and the flush fails immediately.
RETRY_DELAYS = (0.5, 1.0, 2.0)

# Hard ceiling on the whole retry phase. A flush triggered from the message loop runs INSIDE
# `async for message in client.messages`, so for as long as it retries this coroutine takes nothing
# out of aiomqtt's incoming queue.
#
# What that costs is NOT an immediate loss, and the tuning only makes sense once that is clear:
# aiomqtt's reader callback lives on the event loop, not on this coroutine, so while the consumer
# hangs on an await the socket keeps being read and `_on_message` keeps putting messages into the
# client's queue. A blocked consumer therefore buys backlog, not silence. With the queue bounded
# (MAX_QUEUED_INCOMING_MESSAGES below) the backlog stops at that many messages and aiomqtt discards
# further arrivals with a warning; unbounded, it grows in RAM for as long as the stall lasts, and
# the container has no memory limit — the OOM kill that ends it takes the MQTT subscription with
# it, and THAT is the loss nothing can recover.
#
# So the budget rations backlog and memory. Unbounded it would be the ladder's own 3.5s of sleeps
# plus four aiohttp timeouts of 10s each — some 43s of accumulating queue per flush, on every
# flush, for as long as InfluxDB is down. It exceeds the sleeps (3.5s) so that a fast-failing
# InfluxDB still gets all three retries; a slow-failing one gets however many fit.
RETRY_BUDGET = 5.0

# Ceiling on aiomqtt's incoming queue, which defaults to unbounded (`maxsize=0`). Bounded, a stall
# ends in messages discarded with "Message queue is full" in the log — visible, per-message,
# recoverable the moment the drain catches up. Unbounded it ends in an OOM kill, which drops the
# subscription and every batch in memory with it. The number is deliberately generous: at this
# service's real rate (612M messages over its life, tens per second) it is on the order of an hour
# of a total InfluxDB outage before the first message is discarded, while at roughly a kilobyte per
# queued Message it caps the queue's own footprint near 50 MB — far below anything that would
# threaten the host, and far above any backlog an ordinary InfluxDB restart can build.
MAX_QUEUED_INCOMING_MESSAGES = 50_000


class StreamProcessor:
    def __init__(self, cfg: StreamConfig, on_event: Callable):
        self.cfg = cfg
        self.on_event = on_event  # async callback(stream_id, event_type, data)
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # The pending batch is shared between the message loop and the interval timer task, so every
        # read and write of it goes through the lock. Without it one task appends to the list while
        # the other swaps it out, and points are silently lost or written twice — a worse bug than
        # the missing interval flush the timer exists to fix. One deliberate exception, spelled out
        # where it happens: the cancellation handler in _flush puts its batch back without the lock.
        self._batch: list = []
        self._batch_lock = asyncio.Lock()

        # When the batch was last emptied — by the timer, by a size-driven flush or by a tick that
        # found nothing to send. The interval is measured from HERE and not from the timer's own
        # last wakeup, which is what the original in-loop check did: a flush is a flush whoever
        # triggered it, and a tick landing milliseconds after a full batch went out should wait
        # rather than send the one point that has arrived since.
        # Stamped when the batch is TAKEN, not when the write it started finishes: the promise in
        # README is about how long a point may sit unsent, and tying the clock to the write instead
        # would let one 10s InfluxDB timeout hold the next interval flush off for 10s+BATCH_INTERVAL
        # while the backlog it is supposed to drain keeps growing.
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
        ok, worth_retrying = await writer.write_batch_detailed(points)
        if ok:
            return True
        if not worth_retrying:
            # A 400 on a body InfluxDB will not parse, a 401 on a rotated password, a 404 on a
            # database somebody dropped: the same bytes get the same answer until a human changes
            # something. Repeating them buys nothing and spends RETRY_BUDGET per batch of a message
            # loop that is meanwhile not draining the broker's queue.
            return False
        # asyncio.timeout, not a plain elapsed-time check: the check can only run between attempts,
        # so an attempt that hangs in aiohttp's 10s timeout would blow the budget it is supposed to
        # respect. Cancelling mid-attempt is what makes RETRY_BUDGET an actual ceiling.
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
        # Two flushes can be inside their write at the same time — the lock is released before the
        # write, deliberately — and they can finish in the opposite order to the one they started
        # in. Last to finish used to win, so a slow failing flush landing after a fast successful
        # one left the dashboard showing last_flush_ok=False next to the count of a batch that had
        # already been superseded. Nothing happens to the data; what breaks is the only indicator
        # an operator has for whether the bridge is healthy. The flush that STARTED later wins
        # instead: it is the one whose answer describes the more recent state of InfluxDB.
        if started < self._last_stats_started:
            return
        self._last_stats_started = started
        self.last_flush_time = time.time()
        self.last_flush_count = count
        self.last_flush_ok = ok

    async def _flush(self, writer: InfluxWriter, *, retry: bool = True):
        # The lock is taken HERE rather than at the call sites, so that no caller can forget it. The
        # flip side: nothing reachable from this method may call it again, because asyncio.Lock is
        # not reentrant and that would deadlock the stream permanently.
        async with self._batch_lock:
            # Moved even when there is nothing to send. An idle stream's tick is still a moment at
            # which the batch was empty, so the next tick is a whole interval away instead of the
            # loop spinning on a deadline that is permanently in the past.
            self._last_flush_monotonic = time.monotonic()
            if not self._batch:
                return
            pending, self._batch = self._batch, []
            self.batch_current = 0

        # The write runs OUTSIDE the lock. It can take seconds with the retry ladder, and holding
        # the lock across it would block the message loop from appending for exactly that long —
        # which is the throughput problem the retries already have to be rationed against.
        count = len(pending)
        started = time.monotonic()
        try:
            ok = await (self._write_with_retries(writer, pending) if retry else writer.write_batch(pending))
        except asyncio.CancelledError:
            # From the swap above until this write returns, `pending` is the ONLY reference to
            # these points — they are already out of self._batch. A cancellation arriving mid-write
            # is not exotic: it is what a container stop is, and Portainer's updater recreates this
            # container on every push of :latest. Without this the shutdown flush that follows
            # would look at an empty self._batch, return immediately, and up to BATCH_SIZE points
            # would vanish with no error counted, no event emitted and no line in the log.
            #
            # Put back at the FRONT: the message loop keeps appending while a write is in flight,
            # and those points are newer than these.
            #
            # Deliberately NOT under self._batch_lock. Both statements are synchronous, and on a
            # single-threaded event loop a run of statements with no await in it cannot interleave
            # with another task — which is also why every critical section that lock does guard is
            # await-free, so no task can be suspended while holding it and the fast path would be
            # all this ever took. Taking it anyway would put a suspension point inside a
            # cancellation handler, the one place where a second cancellation would destroy exactly
            # the points being rescued.
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
            # The remainder of the interval, not the whole of it: the batch may have gone out on
            # size while this task slept, and sleeping a full interval from the WAKEUP rather than
            # from the last flush is what made a tick land milliseconds after a size-driven flush
            # and post a batch of one or two points. sleep() is called even when the deadline has
            # already passed — with BATCH_INTERVAL at 0 this loop would otherwise never yield,
            # because _flush's lock acquisition has a fast path that does not suspend, and the
            # message loop would never run again.
            remaining = BATCH_INTERVAL - (time.monotonic() - self._last_flush_monotonic)
            await asyncio.sleep(remaining if remaining > 0 else 0)
            if time.monotonic() - self._last_flush_monotonic < BATCH_INTERVAL:
                continue
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
            await self._process_forever(writer)
        finally:
            # THE FINAL FLUSH LIVES HERE, in a finally, and not in a handler for the cancellation
            # that usually causes it. The message loop has more than one way out and only one of
            # them is a cancellation: `if not self._running: break` returns NORMALLY, as does an
            # __aexit__ that turns a cancellation into an MqttError on its way out — its disconnect
            # wait is bounded by the same `timeout=10` the client is built with, and a timeout there
            # raises MqttError, which the loop's `except Exception` then absorbs. Both paths used to
            # walk past the flush with up to BATCH_SIZE points still in self._batch, including the
            # ones the timer's own cancellation handler had just rescued into it, and nothing left
            # running to send them. Put here, the flush is what EXITING the message loop means,
            # whichever way it exited.
            #
            # Order matters and is unchanged: the timer is cancelled first, because a tick landing
            # between here and the flush would race it for the same batch; then it is AWAITED, not
            # merely cancelled, because cancel() only schedules the cancellation and the timer may
            # be sitting in the middle of its own write with the whole batch held in a local. It
            # hands those points back to self._batch when the cancellation actually reaches it,
            # which is a later pass of the event loop than this one. Awaiting it is also what keeps
            # _run()'s finally from closing the writer's HTTP session under a timer still mid-flush.
            # Finally the flush itself runs WITHOUT the retry ladder, so a shutdown never waits out
            # its sleeps against a dead InfluxDB.
            timer.cancel()
            with suppress(asyncio.CancelledError):
                await timer
            await self._flush(writer, retry=False)

    async def _process_forever(self, writer: InfluxWriter):
        while self._running:
            try:
                client_kwargs = dict(
                    hostname=self.cfg.mqtt_host,
                    port=self.cfg.mqtt_port,
                    timeout=10,
                    max_queued_incoming_messages=MAX_QUEUED_INCOMING_MESSAGES,
                )
                if self.cfg.mqtt_user:
                    client_kwargs["username"] = self.cfg.mqtt_user
                    client_kwargs["password"] = self.cfg.mqtt_password

                async with aiomqtt.Client(**client_kwargs) as client:
                    await client.subscribe(self.cfg.mqtt_topic)
                    await self.on_event(self.cfg.id, "connected", {})
                    logger.info("[%s] MQTT connected, subscribed to %s", self.cfg.id, self.cfg.mqtt_topic)

                    async for message in client.messages:
                        # Leaves the loop without an exception, so nothing but _connect_and_process's
                        # finally sends what is already batched. Unreachable through stop() today —
                        # it sets _running and cancels in the same run of statements, so the task
                        # never observes the gap — but any future soft stop (a paused stream, a
                        # drain before a reload) arrives exactly here.
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
