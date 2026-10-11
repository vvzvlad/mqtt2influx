#!/usr/bin/env python3
"""Tests for src/mqtt_processor.py: batching, retries, the flush timer, shutdown and the MQTT link.

Nothing here opens a socket. `aiomqtt.Client` is replaced with a fake whose `messages` iterator the
test drives by hand, and `InfluxWriter` with a recorder that answers from a scripted list, so a
failed write is a scripted `False`.
"""

import asyncio
import importlib
import logging
import os
import threading
import time
from contextlib import suppress

import aiomqtt
import pytest

from src import api, mqtt_processor
from src.config import StreamConfig, save_streams
from src.mqtt_processor import StreamProcessor
from src.stream_manager import manager


# --- doubles ------------------------------------------------------------------------------------

class FakeWriter:
    """Stands in for InfluxWriter. Answers from `results`, then True for anything past the script.

    Shaped like the real writer, `write_batch` wrapping `write_batch_detailed`, so whichever one
    the processor calls answers from the same script. An entry in `results` is a bool (False is a
    failure worth retrying) or an explicit `(stored, worth_retrying)` pair.

    `delay` is how long a write occupies the event loop, so a test can cancel a processor while a
    batch is in flight.
    """

    def __init__(self, results=None, delay=0.0):
        self.results = list(results or [])
        self.calls = []  # one entry per write call, holding the batch as it was handed over
        self.stored = []  # points that got all the way to an "InfluxDB accepted it" answer
        self.delay = delay

    async def write_batch_detailed(self, batch):
        self.calls.append(list(batch))
        # A real write suspends on the network. Without a suspension point here the whole processor
        # would run to completion without ever yielding, and the concurrency test below would only
        # be testing that one task can run alone.
        await asyncio.sleep(self.delay)
        answer = self.results.pop(0) if self.results else True
        stored, worth_retrying = answer if isinstance(answer, tuple) else (answer, True)
        if stored:
            # After the suspension, so a write cancelled in flight leaves no trace here. `calls`
            # records what was HANDED OVER, `stored` what InfluxDB actually took — and the whole
            # question in a shutdown test is which of the two a point ended up in.
            self.stored.extend(batch)
        return stored, worth_retrying

    async def write_batch(self, batch):
        stored, _worth_retrying = await self.write_batch_detailed(batch)
        return stored


class FakeMessage:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload.encode() if isinstance(payload, str) else payload


def install_fake_client(monkeypatch, queue):
    """Replace aiomqtt.Client with one whose messages come out of `queue` and never end."""
    created = []

    class _FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.subscribed = []
            created.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        async def subscribe(self, topic):
            self.subscribed.append(topic)

        @property
        def messages(self):
            async def _iter():
                while True:
                    yield await queue.get()

            return _iter()

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _FakeClient)
    return created


def install_disconnecting_client(monkeypatch, on_construct):
    """Replace aiomqtt.Client with one whose subscription ends immediately.

    An exhausted `messages` iterator is the cheap way to reach the reconnect path: the `async for`
    finishes, the `async with` unwinds and `while self._running` comes round again — the same
    sequence as a dropped connection, without the five-second error sleep.
    """

    class _FakeClient:
        def __init__(self, **kwargs):
            on_construct(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        async def subscribe(self, topic):
            pass

        @property
        def messages(self):
            async def _iter():
                await asyncio.sleep(0)
                for message in ():
                    yield message

            return _iter()

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _FakeClient)


def make_processor(stream_id="s1", **cfg_overrides):
    events = []

    async def on_event(sid, event_type, data):
        events.append((sid, event_type, data))
        await asyncio.sleep(0)  # a real broadcast reaches websockets and suspends

    proc = StreamProcessor(
        StreamConfig(id=stream_id, name="t", mqtt_topic="#", **cfg_overrides), on_event=on_event)
    proc.events = events
    return proc


def live_flush_timers():
    return [t for t in asyncio.all_tasks() if t.get_name().startswith("flush-timer-")]


async def eventually(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return predicate()


async def _stop(proc, task):
    proc._running = False
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


@pytest.fixture
def instant_retries(monkeypatch):
    """Keep the retry ladder three rungs long but take the waiting out of it.

    The tests assert "four attempts" as a literal: RETRY_DELAYS is patched here, so a count read
    back from it would agree with any ladder, the empty one included.
    """
    monkeypatch.setattr(mqtt_processor, "RETRY_DELAYS", (0.0, 0.0, 0.0))


@pytest.fixture
def reimport():
    """Re-execute src.mqtt_processor with a chosen environment, and put it back afterwards.

    A reload, not a call to the readers, because it checks the wiring: a module that assigned
    BATCH_SIZE from a literal would still pass a test of `batch_size_from_env()`. The reload re-runs
    the module body in its existing namespace, so every importer keeps working; the teardown reload
    restores the ambient values.
    """
    def _apply(values):
        for name, value in values.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _reimport(**env):
        """`name="value"` sets the variable, `name=None` removes it — the unconfigured container."""
        previous = {name: os.environ.get(name) for name in env}
        try:
            _apply(env)
            return importlib.reload(mqtt_processor)
        finally:
            _apply(previous)

    yield _reimport
    importlib.reload(mqtt_processor)


# --- a failed write is retried before the batch is dropped ----------------------------------------

async def test_a_batch_that_fails_once_is_written_on_the_retry(instant_retries):
    proc = make_processor()
    writer = FakeWriter([False, True])
    proc._batch = [("dev/temp", 21.5, 1000)]

    await proc._flush(writer)

    assert len(writer.calls) == 2
    assert writer.calls[1] == [("dev/temp", 21.5, 1000)]
    assert proc.points_sent == 1
    assert proc.batches_sent == 1
    assert proc.retries == 1
    assert proc.errors == 0
    assert proc.last_flush_ok is True


async def test_a_write_that_succeeds_first_time_is_not_retried(instant_retries):
    proc = make_processor()
    writer = FakeWriter([True])
    proc._batch = [("dev/temp", 21.5, 1000)]

    await proc._flush(writer)

    # One point sent must cost exactly one request. A retry ladder that runs on success would
    # multiply the write load on InfluxDB by four for no reason at all.
    assert len(writer.calls) == 1
    assert proc.retries == 0


async def test_a_batch_is_dropped_and_counted_once_the_attempts_are_spent(instant_retries):
    proc = make_processor()
    writer = FakeWriter([False, False, False, False])
    proc._batch = [("dev/temp", 21.5, 1000), ("dev/hum", 40.0, 1000)]

    await proc._flush(writer)

    # Four: the first attempt and one per rung of the ladder (a literal, see instant_retries).
    assert len(writer.calls) == 4
    assert proc.errors == 1
    assert proc.retries == 0
    assert proc.points_sent == 0
    assert proc.last_flush_ok is False
    # Dropped, not kept: holding failed batches while InfluxDB is down grows memory without a bound.
    # The loss is the deliberate ceiling, the four attempts are what make it rare.
    assert proc._batch == []
    assert proc.batch_current == 0


async def test_the_retry_ladder_is_capped_in_wall_clock_time(monkeypatch):
    # Every second spent here is a second the message loop is not reading from the broker. The
    # ladder's own sleeps are the part that can be counted; the cap is what stops a slow InfluxDB
    # from turning them into something much longer.
    monkeypatch.setattr(mqtt_processor, "RETRY_DELAYS", (10.0, 10.0, 10.0))
    monkeypatch.setattr(mqtt_processor, "RETRY_BUDGET", 0.05)
    proc = make_processor()
    writer = FakeWriter([False, False, False, False])
    proc._batch = [("dev/temp", 21.5, 1000)]

    started = time.monotonic()
    await proc._flush(writer)
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    assert proc.errors == 1
    assert proc._batch == []


async def test_the_stats_expose_the_retry_counter():
    # The UI reads get_stats(). A write surviving only on its second attempt is the difference
    # between "healthy" and "one blip away from losing points", and it has to be visible.
    proc = make_processor()
    proc.retries = 3

    assert proc.get_stats()["retries"] == 3


# --- the interval flush has a clock of its own ----------------------------------------------------

async def test_a_batch_leaves_on_the_interval_with_no_new_message(monkeypatch):
    # One message, well under BATCH_SIZE, and nothing after it: only the timer can send it.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 0.05)
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/temp", "21.5"))

    try:
        assert await eventually(lambda: writer.calls), "the batch never left without a second message"
    finally:
        await _stop(proc, task)

    (topic, value, _ts), = writer.calls[0]
    assert (topic, value) == ("dev/temp", 21.5)
    assert proc.points_sent == 1
    assert proc.batches_sent == 1


async def test_the_timer_does_not_write_an_empty_batch(monkeypatch):
    # A tick on an idle stream must cost nothing. Without the early return the bridge would post an
    # empty body to InfluxDB every BATCH_INTERVAL for every stream, forever.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 0.005)
    proc = make_processor()
    writer = FakeWriter()

    timer = asyncio.create_task(proc._flush_timer(writer), name="flush-timer-test")
    await asyncio.sleep(0.1)  # ~20 ticks
    timer.cancel()
    with suppress(asyncio.CancelledError):
        await timer

    assert writer.calls == []
    assert proc.batches_sent == 0
    assert proc.last_flush_time == 0.0


async def test_a_reconnect_does_not_leak_a_flush_timer(monkeypatch):
    # `while self._running` reconnects for the life of the stream. A timer started inside that loop
    # would leave the previous one running on every pass, and every leaked timer keeps flushing the
    # same shared batch — so the count is checked on each pass, not only at the end.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    proc = make_processor()
    timers_seen = []

    def on_construct(_client):
        timers_seen.append(len(live_flush_timers()))
        if len(timers_seen) >= 4:
            proc._running = False

    install_disconnecting_client(monkeypatch, on_construct)
    proc._running = True

    await asyncio.wait_for(proc._connect_and_process(FakeWriter()), timeout=5)

    assert len(timers_seen) == 4, "the reconnect loop did not run four times"
    assert timers_seen == [1, 1, 1, 1]
    assert live_flush_timers() == []


async def test_stopping_the_stream_kills_the_timer_and_flushes_without_waiting_out_the_retries(monkeypatch):
    # The timer goes first, or a tick lands in the middle of the final flush and races it for the
    # batch. The final flush skips the ladder: sleeping between retries would spend the SIGTERM
    # grace period.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter([False])  # the shutdown write fails, so a ladder would engage if there were one
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/temp", "21.5"))
    assert await eventually(lambda: proc.batch_current == 1), "the point never reached the batch"

    started = time.monotonic()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, "shutdown waited out the retry ladder"
    assert len(writer.calls) == 1
    assert writer.calls[0] == [("dev/temp", 21.5, writer.calls[0][0][2])]
    assert proc.errors == 1
    assert live_flush_timers() == []


async def test_the_timer_and_the_message_loop_never_lose_or_duplicate_a_point(monkeypatch):
    # The batch is shared state the moment the timer exists. With BATCH_INTERVAL at zero the timer
    # flushes on every pass of the event loop, so its swap of the batch interleaves with the message
    # loop's appends as often as the loop allows — and BATCH_SIZE is small enough that size-driven
    # flushes cut in between. Every point published must come out exactly once.
    total = 300
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 0)
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 7)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    for i in range(total):
        queue.put_nowait(FakeMessage(f"dev/p{i:04d}", str(i)))

    try:
        assert await eventually(lambda: sum(len(c) for c in writer.calls) == total, timeout=10), (
            "only {} of {} points were written".format(sum(len(c) for c in writer.calls), total)
        )
    finally:
        await _stop(proc, task)

    written = [topic for call in writer.calls for topic, _value, _ts in call]
    assert len(written) == total
    assert sorted(written) == sorted(f"dev/p{i:04d}" for i in range(total))


# --- the stream's own precision, from streams.json to the batched point ---------------------------
#
# tests/test_influx_writer.py pins what each precision does; these two pin the wiring. Every layer
# in between defaults to two decimals, so a processor that dropped the config would still look
# healthy.

async def test_a_streams_configured_precision_reaches_the_points_it_batches(monkeypatch):
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor(value_precision=-1)
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage(
        "ue/kiln", '{"calibration_coeff_a": 0.00003618, "adc_raw": 10.74805}'))

    try:
        assert await eventually(lambda: writer.calls), "the batch never reached the writer"
    finally:
        await _stop(proc, task)

    assert sorted((topic, value) for topic, value, _ts in writer.calls[0]) == [
        ("ue/kiln/adc_raw", 10.74805),
        ("ue/kiln/calibration_coeff_a", 0.00003618),
    ]


async def test_a_stream_that_configures_nothing_still_batches_two_decimals(monkeypatch):
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage(
        "ue/kiln", '{"calibration_coeff_a": 0.00003618, "adc_raw": 10.74805}'))

    try:
        assert await eventually(lambda: writer.calls), "the batch never reached the writer"
    finally:
        await _stop(proc, task)

    assert sorted((topic, value) for topic, value, _ts in writer.calls[0]) == [
        ("ue/kiln/adc_raw", 10.75),
        ("ue/kiln/calibration_coeff_a", 0.0),
    ]


# --- the stream's write_strings flag, from streams.json to the batched point ----------------------
#
# The same wiring check as above: every default along the chain is off, so a processor that never
# passed `cfg.write_strings` to `flatten_payload` would drop status strings and look healthy.

async def test_a_stream_with_write_strings_on_batches_a_status_string(monkeypatch):
    """End to end: "Printing" reaches the writer instead of being dropped."""
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor(write_strings=True)
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("data/gcode/status", "Printing"))

    try:
        assert await eventually(lambda: writer.calls), "the batch never reached the writer"
    finally:
        await _stop(proc, task)

    assert [(topic, value) for topic, value, _ts in writer.calls[0]] == [
        ("data/gcode/status", "Printing"),
    ]


async def test_a_stream_that_leaves_the_flag_off_still_drops_a_status_string(monkeypatch):
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("data/gcode/status", "Printing"))
    # A message that DOES produce a point, published after it, so the assertion below waits for real
    # work to finish rather than for a timeout: if the status string were batched it would be here.
    queue.put_nowait(FakeMessage("data/gcode/progress", "42"))

    try:
        assert await eventually(lambda: writer.calls), "the batch never reached the writer"
    finally:
        await _stop(proc, task)

    assert [(topic, value) for call in writer.calls for topic, value, _ts in call] == [
        ("data/gcode/progress", 42.0),
    ]


# --- MQTT credentials -----------------------------------------------------------------------------

async def test_credentials_reach_the_client_and_nothing_else_pretends_to_carry_them(monkeypatch):
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    queue = asyncio.Queue()
    created = install_fake_client(monkeypatch, queue)

    proc = make_processor()
    proc.cfg.mqtt_host = "broker.example"
    proc.cfg.mqtt_user = "u"
    proc.cfg.mqtt_password = "p"
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(FakeWriter()))

    try:
        assert await eventually(lambda: created and created[0].subscribed)
    finally:
        await _stop(proc, task)

    assert created[0].kwargs["username"] == "u"
    assert created[0].kwargs["password"] == "p"
    assert created[0].kwargs["hostname"] == "broker.example"
    assert created[0].subscribed == ["#"]


# --- is_alive -------------------------------------------------------------------------------------

async def test_a_processor_is_not_alive_before_it_is_started():
    # `_task` is None here, and `None.done()` is what a naive check would raise on.
    assert make_processor().is_alive is False


async def test_a_processor_is_alive_while_its_task_runs_and_not_after_it_stops(monkeypatch):
    proc = make_processor()
    running = asyncio.Event()

    async def fake_run():
        running.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(proc, "_run", fake_run)
    proc.start()
    await running.wait()
    assert proc.is_alive is True

    await proc.stop()
    assert proc.is_alive is False


async def test_a_processor_whose_task_died_is_not_alive(monkeypatch):
    proc = make_processor()

    async def fake_run():
        raise RuntimeError("died before entering the message loop")

    monkeypatch.setattr(proc, "_run", fake_run)
    proc.start()
    with suppress(RuntimeError):
        await proc._task

    assert proc.is_alive is False


# --- a cancellation landing in the middle of a write ----------------------------------------------
#
# The batch is taken out of `self._batch` before it is written, so two flushes never send the same
# points. For the length of the write the only reference to them is a local, and a `docker stop`
# can cancel inside that window.

async def test_a_write_cancelled_in_flight_gives_its_points_back_to_the_batch():
    proc = make_processor()
    writer = FakeWriter(delay=5.0)
    proc._batch = [("dev/temp", 21.5, 1000), ("dev/hum", 40.0, 1001)]

    flush = asyncio.create_task(proc._flush(writer, retry=False))
    assert await eventually(lambda: writer.calls), "the write never started"
    flush.cancel()
    with suppress(asyncio.CancelledError):
        await flush

    assert proc._batch == [("dev/temp", 21.5, 1000), ("dev/hum", 40.0, 1001)]
    # batch_current is what the dashboard shows and what the next size check reads; leaving it at 0
    # with two points in the list would hide them from both.
    assert proc.batch_current == 2
    assert writer.stored == []


async def test_points_handed_back_by_a_cancelled_write_go_in_front_of_the_newer_ones():
    # The message loop keeps appending while a write is in flight, so by the time the cancellation
    # lands `self._batch` already holds points that are NEWER than the ones being returned.
    # Appending the rescued batch instead of prepending it would reorder the series it goes on to
    # write, and InfluxDB is queried on time order.
    proc = make_processor()
    writer = FakeWriter(delay=5.0)
    proc._batch = [("dev/a", 1.0, 1000)]

    flush = asyncio.create_task(proc._flush(writer, retry=False))
    assert await eventually(lambda: writer.calls), "the write never started"
    async with proc._batch_lock:
        proc._batch.append(("dev/b", 2.0, 2000))
    flush.cancel()
    with suppress(asyncio.CancelledError):
        await flush

    assert proc._batch == [("dev/a", 1.0, 1000), ("dev/b", 2.0, 2000)]


async def test_a_shutdown_during_a_size_flush_still_accounts_for_every_published_point(monkeypatch):
    # The flush in flight is the message loop's, fired because the batch filled. The cancellation
    # unwinds the loop and the final flush runs in `_connect_and_process`'s finally. Every point
    # published has to end up either written or still in the batch.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 2)
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter(delay=0.2)
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    queue.put_nowait(FakeMessage("dev/b", "2"))
    assert await eventually(lambda: writer.calls), "the size-driven flush never started"

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    accounted = [(topic, value) for topic, value, _ts in writer.stored + proc._batch]
    assert sorted(accounted) == [("dev/a", 1.0), ("dev/b", 2.0)]
    assert live_flush_timers() == []


async def test_a_shutdown_while_the_timer_is_writing_does_not_lose_the_timers_batch(monkeypatch):
    # The flush in flight is the timer's. The shutdown has to await the cancelled timer before the
    # final flush: cancel() only schedules the cancellation, and the timer hands its points back on
    # a later pass of the event loop.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 0.05)
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)  # nothing here flushes on size
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter(delay=0.2)
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    assert await eventually(lambda: writer.calls), "the interval flush never started"

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert [(topic, value) for topic, value, _ts in writer.stored] == [("dev/a", 1.0)]
    assert proc._batch == []
    assert live_flush_timers() == []


async def test_a_cancellation_inside_the_retry_ladder_still_gives_the_points_back(monkeypatch):
    # The tests above flush with `retry=False` and never enter `asyncio.timeout(RETRY_BUDGET)`.
    # Inside it, an expired budget and a shutdown both arrive as a cancellation at the same await:
    # the first becomes a dropped batch, the second must give the points back. RETRY_BUDGET is
    # raised so that nothing here can reach it.
    monkeypatch.setattr(mqtt_processor, "RETRY_DELAYS", (0.0, 0.0, 0.0))
    monkeypatch.setattr(mqtt_processor, "RETRY_BUDGET", 30.0)

    class _HangsOnTheSecondAttempt:
        def __init__(self):
            self.calls = []
            self.retrying = asyncio.Event()

        async def write_batch_detailed(self, batch):
            self.calls.append(list(batch))
            if len(self.calls) == 1:
                await asyncio.sleep(0)
                return False, True  # retryable: this is what opens the ladder
            self.retrying.set()
            await asyncio.sleep(3600)  # an InfluxDB that accepted the connection and went quiet
            raise AssertionError("unreachable: the test cancels long before this")

        async def write_batch(self, batch):
            raise AssertionError("the ladder must go through write_batch_detailed")

    proc = make_processor()
    writer = _HangsOnTheSecondAttempt()
    points = [("dev/temp", 21.5, 1000), ("dev/hum", 40.0, 1001)]
    proc._batch = list(points)

    flush = asyncio.create_task(proc._flush(writer, retry=True))
    await asyncio.wait_for(writer.retrying.wait(), timeout=5)
    flush.cancel()
    with suppress(asyncio.CancelledError):
        await flush

    assert len(writer.calls) == 2, "the ladder was never entered, so this tested nothing"
    assert proc._batch == points
    assert proc.batch_current == 2
    # Not an error and not a drop: this batch is still going to be written by the final flush. An
    # errors of 1 here is the budget path having been taken by mistake.
    assert proc.errors == 0
    assert proc.points_sent == 0


# --- leaving the message loop is what flushes, whichever way it was left ---------------------------

async def test_a_stream_that_leaves_the_message_loop_on_its_own_still_flushes_what_it_batched(monkeypatch):
    """`if not self._running: break` is a normal return, not a cancellation, and still flushes.

    stop() never takes it: it clears the flag and cancels with no await in between.
    """
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)   # no interval flush
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)     # no size flush
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    assert await eventually(lambda: proc.batch_current == 1), "the point never reached the batch"

    # No cancel() anywhere in this test — that is the whole point. The flag is cleared and a second
    # message is what wakes the loop to notice it.
    proc._running = False
    queue.put_nowait(FakeMessage("dev/b", "2"))

    await asyncio.wait_for(task, timeout=5)

    assert [(topic, value) for topic, value, _ts in writer.stored] == [("dev/a", 1.0)]
    assert proc._batch == []
    assert proc.batch_current == 0
    assert live_flush_timers() == []


async def test_a_disconnect_that_times_out_during_a_shutdown_still_flushes_what_it_batched(monkeypatch):
    """aiomqtt's `__aexit__` can turn the cancellation unwinding through it into an `MqttError`.

    Its disconnect wait is bounded by the client's `timeout=`, and the expiry raises
    `MqttError("Operation timed out")` in place of the `CancelledError`. The loop's `except Exception`
    absorbs that, `while self._running` is already false, and `_process_forever` returns normally.
    """
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)   # no interval flush
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)     # no size flush
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    class _DisconnectTimesOut(mqtt_processor.aiomqtt.Client):  # the fake installed just above
        async def __aexit__(self, exc_type, exc, tb):
            if exc_type is asyncio.CancelledError:
                raise aiomqtt.MqttError("Operation timed out")
            return False

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _DisconnectTimesOut)

    proc = make_processor()
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    assert await eventually(lambda: proc.batch_current == 1), "the point never reached the batch"

    # What stop() does: the flag and the cancel in one run of statements.
    proc._running = False
    task.cancel()
    # A normal return, not a CancelledError: the MqttError replaced it, which is the path under test.
    await asyncio.wait_for(task, timeout=5)

    assert proc.errors == 1, "the MqttError never reached the loop's except Exception"
    assert [(topic, value) for topic, value, _ts in writer.stored] == [("dev/a", 1.0)]
    assert proc._batch == []
    assert proc.batch_current == 0
    assert live_flush_timers() == []


async def test_a_shutdown_during_the_reconnect_backoff_still_flushes_what_it_batched(monkeypatch):
    """A cancellation landing in the backoff sleep is raised inside the `except Exception` body.

    No sibling handler of the same `try` catches that, which is why the final flush is a `finally`.
    """
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)   # no interval flush
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)     # no size flush
    queue = asyncio.Queue()
    install_dropping_client(monkeypatch, [queue])

    proc = make_processor()

    # make_processor's callback suspends; this one does not, so nothing between the `except`
    # catching the drop and the backoff sleep yields to the event loop. By the time this test can
    # see `errors` move, the processor is parked in that sleep — at its real length, which the
    # cancellation below cuts short.
    async def on_event_without_a_suspension(sid, event_type, data):
        proc.events.append((sid, event_type, data))

    proc.on_event = on_event_without_a_suspension
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    assert await eventually(lambda: proc.batch_current == 1), "the point never reached the batch"

    queue.put_nowait(aiomqtt.MqttError("Disconnected during message iteration"))
    assert await eventually(lambda: proc.errors == 1), "the link never dropped"

    await _stop(proc, task)

    assert [(topic, value) for topic, value, _ts in writer.stored] == [("dev/a", 1.0)]
    assert proc._batch == []
    assert proc.batch_current == 0
    assert live_flush_timers() == []


async def test_a_flush_timer_that_died_does_not_take_the_final_flush_with_it(monkeypatch, caplog):
    """The timer is awaited before the final flush, and it can end in something other than a
    cancellation; that exception is logged and the flush still runs."""
    monkeypatch.setattr(mqtt_processor, "BATCH_SIZE", 1000)     # no size flush
    queue = asyncio.Queue()
    install_fake_client(monkeypatch, queue)

    proc = make_processor()

    async def dying_timer(_writer):
        raise RuntimeError("timer crashed")

    monkeypatch.setattr(proc, "_flush_timer", dying_timer)
    writer = FakeWriter()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(writer))
    queue.put_nowait(FakeMessage("dev/a", "1"))
    assert await eventually(lambda: proc.batch_current == 1), "the point never reached the batch"

    with caplog.at_level(logging.ERROR, logger="src.mqtt_processor"):
        task.cancel()
        # Waited for rather than awaited, so that how the task ended is judged after the batch.
        await asyncio.wait([task])

    assert [(topic, value) for topic, value, _ts in writer.stored] == [("dev/a", 1.0)]
    assert proc._batch == []
    assert task.cancelled(), "the timer's error escaped the shutdown in place of the cancellation"
    assert ("ERROR", "[s1] Flush timer died: timer crashed") in [
        (r.levelname, r.getMessage()) for r in caplog.records]


# --- a failure InfluxDB will give again is not worth asking about again ---------------------------

async def test_an_answer_that_will_not_change_is_not_retried(instant_retries):
    # 400 on a body the line parser refuses, 401 on a rotated password, 404 on a dropped database:
    # the answer will not change, and the ladder would only stall the message loop.
    proc = make_processor()
    writer = FakeWriter([(False, False)])
    proc._batch = [("dev/temp", 21.5, 1000)]

    await proc._flush(writer)

    assert len(writer.calls) == 1
    assert proc.errors == 1
    assert proc.retries == 0
    assert proc.last_flush_ok is False


async def test_the_ladder_stops_the_moment_the_answer_turns_permanent(instant_retries):
    # The classification has to be re-read on every rung, not only on the first attempt: an
    # InfluxDB that comes back up with the database gone answers 503 and then 404, and the ladder
    # must stop on the 404 rather than run out its remaining attempts against it.
    proc = make_processor()
    writer = FakeWriter([(False, True), (False, False), True])
    proc._batch = [("dev/temp", 21.5, 1000)]

    await proc._flush(writer)

    assert len(writer.calls) == 2
    assert proc.last_flush_ok is False
    assert proc.retries == 0


# --- the dashboard shows the newest answer, not the last one to arrive ----------------------------

async def test_a_slow_flush_finishing_late_does_not_overwrite_a_newer_flushs_result():
    # Two flushes overlap by design (the lock is released before the write) and can finish in the
    # opposite order to the one they started in. The dashboard shows the one that started later.
    proc = make_processor()
    slow = FakeWriter([False], delay=0.2)
    fast = FakeWriter([True])

    proc._batch = [("dev/a", 1.0, 1000)]
    first = asyncio.create_task(proc._flush(slow, retry=False))
    assert await eventually(lambda: slow.calls), "the slow write never started"

    proc._batch = [("dev/b", 2.0, 2000), ("dev/c", 3.0, 3000)]
    await proc._flush(fast, retry=False)
    assert proc.last_flush_ok is True
    assert proc.last_flush_count == 2

    await first

    assert proc.last_flush_ok is True, "the older flush landed on the dashboard after the newer one"
    assert proc.last_flush_count == 2
    # The cumulative counters are a different question and must still see both flushes: one failed.
    assert proc.errors == 1
    assert proc.batches_sent == 1


# --- the interval is measured from the last flush, not from the timer's last wakeup ---------------

async def test_a_size_driven_flush_pushes_the_next_tick_a_whole_interval_out(monkeypatch):
    # The interval means "this long since something was last written". Measured from the timer's
    # own wakeup instead, a tick would land just after a full batch went out and post the one or
    # two points that arrived in between.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 1.0)
    proc = make_processor()
    writer = FakeWriter()

    timer = asyncio.create_task(proc._flush_timer(writer), name="flush-timer-test")
    try:
        await asyncio.sleep(0.5)
        proc._batch = [("dev/a", 1.0, 1000)]
        await proc._flush(writer)  # the size-driven flush, half an interval in
        assert len(writer.calls) == 1
        proc._batch = [("dev/b", 2.0, 2000)]  # one point arrives right behind it

        await asyncio.sleep(0.7)  # past the old deadline at t=1.0, short of the new one at t=1.5
        assert len(writer.calls) == 1, "the timer fired an interval after its wakeup, not after the flush"

        assert await eventually(lambda: len(writer.calls) == 2, timeout=2), "the tick never came at all"
    finally:
        timer.cancel()
        with suppress(asyncio.CancelledError):
            await timer


# --- the constants themselves ---------------------------------------------------------------------
#
# Every other test patches these first, so only the tests below see the real values.

def test_the_retry_ladder_has_rungs_and_all_of_them_wait():
    # An empty tuple makes `_write_with_retries` a single attempt: every InfluxDB blip costs a batch
    # and the retry counter never moves.
    assert len(mqtt_processor.RETRY_DELAYS) > 0
    assert all(delay > 0 for delay in mqtt_processor.RETRY_DELAYS)


def test_every_rung_of_the_ladder_is_reachable_inside_the_budget():
    # The budget cancels the retry phase mid-attempt. Sleeps that add up to more than it mean the
    # last rungs can never be reached no matter how fast InfluxDB answers — a ladder shorter than
    # it looks, and nothing anywhere would say so.
    assert sum(mqtt_processor.RETRY_DELAYS) < mqtt_processor.RETRY_BUDGET


def test_the_retry_budget_stays_within_one_attempts_own_timeout():
    # The budget comes on top of the first attempt's 10s aiohttp timeout, while this stream takes
    # nothing out of aiomqtt's incoming queue. At or under one attempt's timeout, the worst-case
    # flush stays at roughly twice a single write.
    assert 0 < mqtt_processor.RETRY_BUDGET <= 10.0


def test_the_incoming_queue_bound_is_an_actual_bound():
    # aiomqtt reads `0 or less` as unlimited, so a bound of 0 is not a small queue, it is no queue
    # limit at all — the exact default this constant exists to replace.
    assert mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES > 0


def test_the_batch_size_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "220")
    assert mqtt_processor.batch_size_from_env() == 220
    monkeypatch.delenv("BATCH_SIZE")
    assert mqtt_processor.batch_size_from_env() == 100


def test_the_batch_interval_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("BATCH_INTERVAL", "3.0")
    assert mqtt_processor.batch_interval_from_env() == 3.0
    monkeypatch.delenv("BATCH_INTERVAL")
    assert mqtt_processor.batch_interval_from_env() == 1.0


def test_the_module_constants_are_the_environments_values_and_not_literals(reimport):
    # Values other than the defaults (100 and 1.0), so a module that ignored the environment fails.
    reloaded = reimport(BATCH_SIZE="220", BATCH_INTERVAL="3.0")

    assert reloaded.BATCH_SIZE == 220
    assert reloaded.BATCH_INTERVAL == 3.0


def test_the_module_constants_fall_back_to_the_documented_defaults(reimport):
    reloaded = reimport(BATCH_SIZE=None, BATCH_INTERVAL=None)

    assert reloaded.BATCH_SIZE == 100
    assert reloaded.BATCH_INTERVAL == 1.0


# --- the incoming queue, and what a stalled consumer really costs ---------------------------------

async def test_the_client_is_built_with_a_bounded_incoming_queue(monkeypatch):
    # A flush retrying inside `async for message` does not stop aiomqtt reading the socket: its
    # callback keeps filling the client's queue. Unbounded, a long InfluxDB outage grows memory
    # until an OOM kill; bounded, it discards the newest arrivals with a line in aiomqtt's log.
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    queue = asyncio.Queue()
    created = install_fake_client(monkeypatch, queue)

    proc = make_processor()
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(FakeWriter()))
    try:
        assert await eventually(lambda: created and created[0].subscribed)
    finally:
        await _stop(proc, task)

    assert created[0].kwargs["max_queued_incoming_messages"] == mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES


async def test_the_bound_reaches_the_queue_aiomqtt_actually_builds():
    # The fakes swallow **kwargs, so only the real client shows that aiomqtt takes the keyword.
    # `aiomqtt.Client(...)` opens no socket and builds the queue in __init__; the test is async
    # because the constructor asks for the running loop.
    client = aiomqtt.Client(
        hostname="broker.invalid",
        max_queued_incoming_messages=mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES,
    )

    assert client._queue.maxsize == mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES


# --- the MQTT link, as opposed to the task --------------------------------------------------------
#
# The reconnect loop keeps the task alive through every failed connect, so a running task says
# nothing about the link; `connected` and the failure counters describe it.

def install_refusing_client(monkeypatch, on_construct):
    """Replace aiomqtt.Client with one whose connect always fails, the way aiomqtt reports it."""

    class _FakeClient:
        def __init__(self, **kwargs):
            on_construct(self)

        async def __aenter__(self):
            await asyncio.sleep(0)
            raise aiomqtt.MqttError("Operation timed out")

        async def __aexit__(self, *exc_info):
            raise AssertionError("a connect that failed has nothing to exit")

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _FakeClient)


def install_dropping_client(monkeypatch, queues, on_construct=lambda: None):
    """Client number i reads its messages from `queues[i]`. An exception put into a queue is raised
    out of the message iterator — a connection dropped mid-stream, as aiomqtt reports one."""
    constructed = []

    class _FakeClient:
        def __init__(self, **kwargs):
            on_construct()
            self.queue = queues[len(constructed)]
            constructed.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        async def subscribe(self, topic):
            pass

        @property
        def messages(self):
            async def _iter():
                while True:
                    item = await self.queue.get()
                    if isinstance(item, BaseException):
                        raise item
                    yield item

            return _iter()

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _FakeClient)


def event_types(proc):
    return [event_type for _sid, event_type, _data in proc.events]


async def test_a_connect_that_keeps_failing_is_counted_escalated_and_never_reported_connected(monkeypatch, caplog):
    monkeypatch.setattr(mqtt_processor, "RECONNECT_DELAY", 0)
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    proc = make_processor()
    connected_at_each_attempt = []

    def on_construct(_client):
        connected_at_each_attempt.append(proc.connected)
        if len(connected_at_each_attempt) >= 5:
            proc._running = False  # the fifth attempt still runs and fails; then the loop ends

    install_refusing_client(monkeypatch, on_construct)
    proc._running = True

    with caplog.at_level(logging.WARNING, logger="src.mqtt_processor"):
        await asyncio.wait_for(proc._connect_and_process(FakeWriter()), timeout=5)

    assert connected_at_each_attempt == [False] * 5
    assert proc.connected is False
    assert proc.errors == 5
    assert proc.get_stats()["consecutive_failures"] == 5
    assert proc.get_stats()["connected"] is False
    # Four in a row is one minute at the production cadence; from there on the line is an ERROR.
    retry_lines = [r.levelname for r in caplog.records if "Error: Operation timed out" in r.getMessage()]
    assert retry_lines == ["WARNING", "WARNING", "WARNING", "ERROR", "ERROR"]
    assert event_types(proc).count("disconnected") == 5
    assert "connected" not in event_types(proc)


async def test_a_link_dropped_mid_stream_is_down_until_the_reconnect_resets_the_count(monkeypatch):
    monkeypatch.setattr(mqtt_processor, "RECONNECT_DELAY", 0)
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    first, second = asyncio.Queue(), asyncio.Queue()
    proc = make_processor()
    # Read at the moment each client is built, i.e. after the previous one's exit and the delay:
    # with RECONNECT_DELAY at 0 that is the only point at which the down state can be observed.
    state_at_each_attempt = []
    install_dropping_client(
        monkeypatch, [first, second],
        on_construct=lambda: state_at_each_attempt.append((proc.connected, proc.consecutive_failures)))
    proc._running = True
    task = asyncio.create_task(proc._connect_and_process(FakeWriter()))

    try:
        assert await eventually(lambda: proc.connected), "the first connect never happened"
        first.put_nowait(FakeMessage("dev/a", "1"))
        assert await eventually(lambda: proc.last_message_time > 0)
        first.put_nowait(aiomqtt.MqttError("Disconnected during message iteration"))
        assert await eventually(lambda: proc.reconnects == 1), "the stream never reconnected"

        assert state_at_each_attempt == [(False, 0), (False, 1)]
        assert proc.connected is True
        assert proc.consecutive_failures == 0
        stats = proc.get_stats()
        assert (stats["connected"], stats["reconnects"], stats["consecutive_failures"]) == (True, 1, 0)
        assert stats["last_connect_time"] > 0
        assert event_types(proc) == ["connected", "message", "error", "disconnected", "connected"]
    finally:
        await _stop(proc, task)

    # A cancellation leaves the loop without passing through the `except`.
    assert proc.connected is False


async def test_a_stream_whose_link_is_down_is_running_but_not_connected_in_stats_and_api(monkeypatch, data_dir):
    monkeypatch.setattr(mqtt_processor, "RECONNECT_DELAY", 3600)  # parks the task in the retry wait
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    queue = asyncio.Queue()
    install_dropping_client(monkeypatch, [queue])
    proc = make_processor()
    monkeypatch.setattr(proc, "_run", lambda: proc._connect_and_process(FakeWriter()))
    save_streams([proc.cfg])
    manager._processors[proc.cfg.id] = proc
    proc.start()

    try:
        assert await eventually(lambda: proc.connected), "the connect never happened"
        # Healthy first, so the field below is shown to be able to say true at all.
        (listed,) = await api.list_streams()
        assert (listed["running"], listed["connected"]) == (True, True)

        queue.put_nowait(aiomqtt.MqttError("Disconnected during message iteration"))
        assert await eventually(lambda: proc.consecutive_failures == 1)

        assert proc.is_alive is True
        assert proc.get_stats()["connected"] is False
        (listed,) = await api.list_streams()
        assert listed["running"] is True
        assert listed["connected"] is False
    finally:
        manager._processors.pop(proc.cfg.id, None)
        await proc.stop()


async def test_a_connect_that_hangs_in_the_executor_is_bounded_and_retried(monkeypatch):
    # aiomqtt's __aenter__ runs paho's blocking connect() in an executor thread before its own
    # timeout starts; unbounded, a hung DNS lookup or kernel connect would block the stream forever.
    monkeypatch.setattr(mqtt_processor, "MQTT_TIMEOUT", 0.05)
    monkeypatch.setattr(mqtt_processor, "RECONNECT_DELAY", 0)
    monkeypatch.setattr(mqtt_processor, "BATCH_INTERVAL", 60)
    released = threading.Event()
    proc = make_processor()
    attempts = []

    class _HangingClient:
        def __init__(self, **kwargs):
            attempts.append(kwargs)
            if len(attempts) >= 2:
                proc._running = False

        async def __aenter__(self):
            # The same shape as aiomqtt's: a thread no cancellation can interrupt.
            await asyncio.get_running_loop().run_in_executor(None, released.wait)
            return self

        async def __aexit__(self, *exc_info):
            return False

    monkeypatch.setattr(mqtt_processor.aiomqtt, "Client", _HangingClient)
    proc._running = True

    try:
        await asyncio.wait_for(proc._connect_and_process(FakeWriter()), timeout=2)
    finally:
        released.set()

    assert len(attempts) == 2
    assert proc.errors == 2
    assert proc.consecutive_failures == 2
    assert proc.connected is False
    errors = [data["message"] for _sid, event_type, data in proc.events if event_type == "error"]
    # Twice MQTT_TIMEOUT: the second one is aiomqtt's CONNACK wait, which the bound must not pre-empt.
    assert errors == ["Connect did not complete within 0.1s"] * 2
