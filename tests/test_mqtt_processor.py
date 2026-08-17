#!/usr/bin/env python3
"""Batching, retries and the interval timer.

Nothing here opens a socket. `aiomqtt.Client` is replaced with a fake whose `messages` iterator the
test drives by hand, and `InfluxWriter` with a recorder that answers from a scripted list — so a
failed write is a scripted `False`, not a real broker or a real InfluxDB.

The two things being pinned:

* a batch survives a write that fails, up to a bounded number of attempts inside a bounded amount of
  time — the bound matters as much as the retry, because a flush from the message loop runs while
  nothing is being read from the broker, and MQTT drops what it cannot deliver to a subscriber that
  is not reading;
* a batch leaves on the interval with no new message to trigger it, which is what README and
  docker-compose have always promised and what the in-loop time check could never deliver.
"""

import asyncio
import time
from contextlib import suppress

import pytest

from src import mqtt_processor
from src.config import StreamConfig
from src.mqtt_processor import StreamProcessor


# --- doubles ------------------------------------------------------------------------------------

class FakeWriter:
    """Stands in for InfluxWriter. Answers from `results`, then True for anything past the script."""

    def __init__(self, results=None):
        self.results = list(results or [])
        self.calls = []  # one entry per write_batch call, holding the batch as it was handed over

    async def write_batch(self, batch):
        self.calls.append(list(batch))
        # A real write suspends on the network. Without a suspension point here the whole processor
        # would run to completion without ever yielding, and the concurrency test below would only
        # be testing that one task can run alone.
        await asyncio.sleep(0)
        return self.results.pop(0) if self.results else True


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


def make_processor(stream_id="s1"):
    events = []

    async def on_event(sid, event_type, data):
        events.append((sid, event_type, data))
        await asyncio.sleep(0)  # a real broadcast reaches websockets and suspends

    proc = StreamProcessor(StreamConfig(id=stream_id, name="t", mqtt_topic="#"), on_event=on_event)
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
    """Keep the retry ladder's shape but take the waiting out of it."""
    monkeypatch.setattr(mqtt_processor, "RETRY_DELAYS", (0.0, 0.0, 0.0))


# --- bug 1: a failed write used to drop the batch on the first answer -----------------------------

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

    assert len(writer.calls) == 1 + len(mqtt_processor.RETRY_DELAYS)
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


# --- bug 2: the interval flush had no clock of its own -------------------------------------------

async def test_a_batch_leaves_on_the_interval_with_no_new_message(monkeypatch):
    # The one that goes red on the original code. The time check lived inside `async for message in
    # client.messages`, so it could only be evaluated when a message arrived — on a topic that went
    # quiet the batch simply stayed in memory. Exactly one message is published here, and it is well
    # under BATCH_SIZE, so only a clock can get it out.
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
    # Shutdown order: the timer goes first, or a tick lands in the middle of the final flush and the
    # two race for the same batch. And the final flush skips the ladder — a container being stopped
    # has a SIGTERM grace period, and spending it asleep between retries turns a graceful stop into
    # a kill.
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


# --- bug 3: the dead auth placeholder ------------------------------------------------------------

async def test_credentials_reach_the_client_and_nothing_else_pretends_to_carry_them(monkeypatch):
    # There used to be an `auth` local assigned `aiomqtt.Will` under a comment promising it was "set
    # below". Nothing set it and nothing read it; the credentials go where this asserts they go.
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


# --- bug 6: is_alive, the half that lives on the processor ---------------------------------------

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
