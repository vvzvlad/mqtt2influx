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
import importlib
import os
import time
from contextlib import suppress

import aiomqtt
import pytest

from src import mqtt_processor
from src.config import StreamConfig
from src.mqtt_processor import StreamProcessor


# --- doubles ------------------------------------------------------------------------------------

class FakeWriter:
    """Stands in for InfluxWriter. Answers from `results`, then True for anything past the script.

    Shaped like the real writer: `write_batch_detailed` is the primitive and `write_batch` is the
    thin bool wrapper over it, so a test that patches one cannot get a processor that quietly took
    the other path. An entry in `results` is either a bool — a failure of the kind that is worth
    retrying, which is what every failure used to be — or an explicit `(stored, worth_retrying)`
    pair for the tests that care which kind InfluxDB gave.

    `delay` is how long a write occupies the event loop. It is what lets a test cancel a processor
    while a batch is in flight rather than between flushes.
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

    Three, matching the real ladder, so that "four attempts in all" stays the number the tests
    below spell out. They spell it out as a literal rather than reading `len(RETRY_DELAYS)` back:
    that name is patched right here, so an assertion computed from it would agree with any ladder
    at all — including the empty one, under which no retry happens and every test still passes.
    """
    monkeypatch.setattr(mqtt_processor, "RETRY_DELAYS", (0.0, 0.0, 0.0))


@pytest.fixture
def reimport():
    """Re-execute src.mqtt_processor with a chosen environment, and put it back afterwards.

    Reload rather than a call to the readers, because what needs pinning here is the WIRING: a
    module that assigned BATCH_SIZE from a literal would still pass a test of
    `batch_size_from_env()`, and a production container silently running 100/1.0 instead of the
    220/3.0 in docker-compose sends InfluxDB 2.2 times as many write requests with nothing on any
    screen to say so.

    Reload re-runs the module body in the module's EXISTING namespace, so every importer — the
    `from .mqtt_processor import StreamProcessor` in stream_manager, the names imported at the top
    of this file — keeps working against objects whose globals are the ones just rebound. The
    teardown reload restores the ambient environment's values for the tests that follow.
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

    # Four: the first attempt and one per rung of the ladder. A literal, because `instant_retries`
    # has replaced RETRY_DELAYS and an assertion derived from it would hold for any ladder length,
    # the empty one included.
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


# --- the stream's own precision, from streams.json to the batched point ---------------------------
#
# tests/test_influx_writer.py pins what each precision DOES. These two pin the wiring: that the
# number in streams.json is the number the rounding is given. Everything in between is a chain of
# defaults that each keep working on their own if a link comes loose — StreamConfig defaults to
# None, resolve_value_precision turns None into 2, flatten_payload defaults to 2 — so a processor
# that dropped the config entirely would round to two decimals and look completely healthy.

async def test_a_streams_configured_precision_reaches_the_points_it_batches(monkeypatch):
    """The kiln stream, end to end: what it sets is what lands in the batch.

    Deliberately checked through the batch and not through `to_number`, because every layer between
    them defaults to the same two decimals. If `StreamProcessor` ignored `cfg.value_precision`
    outright, every unit test of the rounding would still pass and this is the only thing that
    would go red.
    """
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
    """The same path with an untouched config — the shape every production stream is in today."""
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


# --- a cancellation landing in the middle of a write ----------------------------------------------
#
# The batch is taken out of `self._batch` atomically and only then written, which is what keeps two
# concurrent flushes from sending the same points twice. The cost is a window — the whole duration
# of the write, up to 10s of aiohttp timeout plus the ladder — in which the only reference to those
# points is a local variable. A cancellation arriving inside that window is not a corner case: it
# is every `docker stop`, and Portainer's updater recreates this container on every push of
# `:latest`. Nothing about the loss was visible: no error counted, no event emitted, no log line.

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
    # Path one: the flush in flight belongs to the message loop, fired because the batch filled.
    # The cancellation lands inside it, unwinds the message loop, and `_connect_and_process`'s
    # finally runs the final flush — which, before this, looked at a `self._batch` the cancelled
    # write had already emptied, found nothing, and returned. Every point published here has to end up either written or still in
    # the batch; anything else is a point that existed and then did not.
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
    # Path two, and the wider of the two: at BATCH_INTERVAL=3 against a slow InfluxDB the timer is
    # inside a write most of the time, so this is the likelier of the two to be true at the moment
    # SIGTERM arrives. It needs both halves of the fix — the timer has to hand its points back, AND
    # the shutdown has to await the cancelled timer before flushing, because cancel() only
    # schedules the cancellation and the hand-back happens on a later pass of the event loop.
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
    # The third of the cancellation paths and the one none of the others reach: both tests above
    # flush with `retry=False`, so `async with asyncio.timeout(RETRY_BUDGET)` in _write_with_retries
    # is never entered at all. In production it is entered on every flush the message loop and the
    # timer make — and it is the widest window of the three, because a stream that is retrying is a
    # stream whose InfluxDB is already unwell, which is exactly when a deploy tends to be happening.
    #
    # It is also the window with a second way to lose the batch. The budget's expiry arrives at the
    # same await, as a CancelledError raised by asyncio.timeout, and THAT one is meant to be caught
    # and turned into a dropped batch (test_the_retry_ladder_is_capped_in_wall_clock_time requires
    # it). Widening `except TimeoutError` to take the cancellation with it — the obvious tidy-up —
    # would make a shutdown mid-ladder look like an exhausted budget: the points are gone, an error
    # is counted, and the final flush finds an empty batch.
    #
    # RETRY_BUDGET is raised rather than left alone so that the two cannot be confused here: nothing
    # in this test can reach the budget, so the only thing that can empty the batch is the
    # cancellation.
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
    """`if not self._running: break` is a normal return, not a cancellation.

    It leaves the `async for`, `while self._running` is then false, and `_process_forever` returns —
    so while the final flush lived in a handler for `asyncio.CancelledError` this path walked past
    it with up to BATCH_SIZE points still in `self._batch`, and the timer's `finally` then handed
    the timer's own rescued points into the same batch with nothing left running to send them.

    Nothing reaches it through `stop()` today: it sets `_running` and cancels in the same run of
    statements with no await between them, so the task never observes the gap. The `break` is
    written for a soft stop all the same — a paused stream, a drain before a reload — and this test
    is what keeps that from being a silent loss of one batch per stream the day one is added.
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


# --- a failure InfluxDB will give again is not worth asking about again ---------------------------

async def test_an_answer_that_will_not_change_is_not_retried(instant_retries):
    # 400 on a body the line parser refuses, 401 on a rotated password, 404 on a dropped database.
    # The ladder against those is pure cost: up to 10s of aiohttp timeout plus the whole
    # RETRY_BUDGET per batch, spent by a message loop that is not draining the broker's queue,
    # against an answer that is guaranteed to be identical.
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
    # Two flushes overlap by design — the lock is released before the write — and they can finish
    # in the opposite order to the one they started in. Whoever finished last used to win, so a
    # slow failing flush landing after a fast successful one left `last_flush_ok` false and
    # `last_flush_count` describing a batch that had already been superseded. The data is fine;
    # what is wrong is the only indicator an operator has that the bridge is healthy.
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
    # The original code compared `now - last_flush` and reset `last_flush` on ANY flush, so the
    # interval always meant "this long since something was last written". A timer that sleeps
    # BATCH_INTERVAL unconditionally means "this long since I last woke up", and under load a tick
    # then lands milliseconds after a full batch went out and posts the one or two points that
    # arrived in between — an extra small write per stream per interval, forever.
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
# Every one of these was checked by mutation: emptying the ladder, raising the budget to an hour and
# unhooking either batching constant from the environment each left the whole suite green, because
# the only tests that touched them patched them first.

def test_the_retry_ladder_has_rungs_and_all_of_them_wait():
    # An empty tuple makes `_write_with_retries` a single attempt with a retry counter that can
    # never move: retries disappear in silence and every InfluxDB blip costs a batch again.
    assert len(mqtt_processor.RETRY_DELAYS) > 0
    assert all(delay > 0 for delay in mqtt_processor.RETRY_DELAYS)


def test_every_rung_of_the_ladder_is_reachable_inside_the_budget():
    # The budget cancels the retry phase mid-attempt. Sleeps that add up to more than it mean the
    # last rungs can never be reached no matter how fast InfluxDB answers — a ladder shorter than
    # it looks, and nothing anywhere would say so.
    assert sum(mqtt_processor.RETRY_DELAYS) < mqtt_processor.RETRY_BUDGET


def test_the_retry_budget_stays_within_one_attempts_own_timeout():
    # The budget is added on top of the first attempt's 10s aiohttp timeout, and for its whole
    # length this stream takes nothing out of aiomqtt's incoming queue. Keeping it at or under one
    # attempt's timeout keeps the worst case of a flush at roughly 2x a single write, instead of the
    # 43s blind window the unbounded ladder produced.
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
    # docker-compose sets 220 and 3.0; the defaults are 100 and 1.0. A build that stopped reading
    # either would run at more than twice the write rate against InfluxDB and look identical from
    # every screen the service has.
    reloaded = reimport(BATCH_SIZE="220", BATCH_INTERVAL="3.0")

    assert reloaded.BATCH_SIZE == 220
    assert reloaded.BATCH_INTERVAL == 3.0


def test_the_module_constants_fall_back_to_the_documented_defaults(reimport):
    reloaded = reimport(BATCH_SIZE=None, BATCH_INTERVAL=None)

    assert reloaded.BATCH_SIZE == 100
    assert reloaded.BATCH_INTERVAL == 1.0


# --- the incoming queue, and what a stalled consumer really costs ---------------------------------

async def test_the_client_is_built_with_a_bounded_incoming_queue(monkeypatch):
    # A flush retrying inside `async for message in client.messages` does not stop the socket being
    # read: aiomqtt's reader callback lives on the event loop, and `_on_message` keeps filling the
    # client's queue while this coroutine hangs on an await. Unbounded, a long InfluxDB outage is
    # therefore not silence but linear memory growth, and the container has no memory limit — the
    # OOM kill at the end of it drops the MQTT subscription, which is the loss that cannot be
    # undone. Bounded, the same outage discards the newest arrivals with a line in aiomqtt's log.
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
    # The keyword above is only worth anything if aiomqtt has it. `aiomqtt.Client(...)` opens no
    # socket — the queue is built in __init__, which is also why this test has to be async: the
    # constructor asks for the running loop. It is the only thing standing between a renamed or
    # misspelled parameter and a production client running on the unbounded default while a test
    # suite full of fakes that swallow **kwargs stays green.
    client = aiomqtt.Client(
        hostname="broker.invalid",
        max_queued_incoming_messages=mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES,
    )

    assert client._queue.maxsize == mqtt_processor.MAX_QUEUED_INCOMING_MESSAGES
