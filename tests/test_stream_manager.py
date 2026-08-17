#!/usr/bin/env python3
"""`StreamManager` — the object that owns every running processor and every websocket queue.

Two things live here. The lifecycle half (start/stop/restart/stop_all) is what the REST endpoints
call, and its invariants are all about not leaving a second processor behind: a stream whose task
outlives its dictionary entry keeps its MQTT subscription open, keeps writing points, and is
reachable by nothing in the UI. The fan-out half (`_broadcast` and the ws queues) is what puts
events on the dashboard, and its invariant is that one stalled browser tab must not cost the others
their feed.

`is_running()` is asked of the processor's task rather than of the dictionary, because the manager
never removes a processor whose task died on its own — `stop_stream()` is the only thing that pops
the dictionary — so "the key is there" and "the stream is running" are different questions and
GET /api/streams answers with the second one.

Every test here builds its own StreamManager rather than using the module singleton: the autouse
`no_live_processors` fixture in conftest.py fails any test that leaves a processor in the singleton,
and rightly so. The lifecycle tests additionally replace `StreamProcessor` with a double, because
the real one schedules a task that dials the broker named in the config.
"""

import asyncio
from contextlib import suppress

import pytest

from src import stream_manager
from src.config import StreamConfig
from src.mqtt_processor import StreamProcessor
from src.stream_manager import StreamManager


async def _noop_event(stream_id, event_type, data):
    pass


def _processor(stream_id="s1"):
    return StreamProcessor(StreamConfig(id=stream_id, name="t"), on_event=_noop_event)


# ── is_running: the dictionary is not the answer ──────────────────────────────────────────────────

async def test_is_running_is_false_for_an_id_the_manager_never_saw():
    assert StreamManager().is_running("no-such-stream") is False


async def test_is_running_is_true_while_the_processor_task_runs(monkeypatch):
    mgr = StreamManager()
    proc = _processor()
    running = asyncio.Event()

    async def fake_run():
        running.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(proc, "_run", fake_run)
    mgr._processors[proc.cfg.id] = proc
    proc.start()
    try:
        await running.wait()
        assert mgr.is_running(proc.cfg.id) is True
    finally:
        await proc.stop()


async def test_is_running_is_false_once_the_processor_task_has_died(monkeypatch):
    mgr = StreamManager()
    proc = _processor()

    async def fake_run():
        # What a processor does when the writer or the client blows up before the message loop.
        raise RuntimeError("died before entering the message loop")

    monkeypatch.setattr(proc, "_run", fake_run)
    mgr._processors[proc.cfg.id] = proc
    proc.start()
    with suppress(RuntimeError):
        await proc._task

    # The entry is still there, and nothing will ever remove it. That is the state in which the API
    # used to keep reporting running: true for a stream that had stopped writing points.
    assert proc.cfg.id in mgr._processors
    assert mgr.is_running(proc.cfg.id) is False


# ── the lifecycle, against a processor that opens nothing ─────────────────────────────────────────

class ProcessorLog:
    """What the manager did, in the order it did it.

    Order is the whole point of several tests below — "stopped the old processor" and "stopped the
    old processor before constructing its replacement" are different claims, and only a sequence can
    tell them apart.
    """

    def __init__(self):
        self.events = []     # ("new" | "start" | "stop", stream_id), in call order
        self.instances = []  # every double the manager constructed, in construction order


@pytest.fixture
def processors(monkeypatch):
    """Replace `StreamProcessor` with a recorder for the duration of one test.

    `StreamManager.start_stream` constructs the processor itself, so this is patched on the
    stream_manager module rather than injected: there is no seam to pass a factory through, and the
    real constructor is followed immediately by `.start()`, which schedules a task that connects to
    whatever `mqtt_host` says.
    """
    log = ProcessorLog()

    class _ProcessorDouble:
        def __init__(self, cfg, on_event):
            self.cfg = cfg
            self.on_event = on_event
            self.stats = {"id": cfg.id, "name": cfg.name}
            log.instances.append(self)
            log.events.append(("new", cfg.id))

        def start(self):
            log.events.append(("start", self.cfg.id))

        async def stop(self):
            log.events.append(("stop", self.cfg.id))

        def get_stats(self):
            return self.stats

    monkeypatch.setattr(stream_manager, "StreamProcessor", _ProcessorDouble)
    return log


def _cfg(stream_id="s1", **overrides):
    return StreamConfig(id=stream_id, name="stream " + stream_id, **overrides)


async def test_start_stream_builds_a_processor_for_the_config_starts_it_and_keeps_it(processors):
    """Registering without starting, or starting without registering, both look fine from the API.

    A processor that is constructed and registered but never started leaves `POST .../start`
    answering `{"ok": true}` for a stream that connects to nothing. One that is started but not
    registered runs with nobody holding a reference: `stop_stream` cannot find it, `stop_all` walks
    past it, and it keeps its MQTT subscription until the process ends.
    """
    mgr = StreamManager()
    cfg = _cfg()

    await mgr.start_stream(cfg)

    assert processors.events == [("new", "s1"), ("start", "s1")]
    assert mgr._processors == {"s1": processors.instances[0]}
    # The stored config is handed over whole, not rebuilt from defaults.
    assert processors.instances[0].cfg is cfg


async def test_the_processor_is_wired_to_the_managers_own_broadcast(processors):
    """The processor's `on_event` is the only path from a flush to a browser.

    Wire it to anything else — a stub, a logger, a fresh manager — and the bridge keeps working
    perfectly while the dashboard's live log stays empty forever.
    """
    mgr = StreamManager()

    await mgr.start_stream(_cfg())

    assert processors.instances[0].on_event == mgr._broadcast


async def test_starting_a_stream_that_is_already_running_stops_the_old_one_first(processors):
    """Without the guard the dictionary entry is simply overwritten — and the old task survives it.

    Nothing else holds a reference to the displaced processor, so nothing can ever stop it: it keeps
    its MQTT subscription and keeps writing the same points as its replacement, duplicating every
    series for as long as the process lives. `POST /api/streams/{id}/start` pressed twice in the UI
    is all it takes.
    """
    mgr = StreamManager()

    await mgr.start_stream(_cfg())
    await mgr.start_stream(_cfg())

    # The order matters as much as the fact: the stop lands before the replacement is constructed.
    assert processors.events == [
        ("new", "s1"), ("start", "s1"),
        ("stop", "s1"), ("new", "s1"), ("start", "s1"),
    ]
    assert len(processors.instances) == 2
    assert mgr._processors == {"s1": processors.instances[1]}


async def test_stop_stream_stops_the_processor_and_forgets_it(processors):
    """Forgetting it is what lets the same id be started again; stopping it is what closes the
    subscription. Doing one without the other leaves either a leaked task or an id that can never
    be restarted without a container restart."""
    mgr = StreamManager()
    await mgr.start_stream(_cfg())
    processors.events.clear()

    await mgr.stop_stream("s1")

    assert processors.events == [("stop", "s1")]
    assert mgr._processors == {}


async def test_stopping_a_stream_that_is_not_running_is_a_no_op(processors):
    """`POST /api/streams/{id}/stop` does not check first, and `DELETE` calls this unconditionally.

    Most streams in a config are disabled and were therefore never started, so deleting one is the
    ordinary case — not an edge case. A KeyError here would turn every such delete into a 500 with
    the record still on disk.
    """
    mgr = StreamManager()

    await mgr.stop_stream("never-started")

    assert processors.events == []
    assert mgr._processors == {}


async def test_restart_stream_takes_the_stream_down_and_brings_it_back_when_it_is_enabled(processors):
    """This is what `PUT /api/streams/{id}` calls, and an edit that does not restart is an edit that
    does not apply: the running processor holds its own copy of the config, so a changed topic or a
    changed broker address would sit on disk while the old connection carried on unchanged."""
    mgr = StreamManager()
    await mgr.start_stream(_cfg())
    processors.events.clear()

    await mgr.restart_stream(_cfg(enabled=True))

    assert processors.events == [("stop", "s1"), ("new", "s1"), ("start", "s1")]
    assert mgr._processors == {"s1": processors.instances[-1]}


async def test_restarting_a_disabled_stream_takes_it_down_and_leaves_it_down(processors):
    """Clearing the "enabled" checkbox and saving is the only way to switch a stream off.

    If `restart_stream` started it again regardless, the UI would show the stream disabled, the
    config on disk would say disabled, and the bridge would keep publishing to InfluxDB — with the
    restart having refreshed the credentials from a record its owner had just turned off.
    """
    mgr = StreamManager()
    await mgr.start_stream(_cfg(enabled=True))
    processors.events.clear()

    await mgr.restart_stream(_cfg(enabled=False))

    assert processors.events == [("stop", "s1")]
    assert mgr._processors == {}


async def test_restarting_a_stream_that_was_not_running_just_starts_it(processors):
    """A stream saved as disabled and then enabled through the UI has no processor to stop first."""
    mgr = StreamManager()

    await mgr.restart_stream(_cfg(enabled=True))

    assert processors.events == [("new", "s1"), ("start", "s1")]
    assert mgr._processors == {"s1": processors.instances[0]}


async def test_stop_all_stops_every_stream_and_empties_the_registry(processors):
    """Called from the lifespan's shutdown, which is the container's last chance to flush.

    Leaving its message loop is what makes a processor flush — `_connect_and_process` does it from a
    `finally`, without the retry ladder — and the cancellation `stop()` delivers is what makes it
    leave. So a stream this loop skips never flushes at all and loses whatever was in its batch, up
    to BATCH_SIZE points, on every deploy. The iteration is over `list(self._processors)` because
    `stop_stream` mutates the dictionary it would otherwise be walking.
    """
    mgr = StreamManager()
    for stream_id in ("a", "b", "c"):
        await mgr.start_stream(_cfg(stream_id))
    processors.events.clear()

    await mgr.stop_all()

    assert sorted(processors.events) == [("stop", "a"), ("stop", "b"), ("stop", "c")]
    assert mgr._processors == {}


async def test_stop_all_takes_every_stream_down_at_the_same_time(monkeypatch):
    """Serialised, the first stream in the dictionary spends the whole grace period on its own.

    Each `stop()` cancels a processor and waits for its final flush, and that flush can sit out
    aiohttp's 10s timeout against an InfluxDB that has stopped answering — which is precisely the
    situation in which the batches still in memory matter most. One after another, with docker's
    stop grace period ticking, the streams behind the first one are killed with their batches
    unwritten.

    The doubles here block until all three have entered `stop()`, so a serialised `stop_all` cannot
    finish at all: the first one waits for a signal only the third can send.

    WHERE THE BOUND LIVES IS THE TEST. The rendezvous used to be `wait_for(..., timeout=2)` inside
    the double, and that made the whole thing pass on a `for sid in ...: await self.stop_stream(sid)`
    that swallowed exceptions — the obvious "simplification", and one the neighbouring test about
    tolerating a failure invites: each stop simply waited out its own two seconds, the loop finished
    in six, and both assertions below held. A serialised implementation must not have an escape
    hatch, so the doubles now wait unbounded and the bound sits on the caller instead. Cancelled
    from there, a stop_all that cannot finish fails as a TimeoutError on this test's own line rather
    than by hanging the suite — there is no per-test timeout plugin in this project.
    """
    total = 3
    entered = []
    all_entered = asyncio.Event()

    class _BlockingProcessor:
        def __init__(self, cfg, on_event):
            self.cfg = cfg

        def start(self):
            pass

        async def stop(self):
            entered.append(self.cfg.id)
            if len(entered) == total:
                all_entered.set()
            await all_entered.wait()

    monkeypatch.setattr(stream_manager, "StreamProcessor", _BlockingProcessor)
    mgr = StreamManager()
    for stream_id in ("a", "b", "c"):
        await mgr.start_stream(_cfg(stream_id))

    await asyncio.wait_for(mgr.stop_all(), timeout=5)

    assert sorted(entered) == ["a", "b", "c"]
    assert mgr._processors == {}


async def test_one_stream_failing_on_the_way_down_does_not_keep_the_others_from_stopping(monkeypatch):
    """`stop_all` runs from the lifespan's shutdown and is the last thing that happens.

    A processor whose `stop()` raises — a writer session already closed, a task that died holding
    something — must not abort the shutdown of the streams gathered alongside it: they would be
    killed with their final flush never attempted. The registry has to end up empty either way,
    because `stop_stream` removes the entry before it stops anything and nothing else ever will.
    """
    stopped = []

    class _OneBadProcessor:
        def __init__(self, cfg, on_event):
            self.cfg = cfg

        def start(self):
            pass

        async def stop(self):
            await asyncio.sleep(0)
            if self.cfg.id == "b":
                raise RuntimeError("the writer's session was already closed")
            stopped.append(self.cfg.id)

    monkeypatch.setattr(stream_manager, "StreamProcessor", _OneBadProcessor)
    mgr = StreamManager()
    for stream_id in ("a", "b", "c"):
        await mgr.start_stream(_cfg(stream_id))

    await mgr.stop_all()  # must not raise

    assert sorted(stopped) == ["a", "c"]
    assert mgr._processors == {}


# ── stats ─────────────────────────────────────────────────────────────────────────────────────────

async def test_get_all_stats_reports_one_entry_per_running_stream(processors):
    """`GET /api/stats` and the websocket snapshot are both this list, unmodified.

    A manager that returned the processors themselves, or a list of ids, would fail at JSON
    serialisation; one that returned only the first, or deduplicated by name, would quietly hide
    streams from the only screen that shows whether they are writing.
    """
    mgr = StreamManager()
    await mgr.start_stream(_cfg("a"))
    await mgr.start_stream(_cfg("b"))
    processors.instances[0].stats = {"id": "a", "points_sent": 604_000_000}
    processors.instances[1].stats = {"id": "b", "points_sent": 17}

    assert sorted(mgr.get_all_stats(), key=lambda s: s["id"]) == [
        {"id": "a", "points_sent": 604_000_000},
        {"id": "b", "points_sent": 17},
    ]


async def test_get_stats_answers_for_one_stream_and_returns_an_empty_dict_for_an_unknown_one(processors):
    """The empty dict rather than None: callers index into the result, and None would be an
    AttributeError instead of an obviously-empty row."""
    mgr = StreamManager()
    await mgr.start_stream(_cfg("a"))
    processors.instances[0].stats = {"id": "a", "errors": 2}

    assert mgr.get_stats("a") == {"id": "a", "errors": 2}
    assert mgr.get_stats("no-such-stream") == {}


# ── the websocket fan-out ─────────────────────────────────────────────────────────────────────────

async def test_a_broadcast_reaches_every_connected_queue():
    """Every open dashboard has its own queue and all of them have to see the same event.

    Delivering to one — `next(iter(...))`, an early `return` inside the loop — is invisible with a
    single tab open and looks like a flaky UI with two.
    """
    mgr = StreamManager()
    first, second = asyncio.Queue(), asyncio.Queue()
    mgr.add_ws_queue(first)
    mgr.add_ws_queue(second)

    await mgr._broadcast("s1", "flush", {"count": 100, "status": "ok"})

    expected = {"stream_id": "s1", "type": "flush", "data": {"count": 100, "status": "ok"}}
    assert first.get_nowait() == expected
    assert second.get_nowait() == expected


async def test_a_removed_queue_stops_receiving():
    """`websocket_endpoint` removes its queue in a `finally`, and this is why it has to.

    A queue left behind after its socket closed is never drained again: it fills to its 500-message
    maximum and then sits there, one per closed tab, for the lifetime of the process.
    """
    mgr = StreamManager()
    q = asyncio.Queue()
    mgr.add_ws_queue(q)
    mgr.remove_ws_queue(q)

    await mgr._broadcast("s1", "flush", {"count": 1})

    assert q.empty()
    assert mgr._ws_queues == set()


async def test_a_client_that_stopped_reading_is_dropped_without_taking_the_others_with_it():
    """A stalled reader is the normal failure here — a laptop asleep with the dashboard open.

    Its queue fills, and from then on every broadcast raises QueueFull for it. Letting that
    exception escape would abort the fan-out partway through a set whose iteration order is
    arbitrary, so a healthy tab would stop receiving events because a different tab's owner shut
    their lid. Dropping the queue is also what keeps the set from growing without bound.
    """
    mgr = StreamManager()
    stalled = asyncio.Queue(maxsize=1)
    healthy = asyncio.Queue(maxsize=10)
    mgr.add_ws_queue(stalled)
    mgr.add_ws_queue(healthy)

    await mgr._broadcast("s1", "message", {"n": 1})  # both accept; `stalled` is now full
    await mgr._broadcast("s1", "message", {"n": 2})  # `stalled` overflows and is dropped

    assert mgr._ws_queues == {healthy}
    assert healthy.qsize() == 2
    assert stalled.qsize() == 1


async def test_a_queue_already_dropped_for_being_full_can_still_be_removed():
    """These two paths overlap in production, which is why removal has to be a `discard`.

    `_broadcast` drops an overflowing queue from the set on its own, and the websocket handler then
    removes the same queue again from its `finally` when the socket closes. `set.remove` would raise
    KeyError there — inside a finally, in a task nobody awaits, so the traceback goes to the log and
    the handler's remaining cleanup never runs.
    """
    mgr = StreamManager()
    q = asyncio.Queue(maxsize=1)
    mgr.add_ws_queue(q)
    await mgr._broadcast("s1", "message", {"n": 1})
    await mgr._broadcast("s1", "message", {"n": 2})
    assert mgr._ws_queues == set()

    mgr.remove_ws_queue(q)  # must not raise

    assert mgr._ws_queues == set()


async def test_broadcasting_with_nobody_connected_is_harmless():
    """The ordinary state of the service: nobody has the UI open, and the processors still emit an
    event per flush. This must not raise into `_flush`, which does not guard the call."""
    mgr = StreamManager()

    await mgr._broadcast("s1", "flush", {"count": 100, "status": "ok"})

    assert mgr._ws_queues == set()
