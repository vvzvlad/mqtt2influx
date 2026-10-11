#!/usr/bin/env python3
"""`StreamManager`: the running processors and the websocket queues.

Every test builds its own StreamManager: conftest.py's `no_live_processors` fails a test that
leaves a processor in the singleton. The lifecycle tests replace `StreamProcessor` with a double,
because the real one dials the broker named in the config.
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

    # The entry is still there, and nothing will remove it.
    assert proc.cfg.id in mgr._processors
    assert mgr.is_running(proc.cfg.id) is False


# ── the lifecycle, against a processor that opens nothing ─────────────────────────────────────────

class ProcessorLog:
    """What the manager did, in order: several tests assert the sequence, not just the calls."""

    def __init__(self):
        self.events = []     # ("new" | "start" | "stop", stream_id), in call order
        self.instances = []  # every double the manager constructed, in construction order


@pytest.fixture
def processors(monkeypatch):
    """Replace `StreamProcessor` with a recorder for one test.

    Patched on the stream_manager module: `start_stream` constructs the processor itself, and the
    real one's `.start()` connects to whatever `mqtt_host` says.
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
    """start_stream builds a processor for the config, registers it and starts it.

    Registered but not started, the stream connects to nothing; started but not registered, nothing
    can stop it.
    """
    mgr = StreamManager()
    cfg = _cfg()

    await mgr.start_stream(cfg)

    assert processors.events == [("new", "s1"), ("start", "s1")]
    assert mgr._processors == {"s1": processors.instances[0]}
    # The stored config is handed over whole, not rebuilt from defaults.
    assert processors.instances[0].cfg is cfg


async def test_the_processor_is_wired_to_the_managers_own_broadcast(processors):
    """The processor's `on_event` is the manager's `_broadcast`, the only path to a browser."""
    mgr = StreamManager()

    await mgr.start_stream(_cfg())

    assert processors.instances[0].on_event == mgr._broadcast


async def test_starting_a_stream_that_is_already_running_stops_the_old_one_first(processors):
    """Starting a running stream stops the old processor before building the new one.

    Merely overwritten, the old processor keeps its subscription, duplicates every point, and has
    no reference left to stop it by.
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
    """Stopping closes the subscription; forgetting lets the same id start again."""
    mgr = StreamManager()
    await mgr.start_stream(_cfg())
    processors.events.clear()

    await mgr.stop_stream("s1")

    assert processors.events == [("stop", "s1")]
    assert mgr._processors == {}


async def test_stopping_a_stream_that_is_not_running_is_a_no_op(processors):
    """`DELETE` calls stop_stream unconditionally, and a disabled stream was never started.

    A KeyError here would turn each such delete into a 500 with the record still on disk.
    """
    mgr = StreamManager()

    await mgr.stop_stream("never-started")

    assert processors.events == []
    assert mgr._processors == {}


async def test_restart_stream_takes_the_stream_down_and_brings_it_back_when_it_is_enabled(processors):
    """`PUT /api/streams/{id}` calls this: the running processor holds its own copy of the config,
    so an edit applies only through a restart."""
    mgr = StreamManager()
    await mgr.start_stream(_cfg())
    processors.events.clear()

    await mgr.restart_stream(_cfg(enabled=True))

    assert processors.events == [("stop", "s1"), ("new", "s1"), ("start", "s1")]
    assert mgr._processors == {"s1": processors.instances[-1]}


async def test_restarting_a_disabled_stream_takes_it_down_and_leaves_it_down(processors):
    """Clearing "enabled" and saving switches a stream off, so a restart leaves it down."""
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
    """stop_all stops every stream: the stop is what sends a processor's final batch.

    It iterates over a copy, because `stop_stream` mutates the dictionary.
    """
    mgr = StreamManager()
    for stream_id in ("a", "b", "c"):
        await mgr.start_stream(_cfg(stream_id))
    processors.events.clear()

    await mgr.stop_all()

    assert sorted(processors.events) == [("stop", "a"), ("stop", "b"), ("stop", "c")]
    assert mgr._processors == {}


async def test_stop_all_takes_every_stream_down_at_the_same_time(monkeypatch):
    """stop_all stops the streams concurrently, not one after another.

    Each double blocks until all three have entered stop(), so a serialised stop_all cannot finish.
    The doubles wait unbounded and the bound sits on the caller: a bound inside them would let a
    serialised loop pass slowly, and with none at all the suite would hang.
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
    """A stop() that raises does not keep the other streams from stopping.

    They would be killed with their final flush never attempted. The registry ends up empty either
    way, because `stop_stream` removes the entry before it stops anything.
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
    """`GET /api/stats` and the websocket snapshot are this list: one stats dict per stream."""
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
    """Every open dashboard has its own queue, and every queue gets the event."""
    mgr = StreamManager()
    first, second = asyncio.Queue(), asyncio.Queue()
    mgr.add_ws_queue(first)
    mgr.add_ws_queue(second)

    await mgr._broadcast("s1", "flush", {"count": 100, "status": "ok"})

    expected = {"stream_id": "s1", "type": "flush", "data": {"count": 100, "status": "ok"}}
    assert first.get_nowait() == expected
    assert second.get_nowait() == expected


async def test_a_removed_queue_stops_receiving():
    """A removed queue receives nothing more.

    `websocket_endpoint` removes its queue in a `finally`.
    """
    mgr = StreamManager()
    q = asyncio.Queue()
    mgr.add_ws_queue(q)
    mgr.remove_ws_queue(q)

    await mgr._broadcast("s1", "flush", {"count": 1})

    assert q.empty()
    assert mgr._ws_queues == set()


async def test_a_client_that_stopped_reading_is_dropped_without_taking_the_others_with_it():
    """A full queue is dropped, and the broadcast still reaches the others.

    A stalled reader (a laptop asleep with the dashboard open) is the normal failure here; letting
    QueueFull escape would stop the fan-out partway through the set.
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
    """A queue `_broadcast` already dropped can still be removed by the websocket handler.

    Removal is a `discard`: `set.remove` would raise KeyError out of the handler's `finally`.
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
