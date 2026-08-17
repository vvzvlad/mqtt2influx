#!/usr/bin/env python3
"""`StreamManager.is_running()`.

The manager never removes a processor whose task died on its own — `stop_stream()` is the only thing
that pops the dictionary — so "the key is there" and "the stream is running" are different questions
and GET /api/streams answers with the second one.

Every test here builds its own StreamManager rather than using the module singleton: the autouse
`no_live_processors` fixture in conftest.py fails any test that leaves a processor in the singleton,
and rightly so.
"""

import asyncio
from contextlib import suppress

from src.config import StreamConfig
from src.mqtt_processor import StreamProcessor
from src.stream_manager import StreamManager


async def _noop_event(stream_id, event_type, data):
    pass


def _processor(stream_id="s1"):
    return StreamProcessor(StreamConfig(id=stream_id, name="t"), on_event=_noop_event)


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
