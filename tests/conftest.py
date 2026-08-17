#!/usr/bin/env python3
"""Shared fixtures.

Two things have to be true of every test in this suite, and both are arranged here rather than
repeated in each test file.

**Nothing writes to the real DATA_DIR.** `src/config.py` reads `DATA_DIR` from the environment once,
at import, into a module-level name, and `_config_path()` looks that name up on every call — so
rebinding `src.config.DATA_DIR` redirects every read and write, including the ones reached through
`src.api`, which imported the CRUD helpers by reference. Without the redirect the suite would default
to `/data`: the production path, and on a developer's machine a directory it may well be able to
create.

**Nothing opens a socket.** `src/stream_manager.py` exposes a module-level singleton `manager`, and
`src.api` holds a reference to that same object. `manager.start_stream()` constructs a
`StreamProcessor` and schedules a task that connects to the MQTT broker in the stream's config —
which in a test means a made-up hostname, a five-second retry loop and a suite whose runtime depends
on DNS. The `manager_calls` fixture replaces the three lifecycle methods with recorders, so a test
can assert that the API *asked* for a stream to be started without anything actually starting.

`is_running()` is deliberately left real: it answers from `manager._processors`, which the recorders
never populate, so the `running: false` that GET /api/streams reports is an observation about the
real manager rather than about a stub.
"""

import pytest

from src import config
from src.stream_manager import manager


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Point src.config at a throwaway directory for the duration of one test.

    The directory is deliberately NOT created here: `_config_path()` is supposed to create it on
    first use, and several tests below depend on that being true of a path that does not exist yet.
    """
    path = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", str(path))
    return path


class ManagerCalls:
    """Records what the API asked the stream manager to do, without doing any of it."""

    def __init__(self):
        self.started = []    # StreamConfig objects passed to start_stream()
        self.stopped = []    # stream ids passed to stop_stream()
        self.restarted = []  # StreamConfig objects passed to restart_stream()


@pytest.fixture
def manager_calls(monkeypatch):
    calls = ManagerCalls()

    async def start_stream(cfg):
        calls.started.append(cfg)

    async def stop_stream(stream_id):
        calls.stopped.append(stream_id)

    async def restart_stream(cfg):
        calls.restarted.append(cfg)

    async def stop_all():
        pass

    monkeypatch.setattr(manager, "start_stream", start_stream)
    monkeypatch.setattr(manager, "stop_stream", stop_stream)
    monkeypatch.setattr(manager, "restart_stream", restart_stream)
    monkeypatch.setattr(manager, "stop_all", stop_all)
    return calls


@pytest.fixture(autouse=True)
def no_live_processors():
    """Fail any test that left a real StreamProcessor behind.

    The recorders above are opt-in per test, so this is the backstop for the case they were not
    requested: a processor in the singleton means a real MQTT connection attempt was scheduled, and
    it would outlive the test that made it and leak into the next one. Checked before AND after, so
    the test that gets blamed is the one that did it.
    """
    assert manager._processors == {}, "a previous test left a live stream processor behind"
    yield
    leaked = dict(manager._processors)
    manager._processors.clear()
    assert leaked == {}, "this test started a real stream processor: {}".format(sorted(leaked))
