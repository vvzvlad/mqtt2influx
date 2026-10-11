#!/usr/bin/env python3
"""Shared fixtures: nothing writes to the real DATA_DIR and nothing opens a socket.

`data_dir` rebinds `src.config.DATA_DIR`, which `_config_path()` looks up on every call, so the CRUD
helpers `src.api` imported are redirected too. `manager_calls` replaces the manager's lifecycle
methods with recorders, so a test can assert that the API asked for a stream to start without one
starting. `is_running()` stays real: it answers from `manager._processors`, which the recorders
never populate.
"""

import pytest

from src import config
from src.settings import Settings
from src.stream_manager import manager


@pytest.fixture(autouse=True)
def no_dotenv(monkeypatch):
    """Keep a developer's own .env out of the suite.

    `env_file` is relative to the working directory, the repository root under `make test`, and
    src/mqtt_processor.py builds a fresh Settings() per call, so a BATCH_SIZE in .env would change
    what the default-checking tests see, while CI, with no .env, would pass.
    """
    monkeypatch.setitem(Settings.model_config, "env_file", None)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Point src.config at a throwaway directory for one test.

    Not created here: `_config_path()` creates it on first use, and tests check that.
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

    The recorders are opt-in, so a test without them can schedule a real MQTT connection that
    outlives it. Checked before and after, so the test that did it is the one that fails.
    """
    assert manager._processors == {}, "a previous test left a live stream processor behind"
    yield
    leaked = dict(manager._processors)
    manager._processors.clear()
    assert leaked == {}, "this test started a real stream processor: {}".format(sorted(leaked))
