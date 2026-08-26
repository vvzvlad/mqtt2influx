#!/usr/bin/env python3
"""Tests for src/api.py — the REST surface, the websocket and the static UI mount.

Everything here runs through `fastapi.testclient.TestClient`, which drives the real ASGI app
in-process, including its lifespan. Two isolations make that safe and both come from conftest.py:
DATA_DIR points at a temporary directory, and the stream manager's lifecycle methods are recorders,
so no test opens a socket to an MQTT broker.

Streams are created with `enabled: false` throughout. That is not tidiness — `create_stream()` calls
`manager.start_stream()` the moment `enabled` is true, and the app's lifespan does the same for
every enabled stream it finds on disk at startup.
"""

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from src import api
from src.api import app
from src.config import StreamConfig, load_streams, save_streams
from src.stream_manager import manager


STREAM_BODY = {
    "name": "smoke stream",
    "mqtt_host": "mqtt.invalid",
    "mqtt_port": 1883,
    "mqtt_user": "mqtt-user",
    "mqtt_password": "mqtt-secret",
    "mqtt_topic": "/devices/#",
    "topic_prefix": "home",
    "influx_host": "influx.invalid",
    "influx_port": 8086,
    "influx_user": "influx-user",
    "influx_password": "influx-secret",
    "influx_database": "metrics",
    "enabled": False,
}


@pytest.fixture
def client(data_dir, manager_calls):
    """A client whose lifespan has really run — hence `with`, not a bare TestClient(app).

    Entering the context manager is what triggers startup and shutdown; a bare `TestClient(app)`
    runs neither, so `manager.stop_all()` would never be exercised by anything in this file.

    What this fixture does NOT cover is the startup branch that starts enabled streams: `data_dir`
    hands every test an empty directory, so `load_streams()` inside the lifespan always returns `[]`
    and the loop body never executes. That branch is covered by
    `test_startup_starts_every_enabled_stream_it_finds_on_disk` and its negative twin below, which
    seed streams.json BEFORE constructing their own client — the ordering is the whole point, and it
    is why they cannot use this fixture.
    """
    with TestClient(app) as test_client:
        yield test_client


def create(client, **overrides):
    body = dict(STREAM_BODY)
    body.update(overrides)
    response = client.post("/api/streams", json=body)
    assert response.status_code == 200, response.text
    return response.json()


# ── the empty state ───────────────────────────────────────────────────────────────────────────────

def test_streams_are_empty_on_a_fresh_data_dir(client):
    response = client.get("/api/streams")
    assert response.status_code == 200
    assert response.json() == []


def test_startup_starts_nothing_when_there_is_no_config(client, manager_calls):
    """The lifespan ran (the fixture entered the context manager) and found nothing to start.

    On its own this asserts almost nothing — an app that had no startup logic at all would pass it
    too. It earns its place only as the negative half of a pair whose positive half is
    `test_startup_starts_every_enabled_stream_it_finds_on_disk`: together they separate "started
    nothing because there was nothing to start" from "never starts anything".
    """
    assert manager_calls.started == []


# ── startup: what the lifespan does with the config already on disk ───────────────────────────────

def _seed_on_disk(**overrides):
    """Write one stream into streams.json the way a previous run of the service would have left it.

    Written through `save_streams` rather than by hand so that the file the lifespan reads back is
    produced by the same serialiser production uses — a test fixture that drifted from the real
    format would test the parser against a shape it never meets.
    """
    fields = dict(STREAM_BODY, id="stream-on-disk")
    fields.update(overrides)
    save_streams([StreamConfig(**fields)])


def test_startup_starts_every_enabled_stream_it_finds_on_disk(data_dir, manager_calls):
    """A restart has to bring the bridge back up by itself, and this is the only thing that does it.

    Nothing else in the service ever re-reads streams.json: `create_stream` starts a stream because
    the request said so, and `restart_stream` because a PUT did. After a container restart there is
    no request — the streams that were running are only rows in a file, and if this loop does not
    walk it nothing reconnects. MQTT does not buffer for an absent subscriber, so that is not a
    delayed bridge, it is data that never existed.

    This test constructs its own client instead of using the `client` fixture because the file has
    to exist before the lifespan reads it, and the fixture is entered before the test body runs.
    """
    _seed_on_disk(enabled=True)

    with TestClient(app):
        # Asserted INSIDE the context manager: the app is up, before any request has been made, so
        # the only thing that could have asked for this stream is startup itself.
        assert [cfg.id for cfg in manager_calls.started] == ["stream-on-disk"]
        started = manager_calls.started[0]

    # The stored config is handed over whole. Half of it is credentials, and a stream started with
    # a default-constructed config would connect anonymously to port 1883 of nowhere and look
    # "running" while writing nothing.
    assert started.mqtt_host == "mqtt.invalid"
    assert started.mqtt_user == "mqtt-user"
    assert started.mqtt_password == "mqtt-secret"
    assert started.mqtt_topic == "/devices/#"
    assert started.topic_prefix == "home"
    assert started.influx_host == "influx.invalid"
    assert started.influx_database == "metrics"
    assert started.enabled is True


def test_startup_leaves_a_disabled_stream_on_disk_alone(data_dir, manager_calls):
    """`enabled: false` is the only off switch a stream has, and it has to survive a restart.

    A stream is disabled precisely so that it stops dialling a broker — someone else's broker, in
    the common case, since these configs point at other people's installations. Starting it anyway
    on the next deploy would reconnect a bridge its owner had switched off, with credentials that
    may since have been revoked.
    """
    _seed_on_disk(enabled=False)

    with TestClient(app):
        assert manager_calls.started == []


# ── the CRUD cycle over HTTP ──────────────────────────────────────────────────────────────────────

def test_post_creates_a_stream_and_returns_a_generated_id(client):
    created = create(client)
    assert created["id"]
    assert created["name"] == "smoke stream"
    assert created["mqtt_topic"] == "/devices/#"
    assert created["enabled"] is False


def test_post_ignores_a_client_supplied_id(client):
    """`body.pop("id", None)`: ids are the server's to mint, so a client cannot overwrite a stream
    by POSTing the id of an existing one."""
    created = create(client, id="id-chosen-by-the-client")
    assert created["id"] != "id-chosen-by-the-client"


def test_post_ignores_fields_that_are_not_part_of_the_config(client):
    """An extra key must be dropped, not raise: the UI is free to grow a field before the API has."""
    created = create(client, not_a_real_field="whatever")
    assert "not_a_real_field" not in created


def test_get_lists_the_created_stream_and_reports_it_as_not_running(client):
    created = create(client)

    listed = client.get("/api/streams").json()
    assert len(listed) == 1
    assert listed[0]["id"] == created["id"]
    assert listed[0]["name"] == "smoke stream"
    # `running` comes from the REAL manager (conftest.py leaves is_running alone), so this is an
    # observation and not the echo of a stub: a stream created with enabled=false was never started.
    assert listed[0]["running"] is False


def test_post_persists_the_stream_to_the_config_file(client, data_dir):
    """Read back through src.config rather than through the API, which would only echo itself."""
    created = create(client)

    on_disk = load_streams()
    assert [s.id for s in on_disk] == [created["id"]]
    assert on_disk[0].influx_database == "metrics"


def test_put_updates_a_field_and_leaves_the_rest_alone(client):
    created = create(client)

    response = client.put("/api/streams/{}".format(created["id"]), json={"name": "renamed"})
    assert response.status_code == 200
    updated = response.json()
    assert updated["id"] == created["id"]
    assert updated["name"] == "renamed"
    # Not sent in the PUT body, so it has to have been carried over from the stored record.
    assert updated["influx_database"] == "metrics"
    assert updated["mqtt_password"] == "mqtt-secret"

    listed = client.get("/api/streams").json()
    assert listed[0]["name"] == "renamed"


def test_put_on_an_unknown_id_returns_404(client):
    response = client.put("/api/streams/no-such-id", json={"name": "x"})
    assert response.status_code == 404


def test_a_stream_created_without_a_precision_reports_it_as_unset(client):
    """The default has to come back as null and not as 2, or "unset" stops being distinguishable.

    The UI prefills its form from this response. If the server answered with the resolved 2, then
    opening and saving a stream would write an explicit `"value_precision": 2` into streams.json —
    turning every edit into an opt-in to a key that an older image cannot read.
    """
    created = create(client)

    assert created["value_precision"] is None
    assert client.get("/api/streams").json()[0]["value_precision"] is None


def test_put_sets_a_precision_and_it_reaches_the_config_file(client, data_dir):
    """The operator's real path for turning rounding off on one stream, read back off disk."""
    created = create(client)

    response = client.put("/api/streams/{}".format(created["id"]), json={"value_precision": -1})
    assert response.status_code == 200
    assert response.json()["value_precision"] == -1

    on_disk = load_streams()
    assert on_disk[0].value_precision == -1
    # Not sent in the PUT body, so it has to have been carried over rather than reset.
    assert on_disk[0].influx_database == "metrics"


def test_a_put_that_does_not_mention_the_precision_leaves_it_alone(client):
    """Every other field is carried over on a partial PUT, and this one is not special.

    Worth its own test because the carry-over reads `body.get(k, getattr(existing, k))`: a field
    whose stored value is None is exactly the case where a `body.get(k)` written without the
    fallback would look like it worked.
    """
    created = create(client, value_precision=8)

    updated = client.put("/api/streams/{}".format(created["id"]), json={"name": "renamed"}).json()

    assert updated["name"] == "renamed"
    assert updated["value_precision"] == 8


def test_put_can_clear_a_precision_back_to_the_default(client, data_dir):
    """An explicit null is how the UI sends a cleared box, and it has to mean "unset" again."""
    created = create(client, value_precision=-1)

    path = "/api/streams/{}".format(created["id"])
    updated = client.put(path, json={"value_precision": None}).json()

    assert updated["value_precision"] is None
    assert load_streams()[0].value_precision is None


def test_delete_removes_the_stream(client):
    created = create(client)

    response = client.delete("/api/streams/{}".format(created["id"]))
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert client.get("/api/streams").json() == []


def test_deleting_the_same_stream_twice_returns_404(client):
    created = create(client)

    assert client.delete("/api/streams/{}".format(created["id"])).status_code == 200
    assert client.delete("/api/streams/{}".format(created["id"])).status_code == 404


# ── what the API asks the stream manager to do ────────────────────────────────────────────────────

def test_creating_an_enabled_stream_asks_the_manager_to_start_it(client, manager_calls):
    """The one place `enabled: true` is exercised, and only against a recorder.

    With the real manager this call constructs a StreamProcessor and schedules a task that connects
    to mqtt.invalid, retrying every five seconds for as long as the test session lives.
    """
    created = create(client, enabled=True)

    assert [cfg.id for cfg in manager_calls.started] == [created["id"]]
    assert manager_calls.started[0].mqtt_host == "mqtt.invalid"


def test_creating_a_disabled_stream_starts_nothing(client, manager_calls):
    """The negative half of the pair above: `if stream.enabled` has to be a real branch.

    Alone this test is satisfied by an API that never starts anything; paired with
    `test_creating_an_enabled_stream_asks_the_manager_to_start_it` it pins the condition, because no
    single implementation of that `if` passes both unless it reads `enabled`. It matters because the
    UI's "add stream" form defaults the checkbox off precisely so that a half-filled config can be
    saved without dialling the broker it names.
    """
    create(client, enabled=False)
    assert manager_calls.started == []


def test_put_restarts_the_stream_so_the_new_config_takes_effect(client, manager_calls):
    """Editing a running stream must not leave the old connection in place with the old settings."""
    created = create(client)

    client.put("/api/streams/{}".format(created["id"]), json={"mqtt_topic": "/other/#"})

    assert [cfg.id for cfg in manager_calls.restarted] == [created["id"]]
    assert manager_calls.restarted[0].mqtt_topic == "/other/#"


def test_delete_stops_the_stream_before_removing_it(client, manager_calls):
    """Order matters: dropping the record while the processor still ran would leave a stream
    writing to InfluxDB that nothing in the UI can see, let alone stop."""
    created = create(client)

    client.delete("/api/streams/{}".format(created["id"]))

    assert manager_calls.stopped == [created["id"]]


def test_start_and_stop_endpoints_drive_the_manager(client, manager_calls):
    created = create(client)

    assert client.post("/api/streams/{}/start".format(created["id"])).status_code == 200
    assert [cfg.id for cfg in manager_calls.started] == [created["id"]]

    assert client.post("/api/streams/{}/stop".format(created["id"])).status_code == 200
    assert manager_calls.stopped == [created["id"]]


def test_starting_an_unknown_stream_returns_404_and_starts_nothing(client, manager_calls):
    response = client.post("/api/streams/no-such-id/start")
    assert response.status_code == 404
    assert manager_calls.started == []


# ── stats ─────────────────────────────────────────────────────────────────────────────────────────

class _StatsDouble:
    """Occupies a slot in `manager._processors` and answers `get_stats()`. Nothing else.

    A real StreamProcessor cannot stand in here: constructing one is harmless, but `manager` is a
    singleton shared with the app, and the autouse `no_live_processors` fixture exists because a
    processor left in it means a scheduled MQTT connection outliving the test that made it.
    """

    def __init__(self, stats):
        self._stats = stats

    def get_stats(self):
        return self._stats


def test_stats_reports_the_numbers_the_running_processors_report(client):
    """/api/stats is the only window onto whether points are actually reaching InfluxDB.

    The endpoint is also the container's healthcheck target (see docker-compose.yml), which is why
    the empty case has to be a 200 and not a 404 — that is the state right after a deploy onto a
    fresh volume, and a healthcheck failing there restarts the container forever.

    The second half is what makes this test more than a smoke check. An endpoint hardcoded to
    `return []` satisfies the empty case perfectly, and would leave the UI showing a permanently
    idle bridge — every counter zero, every stream apparently dead — while the bridge ran fine. So
    the numbers a processor reports have to be observed coming back out of the response body.
    """
    assert client.get("/api/stats").json() == []

    stats = {
        "id": "stream-1",
        "name": "boiler room",
        "msgs_received": 612_000_000,
        "points_sent": 604_000_000,
        "errors": 3,
        "last_flush_ok": True,
    }
    manager._processors["stream-1"] = _StatsDouble(stats)
    try:
        response = client.get("/api/stats")
        assert response.status_code == 200
        assert response.json() == [stats]
    finally:
        # Cleared here rather than left to `no_live_processors`: that fixture is a tripwire for
        # tests that leak a processor, and a test tripping it deliberately would train the next
        # reader to ignore it.
        manager._processors.pop("stream-1", None)


# ── credentials in API responses: a decision, not an accident ─────────────────────────────────────

def test_the_api_returns_stream_credentials_in_the_clear(client):
    """Every response carrying a stream carries its MQTT and InfluxDB passwords, on purpose.

    This is the owner's decision, recorded here so that the next person to read a response body does
    not file it as an oversight and "fix" it. The service is LAN-only: docker-compose publishes it
    as `8111:8000`, there is no Traefik label, no TLS and no authentication anywhere in `src/api.py`
    — deliberately, because there is nothing here to authenticate against and a login screen on a
    single-user bridge is friction without a threat model behind it.

    The passwords are in the responses because the UI needs them: `static/index.html` fills the edit
    form from `GET /api/streams` and puts `f.mqtt_password` straight into the password input, so a
    stream edited through the UI would otherwise have its credentials blanked on the first save.

    If that policy ever changes — the service goes on a network it does not own, or gains a login —
    this test goes red first, and it is the reminder that masking the field is not a one-line change
    to the API. Two other places would have to move with it: `static/index.html` (lines 440 and 446)
    stops being able to refill the form, and `ci/smoke.py` compares the POST echo against every key
    of its own STREAM_BODY (line 548), passwords included, so the gate would fail the build.
    """
    created = create(client)
    assert created["mqtt_password"] == "mqtt-secret"
    assert created["influx_password"] == "influx-secret"

    listed = client.get("/api/streams").json()
    assert listed[0]["mqtt_password"] == "mqtt-secret"
    assert listed[0]["influx_password"] == "influx-secret"

    updated = client.put("/api/streams/{}".format(created["id"]), json={"name": "renamed"}).json()
    assert updated["mqtt_password"] == "mqtt-secret"
    assert updated["influx_password"] == "influx-secret"


# ── the static UI ─────────────────────────────────────────────────────────────────────────────────

def test_the_index_page_is_served_at_the_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "<h1>mqtt2influx</h1>" in response.text


def test_an_unknown_path_is_404_and_not_the_index_page(client):
    """`StaticFiles(html=True)` does NOT fall back to index.html on a miss — it looks for a
    `404.html`, does not find one in static/, and raises 404. Asserted rather than assumed, because
    the opposite (an SPA-style catch-all) is what most people expect of `html=True`, and a fallback
    would turn every mistyped API path into a 200 carrying a page of HTML."""
    response = client.get("/no-such-page")
    assert response.status_code == 404
    assert "<h1>mqtt2influx</h1>" not in response.text


def test_the_api_routes_win_over_the_static_mount_at_the_root(client):
    """The mount is on "/" and would swallow every API path if it were registered before them.

    Starlette matches routes in registration order and a Mount at "/" matches everything, so the
    `app.mount(...)` at the bottom of src/api.py is load-bearing *because* it is at the bottom.
    Move it above the decorated routes and `GET /api/streams` becomes a lookup for a file named
    `api/streams` in static/ — a 404, with no error anywhere pointing at the reason.

    Asserted structurally as well as behaviourally. Every other test in this file would also go red
    on a reordering, so a bare status check here adds nothing they do not already say; what this one
    contributes is naming the cause, and pinning the ordering as a property of `app.routes` rather
    than as a side effect noticed downstream.
    """
    response = client.get("/api/streams")
    assert response.status_code == 200
    # The API answers as the API, not as a file that happened to be found under static/.
    assert response.headers["content-type"].startswith("application/json")

    # Starlette rewrites a Mount at "/" to the empty prefix, so the mount is identified by its name
    # rather than by a path string that no longer looks like the one src/api.py passed.
    mounted_at = [i for i, route in enumerate(app.routes) if getattr(route, "name", None) == "static"]
    api_routes = [i for i, route in enumerate(app.routes) if getattr(route, "path", "").startswith("/api/")]
    assert len(mounted_at) == 1, "the static catch-all should be mounted exactly once"
    assert api_routes, "no /api routes registered at all"
    # Last, not merely "after the API routes": anything appended below the mount in src/api.py would
    # be shadowed by it, and the failure mode is a route that silently 404s.
    assert mounted_at[0] == len(app.routes) - 1
    assert max(api_routes) < mounted_at[0]


# ── the websocket ─────────────────────────────────────────────────────────────────────────────────

def test_the_websocket_sends_a_snapshot_frame_immediately(client):
    """The UI has no polling fallback: if /ws does not deliver this first frame, the dashboard
    connects and then shows nothing at all."""
    with client.websocket_connect("/ws") as websocket:
        first = websocket.receive_json()

    assert first["type"] == "snapshot"
    assert first["data"] == []


async def _wait_until(predicate, timeout=5.0):
    """Poll `predicate` on the app's own event loop until it holds, or fail the test.

    Every wait in the websocket tests below has a deadline for the same reason: the frames come from
    background tasks, and the failure mode of a task that was never created is silence.
    `receive_json()` blocks with no timeout of its own, so without a deadline somewhere a broken
    `sender()` or `stats_pusher()` would hang the whole run instead of reddening one test — and a
    suite that hangs teaches people to kill it rather than to read it.
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "still not true after {}s".format(timeout)
        await asyncio.sleep(0.01)


def test_the_websocket_delivers_events_broadcast_while_it_is_connected(client, monkeypatch):
    """The snapshot is one frame; everything after it comes through `sender()`, and it is the log.

    A processor calls `on_event` per flush, per message and per error, `_broadcast` fans those out
    into the per-connection queue, and `sender()` is the only thing that ever takes them out again.
    Drop that task and the dashboard still connects, still paints its initial numbers and then sits
    frozen — no live log, no flush counter moving — with no error on either side to say why.

    `websocket.portal` is what runs a coroutine on the event loop the ASGI app is running on;
    TestClient drives the app from a thread of its own, and `Queue.put_nowait` called from this
    thread would resolve a future belonging to that loop without waking it, so the frame would
    arrive by luck rather than by design.
    """
    # This connection's own queue, not `manager._ws_queues` at large: the manager is a singleton and
    # the set is shared, so a predicate written over all of it would be answering a question about
    # whatever previous tests happened to leave behind.
    connection_queues = []
    real_add_ws_queue = manager.add_ws_queue

    def capture(q):
        connection_queues.append(q)
        real_add_ws_queue(q)

    monkeypatch.setattr(manager, "add_ws_queue", capture)

    with client.websocket_connect("/ws") as websocket:
        assert websocket.receive_json()["type"] == "snapshot"
        assert len(connection_queues) == 1

        websocket.portal.call(
            manager._broadcast, "stream-1", "message", {"topic": "home/temp", "value": 21.5})
        # The queue draining is `sender()` doing its job — the step actually under test.
        websocket.portal.call(_wait_until, connection_queues[0].empty)

        event = websocket.receive_json()

    assert event == {
        "stream_id": "stream-1",
        "type": "message",
        "data": {"topic": "home/temp", "value": 21.5},
    }


class _ImpatientAsyncio:
    """The real asyncio module with `sleep` shortened, and nothing else changed.

    `stats_pusher()` waits two seconds between pushes. Waiting them out for real would put four
    times the entire suite's runtime into a single test, and a suite that slow is one people start
    running less often. src.api resolves `asyncio.sleep` through its module global at call time, so
    swapping that global for this proxy shortens the interval without touching the asyncio module
    itself; every other attribute the endpoint reaches for — Queue, create_task — is forwarded to
    the real one, so the code under test is otherwise unmodified.
    """

    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    async def sleep(_delay):
        await asyncio.sleep(0.01)


def test_the_websocket_keeps_pushing_stats_after_the_snapshot(client, monkeypatch):
    """The counters on the dashboard are refreshed by this task and by nothing else.

    `sender()` only ever forwards events, and an event is emitted per flush — so on a stream that is
    connected but quiet, or one whose broker went silent, no event arrives and the last snapshot
    would stay on screen forever. `stats_pusher()` is what makes "0 msgs in the last minute" visible
    as a change rather than as an absence.

    The second call to `get_all_stats` is the marker waited for: the first one is the snapshot the
    endpoint sends inline, so a second can only have come from the pusher's own loop.
    """
    monkeypatch.setattr(api, "asyncio", _ImpatientAsyncio())
    real_get_all_stats = manager.get_all_stats
    calls = []

    def counting_get_all_stats():
        calls.append(1)
        return real_get_all_stats()

    monkeypatch.setattr(manager, "get_all_stats", counting_get_all_stats)

    with client.websocket_connect("/ws") as websocket:
        assert websocket.receive_json()["type"] == "snapshot"
        websocket.portal.call(_wait_until, lambda: len(calls) >= 2)
        pushed = websocket.receive_json()

    assert pushed["type"] == "stats"
    assert pushed["data"] == []
