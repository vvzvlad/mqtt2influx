#!/usr/bin/env python3
"""Tests for src/api.py: the REST surface, the websocket and the static UI mount.

TestClient drives the real ASGI app in-process, lifespan included. conftest.py points DATA_DIR at a
temporary directory and replaces the manager's lifecycle methods with recorders. Streams are created
with `enabled: false`: creating an enabled stream, or starting up with one on disk, starts it.
"""

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from src import api
from src.api import app
from src.config import StreamConfig, load_streams, save_streams
from src.influx_writer import DEFAULT_VALUE_PRECISION, resolve_value_precision
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
    """A client whose lifespan runs: only `with TestClient(app)` triggers startup and shutdown.

    The data directory starts empty, so startup finds no streams; the startup tests below seed
    streams.json first and build their own client.
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
    """Startup with no config starts nothing.

    The negative half of `test_startup_starts_every_enabled_stream_it_finds_on_disk`.
    """
    assert manager_calls.started == []


# ── startup: what the lifespan does with the config already on disk ───────────────────────────────

def _seed_on_disk(**overrides):
    """Write one stream to streams.json with save_streams, the serialiser the service uses."""
    fields = dict(STREAM_BODY, id="stream-on-disk")
    fields.update(overrides)
    save_streams([StreamConfig(**fields)])


def test_startup_starts_every_enabled_stream_it_finds_on_disk(data_dir, manager_calls):
    """Startup starts every enabled stream it finds in streams.json.

    Nothing else re-reads the file, so after a container restart this is what reconnects the
    streams. Its own client, because the file has to exist before the lifespan reads it.
    """
    _seed_on_disk(enabled=True)

    with TestClient(app):
        # Asserted INSIDE the context manager: the app is up, before any request has been made, so
        # the only thing that could have asked for this stream is startup itself.
        assert [cfg.id for cfg in manager_calls.started] == ["stream-on-disk"]
        started = manager_calls.started[0]

    # The stored config is handed over whole, credentials included.
    assert started.mqtt_host == "mqtt.invalid"
    assert started.mqtt_user == "mqtt-user"
    assert started.mqtt_password == "mqtt-secret"
    assert started.mqtt_topic == "/devices/#"
    assert started.topic_prefix == "home"
    assert started.influx_host == "influx.invalid"
    assert started.influx_database == "metrics"
    assert started.enabled is True


def test_startup_leaves_a_disabled_stream_on_disk_alone(data_dir, manager_calls):
    """`enabled: false` is the only off switch a stream has, and it has to survive a restart."""
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
    """An unset precision comes back as null, not as the resolved 2.

    The UI prefills its form from this response, so a 2 here would be saved back as an explicit key
    that an older image cannot read.
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
    """A partial PUT carries the stored precision over.

    Stored as 8, not None: with None, a `body.get(k)` missing its fallback would pass too.
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


# ── value_precision: what the API refuses to store ────────────────────────────────────────────────
#
# `StreamConfig(**body)` checks no types, and resolve_value_precision() falls back to the default
# for a non-integer, so without the API check each value below would be stored and shown while the
# stream rounds to two decimals.

REJECTED_PRECISIONS = [
    "8",     # the number typed into what the operator took for a text field
    "2",     # ... including the one that spells the current default, which looks harmless
    2.0,     # what json.loads() makes of `2.0`; round(x, 2.0) is a TypeError
    True,    # bool is a subclass of int — the case the isinstance() ordering exists for
    False,
    [],
    {},
]
REJECTED_IDS = ["str-8", "str-2", "float", "true", "false", "list", "dict"]


@pytest.mark.parametrize("bad", REJECTED_PRECISIONS, ids=REJECTED_IDS)
def test_post_refuses_a_precision_that_is_not_an_integer(client, bad):
    response = client.post("/api/streams", json=dict(STREAM_BODY, value_precision=bad))

    assert response.status_code == 422, response.text
    assert "value_precision" in response.json()["detail"]
    # Refused BEFORE the write, not after it: nothing reached the file and nothing is listed.
    assert load_streams() == []
    assert client.get("/api/streams").json() == []


@pytest.mark.parametrize("bad", REJECTED_PRECISIONS, ids=REJECTED_IDS)
def test_put_refuses_a_precision_that_is_not_an_integer(client, manager_calls, bad):
    """A refused PUT leaves the stored precision alone and does not restart the stream."""
    created = create(client, value_precision=8)
    manager_calls.restarted.clear()

    response = client.put("/api/streams/{}".format(created["id"]), json={"value_precision": bad})

    assert response.status_code == 422, response.text
    assert "value_precision" in response.json()["detail"]
    # The stored stream is untouched — on disk and in what the API reports.
    assert load_streams()[0].value_precision == 8
    assert client.get("/api/streams").json()[0]["value_precision"] == 8
    # And the running stream was not restarted for a request that was refused.
    assert manager_calls.restarted == []


@pytest.mark.parametrize("method", ["post", "put"])
def test_a_precision_of_true_is_refused_and_not_read_as_one_decimal(client, method):
    """`true` is refused, not stored as a precision of one decimal.

    bool is a subclass of int, so `isinstance(True, int)` holds and `round(x, True)` rounds to one
    decimal; the bool check has to come before the int check.
    """
    if method == "post":
        response = client.post("/api/streams", json=dict(STREAM_BODY, value_precision=True))
        assert response.status_code == 422, response.text
        assert load_streams() == []
        stored = None
    else:
        created = create(client)
        response = client.put("/api/streams/{}".format(created["id"]), json={"value_precision": True})
        assert response.status_code == 422, response.text
        stored = load_streams()[0].value_precision

    # Nothing was stored, so the stream rounds to the default two decimals, not to one.
    assert stored is None
    assert resolve_value_precision(stored) == DEFAULT_VALUE_PRECISION == 2


@pytest.mark.parametrize("good", [8, 0, -1, None])
def test_post_accepts_every_integer_precision_and_the_explicit_null(client, good, data_dir):
    """The three configured states plus the null, each read back off disk rather than off the echo.

    `0` is in the list on purpose: it is a legitimate precision (round to whole numbers) and it is
    the value a validator written with a truthiness test would throw away.
    """
    created = create(client, value_precision=good)

    assert created["value_precision"] == good
    assert load_streams()[0].value_precision == good
    assert client.get("/api/streams").json()[0]["value_precision"] == good


def test_a_body_with_no_precision_key_at_all_is_accepted(client):
    """The commonest body there is — the UI sends the key, a curl one-liner does not."""
    body = {k: v for k, v in STREAM_BODY.items() if k != "value_precision"}
    assert "value_precision" not in body

    response = client.post("/api/streams", json=body)

    assert response.status_code == 200, response.text
    assert response.json()["value_precision"] is None
    assert load_streams()[0].value_precision is None


# ── write_strings: the flag that decides whether a status string is written at all ────────────────
#
# The processor reads this field with `bool(...)`, so any truthy non-bool, `"false"` included, would
# turn string mode on, and InfluxDB refuses whole batches on fields that already hold numbers.

def test_a_stream_created_without_the_flag_reports_it_as_off(client):
    """Absent means the default, and the default is what every existing stream does."""
    created = create(client)

    assert created["write_strings"] is False
    assert client.get("/api/streams").json()[0]["write_strings"] is False
    assert load_streams()[0].write_strings is False


def test_the_flag_round_trips_through_post(client):
    created = create(client, write_strings=True)

    assert created["write_strings"] is True
    assert load_streams()[0].write_strings is True
    assert client.get("/api/streams").json()[0]["write_strings"] is True


def test_put_turns_the_flag_on_and_it_reaches_the_config_file(client):
    """The operator's real path: an existing stream edited in the UI."""
    created = create(client)

    response = client.put("/api/streams/{}".format(created["id"]), json={"write_strings": True})

    assert response.status_code == 200, response.text
    assert response.json()["write_strings"] is True
    on_disk = load_streams()
    assert on_disk[0].write_strings is True
    # Not sent in the PUT body, so it has to have been carried over rather than reset.
    assert on_disk[0].influx_database == "metrics"


def test_put_can_turn_the_flag_off_again(client):
    created = create(client, write_strings=True)

    updated = client.put("/api/streams/{}".format(created["id"]),
                         json={"write_strings": False}).json()

    assert updated["write_strings"] is False
    assert load_streams()[0].write_strings is False


def test_a_put_that_does_not_mention_the_flag_leaves_it_alone(client):
    created = create(client, write_strings=True)

    updated = client.put("/api/streams/{}".format(created["id"]), json={"name": "renamed"}).json()

    assert updated["name"] == "renamed"
    assert updated["write_strings"] is True


REJECTED_WRITE_STRINGS = [
    "false",   # the spelling a shell script produces — and a truthy string in Python
    "true",
    1,         # int, not bool: `bool(1)` is True and nothing else would have said no
    0,
    None,      # unlike value_precision, there is no "unset" state to spell
    [],
    {},
]
REJECTED_WS_IDS = ["str-false", "str-true", "one", "zero", "null", "list", "dict"]


@pytest.mark.parametrize("bad", REJECTED_WRITE_STRINGS, ids=REJECTED_WS_IDS)
def test_post_refuses_a_write_strings_that_is_not_a_bool(client, bad):
    response = client.post("/api/streams", json=dict(STREAM_BODY, write_strings=bad))

    assert response.status_code == 422, response.text
    assert "write_strings" in response.json()["detail"]
    # Refused BEFORE the write, not after it: nothing reached the file and nothing is listed.
    assert load_streams() == []
    assert client.get("/api/streams").json() == []


@pytest.mark.parametrize("bad", REJECTED_WRITE_STRINGS, ids=REJECTED_WS_IDS)
def test_put_refuses_a_write_strings_that_is_not_a_bool(client, manager_calls, bad):
    """And leaves the stored stream and the running processor exactly as they were."""
    created = create(client, write_strings=True)
    manager_calls.restarted.clear()

    response = client.put("/api/streams/{}".format(created["id"]), json={"write_strings": bad})

    assert response.status_code == 422, response.text
    assert "write_strings" in response.json()["detail"]
    assert load_streams()[0].write_strings is True
    assert client.get("/api/streams").json()[0]["write_strings"] is True
    assert manager_calls.restarted == []


@pytest.mark.parametrize("method", ["post", "put"])
def test_the_string_false_is_refused_rather_than_read_as_a_yes(client, method):
    """The string "false" is refused rather than stored.

    `bool("false")` is True, so storing it would switch the stream into string mode.
    """
    if method == "post":
        response = client.post("/api/streams", json=dict(STREAM_BODY, write_strings="false"))
        assert response.status_code == 422, response.text
        assert load_streams() == []
    else:
        created = create(client)
        response = client.put("/api/streams/{}".format(created["id"]),
                              json={"write_strings": "false"})
        assert response.status_code == 422, response.text
        assert load_streams()[0].write_strings is False


def test_a_body_with_no_write_strings_key_at_all_is_accepted(client):
    """A body without the key is accepted, with the flag off."""
    # Asserted rather than filtered, so the test fails if STREAM_BODY ever gains the key.
    assert "write_strings" not in STREAM_BODY

    response = client.post("/api/streams", json=STREAM_BODY)

    assert response.status_code == 200, response.text
    assert response.json()["write_strings"] is False
    assert load_streams()[0].write_strings is False


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
    created = create(client, enabled=True)

    assert [cfg.id for cfg in manager_calls.started] == [created["id"]]
    assert manager_calls.started[0].mqtt_host == "mqtt.invalid"


def test_creating_a_disabled_stream_starts_nothing(client, manager_calls):
    """The negative half of the test above: `if stream.enabled` has to be a real branch."""
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
    """Occupies a slot in `manager._processors` and answers `get_stats()`. Nothing else."""

    def __init__(self, stats):
        self._stats = stats

    def get_stats(self):
        return self._stats


def test_stats_reports_the_numbers_the_running_processors_report(client):
    """/api/stats answers 200 with an empty list, then with what each processor reports.

    The empty case is what the compose healthcheck sees after a fresh deploy; the second half
    catches an endpoint that always returns [].
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
        # Cleared here: no_live_processors fails any test that leaves a processor behind.
        manager._processors.pop("stream-1", None)


# ── credentials in API responses ──────────────────────────────────────────────────────────────────

def test_the_api_returns_stream_credentials_in_the_clear(client):
    """Every response carrying a stream carries its MQTT and InfluxDB passwords, on purpose.

    The UI fills its edit form from GET /api/streams, so masked passwords would be blanked on the
    next save. The service is LAN-only and has no authentication.
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
    """A miss is a 404: `StaticFiles(html=True)` looks for a 404.html, not for index.html.

    A catch-all fallback would answer every mistyped API path with a 200 and a page of HTML.
    """
    response = client.get("/no-such-page")
    assert response.status_code == 404
    assert "<h1>mqtt2influx</h1>" not in response.text


def test_the_api_routes_win_over_the_static_mount_at_the_root(client):
    """Starlette matches in registration order, so the catch-all mount on "/" has to come last."""
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

    `receive_json()` has no timeout, so this deadline is what turns a missing background task into
    a failure instead of a hung suite.
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "still not true after {}s".format(timeout)
        await asyncio.sleep(0.01)


def test_the_websocket_delivers_events_broadcast_while_it_is_connected(client, monkeypatch):
    """An event broadcast after the snapshot reaches the client through `sender()`.

    `websocket.portal` runs the broadcast on the app's event loop: TestClient runs the app in a
    thread of its own, and `put_nowait` from this thread would not wake that loop.
    """
    # This connection's own queue: the manager is a singleton, and `_ws_queues` is shared.
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

    `stats_pusher()` waits two seconds between pushes. src.api looks `asyncio` up as a module
    global, so swapping it for this proxy shortens the wait without touching asyncio itself.
    """

    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    async def sleep(_delay):
        await asyncio.sleep(0.01)


def test_the_websocket_keeps_pushing_stats_after_the_snapshot(client, monkeypatch):
    """`stats_pusher()` keeps sending stats frames after the snapshot, even when no events arrive.

    The second `get_all_stats` call is the marker: the first is the snapshot sent inline.
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
