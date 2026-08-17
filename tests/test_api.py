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

import pytest
from fastapi.testclient import TestClient

from src.api import app
from src.config import load_streams


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

    Entering the context manager is what triggers startup, and startup is where the app reads the
    config and starts every enabled stream. Skipping it would leave the most dangerous line in
    src/api.py untested by every test in this file.
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
    """The lifespan ran (the fixture entered the context manager) and found nothing to start."""
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

def test_stats_answers_with_a_list_when_nothing_is_running(client):
    """This endpoint is also the container's healthcheck target (see docker-compose.yml), so it has
    to answer 200 on a bridge with no streams configured at all — the state right after a deploy
    onto a fresh volume."""
    response = client.get("/api/stats")
    assert response.status_code == 200
    assert response.json() == []


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
    """The mount is on "/" and would swallow everything if it were registered first."""
    assert client.get("/api/streams").status_code == 200


# ── the websocket ─────────────────────────────────────────────────────────────────────────────────

def test_the_websocket_sends_a_snapshot_frame_immediately(client):
    """The UI has no polling fallback: if /ws does not deliver this first frame, the dashboard
    connects and then shows nothing at all."""
    with client.websocket_connect("/ws") as websocket:
        first = websocket.receive_json()

    assert first["type"] == "snapshot"
    assert first["data"] == []
