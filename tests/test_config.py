#!/usr/bin/env python3
"""Tests for src/config.py — the on-disk stream store.

This module is the whole persistence layer of the service. There is no database and there are no
required environment variables: everything the bridge knows about where to read MQTT from and where
to write InfluxDB to lives in one JSON file under DATA_DIR, and in production that file sits on a
docker volume that survives image updates. A bug here does not corrupt one request, it silently
empties the configuration of every stream.

Every test runs against a temporary DATA_DIR (see conftest.py) — nothing here touches /data.
"""

import json

from src.config import (
    StreamConfig,
    delete_stream,
    get_stream,
    load_streams,
    save_streams,
    upsert_stream,
)


def make_stream(**overrides):
    """A stream pointing at addresses that resolve nowhere. Never started by these tests."""
    values = dict(
        name="test stream",
        mqtt_host="mqtt.invalid",
        mqtt_port=1883,
        mqtt_user="mqtt-user",
        mqtt_password="mqtt-secret",
        mqtt_topic="/devices/#",
        topic_prefix="home",
        influx_host="influx.invalid",
        influx_port=8086,
        influx_user="influx-user",
        influx_password="influx-secret",
        influx_database="metrics",
        enabled=False,
    )
    values.update(overrides)
    return StreamConfig(**values)


# ── reading a store that is not there, or not readable ────────────────────────────────────────────

def test_load_streams_returns_empty_list_when_the_file_does_not_exist(data_dir):
    """A fresh volume is the normal first boot, not an error."""
    assert load_streams() == []


def test_load_streams_creates_the_data_directory_on_first_read(data_dir):
    """`_config_path()` mkdir -p's DATA_DIR, which is what makes an empty docker volume work."""
    assert not data_dir.exists()
    load_streams()
    assert data_dir.is_dir()


def test_load_streams_returns_empty_list_on_corrupt_json(data_dir):
    """Documented behaviour: a truncated or hand-mangled streams.json degrades to "no streams".

    This is the price of `except Exception: return []` in load_streams(). It keeps the API up when
    the file is unreadable, and it also means a damaged file is INVISIBLE from the outside — the UI
    shows an empty list, and the first save_streams() then overwrites the damaged file with `[]`.
    Worth knowing when a stream "disappears" in production.
    """
    data_dir.mkdir(parents=True)
    (data_dir / "streams.json").write_text('[{"id": "a", "name": "half a fi')
    assert load_streams() == []


def test_load_streams_returns_empty_list_when_a_record_has_an_unknown_field(data_dir):
    """Same swallowing, different cause, and the one most likely to bite during an upgrade.

    `StreamConfig(**s)` raises TypeError on a key it does not declare, and the blanket handler turns
    that into "no streams at all" — not "skip the one bad record". So rolling BACK to an image whose
    StreamConfig lacks a field a newer image wrote would drop every stream at once, including the
    records that were perfectly readable.
    """
    data_dir.mkdir(parents=True)
    (data_dir / "streams.json").write_text(json.dumps([{"id": "a", "field_from_the_future": 1}]))
    assert load_streams() == []


# ── value_precision: an optional field on a file that predates it ─────────────────────────────────
#
# `load_streams()` builds every record with `StreamConfig(**s)` under a blanket `except Exception:
# return []`, so the schema of this file is load-bearing in a way most JSON is not: one key too
# many is not a bad record, it is NO records, and the next save writes that emptiness back. Adding
# a field is therefore a compatibility event in both directions, and these are the tests for it.

def test_a_streams_json_written_before_the_field_existed_still_loads(data_dir):
    """The upgrade direction: the file already on the production volume has no such key.

    This is the whole config of a service that carries 134 million messages a year, and there is no
    migration step anywhere — the new image simply reads what the old one left. If a missing key
    were anything other than "use the default", the first boot after the update would come up with
    no streams and the first save would make that permanent.
    """
    data_dir.mkdir(parents=True)
    record = {
        "id": "wirenboard", "name": "wb", "mqtt_host": "10.0.0.1", "mqtt_port": 1883,
        "mqtt_user": "", "mqtt_password": "", "mqtt_topic": "/devices/#", "topic_prefix": "",
        "influx_host": "10.0.0.2", "influx_port": 8086, "influx_user": "", "influx_password": "",
        "influx_database": "metrics", "enabled": False,
    }
    (data_dir / "streams.json").write_text(json.dumps([record]))

    loaded = load_streams()

    assert len(loaded) == 1
    assert loaded[0].id == "wirenboard"
    assert loaded[0].influx_database == "metrics"
    # Unset, which resolve_value_precision() turns into the two decimals it has always used.
    assert loaded[0].value_precision is None


def test_a_streams_json_carrying_the_field_loads_it(data_dir):
    """The same file after someone sets a precision, read by the code that understands it."""
    data_dir.mkdir(parents=True)
    (data_dir / "streams.json").write_text(json.dumps([
        {"id": "kiln", "name": "ucontroller", "value_precision": -1},
        {"id": "five", "name": "five decimals", "value_precision": 5},
    ]))

    loaded = load_streams()

    assert [s.id for s in loaded] == ["kiln", "five"]
    assert loaded[0].value_precision == -1
    assert loaded[1].value_precision == 5


def test_a_stream_that_never_set_a_precision_writes_no_such_key(data_dir):
    """The rollback direction, and the reason `save_streams` does not just call `asdict`.

    An older image's StreamConfig has no `value_precision`, so `StreamConfig(**record)` would raise
    TypeError on it and `load_streams()` would answer `[]` — every stream gone, and gone for real as
    soon as anything saves. Since this file is rewritten in full on every edit, a default that
    serialised itself would put the key on records that never asked for it and turn a routine image
    rollback into a total config loss.

    Leaving it out keeps those records byte-for-byte what an older image already reads. A stream
    that DOES opt in still carries the key and still poisons the file for an older image — that one
    is the price of the feature and is written down in README, not something this test can fix.
    """
    upsert_stream(make_stream(name="untouched"))

    raw = json.loads((data_dir / "streams.json").read_text())

    assert len(raw) == 1
    assert "value_precision" not in raw[0]
    # And the rest of the record is exactly the shape it has always been.
    assert raw[0]["name"] == "untouched"
    assert raw[0]["influx_database"] == "metrics"


def test_a_configured_precision_is_written_and_survives_a_round_trip(data_dir):
    """Including -1, which is the value the kiln stream will actually carry."""
    for configured in (-1, 0, 5, 8):
        save_streams([make_stream(name="kiln", value_precision=configured)])

        raw = json.loads((data_dir / "streams.json").read_text())
        assert raw[0]["value_precision"] == configured
        assert load_streams()[0].value_precision == configured


def test_clearing_a_precision_removes_the_key_again(data_dir):
    """Setting one must not be a one-way door — the file has to be able to go back to its old shape.

    Otherwise a stream that was given a precision once could never return to a record an older
    image can read, even after the setting was undone.
    """
    stream = make_stream(name="kiln", value_precision=-1)
    upsert_stream(stream)
    assert "value_precision" in json.loads((data_dir / "streams.json").read_text())[0]

    stream.value_precision = None
    upsert_stream(stream)

    assert "value_precision" not in json.loads((data_dir / "streams.json").read_text())[0]
    assert load_streams()[0].value_precision is None


def test_the_field_defaults_to_unset_on_a_bare_stream_config():
    """A stream created through the API without mentioning precision must not opt itself in."""
    assert StreamConfig().value_precision is None
    assert make_stream().value_precision is None


# ── the CRUD cycle ────────────────────────────────────────────────────────────────────────────────

def test_create_read_update_delete_cycle(data_dir):
    stream = make_stream(name="first")

    # create
    upsert_stream(stream)
    assert [s.id for s in load_streams()] == [stream.id]

    # read, by id
    fetched = get_stream(stream.id)
    assert fetched is not None
    assert fetched.name == "first"
    assert fetched.mqtt_topic == "/devices/#"

    # update, by id: the same id must be replaced in place, not appended alongside
    fetched.name = "renamed"
    fetched.influx_database = "other-db"
    upsert_stream(fetched)
    after_update = load_streams()
    assert len(after_update) == 1
    assert after_update[0].id == stream.id
    assert after_update[0].name == "renamed"
    assert after_update[0].influx_database == "other-db"

    # delete
    assert delete_stream(stream.id) is True
    assert load_streams() == []
    assert get_stream(stream.id) is None


def test_upsert_replaces_in_place_and_keeps_the_order_of_the_others(data_dir):
    """Order is what the UI's stream list is drawn in, so an update must not reshuffle it."""
    first, second, third = make_stream(name="a"), make_stream(name="b"), make_stream(name="c")
    for stream in (first, second, third):
        upsert_stream(stream)

    second.name = "b renamed"
    upsert_stream(second)

    streams = load_streams()
    assert [s.id for s in streams] == [first.id, second.id, third.id]
    assert [s.name for s in streams] == ["a", "b renamed", "c"]


def test_delete_returns_false_for_an_unknown_id_and_leaves_the_store_alone(data_dir):
    """The API turns this False into a 404, so it has to be False and not a silent success."""
    stream = make_stream()
    upsert_stream(stream)

    assert delete_stream("no-such-id") is False
    assert [s.id for s in load_streams()] == [stream.id]


def test_delete_is_not_idempotent_and_reports_the_second_call_as_a_miss(data_dir):
    stream = make_stream()
    upsert_stream(stream)

    assert delete_stream(stream.id) is True
    assert delete_stream(stream.id) is False


def test_get_stream_returns_none_for_an_unknown_id(data_dir):
    upsert_stream(make_stream())
    assert get_stream("no-such-id") is None


# ── what actually lands on the volume ─────────────────────────────────────────────────────────────

def test_every_field_survives_a_save_and_load_round_trip(data_dir):
    """Field-by-field, because a dropped credential means a stream that connects to nothing.

    Compared as dicts rather than field by field on purpose: `asdict` covers fields added to
    StreamConfig later, which a hand-written list of assertions would not.
    """
    from dataclasses import asdict

    stream = make_stream(name="round trip", topic_prefix="prefix", enabled=True)
    save_streams([stream])

    loaded = load_streams()
    assert len(loaded) == 1
    assert asdict(loaded[0]) == asdict(stream)


def test_the_store_is_a_json_list_of_objects_at_data_dir_streams_json(data_dir):
    """The file's location and shape are a contract: ci/smoke.py reads it inside the container.

    Also the file a human edits by hand when the UI cannot be reached, so the layout is checked
    rather than left to whatever json.dump happened to produce.
    """
    stream = make_stream(name="on disk")
    upsert_stream(stream)

    path = data_dir / "streams.json"
    assert path.is_file()
    raw = json.loads(path.read_text())
    assert isinstance(raw, list)
    assert len(raw) == 1
    assert raw[0]["id"] == stream.id
    assert raw[0]["name"] == "on disk"
    assert raw[0]["enabled"] is False


def test_ids_are_generated_and_unique(data_dir):
    """Nothing supplies an id — the API strips any the client sends — so the default factory is it."""
    ids = {StreamConfig().id for _ in range(50)}
    assert len(ids) == 50
    assert all(ids)
