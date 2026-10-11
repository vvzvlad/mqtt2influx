#!/usr/bin/env python3
"""Tests for src/config.py, the stream store.

One JSON file under DATA_DIR holds the whole configuration, so a bug here empties every stream.
Every test runs against a temporary DATA_DIR (see conftest.py).
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
    """A truncated or hand-mangled streams.json reads as "no streams".

    The price of `except Exception: return []`: the API stays up, the damage is invisible, and the
    next save overwrites the file with `[]`.
    """
    data_dir.mkdir(parents=True)
    (data_dir / "streams.json").write_text('[{"id": "a", "name": "half a fi')
    assert load_streams() == []


def test_load_streams_returns_empty_list_when_a_record_has_an_unknown_field(data_dir):
    """An undeclared key empties the whole list, not just its record.

    `StreamConfig(**s)` raises TypeError and the blanket handler answers `[]`; this is what a
    rollback to an image without a field a newer image wrote runs into.
    """
    data_dir.mkdir(parents=True)
    (data_dir / "streams.json").write_text(json.dumps([{"id": "a", "field_from_the_future": 1}]))
    assert load_streams() == []


# ── value_precision: an optional field on a file that predates it ─────────────────────────────────
#
# One undeclared key empties the whole file (see above), so adding a field is a compatibility event
# in both directions: an older file read by this image, and this image's file read by an older one.

def test_a_streams_json_written_before_the_field_existed_still_loads(data_dir):
    """The upgrade direction: a file without the key loads, with the field unset.

    There is no migration step; the new image reads what the old one left.
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
    # Unset: resolve_value_precision() turns it into the default two decimals.
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
    """The rollback direction: an unset precision is not written.

    The file is rewritten in full on every edit, and an older image reads a record carrying
    `value_precision` as no streams at all. A stream that sets one still carries it (see README).
    """
    upsert_stream(make_stream(name="untouched"))

    raw = json.loads((data_dir / "streams.json").read_text())

    assert len(raw) == 1
    assert "value_precision" not in raw[0]
    # The rest of the record is still written.
    assert raw[0]["name"] == "untouched"
    assert raw[0]["influx_database"] == "metrics"


def test_a_configured_precision_is_written_and_survives_a_round_trip(data_dir):
    """Including -1, the canonical "do not round"."""
    for configured in (-1, 0, 5, 8):
        save_streams([make_stream(name="kiln", value_precision=configured)])

        raw = json.loads((data_dir / "streams.json").read_text())
        assert raw[0]["value_precision"] == configured
        assert load_streams()[0].value_precision == configured


def test_clearing_a_precision_removes_the_key_again(data_dir):
    """Clearing a precision removes the key, so an older image can read the record again."""
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


# ── write_strings: a second optional field, under the same rollback rule ──────────────────────────
#
# A bool cannot be unset, so here it is the default, false, that stays out of the file.

def test_a_streams_json_written_before_write_strings_existed_still_loads(data_dir):
    """The upgrade direction: a file without the key loads with the default."""
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
    assert loaded[0].write_strings is False


def test_a_stream_that_never_turned_write_strings_on_writes_no_such_key(data_dir):
    """The rollback direction: `write_strings: false` is left out of the file.

    Written on every record, it would make the whole file unreadable to an older image.
    """
    upsert_stream(make_stream(name="untouched"))

    raw = json.loads((data_dir / "streams.json").read_text())

    assert len(raw) == 1
    assert "write_strings" not in raw[0]
    # The rest of the record is still written.
    assert raw[0]["name"] == "untouched"
    assert raw[0]["influx_database"] == "metrics"


def test_write_strings_is_written_and_survives_a_round_trip(data_dir):
    """A stream that does opt in carries the key, and reads back as the same stream."""
    upsert_stream(make_stream(name="printer", write_strings=True))

    raw = json.loads((data_dir / "streams.json").read_text())
    assert raw[0]["write_strings"] is True
    assert load_streams()[0].write_strings is True


def test_turning_write_strings_off_again_removes_the_key(data_dir):
    """Opting in must not be a one-way door out of the shape an older image can read."""
    stream = make_stream(name="printer", write_strings=True)
    upsert_stream(stream)
    assert "write_strings" in json.loads((data_dir / "streams.json").read_text())[0]

    stream.write_strings = False
    upsert_stream(stream)

    assert "write_strings" not in json.loads((data_dir / "streams.json").read_text())[0]
    assert load_streams()[0].write_strings is False


def test_write_strings_defaults_to_off_on_a_bare_stream_config():
    """A stream created through the API without mentioning it must not opt itself in."""
    assert StreamConfig().write_strings is False
    assert make_stream().write_strings is False


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
    """Every field survives, because a dropped credential means a stream that connects to nothing.

    Compared through asdict(), so a field added to StreamConfig later is covered too.
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
