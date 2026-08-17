#!/usr/bin/env python3
"""Durability of `save_streams()`.

`{DATA_DIR}/streams.json` is the only copy of everything this service is configured with — every
MQTT and InfluxDB credential included. There is no database behind it and no environment fallback,
and `load_streams()` reports a damaged file as an empty one, so a save that fails halfway does not
degrade the config, it deletes it and hides the fact.

These tests are about the WRITE only. That `load_streams()` swallows errors is deliberate and
documented in AGENTS.md, and tests/test_config.py pins it.
"""

import json
import os

import pytest

from src.config import StreamConfig, load_streams, save_streams


def test_a_saved_stream_reads_back_unchanged(data_dir):
    save_streams([StreamConfig(id="a", name="one", mqtt_host="broker", influx_database="db")])

    loaded = load_streams()

    assert len(loaded) == 1
    assert loaded[0].id == "a"
    assert loaded[0].name == "one"
    assert loaded[0].mqtt_host == "broker"
    assert loaded[0].influx_database == "db"


def test_a_successful_save_leaves_no_temporary_file_behind(data_dir):
    save_streams([StreamConfig(id="a", name="one")])

    # The rename target is the only thing that may survive: a temp file that outlives the save
    # accumulates one copy of the credentials per write.
    assert sorted(os.listdir(data_dir)) == ["streams.json"]


def test_a_failure_partway_through_serialization_leaves_the_previous_file_intact(data_dir):
    save_streams([StreamConfig(id="a", name="one")])
    before = (data_dir / "streams.json").read_text()

    # A field whose value json cannot encode. asdict() copies it happily, so the write is already
    # under way when json.dump gives up — which is exactly the case `open(path, "w")` could not
    # survive: it truncated the real file before the first byte was produced.
    broken = StreamConfig(id="b", name="two")
    broken.mqtt_port = object()

    with pytest.raises(TypeError):
        save_streams([StreamConfig(id="a", name="one"), broken])

    assert (data_dir / "streams.json").read_text() == before
    assert [s["id"] for s in json.loads(before)] == ["a"]
    assert sorted(os.listdir(data_dir)) == ["streams.json"]
