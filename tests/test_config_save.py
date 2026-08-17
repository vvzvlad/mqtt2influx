#!/usr/bin/env python3
"""Durability of `save_streams()`.

`{DATA_DIR}/streams.json` is the only copy of everything this service is configured with — every
MQTT and InfluxDB credential included. There is no database behind it and no environment fallback,
and `load_streams()` reports a damaged file as an empty one, so a save that fails halfway does not
degrade the config, it deletes it and hides the fact.

These tests are about the WRITE only. That `load_streams()` swallows errors is deliberate and
documented in AGENTS.md, and tests/test_config.py pins it.
"""

import errno
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


def test_the_directory_is_synced_after_the_rename(data_dir, monkeypatch):
    """`os.fsync(f.fileno())` makes the CONTENTS durable; the rename is a change to the DIRECTORY.

    Until the directory's own entry is flushed, a host losing power in that window can come back
    with the old streams.json, or with neither name pointing at the new data — and this file is the
    only copy of every stream's MQTT and InfluxDB credentials, with no backup behind it. Against a
    SIGKILL of the container it makes no difference (the page cache outlives the process); against
    the machine going down it is the whole difference.

    Order is asserted, not just occurrence: a directory synced BEFORE the rename records the state
    the rename was about to change.
    """
    events = []
    real_open, real_fsync, real_replace = os.open, os.fsync, os.replace
    directory_fds = set()

    def spy_open(path, flags, *args, **kwargs):
        fd = real_open(path, flags, *args, **kwargs)
        if os.path.isdir(path):
            directory_fds.add(fd)
        return fd

    def spy_fsync(fd):
        events.append(("fsync", "directory" if fd in directory_fds else "file"))
        return real_fsync(fd)

    def spy_replace(src, dst):
        events.append(("replace", dst))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "replace", spy_replace)

    save_streams([StreamConfig(id="a", name="one")])

    assert events == [
        ("fsync", "file"),
        ("replace", str(data_dir / "streams.json")),
        ("fsync", "directory"),
    ]


def test_a_directory_that_refuses_to_sync_does_not_fail_a_save_that_already_happened(data_dir, monkeypatch):
    """The rename has already succeeded by then, so the new config IS the file on disk.

    Turning "the directory entry could not be flushed" into an exception would answer a completed
    save with a 500 and invite the operator to save again over a file that is already correct.
    Filesystems and platforms that will not open or fsync a directory exist; a config that is saved
    but not yet guaranteed against a power cut beats a config the API claims it failed to write.
    """
    def refusing_open(path, flags, *args, **kwargs):
        raise OSError(errno.EINVAL, "this filesystem will not open a directory")

    monkeypatch.setattr(os, "open", refusing_open)

    save_streams([StreamConfig(id="a", name="one")])  # must not raise

    assert [s.id for s in load_streams()] == ["a"]
    assert sorted(os.listdir(data_dir)) == ["streams.json"]
