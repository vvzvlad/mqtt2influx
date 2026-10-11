#!/usr/bin/env python3
"""Durability of `save_streams()`.

streams.json is the only copy of the configuration, credentials included, and `load_streams()`
reads a damaged file as empty, so a save that fails halfway deletes the config. These tests cover
the write; tests/test_config.py covers the read.
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

    # A value json cannot encode: asdict() copies it, so json.dump fails with the write under way,
    # which an in-place `open(path, "w")` would not survive.
    broken = StreamConfig(id="b", name="two")
    broken.mqtt_port = object()

    with pytest.raises(TypeError):
        save_streams([StreamConfig(id="a", name="one"), broken])

    assert (data_dir / "streams.json").read_text() == before
    assert [s["id"] for s in json.loads(before)] == ["a"]
    assert sorted(os.listdir(data_dir)) == ["streams.json"]


def test_the_directory_is_synced_after_the_rename(data_dir, monkeypatch):
    """The directory is fsynced after the rename, which makes the rename survive a power cut.

    Order is asserted, not just occurrence: a directory synced before the rename records the state
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
    """By then the rename has succeeded and the save is done.

    Some filesystems refuse to open or fsync a directory; raising would report the save as failed.
    """
    def refusing_open(path, flags, *args, **kwargs):
        raise OSError(errno.EINVAL, "this filesystem will not open a directory")

    monkeypatch.setattr(os, "open", refusing_open)

    save_streams([StreamConfig(id="a", name="one")])  # must not raise

    assert [s.id for s in load_streams()] == ["a"]
    assert sorted(os.listdir(data_dir)) == ["streams.json"]
