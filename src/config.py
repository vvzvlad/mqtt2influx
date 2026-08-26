#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import json
import os
import uuid
from dataclasses import dataclass, field, asdict
from typing import List, Optional

DATA_DIR = os.environ.get("DATA_DIR", "/data")


@dataclass
class StreamConfig:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    mqtt_host: str = ""
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_topic: str = "#"
    topic_prefix: str = ""
    influx_host: str = ""
    influx_port: int = 8086
    influx_user: str = ""
    influx_password: str = ""
    influx_database: str = ""
    enabled: bool = True
    # Decimals every numeric value from this stream is rounded to before it is written.
    # None — and an absent key, which is the same thing — means "not configured" and gets the two
    # decimals this service has always applied. A negative number means "store it as it arrived".
    # See DEFAULT_VALUE_PRECISION / RAW_VALUE_PRECISION in influx_writer.py for why the states are
    # spelled this way, and _serialize() below for why an unset one never reaches the file.
    value_precision: Optional[int] = None


def _config_path():
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, "streams.json")


def load_streams() -> List[StreamConfig]:
    path = _config_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        return [StreamConfig(**s) for s in data]
    except Exception:
        return []


def _fsync_directory(directory: str):
    # Failures are swallowed on purpose, and only here. By the time this runs os.replace() has
    # already succeeded, so the new config IS the file on disk: turning "the directory entry could
    # not be flushed" into an exception would answer a completed save with a 500 and invite the
    # operator to save again. Filesystems and platforms that refuse to open or fsync a directory
    # exist; a config that is saved but not yet guaranteed against a power cut is strictly better
    # than a config the API claims it failed to save.
    try:
        fd = os.open(directory or ".", os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _serialize(stream: StreamConfig) -> dict:
    """asdict(), minus the optional keys this stream never set.

    Adding a field to StreamConfig is not a free act, because `load_streams()` builds its objects
    with `StreamConfig(**record)` and an undeclared keyword is a TypeError — which the bare `except`
    below turns into an empty list, i.e. EVERY STREAM GONE, and the next save writes that emptiness
    back over the file. The version of the code that will choke is not this one, it is whatever
    older image someone rolls back to after this one has already rewritten streams.json.

    Writing `value_precision` only when it is actually set is what keeps that door shut for the
    streams that do not use the feature: their record stays byte-for-byte the record an older image
    already knows how to read, so a rollback is as safe as it was before this field existed. A
    stream that DOES opt in carries the key and an older image would refuse the whole file — that
    one is unavoidable, is the price of the setting, and is written down in README.
    """
    record = asdict(stream)
    if record.get("value_precision") is None:
        del record["value_precision"]
    return record


def save_streams(streams: List[StreamConfig]):
    path = _config_path()
    # Written beside the target and renamed over it, never opened in place: `open(path, "w")`
    # truncates before the first byte is serialized, so an exception mid-dump, a full disk or a
    # killed container leaves a half-written streams.json — and this file is the ONLY copy of every
    # stream's MQTT and InfluxDB credentials, with no database and no environment fallback behind
    # it. load_streams() reports a damaged file as an empty one, so the next save would then write
    # `[]` over it and make the loss permanent.
    # The temporary file sits in the same directory because os.replace() is only atomic within a
    # single filesystem; across one it degrades to copy-and-delete, which is the same truncation
    # window again.
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump([_serialize(s) for s in streams], f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        # fsync of the file made its CONTENTS durable; the rename is a change to the DIRECTORY and
        # is not durable until the directory itself is synced. Lose power in the window between the
        # two and the filesystem can come back with the old streams.json, or with neither name
        # pointing at the new data — and this file has no backup and no second copy. Against a
        # SIGKILL of the container it makes no difference (the page cache survives the process);
        # against the host losing power it is the difference between a saved config and a lost one.
        _fsync_directory(os.path.dirname(path))
    except BaseException:
        # Without this the partial temp file survives every failure and accumulates in DATA_DIR.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def get_stream(stream_id: str) -> Optional[StreamConfig]:
    return next((s for s in load_streams() if s.id == stream_id), None)


def upsert_stream(stream: StreamConfig):
    streams = load_streams()
    for i, s in enumerate(streams):
        if s.id == stream.id:
            streams[i] = stream
            save_streams(streams)
            return
    streams.append(stream)
    save_streams(streams)


def delete_stream(stream_id: str) -> bool:
    streams = load_streams()
    new_streams = [s for s in streams if s.id != stream_id]
    if len(new_streams) == len(streams):
        return False
    save_streams(new_streams)
    return True
