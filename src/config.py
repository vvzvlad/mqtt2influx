#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import json
import os
import uuid
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from .settings import settings

# Read once, at import. `_config_path()` looks this name up on every call, so tests/conftest.py
# rebinds it to keep the suite off the real directory; do not inline `settings.data_dir`.
DATA_DIR = settings.data_dir


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
    # Decimals numeric values are rounded to before they are written. None (or an absent key) means
    # DEFAULT_VALUE_PRECISION; a negative number stores values as they arrived (RAW_VALUE_PRECISION,
    # influx_writer.py). See _serialize() for why an unset value never reaches the file.
    value_precision: Optional[int] = None
    # Write non-numeric payloads as InfluxDB string fields instead of dropping them. Off by default:
    # existing series were built on numbers only. See _serialize() for why "off" writes no key.
    write_strings: bool = False


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
    # Failures are swallowed: os.replace() has already succeeded, so the save is done, and some
    # filesystems refuse to open or fsync a directory. Raising would answer a done save with a 500.
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

    `load_streams()` builds `StreamConfig(**record)` and turns the TypeError of an undeclared key
    into an empty list, which the next save writes over the file. Leaving unset optional keys out
    keeps a record readable by an older image after a rollback; a stream that sets one is not
    (see README).
    """
    record = asdict(stream)
    if record.get("value_precision") is None:
        del record["value_precision"]
    # Falsy rather than `is False`: a hand-edited `null` or `0` means off too, and writing it back
    # would make the file unreadable for an older image.
    if not record.get("write_strings"):
        del record["write_strings"]
    return record


def save_streams(streams: List[StreamConfig]):
    path = _config_path()
    # Written beside the target and renamed over it: `open(path, "w")` truncates first, so a failed
    # dump would leave a half-written file that load_streams() reads as empty and the next save
    # overwrites. Same directory, because os.replace() is atomic only within one filesystem.
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump([_serialize(s) for s in streams], f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        # The rename changes the directory, which survives a power cut only once it is synced too.
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
