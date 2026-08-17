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
            json.dump([asdict(s) for s in streams], f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
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
