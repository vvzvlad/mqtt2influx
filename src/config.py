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
    with open(path, "w") as f:
        json.dump([asdict(s) for s in streams], f, indent=2)


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
