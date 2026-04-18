#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import logging
import time
from typing import List
import aiohttp

logger = logging.getLogger(__name__)

EXCLUDED_SUBSTRINGS = [
    "zigbee2mqtt/bridge",
    "config/enabled_by_default",
    "homeassistant/",
    "update/installed_version",
    "update/latest_version",
    "/meta/",
    "/rpc/v1",
    "/fragment/",
    "play_from_rom",
    "system__networks",
    "learn_to_rom",
    "controls/serial",
    "controls/batch_no",
    "_version",
    "/devices/power_status/controls/Vin",
    "/devices/wb-adc/controls",
]


def contains_excluded(topic: str) -> bool:
    return any(sub in topic for sub in EXCLUDED_SUBSTRINGS)


def to_number(value):
    if isinstance(value, bool):
        return 10 if value else 0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    if isinstance(value, str):
        low = value.lower()
        if low == "true":
            return 10
        if low == "false":
            return 0
        try:
            return round(float(value), 2)
        except ValueError:
            return None
    return None


def flatten_payload(topic: str, payload):
    """Recursively expand JSON objects into flat topic/value pairs."""
    if isinstance(payload, dict):
        suffix = "" if topic.endswith("/") else "/"
        for key, val in payload.items():
            if not key.startswith("_"):
                yield from flatten_payload(f"{topic}{suffix}{key}", val)
    else:
        num = to_number(payload)
        if num is not None:
            clean_topic = topic.replace(" ", "_").lower()
            yield clean_topic, num


def make_line(measurement: str, value: float, timestamp_ns: int) -> str:
    safe = measurement.replace(",", "\\,").replace(" ", "\\ ")
    return f"{safe} value={value} {timestamp_ns}"


class InfluxWriter:
    def __init__(self, host: str, port: int, user: str, password: str, database: str, stream_id: str):
        self.url = f"http://{host}:{port}/write"
        self.params = {"db": database, "precision": "ms"}
        self.auth = aiohttp.BasicAuth(user, password) if user else None
        self.stream_id = stream_id
        self._session: aiohttp.ClientSession = None

    async def start(self):
        self._session = aiohttp.ClientSession()

    async def stop(self):
        if self._session:
            await self._session.close()

    async def write_batch(self, batch: list) -> bool:
        """batch: list of (measurement, value, timestamp_ms)"""
        if not batch:
            return True
        lines = [make_line(m, v, t) for m, v, t in batch]
        body = "\n".join(lines)
        try:
            async with self._session.post(
                self.url, params=self.params, data=body, auth=self.auth, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status in (200, 204):
                    return True
                text = await resp.text()
                logger.error("[%s] InfluxDB error %s: %s", self.stream_id, resp.status, text)
                return False
        except Exception as e:
            logger.error("[%s] InfluxDB write failed: %s", self.stream_id, e)
            return False
