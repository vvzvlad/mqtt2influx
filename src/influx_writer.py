#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import logging
import math
import time
from typing import List, Tuple
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


def _to_storable_float(value):
    """round(float(value), 2), or None for anything InfluxDB could not store anyway."""
    try:
        num = round(float(value), 2)
    except ValueError:
        return None
    except OverflowError:
        # json.loads() parses an integer of any length; float() refuses one that does not fit.
        # Raising here would escape flatten_payload, escape the per-message block, be caught by
        # the outer `except Exception` in _process_forever and cost a five-second reconnect —
        # one device publishing a 400-digit number would throttle the whole stream.
        return None
    if not math.isfinite(num):
        # NaN and the infinities arrive by three routes that all look ordinary: json.loads("NaN")
        # and json.loads("Infinity") accept them by default, float("nan") parses the plain string
        # a sensor with no reading publishes, and 1e400 in a payload overflows to inf. None of
        # them can be stored: InfluxDB 1.x scans a field value that does not start with a digit or
        # a sign as an invalid number and rejects the ENTIRE request body — so one such point
        # costs the other 219 points batched with it, and then a full retry ladder against a 400
        # that will never come back different. Dropping them here rewrites no history, because no
        # value like this has ever reached the database.
        return None
    return num


def to_number(value):
    if isinstance(value, bool):
        return 10 if value else 0
    if isinstance(value, (int, float)):
        return _to_storable_float(value)
    if isinstance(value, str):
        low = value.lower()
        if low == "true":
            return 10
        if low == "false":
            return 0
        return _to_storable_float(value)
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


def make_line(measurement: str, value: float, timestamp_ms: int) -> str:
    # The backslash is replaced FIRST: doing it after the others would go back over the backslashes
    # they just inserted and escape those too, turning `a,b` into `a\\,b` — an escaped backslash
    # followed by a bare comma, which is the field separator again.
    # The newline is the one that matters for safety: write_batch joins the batch with "\n", and a
    # measurement name carrying a raw newline therefore injects whole extra line-protocol records
    # into the request body — writes into any measurement the sender likes. Names are built from
    # MQTT topics and from JSON keys inside the payload (flatten_payload), so the content is
    # attacker-supplied by construction.
    safe = (
        measurement.replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace(" ", "\\ ")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    return f"{safe} value={value} {timestamp_ms}"


# Non-2xx statuses worth sending the same body again for. Everything else that is not a 2xx is a
# statement about the REQUEST — a rotated password (401), a dropped database (404), a value
# InfluxDB's line parser refuses (400) — and the same bytes will get the same answer for as long as
# the cause lasts. 429 and 5xx are the database being overloaded, restarting, or proxied by
# something that is: a second later the identical body can succeed.
RETRY_AFTER_STATUSES = frozenset({429})


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
        """batch: list of (measurement, value, timestamp_ms). True if InfluxDB stored it.

        The bool-only shape is the contract every caller that does not retry is written against,
        including the shutdown flush; write_batch_detailed() is the one to call when the answer is
        going to be acted on.
        """
        stored, _worth_retrying = await self.write_batch_detailed(batch)
        return stored

    async def write_batch_detailed(self, batch: list) -> Tuple[bool, bool]:
        """batch: list of (measurement, value, timestamp_ms). Returns (stored, worth_retrying).

        The second half exists because collapsing every non-2xx into one `False` makes the retry
        ladder run against answers that cannot change. A rotated InfluxDB password or a deleted
        database costs, per batch, one 10s aiohttp timeout plus the whole RETRY_BUDGET of a message
        loop that is not draining the broker — and it does that for every batch until someone
        notices. Same for a single NaN in a body InfluxDB parses as a whole: a permanent 400 on 220
        good points.
        """
        if not batch:
            return True, False
        lines = [make_line(m, v, t) for m, v, t in batch]
        body = "\n".join(lines)
        try:
            async with self._session.post(
                self.url, params=self.params, data=body, auth=self.auth, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status in (200, 204):
                    return True, False
                text = await resp.text()
                worth_retrying = resp.status >= 500 or resp.status in RETRY_AFTER_STATUSES
                logger.error(
                    "[%s] InfluxDB error %s (%s): %s | url=%s params=%s body=%s",
                    self.stream_id, resp.status, "transient" if worth_retrying else "permanent",
                    text, self.url, self.params, body[:500],
                )
                return False, worth_retrying
        except Exception as e:
            # A refused connection, a DNS miss, a truncated response, the 10s ClientTimeout: the
            # transient class by definition — nothing about the request was ever judged.
            logger.error("[%s] InfluxDB write failed: %s", self.stream_id, e)
            return False, True
