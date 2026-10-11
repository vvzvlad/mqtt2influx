#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import logging
import math
import time
from typing import List, Optional, Tuple, Union
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


# Decimals for a stream that sets no precision; existing series and the dashboards built on them
# assume two.
DEFAULT_VALUE_PRECISION = 2

# The `value_precision` a stream sets to be stored unrounded. Any negative number means the same;
# -1 is the canonical spelling and the one the UI writes. Not null: an absent key and `null` (a
# cleared UI field) both mean the default, so turning rounding off has to be typed on purpose.
RAW_VALUE_PRECISION = -1


def resolve_value_precision(configured, stream_id: str = "") -> Optional[int]:
    """Turn a stream's configured `value_precision` into the argument the rounding takes.

    The config has three states (unset, N decimals, do not round); the rounding takes an int, or
    None for "do not round".
    """
    if configured is None:
        return DEFAULT_VALUE_PRECISION
    # bool before int: bool is a subclass of int, and `round(1.234, True)` silently rounds to one
    # decimal.
    if isinstance(configured, bool) or not isinstance(configured, int):
        # Floats and strings (`2.0`, `"2"`) too: `round(x, 2.0)` raises TypeError, which would cost
        # a reconnect per message. Fall back to the default and warn, so the ignored value shows.
        logger.warning(
            "[%s] Ignoring value_precision=%r: expected an integer. Falling back to %d decimals.",
            stream_id, configured, DEFAULT_VALUE_PRECISION)
        return DEFAULT_VALUE_PRECISION
    if configured < 0:
        return None
    return configured


def _to_storable_float(value, precision: Optional[int] = DEFAULT_VALUE_PRECISION):
    """round(float(value), precision), or None for anything InfluxDB could not store anyway.

    `precision=None` stores the number as it arrived. It has to be a branch: `round(1.234, None)`
    returns the int 1, not the unrounded float.
    """
    try:
        num = float(value)
        if precision is not None:
            num = round(num, precision)
    except (ValueError, OverflowError):
        # ValueError: a string that is not a number. OverflowError: float() of an integer too large
        # for a float, or round() with a negative ndigits (`round(1.7e308, -308)`). Raising would
        # escape flatten_payload and cost the stream a five-second reconnect.
        return None
    if not math.isfinite(num):
        # NaN and the infinities (json.loads("NaN"), float("nan"), 1e400) cannot be stored:
        # InfluxDB 1.x rejects the whole request body over one, losing every point batched with it.
        return None
    return num


def to_number(value, precision: Optional[int] = DEFAULT_VALUE_PRECISION):
    # The booleans skip the rounding: 10 and 0 are markers, not measurements, and
    # _to_storable_float() would turn `value=10` into `value=10.0`.
    if isinstance(value, bool):
        return 10 if value else 0
    if isinstance(value, (int, float)):
        return _to_storable_float(value, precision)
    if isinstance(value, str):
        low = value.lower()
        if low == "true":
            return 10
        if low == "false":
            return 0
        # Rounded like a number, so a reading is stored the same way whether or not the device
        # quoted it.
        return _to_storable_float(value, precision)
    return None


def _is_a_number_we_refused(text: str) -> bool:
    """True when float() reads `text` as a number: to_number() refused a number, not a word.

    `write_strings` must not pick those up: "nan", "inf" and overflowing literals live on topics
    that otherwise carry floats, and a string in a float-typed field is a permanent 400 for the
    whole batch. float() of a string returns inf instead of raising OverflowError, so ValueError is
    all there is to catch.
    """
    try:
        float(text)
    except ValueError:
        return False
    return True


def flatten_payload(topic: str, payload, precision: Optional[int] = DEFAULT_VALUE_PRECISION,
                    write_strings: bool = False):
    """Recursively expand JSON objects into flat topic/value pairs."""
    if isinstance(payload, dict):
        suffix = "" if topic.endswith("/") else "/"
        for key, val in payload.items():
            if not key.startswith("_"):
                yield from flatten_payload(f"{topic}{suffix}{key}", val, precision, write_strings)
    else:
        num = to_number(payload, precision)
        if num is not None:
            clean_topic = topic.replace(" ", "_").lower()
            yield clean_topic, num
        elif write_strings and isinstance(payload, str) and payload and not _is_a_number_we_refused(payload):
            # Only values to_number() refused reach here, so this adds series and never changes one;
            # refused numbers ("nan", "inf") stay dropped. The value is kept verbatim ("Printing"
            # and "printing" are different states); an empty payload, which clears a retained
            # topic, is dropped.
            clean_topic = topic.replace(" ", "_").lower()
            yield clean_topic, payload


def make_line(measurement: str, value: Union[float, str], timestamp_ms: int) -> str:
    # Backslash first, or the backslashes the later replacements insert would be escaped again.
    # Newlines are escaped because write_batch joins records with "\n" and names come from MQTT
    # topics and payload keys: a raw newline would inject extra records.
    safe = (
        measurement.replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace(" ", "\\ ")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    if isinstance(value, str):
        # Inside a quoted string field line protocol escapes only the backslash (replaced first, as
        # above) and the double quote. Newlines are escaped against the same injection, so a value
        # with a real newline is stored with the two characters `\n` in its place.
        safe_value = (
            value.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
        return f'{safe} value="{safe_value}" {timestamp_ms}'
    return f"{safe} value={value} {timestamp_ms}"


# Retried besides 5xx: an overloaded or restarting database. Other statuses (400, 401, 404)
# describe the request, and the same body gets the same answer. A proxy that answers 404 for a
# missing backend (Traefik does) is fixed at the proxy, by answering 502/503, not here.
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

        For callers that do not retry, such as the shutdown flush; a retrying caller uses
        write_batch_detailed().
        """
        stored, _worth_retrying = await self.write_batch_detailed(batch)
        return stored

    async def write_batch_detailed(self, batch: list) -> Tuple[bool, bool]:
        """batch: list of (measurement, value, timestamp_ms). Returns (stored, worth_retrying).

        worth_retrying is False for an answer that cannot change (a rotated password, a dropped
        database, a 400), so the retry ladder does not spend its budget on it.
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
