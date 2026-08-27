#!/usr/bin/env python3
# flake8: noqa
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring

import logging
import math
import time
from typing import List, Optional, Tuple
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


# How many decimals a value is rounded to when the stream does not ask for anything else. Two is
# what this service has always done, to every number, unconditionally — 604 million points were
# written under that rule and every dashboard and continuous query downstream was built on top of
# it. So it stays the default: a stream that says nothing about precision must keep writing exactly
# the numbers it wrote yesterday.
DEFAULT_VALUE_PRECISION = 2

# The `value_precision` a stream sets to be stored unrounded. Any negative number means the same
# thing; -1 is the canonical spelling and the one the UI writes.
#
# A negative sentinel rather than a null, and that is the whole design decision. The three states a
# stream needs are "not configured", "N decimals" and "do not round", but a single Optional[int]
# field only spells two of them, so one state has to be encoded. Null is the wrong one to spend:
# an absent key and an explicit `null` are the same thing to every JSON producer there is — a
# cleared number input in the UI, a config generator filling in blanks — and if `null` meant "do
# not round" then clearing that box on the Wirenboard stream would silently switch 134 million
# messages a year to full precision. So both spellings of "unset" collapse to the default, and the
# dangerous state is the one you have to type on purpose. `round(x, -1)` does mean "round to tens"
# in Python, which is what this steals; no telemetry bridge has ever wanted that.
RAW_VALUE_PRECISION = -1


def resolve_value_precision(configured, stream_id: str = "") -> Optional[int]:
    """Turn a stream's configured `value_precision` into the argument the rounding takes.

    Two layers, two vocabularies, and they are deliberately not the same one. The CONFIG has three
    states (unset, N decimals, do not round) because an operator needs all three; the rounding has
    two (an int, or None for "skip the round() call"). This is the one place that maps between
    them, so `None` never has to mean two different things in the same scope.
    """
    if configured is None:
        return DEFAULT_VALUE_PRECISION
    # bool BEFORE int, because bool is a subclass of int and `round(1.234, True)` is a legal call
    # that quietly rounds to one decimal. `"value_precision": true` in a hand-edited streams.json
    # is a mistake, not a request for one decimal.
    if isinstance(configured, bool) or not isinstance(configured, int):
        # Includes the float and the string forms — `2.0` and `"2"` both come back from json.load()
        # looking close enough to right to be typed by accident, and `round(x, 2.0)` raises
        # TypeError, which inside flatten_payload would cost a five-second reconnect per message.
        # Falling back to the default keeps the stream writing; the warning is what makes the
        # silence stop, because a precision that is quietly ignored is exactly the bug this whole
        # setting exists to fix.
        logger.warning(
            "[%s] Ignoring value_precision=%r: expected an integer. Falling back to %d decimals.",
            stream_id, configured, DEFAULT_VALUE_PRECISION)
        return DEFAULT_VALUE_PRECISION
    if configured < 0:
        return None
    return configured


def _to_storable_float(value, precision: Optional[int] = DEFAULT_VALUE_PRECISION):
    """round(float(value), precision), or None for anything InfluxDB could not store anyway.

    `precision=None` stores the number as it arrived — which is what Node-RED does for a value that
    came out of json.loads() already numeric, and the only setting under which a calibration
    coefficient like 0.00003618 survives the trip at all. Note that it has to be a branch and not
    `round(num, precision)` with a None: `round(1.234, None)` is not "do not round", it returns the
    INT 1, so passing the None straight through would turn every value into an integer.
    """
    try:
        num = float(value)
        if precision is not None:
            num = round(num, precision)
    except (ValueError, OverflowError):
        # ValueError is the ordinary case: a string that is not a number at all.
        #
        # OverflowError has two sources, and the rounding is INSIDE this guard because of the
        # second one. float() raises it for an integer that does not fit a float, which json.loads()
        # will happily parse at any length — and round() raises it too: `round(1.7e308, -308)` is
        # "rounded value too large to represent". No stream can reach that through
        # resolve_value_precision(), which maps every negative to None, but `precision` is a public
        # parameter of this function, of to_number() and of flatten_payload(), and a negative
        # ndigits is a meaningful argument in Python.
        #
        # Either way raising here would escape flatten_payload, escape the per-message block, be
        # caught by the outer `except Exception` in _process_forever and cost a five-second
        # reconnect — one device publishing a 400-digit number would throttle the whole stream.
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


def to_number(value, precision: Optional[int] = DEFAULT_VALUE_PRECISION):
    # The two booleans do NOT go through the rounding, at any precision: 10 and 0 are markers this
    # bridge invents, not measurements, and `round(10, 8)` would turn the int into a float and
    # change the line protocol it writes from `value=10` to `value=10.0`.
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
        # A numeric string takes the same precision as a number would. Node-RED's bridge rounds
        # exactly here and nowhere else, which is how the same reading ends up stored two different
        # ways depending only on whether the device quoted it in its JSON.
        return _to_storable_float(value, precision)
    return None


def flatten_payload(topic: str, payload, precision: Optional[int] = DEFAULT_VALUE_PRECISION):
    """Recursively expand JSON objects into flat topic/value pairs."""
    if isinstance(payload, dict):
        suffix = "" if topic.endswith("/") else "/"
        for key, val in payload.items():
            if not key.startswith("_"):
                yield from flatten_payload(f"{topic}{suffix}{key}", val, precision)
    else:
        num = to_number(payload, precision)
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
#
# This split assumes the bridge talks to InfluxDB DIRECTLY. influx_host/influx_port come from
# streams.json and may well name a reverse proxy, and Traefik answers 404 — not 503 — for the
# seconds a docker-provider backend is being recreated, which is exactly the outage the ladder
# exists for. 404 is still classed as permanent on purpose: it also means "the database was
# dropped", and 401 means "the password was rotated", and retrying those costs the whole budget
# per batch, forever, in a loop that is not draining the broker queue while it waits. Losing one
# batch to a proxy blip is bounded and shows up in `errors`; the other way round is not. Fix that
# case at the proxy — make it answer 502/503 for a missing backend — not here.
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
