#!/usr/bin/env python3
"""Everything between an MQTT message and the body of an InfluxDB write request.

Four pure functions and one HTTP call, and every one of them handles input supplied by whoever can
publish to the broker: `contains_excluded` decides what is thrown away, `to_number` decides what
counts as a measurement at all, `flatten_payload` turns a JSON document into topic/value pairs and
`make_line` renders them as line protocol. `InfluxWriter.write_batch` is the only part that touches
the network, and it is driven here against a session double — a test that opened a socket would be
a test that fails on an aeroplane.

`make_line()` builds the measurement half of an InfluxDB line-protocol record out of an MQTT topic —
and, via `flatten_payload()`, out of the KEYS of whatever JSON arrived on that topic. Both are
supplied by whoever can publish to the broker, so every character with a meaning in line protocol
has to be neutralised here or it is neutralised nowhere.
"""

import aiohttp
import pytest

from src.influx_writer import (
    EXCLUDED_SUBSTRINGS,
    InfluxWriter,
    contains_excluded,
    flatten_payload,
    make_line,
    to_number,
)


# ── make_line: escaping ───────────────────────────────────────────────────────────────────────────

def test_a_comma_in_a_measurement_is_escaped():
    assert make_line("a,b", 1.0, 5) == "a\\,b value=1.0 5"


def test_a_space_in_a_measurement_is_escaped():
    assert make_line("a b", 1.0, 5) == "a\\ b value=1.0 5"


def test_a_backslash_in_a_measurement_is_escaped():
    assert make_line("a\\b", 1.0, 5) == "a\\\\b value=1.0 5"


def test_a_newline_in_a_measurement_is_escaped():
    assert make_line("a\nb", 1.0, 5) == "a\\nb value=1.0 5"


def test_a_carriage_return_in_a_measurement_is_escaped():
    assert make_line("a\rb", 1.0, 5) == "a\\rb value=1.0 5"


def test_the_backslash_is_escaped_before_the_other_characters():
    # Input is the four characters  a \ , b.
    # Escaping the backslash LAST would go back over the backslash the comma rule had just inserted
    # and double it, leaving the comma preceded by an even number of backslashes — unescaped again,
    # and a field separator in the middle of a measurement name. Order is the whole fix here, and
    # only an input containing both characters can tell the two orders apart.
    assert make_line("a\\,b", 1.0, 5) == "a\\\\\\,b value=1.0 5"


def test_make_line_names_its_timestamp_parameter_in_the_unit_it_receives():
    # write_batch() passes milliseconds and sends precision=ms; the parameter used to be called
    # timestamp_ns, which says the opposite and invites the next reader to "fix" the caller by
    # multiplying by a million. Passing it by keyword is what pins the name.
    assert make_line(measurement="m", value=1.0, timestamp_ms=7) == "m value=1.0 7"


# ── contains_excluded: the topic filter ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("substring", EXCLUDED_SUBSTRINGS)
def test_every_listed_substring_really_filters_a_topic(substring):
    """The list is only a filter if every entry in it is reachable.

    Parametrised over the constant rather than over a copied literal, so an entry added to
    EXCLUDED_SUBSTRINGS is tested the moment it is added — and, more to the point, an entry that
    never matches anything (a leading slash too many, a renamed vendor prefix) cannot sit in the
    list looking like protection it is not providing. These topics are excluded because they carry
    firmware versions, serial numbers and Home Assistant discovery documents: high-cardinality
    strings that `to_number` discards anyway, after the whole payload has been parsed and walked.
    """
    assert contains_excluded("/devices/x{}/controls/y".format(substring)) is True


def test_a_topic_matching_nothing_in_the_list_is_kept():
    """The other half of the filter: it has to let the actual measurements through.

    `any()` over an empty list is False, but `any()` over a list containing "" would be True for
    every topic on the broker — the filter would silently drop the entire stream, and the stats
    would show messages received and nothing sent.
    """
    assert contains_excluded("/devices/wb-msw-v3/controls/Temperature") is False


def test_the_filter_matches_a_substring_anywhere_not_just_a_prefix():
    """`/meta/` and `_version` are mid-topic by construction; a prefix match would never fire."""
    assert contains_excluded("smoke//devices/relay/meta/error") is True
    assert contains_excluded("/devices/system/controls/fw_version") is True


# ── to_number: what counts as a measurement ───────────────────────────────────────────────────────

def test_a_boolean_becomes_ten_or_zero():
    """10 rather than 1 so that a switch drawn on the same axis as a percentage stays visible.

    The bool branch has to come FIRST: `isinstance(True, int)` is True in Python, so an `int` check
    placed above it would swallow booleans and write 1/0 instead — every relay graph in the UI would
    flatten against the bottom of a chart shared with a 0..100 sensor.
    """
    assert to_number(True) == 10
    assert to_number(False) == 0


def test_the_strings_true_and_false_are_read_as_booleans_whatever_their_case():
    """MQTT payloads are bytes; a broker that publishes `true` sends five characters, not a bool.

    Half the devices on a Wiren Board bus publish `true`/`false` as plain text and never as JSON, so
    without this branch `float("true")` raises, `to_number` returns None and the topic silently
    produces no points at all — a switch that simply never appears in InfluxDB.
    """
    assert to_number("true") == 10
    assert to_number("TRUE") == 10
    assert to_number("True") == 10
    assert to_number("false") == 0
    assert to_number("FALSE") == 0
    assert to_number("False") == 0


def test_a_numeric_string_is_parsed():
    assert to_number("12") == 12.0
    assert to_number("-4.5") == -4.5


def test_a_non_numeric_string_is_dropped_rather_than_raising():
    """Returning None is what makes a text payload a no-op instead of an exception in the read loop.

    An exception here would escape `flatten_payload`, escape the per-message block in
    `_process_forever`, be caught by its outer `except Exception`, and cost a five-second reconnect
    sleep — during which the broker keeps publishing to a subscriber that is not there. One device
    sending a status string would throttle the entire stream.
    """
    assert to_number("online") is None
    assert to_number("") is None


@pytest.mark.parametrize("value", [None, [1, 2], {"a": 1}, object()])
def test_anything_that_is_not_a_number_a_bool_or_a_string_is_dropped(value):
    """The final `return None` is the catch-all, and it has to stay a return rather than a raise."""
    assert to_number(value) is None


def test_a_number_is_rounded_to_two_decimals():
    assert to_number(5.678) == 5.68
    assert to_number(-0.006) == -0.01


def test_rounding_to_two_decimals_flattens_small_readings_to_zero():
    """Documented, not endorsed: this is lossy, and the loss is silent.

    `round(value, 2)` is applied to every numeric payload, so anything below 0.005 in magnitude is
    stored as exactly 0.0 — and it is stored, not dropped, because 0.0 is not None. A current
    sensor reading 0.003 A, a rainfall gauge in millimetres, a power factor delta: each writes a
    flat zero line into InfluxDB that looks like a working sensor reporting nothing.

    The behaviour is pinned here as-is because changing it would rewrite the meaning of the
    historical series already in production. This test exists so that the next person to notice a
    suspiciously flat graph finds the cause written down instead of rediscovering it.
    """
    assert to_number(0.001) == 0.0
    assert to_number(0.004) == 0.0
    assert to_number(1e-9) == 0.0


def test_a_bool_yields_a_python_int_while_a_number_yields_a_float():
    """The two branches return different Python types, and that leaks all the way out.

    `10` and `10.0` render differently in the line body and as different JSON types on the /ws feed,
    so a topic that alternates between `true` and `1.5` produces `value=10` on one record and
    `value=1.5` on the next.

    Worth being precise about the consequence, because it is smaller than it looks: InfluxDB 1.x
    line protocol treats an unsuffixed number as a float and reserves integers for the explicit `i`
    suffix, which `make_line` never emits — so both records land in the same float field and there
    is NO field type conflict at the database. The difference is visible in the request body and in
    the websocket payload the UI renders, and nowhere else.
    """
    assert type(to_number(True)) is int
    assert type(to_number(1.5)) is float
    # And the difference as InfluxDB actually receives it — no `i` suffix on either.
    assert make_line("m", to_number(True), 1) == "m value=10 1"
    assert make_line("m", to_number(1.5), 1) == "m value=1.5 1"


# ── flatten_payload: JSON document to topic/value pairs ───────────────────────────────────────────

def test_a_scalar_payload_yields_one_pair_named_after_the_topic():
    assert list(flatten_payload("devices/temp", 21.5)) == [("devices/temp", 21.5)]


def test_a_nested_object_is_expanded_into_a_path_per_leaf():
    """One MQTT message carrying a document has to become one point per number inside it.

    Without the recursion the whole dict reaches `to_number`, which returns None for a dict, and the
    message produces nothing — a device that reports its state as JSON would be invisible while the
    stats counted its messages as received.
    """
    payload = {"outer": {"inner": 1.5}, "flat": 2}

    assert sorted(flatten_payload("devices/x", payload)) == [
        ("devices/x/flat", 2.0),
        ("devices/x/outer/inner", 1.5),
    ]


def test_keys_beginning_with_an_underscore_are_skipped_along_with_everything_under_them():
    """`_`-prefixed keys are the convention for a payload's own metadata, and it is metadata that
    blows up cardinality: `_id`, `_timestamp`, `_meta` change on every single message. Skipping the
    key skips its whole subtree — a nested `_meta` object contributes nothing at all, not just no
    top-level field."""
    payload = {"temp": 21.0, "_id": 7, "_meta": {"seq": 1, "src": {"deep": 2}}}

    assert list(flatten_payload("devices/x", payload)) == [("devices/x/temp", 21.0)]


def test_the_assembled_topic_is_lowercased_and_has_its_spaces_replaced():
    """A space is a field separator in line protocol and `make_line` escapes it, so an unnormalised
    key would reach InfluxDB as `Living\\ Room` — a measurement name with a backslash in it, and a
    second series for what is the same sensor as `living_room`. Case is folded for the same reason:
    InfluxDB measurement names are case-sensitive, so `Temp` and `temp` are two graphs."""
    assert list(flatten_payload("Home/Living Room", {"Temp Now": 21.0})) == [
        ("home/living_room/temp_now", 21.0)
    ]


def test_a_topic_that_already_ends_in_a_slash_does_not_get_a_second_one():
    """`topic_prefix` is user-typed in the UI and a trailing slash is the obvious way to type it.
    The doubled separator would not fail — it would quietly create `home//devices/...` as a series
    distinct from `home/devices/...`, splitting one sensor's history across two names."""
    assert list(flatten_payload("home/", {"temp": 21.0})) == [("home/temp", 21.0)]
    assert list(flatten_payload("home", {"temp": 21.0})) == [("home/temp", 21.0)]


def test_values_that_are_not_numbers_are_dropped_and_the_rest_of_the_document_survives():
    """One unparseable field must not cost the other fields in the same message.

    Yielding nothing for the whole payload — or raising — would mean a device that reports a status
    string alongside its readings loses the readings too.
    """
    payload = {"temp": 21.0, "status": "online", "name": None, "readings": [1, 2]}

    assert list(flatten_payload("devices/x", payload)) == [("devices/x/temp", 21.0)]


def test_a_json_array_payload_produces_nothing():
    """Lists are not recursed into — `to_number` returns None for one and the message is dropped.

    Pinned because it is a silent hole rather than an error: a device publishing `[1, 2, 3]` counts
    as received, is not counted as filtered, and writes no points. If array payloads ever have to be
    supported, this is the test that has to change, and it names where.
    """
    assert list(flatten_payload("devices/x", [1, 2, 3])) == []


# ── InfluxWriter.write_batch: the HTTP write ──────────────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, status=204, text=""):
        self.status = status
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _ExplodingResponse:
    """aiohttp does its I/O in `__aenter__`, not in `post()`, so that is where a network error
    surfaces — a double that raised from `post()` would exercise a code path aiohttp never takes."""

    def __init__(self, error):
        self._error = error

    async def __aenter__(self):
        raise self._error

    async def __aexit__(self, *exc_info):
        return False


class _FakeSession:
    """Captures the request instead of sending it."""

    def __init__(self, response=None):
        self.calls = []
        self._response = response if response is not None else _FakeResponse()

    @property
    def bodies(self):
        return [call["data"] for call in self.calls]

    def post(self, url, params=None, data=None, auth=None, timeout=None):
        self.calls.append(
            {"url": url, "params": params, "data": data, "auth": auth, "timeout": timeout})
        return self._response


def _writer_with_fake_session(response=None, **overrides):
    kwargs = dict(host="h", port=8086, user="", password="", database="db", stream_id="s")
    kwargs.update(overrides)
    writer = InfluxWriter(**kwargs)
    session = _FakeSession(response)
    writer._session = session
    return writer, session


async def test_a_newline_in_a_measurement_cannot_inject_extra_records_into_the_batch_body():
    # The batch is serialised with "\n".join(...), so an unescaped newline inside a measurement name
    # is not a formatting blemish — it closes the current record and opens one the publisher wrote,
    # which is a write into any measurement it names. Three points in, three records out.
    writer, session = _writer_with_fake_session()
    batch = [
        ("home/temp", 21.5, 1),
        ("evil\ncpu value=999 2", 1.0, 2),
        ("home/hum", 40.0, 3),
    ]

    assert await writer.write_batch(batch) is True

    lines = session.bodies[0].splitlines()
    assert len(lines) == 3
    assert lines[0] == "home/temp value=21.5 1"
    assert lines[2] == "home/hum value=40.0 3"
    # The hostile string stays a measurement NAME on the middle record — its own value and timestamp
    # are still appended after it — instead of closing that record and opening one of its own.
    assert lines[1].startswith("evil\\ncpu")
    assert lines[1].endswith(" value=1.0 2")


async def test_an_empty_batch_succeeds_without_going_near_the_network():
    """The interval timer fires every second whether or not a message arrived.

    `_flush` returns early on an empty batch today, but `write_batch` is the one that must not
    depend on that: an HTTP round trip per second per idle stream is a request InfluxDB has to
    parse and answer for no reason, and an empty body is a 400 from some versions. Returning True
    also matters — False would mark `last_flush_ok` false and count an error on every quiet second.
    """
    writer, session = _writer_with_fake_session()

    assert await writer.write_batch([]) is True
    assert session.calls == []


@pytest.mark.parametrize("status", [200, 204])
async def test_the_two_statuses_influxdb_answers_with_both_count_as_written(status):
    """204 is what InfluxDB 1.x returns from /write on success; 200 is what a proxy in front of it
    may normalise that to. Treating either as a failure would send the whole batch through the retry
    ladder and then count it as lost — points that were in fact stored, reported as errors, while
    the message loop stalls for up to RETRY_BUDGET seconds against a database that is working."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status))

    assert await writer.write_batch([("m", 1.0, 1)]) is True


@pytest.mark.parametrize("status", [400, 401, 404, 413, 500, 503])
async def test_any_other_status_is_a_failed_write(status):
    """False is what triggers the retry ladder in `_write_with_retries` and, failing that, the error
    counter the UI shows. Returning True on a 401 or a 404 — a rotated password, a dropped database
    — would show a stream running cleanly while every point it produced went nowhere."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status, text="boom"))

    assert await writer.write_batch([("m", 1.0, 1)]) is False


@pytest.mark.parametrize("error", [
    aiohttp.ClientConnectionError("cannot connect to influx.example:8086"),
    TimeoutError(),                       # what the 10s ClientTimeout raises
    aiohttp.ClientPayloadError("truncated response"),
])
async def test_a_network_failure_is_a_failed_write_and_not_an_exception(error):
    """The exception has to stop here. `_flush` awaits this from two places — the message loop and
    the interval timer — and neither wraps it: an escaping error kills the timer task in silence,
    or costs a five-second reconnect on the read loop. An InfluxDB restart is routine; losing the
    MQTT subscription because of one is not.

    Note what the broad `except Exception` deliberately does NOT swallow: `asyncio.CancelledError`
    is a BaseException, so the RETRY_BUDGET timeout in `_write_with_retries` and a shutdown can
    still cut an in-flight write short.
    """
    writer, _session = _writer_with_fake_session(_ExplodingResponse(error))

    assert await writer.write_batch([("m", 1.0, 1)]) is False


async def test_the_write_is_addressed_to_the_databases_write_endpoint_in_milliseconds():
    """`precision=ms` has to match the timestamps `make_line` is handed.

    InfluxDB defaults to nanoseconds when no precision is given, so dropping this parameter does not
    fail — it reads a millisecond timestamp as nanoseconds and files every point at 1970-01-20. The
    write succeeds, the API reports success, and the data is simply not where anyone looks for it.
    """
    writer, session = _writer_with_fake_session(host="influx.example", port=8087, database="metrics")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["url"] == "http://influx.example:8087/write"
    assert session.calls[0]["params"] == {"db": "metrics", "precision": "ms"}


async def test_credentials_are_attached_when_a_user_is_configured():
    writer, session = _writer_with_fake_session(user="influx-user", password="influx-secret")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["auth"] == aiohttp.BasicAuth("influx-user", "influx-secret")


async def test_no_auth_header_is_sent_when_no_user_is_configured():
    """An unauthenticated InfluxDB is the common home setup, and `BasicAuth("", "")` is not the same
    as no auth at all — it sends an `Authorization` header carrying an empty username, which an
    InfluxDB with authentication switched on rejects with a 401 rather than ignoring."""
    writer, session = _writer_with_fake_session(user="", password="")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["auth"] is None
