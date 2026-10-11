#!/usr/bin/env python3
"""Tests for src/influx_writer.py: from an MQTT message to the body of an InfluxDB write.

Topics, payload keys and payloads come from whoever can publish to the broker, so every character
with a meaning in line protocol is neutralised here or nowhere. write_batch runs against a session
double; one cancellation test uses a loopback server.
"""

import asyncio
import logging

import aiohttp
import pytest

from src.influx_writer import (
    DEFAULT_VALUE_PRECISION,
    EXCLUDED_SUBSTRINGS,
    InfluxWriter,
    RAW_VALUE_PRECISION,
    _to_storable_float,
    contains_excluded,
    flatten_payload,
    make_line,
    resolve_value_precision,
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
    # Input is the four characters  a \ , b. Escaping the backslash last would double the one the
    # comma rule inserts and leave the comma unescaped; only an input with both characters tells
    # the two orders apart.
    assert make_line("a\\,b", 1.0, 5) == "a\\\\\\,b value=1.0 5"


def test_make_line_names_its_timestamp_parameter_in_the_unit_it_receives():
    # write_batch() passes milliseconds and sends precision=ms. Passing by keyword pins the name.
    assert make_line(measurement="m", value=1.0, timestamp_ms=7) == "m value=1.0 7"


# ── make_line: string fields ──────────────────────────────────────────────────────────────────────
#
# With `write_strings` on, the field value is the MQTT payload itself, as untrusted as the topic.
# Line protocol quotes a string field and escapes the backslash and the double quote inside it; the
# newline is escaped too, or a payload could write into measurements nobody configured.

def test_a_string_value_is_written_as_a_quoted_field():
    """The whole point of the feature, as InfluxDB receives it."""
    assert make_line("data/gcode/status", "Printing", 5) == 'data/gcode/status value="Printing" 5'


def test_a_numeric_value_is_still_written_unquoted():
    """A number is written unquoted, as a float field.

    InfluxDB fixes a field's type on the first write, so a quoted number would be refused.
    """
    assert make_line("m", 1.5, 5) == "m value=1.5 5"
    assert make_line("m", 10, 5) == "m value=10 5"
    assert make_line("m", 0.0, 5) == "m value=0.0 5"


def test_a_double_quote_inside_a_string_value_is_escaped():
    """Unescaped, it closes the field early and the rest of the payload is parsed as syntax."""
    assert make_line("m", 'say "hi"', 5) == 'm value="say \\"hi\\"" 5'


def test_a_backslash_inside_a_string_value_is_escaped():
    assert make_line("m", "a\\b", 5) == 'm value="a\\\\b" 5'


def test_a_newline_inside_a_string_value_is_escaped():
    assert make_line("m", "a\nb", 5) == 'm value="a\\nb" 5'


def test_a_carriage_return_inside_a_string_value_is_escaped():
    assert make_line("m", "a\rb", 5) == 'm value="a\\rb" 5'


def test_the_backslash_in_a_string_value_is_escaped_before_the_quote():
    # Input is the four characters  a \ " b. Escaping the backslash last would double the one the
    # quote rule inserts and close the field early; only an input with both characters tells the
    # two orders apart.
    assert make_line("m", 'a\\"b', 5) == 'm value="a\\\\\\"b" 5'


def test_a_newline_in_a_string_payload_cannot_inject_a_second_record():
    """A raw newline in a string payload would end the record and start one the publisher wrote.

    `write_batch` joins the batch with "\\n".
    """
    line = make_line("m", "x\nspoofed value=1 1", 5)

    assert "\n" not in line
    assert line == 'm value="x\\nspoofed value=1 1" 5'


# ── contains_excluded: the topic filter ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("substring", EXCLUDED_SUBSTRINGS)
def test_every_listed_substring_really_filters_a_topic(substring):
    """Every entry in EXCLUDED_SUBSTRINGS filters a topic that contains it.

    Parametrised over the constant, so a new entry is covered as soon as it is added.
    """
    assert contains_excluded("/devices/x{}/controls/y".format(substring)) is True


def test_a_topic_matching_nothing_in_the_list_is_kept():
    """The other half of the filter: it lets the actual measurements through.

    An empty string in the list would match every topic and drop the whole stream.
    """
    assert contains_excluded("/devices/wb-msw-v3/controls/Temperature") is False


def test_the_filter_matches_a_substring_anywhere_not_just_a_prefix():
    """`/meta/` and `_version` are mid-topic by construction; a prefix match would never fire."""
    assert contains_excluded("smoke//devices/relay/meta/error") is True
    assert contains_excluded("/devices/system/controls/fw_version") is True


# ── to_number: what counts as a measurement ───────────────────────────────────────────────────────

def test_a_boolean_becomes_ten_or_zero():
    """10 rather than 1 so that a switch drawn on the same axis as a percentage stays visible.

    The bool branch comes first: bool is a subclass of int, and the int branch would store 1.0/0.0.
    """
    assert to_number(True) == 10
    assert to_number(False) == 0


def test_the_strings_true_and_false_are_read_as_booleans_whatever_their_case():
    """A text payload `true` or `false`, in any case, counts as a boolean.

    Without this branch the string fails float() and the topic writes no points at all.
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
    """A text payload is dropped with None, not an exception.

    An exception would reach the outer handler in `_process_forever` and cost a five-second
    reconnect, during which the broker publishes to a subscriber that is not there.
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
    """The default rounding stores anything below 0.005 in magnitude as 0.0; it is not dropped.

    Lossy and silent: such a sensor draws a flat zero line. A stream that needs small values sets
    value_precision.
    """
    assert to_number(0.001) == 0.0
    assert to_number(0.004) == 0.0
    assert to_number(1e-9) == 0.0


@pytest.mark.parametrize("value", [
    float("nan"), float("inf"), float("-inf"),   # json.loads accepts NaN and Infinity by default
    "nan", "NaN", "inf", "-inf", "Infinity",     # and float() parses all of these from a payload
    1e400,                                       # overflows to inf on the way in
])
def test_a_value_influxdb_cannot_parse_is_dropped_like_a_status_string(value):
    """NaN and the infinities are dropped: `value=nan` makes InfluxDB 1.x reject the whole body.

    A probe with no reading publishes "nan", which would cost every point batched with it.
    """
    assert to_number(value) is None


def test_an_integer_too_large_for_a_float_is_dropped_rather_than_raising():
    """json.loads() parses an integer of any length, and float() raises OverflowError on one.

    Uncaught, it would cost the stream a five-second reconnect.
    """
    assert to_number(10 ** 400) is None


def test_a_bool_yields_a_python_int_while_a_number_yields_a_float():
    """A bool yields an int and a number a float, so the body reads `value=10` or `value=1.5`.

    Both are float fields to InfluxDB 1.x, which reserves integers for the `i` suffix that
    make_line never emits, so the difference shows only in the body and on the /ws feed.
    """
    assert type(to_number(True)) is int
    assert type(to_number(1.5)) is float
    # And the difference as InfluxDB actually receives it — no `i` suffix on either.
    assert make_line("m", to_number(True), 1) == "m value=10 1"
    assert make_line("m", to_number(1.5), 1) == "m value=1.5 1"


# ── value_precision: how many decimals reach InfluxDB ─────────────────────────────────────────────
#
# One stream can ask for other rounding without any other stream noticing, so half of this section
# checks the setting and half checks that a stream that sets nothing still gets two decimals.

def test_the_default_is_still_two_decimals_when_no_precision_is_given():
    """With no precision given, a number is rounded to two decimals.

    Existing streams set none, so another default would silently change their series mid-way.
    """
    assert to_number(10.74805) == 10.75
    assert to_number(5.678) == 5.68
    assert list(flatten_payload("devices/x", {"temp": 10.74805})) == [("devices/x/temp", 10.75)]


def test_a_stream_can_ask_for_more_decimals():
    assert to_number(10.74805, 5) == 10.74805
    assert to_number(10.83213, 5) == 10.83213
    assert to_number(1.00722098, 8) == 1.00722098


def test_a_precision_of_none_stores_the_number_exactly_as_it_arrived():
    """With precision None, a number that came out of json.loads() is written untouched."""
    assert to_number(0.00003618, None) == 0.00003618
    assert to_number(10.74805, None) == 10.74805
    assert to_number(1.00722098, None) == 1.00722098
    assert to_number(1125.12, None) == 1125.12


def test_a_fixed_precision_cannot_rescue_a_calibration_coefficient():
    """More decimals are no substitute for no rounding on a small coefficient.

    0.00003618 is stored as 0.0 at two decimals and as 4e-05 (a 10% error) at five; the precision it
    needs depends on a magnitude nobody controls.
    """
    assert to_number(0.00003618) == 0.0
    assert to_number(0.00003618, 5) == 4e-05
    assert to_number(0.00003618, 8) == 0.00003618
    assert to_number(0.00003618, None) == 0.00003618


def test_the_two_booleans_ignore_the_precision_completely():
    """10 and 0 are markers, not readings: no precision rounds them or turns them into floats.

    `_to_storable_float` would turn `value=10` into `value=10.0`.
    """
    for precision in (DEFAULT_VALUE_PRECISION, 0, 5, 8, None):
        assert to_number(True, precision) == 10
        assert to_number(False, precision) == 0
        assert type(to_number(True, precision)) is int
        assert to_number("true", precision) == 10
        assert to_number("FALSE", precision) == 0

    assert make_line("m", to_number(True, None), 1) == "m value=10 1"


def test_a_numeric_string_takes_the_same_precision_as_a_number():
    """Whether a device quotes its reading in JSON must not decide how much of it is kept."""
    assert to_number("10.74805") == 10.75
    assert to_number("10.74805", 5) == 10.74805
    assert to_number("10.74805", None) == 10.74805
    assert to_number("0.00003618", None) == 0.00003618
    assert to_number("-4.5", None) == -4.5


def test_whole_numbers_come_through_every_precision_unchanged():
    """Nothing to round away, so every setting has to agree — including the no-rounding one."""
    for precision in (DEFAULT_VALUE_PRECISION, 0, 8, None):
        assert to_number(12, precision) == 12.0
        assert to_number("12", precision) == 12.0
        assert to_number(-7, precision) == -7.0
        assert type(to_number(12, precision)) is float


def test_a_precision_of_zero_rounds_to_whole_numbers():
    """0 is a real setting, whole numbers, and not a synonym for "unset"; null means unset."""
    assert to_number(10.74805, 0) == 11.0
    assert to_number(5.4, 0) == 5.0


@pytest.mark.parametrize("value", [
    float("nan"), float("inf"), float("-inf"),
    "nan", "Infinity",
    1e400,
])
def test_a_value_influxdb_cannot_parse_is_still_dropped_when_nothing_is_rounded(value):
    """The `math.isfinite` check also runs on the no-rounding path."""
    assert to_number(value, None) is None


def test_an_integer_too_large_for_a_float_is_still_dropped_when_nothing_is_rounded():
    """The OverflowError comes from `float()`, which runs before the rounding either way."""
    assert to_number(10 ** 400, None) is None
    assert to_number(10 ** 400, 8) is None


def test_a_precision_that_makes_the_rounding_overflow_drops_the_value_instead_of_raising():
    """`round()` raises OverflowError too, which is why the call sits inside the same try.

    `round(1.7e308, -308)` overflows. resolve_value_precision() never returns a negative, but the
    three functions take a precision from any caller.
    """
    assert _to_storable_float(1.7e308, -308) is None
    assert to_number(1.7e308, -308) is None
    assert list(flatten_payload("ue/932c8c4aea2f", {"v": 1.7e308}, -308)) == []


@pytest.mark.parametrize("value", [
    float("nan"), float("inf"), float("-inf"),
    "nan", "Infinity",
    1e400,
])
@pytest.mark.parametrize("precision", [None, 0, 2, 8])
def test_the_finite_check_still_runs_after_the_rounding_guard(value, precision):
    """`math.isfinite` runs after the try block, at every precision including no rounding.

    `round(inf, 2)` is inf and does not raise, so nothing in the try catches NaN or the infinities.
    """
    assert _to_storable_float(value, precision) is None


def test_the_precision_reaches_every_leaf_of_a_nested_document():
    payload = {
        "sensor": {"adc_raw": 10.74805, "calibration_coeff_a": 0.00003618},
        "kiln_temp": 1125.12,
    }

    assert sorted(flatten_payload("ue/932c8c4aea2f", payload, None)) == [
        ("ue/932c8c4aea2f/kiln_temp", 1125.12),
        ("ue/932c8c4aea2f/sensor/adc_raw", 10.74805),
        ("ue/932c8c4aea2f/sensor/calibration_coeff_a", 0.00003618),
    ]
    assert sorted(flatten_payload("ue/932c8c4aea2f", payload)) == [
        ("ue/932c8c4aea2f/kiln_temp", 1125.12),
        ("ue/932c8c4aea2f/sensor/adc_raw", 10.75),
        ("ue/932c8c4aea2f/sensor/calibration_coeff_a", 0.0),
    ]


def test_no_rounding_renders_a_small_coefficient_as_scientific_notation():
    """`str(3.618e-05)` is not `0.00003618`, and the body carries the scientific form.

    InfluxDB 1.x parses a float field with Go's ParseFloat, which accepts it.
    """
    assert make_line("m", to_number(0.00003618, None), 1) == "m value=3.618e-05 1"
    assert make_line("m", to_number(10.74805, None), 1) == "m value=10.74805 1"


# ── resolve_value_precision: three config states, two rounding arguments ──────────────────────────

def test_an_unset_precision_resolves_to_the_historical_two_decimals():
    """Both spellings of "unset", an absent key and an explicit null, land on the same default."""
    assert resolve_value_precision(None) == DEFAULT_VALUE_PRECISION
    assert resolve_value_precision(None) == 2


def test_a_negative_precision_resolves_to_no_rounding_at_all():
    """Any negative precision means no rounding; -1 is the spelling the UI writes."""
    assert resolve_value_precision(RAW_VALUE_PRECISION) is None
    assert resolve_value_precision(-1) is None
    assert resolve_value_precision(-7) is None


def test_a_non_negative_precision_is_passed_through_as_the_number_of_decimals():
    assert resolve_value_precision(0) == 0
    assert resolve_value_precision(2) == 2
    assert resolve_value_precision(5) == 5
    assert resolve_value_precision(8) == 8


@pytest.mark.parametrize("configured", [2.0, "2", "", "eight", [], {}])
def test_a_precision_that_is_not_an_integer_falls_back_to_the_default(configured, caplog):
    """A non-integer precision falls back to the default, with a warning naming the stream.

    `round(x, 2.0)` is a TypeError, which would cost a reconnect per message.
    """
    with caplog.at_level(logging.WARNING, logger="src.influx_writer"):
        assert resolve_value_precision(configured, "stream-id") == DEFAULT_VALUE_PRECISION

    assert "value_precision" in caplog.text
    assert "stream-id" in caplog.text


def test_a_boolean_precision_is_rejected_rather_than_read_as_one_decimal(caplog):
    """bool is a subclass of int, so `round(1.234, True)` is a legal call that rounds to 1 decimal.

    Without an explicit bool check before the int check, `"value_precision": true` would be accepted
    as a request for one decimal instead of reported as the mistake it is.
    """
    with caplog.at_level(logging.WARNING, logger="src.influx_writer"):
        assert resolve_value_precision(True) == DEFAULT_VALUE_PRECISION
        assert resolve_value_precision(False) == DEFAULT_VALUE_PRECISION

    assert "value_precision" in caplog.text


def test_the_resolved_none_is_never_handed_to_round():
    """No rounding is a branch that skips round(): `round(1.234, None)` returns the int 1."""
    assert round(1.234, None) == 1
    assert type(round(1.234, None)) is int
    # The real path, through the same resolved None:
    assert to_number(1.234, resolve_value_precision(-1)) == 1.234
    assert type(to_number(1.234, resolve_value_precision(-1))) is float


# ── flatten_payload: JSON document to topic/value pairs ───────────────────────────────────────────

def test_a_scalar_payload_yields_one_pair_named_after_the_topic():
    assert list(flatten_payload("devices/temp", 21.5)) == [("devices/temp", 21.5)]


def test_a_nested_object_is_expanded_into_a_path_per_leaf():
    """One MQTT message carrying a document becomes one point per number inside it."""
    payload = {"outer": {"inner": 1.5}, "flat": 2}

    assert sorted(flatten_payload("devices/x", payload)) == [
        ("devices/x/flat", 2.0),
        ("devices/x/outer/inner", 1.5),
    ]


def test_keys_beginning_with_an_underscore_are_skipped_along_with_everything_under_them():
    """`_`-prefixed keys are a payload's own metadata (`_id`, `_meta`), which changes on every
    message; skipping the key skips its whole subtree."""
    payload = {"temp": 21.0, "_id": 7, "_meta": {"seq": 1, "src": {"deep": 2}}}

    assert list(flatten_payload("devices/x", payload)) == [("devices/x/temp", 21.0)]


def test_the_assembled_topic_is_lowercased_and_has_its_spaces_replaced():
    """Otherwise `Living Room` and `living_room` would be two series, and so would `Temp` and
    `temp`: measurement names are case-sensitive, and a space reaches InfluxDB escaped."""
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
    """One unparseable field must not cost the other fields in the same message."""
    payload = {"temp": 21.0, "status": "online", "name": None, "readings": [1, 2]}

    assert list(flatten_payload("devices/x", payload)) == [("devices/x/temp", 21.0)]


def test_a_json_array_payload_produces_nothing():
    """Lists are not recursed into: `to_number` returns None for one, so nothing is written.

    Silent: the message counts as received, not as filtered.
    """
    assert list(flatten_payload("devices/x", [1, 2, 3])) == []


# ── write_strings: non-numeric payloads as string fields ──────────────────────────────────────────
#
# The flag is per stream and off by default, and its branch only sees what `to_number()` refused,
# so a value the number parser accepts is written the same way with the flag on or off.

def test_a_non_numeric_string_is_still_dropped_when_the_flag_is_off():
    assert list(flatten_payload("data/gcode/status", "Printing")) == []
    assert list(flatten_payload("data/gcode/status", "Printing", write_strings=False)) == []


def test_a_non_numeric_string_is_yielded_when_the_flag_is_on():
    assert list(flatten_payload("data/gcode/status", "Printing", write_strings=True)) == [
        ("data/gcode/status", "Printing")]


def test_the_flag_reaches_a_string_leaf_of_a_nested_document():
    """The recursion has to carry the flag, or only bare scalar payloads ever benefit."""
    payload = {"temp": 21.0, "status": "Printing"}

    assert sorted(flatten_payload("devices/x", payload, write_strings=True)) == [
        ("devices/x/status", "Printing"),
        ("devices/x/temp", 21.0),
    ]


def test_the_string_value_is_kept_verbatim_while_the_topic_is_still_normalised():
    """Only the topic is lowercased and de-spaced — the value is a field, not a measurement name.

    "Printing" and "printing" are different states of a printer, and lowercasing the value would
    merge them in every dashboard downstream with nothing to say it happened.
    """
    assert list(flatten_payload("Data/Gcode Status", "Printing Now", write_strings=True)) == [
        ("data/gcode_status", "Printing Now")]


@pytest.mark.parametrize("flag", [False, True])
def test_an_empty_payload_is_dropped_whether_or_not_the_flag_is_on(flag):
    """Clearing a retained topic publishes zero bytes, and that is not a reading.

    MQTT deletes a retained message by publishing an empty payload to the same topic. Writing it
    would turn every "this value no longer exists" into a data point claiming the state is "".
    """
    assert list(flatten_payload("devices/x/status", "", write_strings=flag)) == []
    assert list(flatten_payload("devices/x", {"status": ""}, write_strings=flag)) == []


@pytest.mark.parametrize("payload, expected", [
    (21.456, 21.46),      # a number, still rounded to the stream's precision
    (21, 21.0),
    ("12", 12.0),         # a numeric string, still parsed rather than stored as text
    ("-4.5", -4.5),
    (True, 10),           # the bridge's own markers, still 10 and 0 and still ints
    (False, 0),
    ("true", 10),
    ("TRUE", 10),
    ("false", 0),
    ("False", 0),
])
def test_a_value_the_number_parser_accepts_is_untouched_by_the_flag(payload, expected):
    """A value `to_number()` accepts is written the same way with the flag on.

    The string branch only sees what `to_number()` refused; otherwise `"12"` would be written as a
    string into a float-typed field, and InfluxDB refuses the whole batch over that.
    """
    off = list(flatten_payload("devices/x", {"v": payload}))
    on = list(flatten_payload("devices/x", {"v": payload}, write_strings=True))

    assert off == on == [("devices/x/v", expected)]
    # And the same Python type, because `10` and `10.0` are different bytes in the request body.
    assert type(on[0][1]) is type(off[0][1])


@pytest.mark.parametrize("payload", ["nan", "NaN", "inf", "-inf", "Infinity", "1e400", "9" * 400])
@pytest.mark.parametrize("flag", [False, True])
def test_a_number_the_bridge_refuses_to_store_is_dropped_rather_than_stringified(payload, flag):
    """A number `to_number()` refused (NaN, an infinity, an overflow) is not written as a string.

    These arrive on topics that otherwise carry floats, and a string in a float-typed field is a
    permanent 400 for the whole batch.
    """
    assert list(flatten_payload("devices/x/temp", payload, write_strings=flag)) == []


@pytest.mark.parametrize("value", [None, [1, 2], 21.456, "12", True])
def test_only_a_string_can_take_the_new_path(value):
    """The flag does not make every dropped payload storable: a null and a list stay dropped.

    The numbers and the numeric string are yielded, but as numbers, never as strings.
    """
    yielded = list(flatten_payload("devices/x", {"v": value}, write_strings=True))

    if value is None or isinstance(value, list):
        # Stated as emptiness rather than as "nothing here is a str": for these two the list IS
        # empty, and `all()` over an empty list is true no matter what the branch does.
        assert yielded == []
    else:
        assert all(not isinstance(v, str) for _topic, v in yielded)


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
    # The batch is joined with "\n", so an unescaped newline in a measurement name would close the
    # record and open one the publisher wrote. Three points in, three records out.
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
    """An empty batch is stored without a request: True, so no caller counts it as an error."""
    writer, session = _writer_with_fake_session()

    assert await writer.write_batch([]) is True
    assert session.calls == []


@pytest.mark.parametrize("status", [200, 204])
async def test_the_two_statuses_influxdb_answers_with_both_count_as_written(status):
    """204 is what InfluxDB 1.x returns from /write on success; 200 is what a proxy in front of it
    may normalise that to."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status))

    assert await writer.write_batch([("m", 1.0, 1)]) is True


@pytest.mark.parametrize("status", [400, 401, 404, 413, 500, 503])
async def test_any_other_status_is_a_failed_write(status):
    """Any other status is False: a 401 or a 404 must not look like a stream running cleanly."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status, text="boom"))

    assert await writer.write_batch([("m", 1.0, 1)]) is False


@pytest.mark.parametrize("error", [
    aiohttp.ClientConnectionError("cannot connect to influx.example:8086"),
    TimeoutError(),                       # what the 10s ClientTimeout raises
    aiohttp.ClientPayloadError("truncated response"),
])
async def test_a_network_failure_is_a_failed_write_and_not_an_exception(error):
    """A network error is returned as False, not raised.

    Raised, it would cost the message loop a five-second reconnect. CancelledError is a
    BaseException, so the RETRY_BUDGET timeout and a shutdown still cut a write short.
    """
    writer, _session = _writer_with_fake_session(_ExplodingResponse(error))

    assert await writer.write_batch([("m", 1.0, 1)]) is False


async def test_the_write_is_addressed_to_the_databases_write_endpoint_in_milliseconds():
    """`precision=ms` has to match the timestamps `make_line` is handed.

    Without it InfluxDB reads milliseconds as nanoseconds and files every point in January 1970.
    """
    writer, session = _writer_with_fake_session(host="influx.example", port=8087, database="metrics")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["url"] == "http://influx.example:8087/write"
    assert session.calls[0]["params"] == {"db": "metrics", "precision": "ms"}


async def test_credentials_are_attached_when_a_user_is_configured():
    writer, session = _writer_with_fake_session(user="influx-user", password="influx-secret")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["auth"] == aiohttp.BasicAuth("influx-user", "influx-secret")


# ── write_batch_detailed: which failures are worth repeating ──────────────────────────────────────

@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_a_failure_that_a_second_attempt_could_fix_says_so(status):
    """The database being overloaded, restarting, or behind a proxy that is: the same body sent a
    second later can succeed, and this is the whole reason the retry ladder exists."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status, text="boom"))

    assert await writer.write_batch_detailed([("m", 1.0, 1)]) == (False, True)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 405, 413, 422])
async def test_a_failure_that_will_answer_the_same_way_forever_says_so(status):
    """A rotated password, a dropped database, a NaN the line parser refuses.

    The same body gets the same answer; a retry would only spend RETRY_BUDGET.
    """
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status, text="boom"))

    assert await writer.write_batch_detailed([("m", 1.0, 1)]) == (False, False)


@pytest.mark.parametrize("status", [200, 204])
async def test_a_stored_batch_is_never_offered_for_a_retry(status):
    writer, _session = _writer_with_fake_session(_FakeResponse(status=status))

    assert await writer.write_batch_detailed([("m", 1.0, 1)]) == (True, False)


async def test_an_empty_batch_is_stored_without_a_request_and_without_a_retry():
    writer, session = _writer_with_fake_session()

    assert await writer.write_batch_detailed([]) == (True, False)
    assert session.calls == []


@pytest.mark.parametrize("error", [
    aiohttp.ClientConnectionError("cannot connect to influx.example:8086"),
    TimeoutError(),
    aiohttp.ClientPayloadError("truncated response"),
])
async def test_a_network_failure_is_always_worth_repeating(error):
    """Nothing about the request was ever judged: it did not arrive."""
    writer, _session = _writer_with_fake_session(_ExplodingResponse(error))

    assert await writer.write_batch_detailed([("m", 1.0, 1)]) == (False, True)


async def test_write_batch_stays_a_plain_bool_over_the_detailed_answer():
    """The shutdown flush and the writer doubles in the suite expect a plain bool."""
    writer, _session = _writer_with_fake_session(_FakeResponse(status=204))
    assert await writer.write_batch([("m", 1.0, 1)]) is True

    writer, _session = _writer_with_fake_session(_FakeResponse(status=401, text="nope"))
    assert await writer.write_batch([("m", 1.0, 1)]) is False


# ── a cancellation is not a failed write ──────────────────────────────────────────────────────────

async def test_a_cancellation_leaves_the_real_writer_instead_of_becoming_a_failed_write():
    """A cancelled write raises CancelledError instead of answering (False, ...).

    `_flush` has to see the cancellation to put the in-flight batch back for the final flush. The
    processor tests use a FakeWriter, so this runs real aiohttp against a loopback server that
    accepts the connection and answers nothing.
    """
    accepted = asyncio.Event()
    peers = []

    async def hold(reader, peer):
        # Answer nothing and keep the socket open: returning from the handler closes it, aiohttp
        # reads a disconnect and the write completes as a failure — the opposite of the state under
        # test. `read()` returns when the client goes away, which is this handler's own cleanup.
        peers.append(peer)
        accepted.set()
        await reader.read()

    server = await asyncio.start_server(hold, "127.0.0.1", 0)
    writer = InfluxWriter(
        host="127.0.0.1", port=server.sockets[0].getsockname()[1],
        user="", password="", database="db", stream_id="s")
    await writer.start()
    try:
        write = asyncio.create_task(writer.write_batch_detailed([("m", 1.0, 1)]))
        await asyncio.wait_for(accepted.wait(), timeout=5)

        write.cancel()

        # The assertion is the SHAPE of the exit, not a value. A `(False, True)` here is a
        # cancellation that was swallowed and reported as an ordinary failed write.
        with pytest.raises(asyncio.CancelledError):
            await write
    finally:
        await writer.stop()
        for peer in peers:
            peer.close()
        server.close()
        await server.wait_closed()


async def test_no_auth_header_is_sent_when_no_user_is_configured():
    """An unauthenticated InfluxDB is the common home setup, and `BasicAuth("", "")` is not the same
    as no auth at all — it sends an `Authorization` header carrying an empty username, which an
    InfluxDB with authentication switched on rejects with a 401 rather than ignoring."""
    writer, session = _writer_with_fake_session(user="", password="")

    assert await writer.write_batch([("m", 1.0, 1)]) is True

    assert session.calls[0]["auth"] is None
