#!/usr/bin/env python3
"""Line-protocol formatting.

`make_line()` builds the measurement half of an InfluxDB line-protocol record out of an MQTT topic —
and, via `flatten_payload()`, out of the KEYS of whatever JSON arrived on that topic. Both are
supplied by whoever can publish to the broker, so every character with a meaning in line protocol
has to be neutralised here or it is neutralised nowhere.
"""

from src.influx_writer import InfluxWriter, make_line


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


class _FakeResponse:
    status = 204

    async def text(self):
        return ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _FakeSession:
    """Captures the request body instead of sending it."""

    def __init__(self):
        self.bodies = []

    def post(self, url, params=None, data=None, auth=None, timeout=None):
        self.bodies.append(data)
        return _FakeResponse()


def _writer_with_fake_session():
    writer = InfluxWriter(host="h", port=8086, user="", password="", database="db", stream_id="s")
    session = _FakeSession()
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
