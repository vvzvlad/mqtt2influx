#!/usr/bin/env python3
"""What the one-file web UI must not stop doing — asserted against its source text.

`static/index.html` is the whole UI, and CI runs this suite in `python:3.11-slim`, which has no JS
runtime. So these tests read the file and pin the few mechanisms whose absence is silent: a form
that sends a precision the operator did not type still saves, closes the modal and looks fine.

Identifiers are discovered from the source and blocks are found by matching brackets, so a rename
or a reformat leaves the tests green and removing a mechanism turns them red.
"""

import os
import re

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(BASE_DIR, "static", "index.html")

# Each of these turns a typed `0.5` into a different, valid precision. `Number()` keeps it `0.5`,
# which the API refuses with 422.
TRUNCATORS = ("Math.trunc", "Math.round", "Math.floor", "Math.ceil",
              "parseInt", "toFixed", "|0", "| 0", "~~", ">>0", ">> 0", ">>>0", ">>> 0")


def _strip_comments(js):
    """Blank out `//` comments, so an assertion about the code cannot be satisfied by prose.

    Quote-aware, because the same lines build URLs and template literals.
    """
    out = []
    for line in js.split("\n"):
        quote = None
        i = 0
        while i < len(line):
            ch = line[i]
            if quote:
                if ch == "\\":
                    i += 2
                    continue
                if ch == quote:
                    quote = None
            elif ch in "'\"`":
                quote = ch
            elif ch == "/" and line[i + 1:i + 2] == "/":
                line = line[:i]
                break
            i += 1
        out.append(line)
    return "\n".join(out)


def _balanced(code, start, opener="{", closer="}"):
    """`code` from the first `opener` at or after `start` through the `closer` that matches it.

    Used instead of a regex so that moving a `{` to the next line, or reindenting a block, is not a
    test failure — only removing what is inside it is.
    """
    begin = code.index(opener, start)
    depth = 0
    for i in range(begin, len(code)):
        if code[i] == opener:
            depth += 1
        elif code[i] == closer:
            depth -= 1
            if depth == 0:
                return code[begin:i + 1]
    raise AssertionError("unbalanced {!r} in saveStream()".format(opener))


def _request_field(code, field):
    """The expression an object literal assigns to `field`, however it is written or wrapped."""
    at = code.index(field + ":")
    start = at + len(field) + 1
    depth = 0
    for i in range(start, len(code)):
        ch = code[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                return code[start:i]
            depth -= 1
        elif ch == "," and depth == 0:
            return code[start:i]
    raise AssertionError("{} has no end in the request body".format(field))


def _statement_defining(code, name):
    """The whole `const <name> = ...;` statement, wherever it sits and however it is spaced."""
    match = re.search(r"(?:const|let|var)\s+" + re.escape(name) + r"\b\s*=\s*[^;]*;", code)
    assert match, "saveStream() no longer defines {}".format(name)
    return match.group(0)


def _tests_for_an_empty_string(expr, name):
    """Does `expr` compare `name` against an empty string literal, in either order?"""
    ident = re.escape(name)
    return bool(re.search(r"\b" + ident + r"\b\s*===\s*(['\"])\1", expr)
                or re.search(r"(['\"])\1\s*===\s*\b" + ident + r"\b", expr))


def _function_source(ui, declaration):
    """One top-level function's source, from its declaration to the next one.

    Same slicing rule as `save_stream_body` below, and it relies on the same property of this file:
    every function in the script block starts in column zero.
    """
    start = ui.index(declaration)
    following = re.compile(r"^(?:async )?function ", re.M).search(ui, start + 1)
    return ui[start:following.start() if following else len(ui)]


@pytest.fixture(scope="module")
def ui():
    with open(INDEX_HTML, encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def save_stream_body(ui):
    """The source of `saveStream()` alone, so a match cannot come from somewhere else in the file.

    Sliced from its signature to the next top-level function declaration; every function in the
    script block starts in column zero.
    """
    start = ui.index("async function saveStream()")
    following = re.compile(r"^(?:async )?function ", re.M).search(ui, start + 1)
    return ui[start:following.start() if following else len(ui)]


@pytest.fixture(scope="module")
def code(save_stream_body):
    """`saveStream()` with its commentary removed — the part that actually runs."""
    return _strip_comments(save_stream_body)


def precision_identifiers(code):
    """The two identifiers the precision travels through, read out of the source, not assumed.

    Renaming a local variable is then a refactor rather than a failure, and the suite cannot pass
    just because the word `precisionRaw` still appears somewhere in the file.
    """
    read = re.search(
        r"(?:const|let|var)\s+(\w+)\s*=\s*document\.getElementById\(\s*(['\"])fValuePrecision\2\s*\)",
        code)
    assert read, "saveStream() no longer reads the value-precision input"
    raw = read.group(1)
    parsed = re.search(
        r"(?:const|let|var)\s+(\w+)\s*=\s*[^;]*\b" + re.escape(raw) + r"\b[^;]*;", code[read.end():])
    assert parsed, "nothing in saveStream() turns the value-precision box into a number"
    return raw, parsed.group(1)


@pytest.fixture(scope="module")
def precision_names(code):
    return precision_identifiers(code)


def test_the_precision_input_declares_its_lower_bound(ui):
    """`min="-1"` stops the spinner and the browser's own validation below -1.

    It is a hint, not a guarantee: nothing calls `checkValidity()`, and the API validates the value.
    """
    tag = re.search(r"""<input[^>]*id=['"]fValuePrecision['"][^>]*>""", ui)
    assert tag, "the value-precision input is gone from the form"
    for attr, value in (("type", "number"), ("step", "1"), ("min", "-1")):
        assert re.search(attr + r"""=['"]""" + value + r"""['"]""", tag.group(0)), \
            "the value-precision input lost {}={}".format(attr, value)


def test_the_precision_is_read_with_number_and_not_with_parseint(code, precision_names):
    """Nothing on the path from the box to the request may round or truncate.

    The box hands back a typed `0.5` or `1e-5` unchanged (`step="1"` is only a validity hint).
    `parseInt` reads them as 0 and 1, both precisions the API stores; `Number()` keeps them, so the
    API refuses them with 422. Checked as the absence of every truncating operator, because
    `Math.trunc(Number(raw))` still contains `Number(raw)`.
    """
    raw, parsed = precision_names
    path = "\n".join([_statement_defining(code, raw),
                      _statement_defining(code, parsed),
                      _request_field(code, "value_precision")])
    for truncator in TRUNCATORS:
        assert truncator not in path, \
            "{} is back on the path from the precision box to the request".format(truncator)


def test_an_empty_precision_box_becomes_null_where_it_is_read(code, precision_names):
    """An empty box must become null where it is read.

    `Number('')` is 0, not NaN, so without this branch an untouched box sends `value_precision: 0`,
    which the API accepts and which rounds every value the stream writes to a whole number.
    """
    raw, parsed = precision_names
    parse = _statement_defining(code, parsed)
    assert _tests_for_an_empty_string(parse, raw), \
        "the empty-box branch is gone from the read: Number('') is 0, and 0 is a precision"
    assert "null" in parse, "the empty box must become null, which is what means 'not configured'"


def test_the_request_sends_the_precision_that_was_read(code, precision_names):
    """`value_precision` must carry the number this function parsed, not a constant.

    The failure is invisible from the outside: the form saves, the modal closes, and every stream
    reverts to the default two decimals on every edit.
    """
    raw, parsed = precision_names
    sent = _request_field(code, "value_precision")
    assert re.search(r"\b" + re.escape(parsed) + r"\b", sent), \
        "value_precision no longer carries the number saveStream() read from the form"


def test_a_refused_save_keeps_the_modal_open(code):
    """A save the server rejected (a 422, a 404, a 500) must not look like a save that worked.

    Closing the modal would discard every other edit made in the same window, with nothing on
    screen to say the stream was never written.
    """
    bound = re.search(r"(?:const|let|var)\s+(\w+)\s*=\s*await\s+fetch\(", code)
    assert bound, "the result of the save request is thrown away again"
    res = bound.group(1)
    check = re.search(r"\b" + re.escape(res) + r"\.ok\b", code)
    assert check, "nothing looks at whether the save succeeded"
    failure = _balanced(code, check.end())
    assert "alert(" in failure, "a failure the operator cannot see is indistinguishable from a save"
    assert re.search(r"\b" + re.escape(res) + r"\.status\b", failure), \
        "the message must name the status, or a 422 and a 500 read the same to whoever reports it"
    assert "detail" in failure, "FastAPI says what was wrong in `detail`; it belongs in the message"
    assert "return;" in failure, "without the return the modal closes anyway"
    assert "closeModal()" not in failure, "the modal must stay open, or the other edits are lost"
    assert check.start() < code.index("closeModal()"), "the modal closes before the check runs"


# ── write_strings: the checkbox, and the value it renders into the log ────────────────────────────

def test_the_string_values_checkbox_is_in_the_form(ui):
    """The only way to turn the setting on without hand-editing streams.json or writing curl."""
    tag = re.search(r"""<input[^>]*id=['"]fWriteStrings['"][^>]*>""", ui)
    assert tag, "the write-strings checkbox is gone from the form"
    assert re.search(r"""type=['"]checkbox['"]""", tag.group(0)), \
        "the write-strings input must be a checkbox, or `.checked` is always undefined"


def test_the_edit_form_prefills_the_checkbox_from_the_stream(ui):
    """Without the prefill the box comes up empty for a stream that has the setting on.

    Every edit of that stream, a rename say, would then silently save `write_strings: false`.
    """
    body = _strip_comments(_function_source(ui, "function openModal("))
    assert re.search(r"""getElementById\(\s*(['"])fWriteStrings\1\s*\)\s*\.checked\s*=""", body), \
        "openModal() no longer prefills the write-strings checkbox"
    assert "write_strings" in body, "the prefill must read the stream's own value"


def test_the_request_sends_the_checkbox_state(code):
    """`write_strings` must carry what the box says, not a constant and not a string.

    The API answers anything that is not a real bool with 422.
    """
    sent = _request_field(code, "write_strings")
    assert "fWriteStrings" in sent, "the request no longer reads the write-strings checkbox"
    assert ".checked" in sent, "`.checked` is what makes it a real boolean for the API"


def test_the_log_value_is_html_escaped_like_the_topic_beside_it(ui):
    """With string values on, a log line renders whatever was published to the broker.

    Unescaped, a payload of `<img src=x onerror=...>` on any subscribed topic runs script in the
    page of whoever has the UI open.
    """
    body = _strip_comments(_function_source(ui, "function appendLogEntryDOM("))
    assert "innerHTML" in body, "the log row is built some other way now; re-check the escaping"
    assert re.search(r"escHtml\(\s*entry\.value\s*\)", body), \
        "the log value is no longer escaped; an MQTT payload can inject markup into the page"
    assert not re.search(r"\$\{\s*entry\.value\s*\}", body), \
        "the log value is still interpolated raw somewhere in the row"


def test_the_edit_form_still_prefills_a_precision_of_zero(ui):
    """`??` and not `||`, because 0 is a legitimate precision — round to whole numbers.

    With `||` the box comes up empty for a stream configured with 0, and saving the untouched form
    sends null and resets that stream to the default two decimals.
    """
    assert "f.value_precision ?? ''" in ui
    assert "f.value_precision ||" not in ui
