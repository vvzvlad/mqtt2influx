#!/usr/bin/env python3
"""What the one-file web UI must not stop doing — asserted against its source text.

`static/index.html` is the whole UI, markup and script in one file, and there is no JS runtime
anywhere in this project: the suite runs under pytest and CI runs it inside `python:3.11-slim`, the
same base the application image is built from. So these tests read the file and pin the few lines
whose absence is SILENT — a form that sends a precision the operator did not type saves without an
error, closes the modal and looks exactly like a form that worked.

They are string assertions, which is a weaker instrument than executing the function would be, and
they are written narrowly to make up for it: each one names one mechanism and goes red when that
mechanism is removed, rather than matching a shape that a rewrite would keep by accident.
"""

import os
import re

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(BASE_DIR, "static", "index.html")


@pytest.fixture(scope="module")
def ui():
    with open(INDEX_HTML, encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def save_stream_body(ui):
    """The source of `saveStream()` alone, so a match cannot come from somewhere else in the file.

    Sliced from its signature to the next top-level function declaration, which is what the file's
    layout has always been: every function in the script block starts in column zero.
    """
    start = ui.index("async function saveStream()")
    following = re.compile(r"^(?:async )?function ", re.M).search(ui, start + 1)
    return ui[start:following.start() if following else len(ui)]


def test_the_precision_input_declares_its_lower_bound(ui):
    """`min="-1"` is what stops the spinner and the browser's own validation below -1.

    It is a hint and not a guarantee — nothing here calls `checkValidity()`, and a value can always
    be typed or pasted past it — which is why the guard in `saveStream()` exists as well. Both, not
    either: the attribute is what a browser shows the operator before they hit Save.
    """
    tag = re.search(r'<input[^>]*id="fValuePrecision"[^>]*>', ui)
    assert tag, "the value-precision input is gone from the form"
    assert 'type="number"' in tag.group(0)
    assert 'step="1"' in tag.group(0)
    assert 'min="-1"' in tag.group(0)


def test_the_precision_is_read_with_number_and_not_with_parseint(save_stream_body):
    """`parseInt` is the trap: it answers a typed `0.5` with 0 and a typed `1e-5` with 1.

    Both are numbers the input accepts (`step="1"` is a stepMismatch, not a parse error, and the
    browser hands them back through `.value` unchanged), and both come out of `parseInt` as a
    DIFFERENT, perfectly valid precision — rounding to whole numbers, and to one decimal, on a
    stream whose operator asked for neither. `Number()` keeps the value as entered so the guard
    below has something real to refuse.
    """
    assert "Number(precisionRaw)" in save_stream_body
    assert "parseInt(precisionRaw" not in save_stream_body


def test_a_precision_that_is_not_a_whole_number_never_reaches_the_request(save_stream_body):
    """The guard, and the two things that make it a guard: it alerts and it returns."""
    guard = re.search(
        r"if \(valuePrecision !== null && !Number\.isInteger\(valuePrecision\)\) \{(.*?)\n  \}",
        save_stream_body, re.S)
    assert guard, "the whole-number check on value_precision is gone from saveStream()"
    assert "alert(" in guard.group(1), "a refusal the operator cannot see is not a refusal"
    assert "return;" in guard.group(1), "without the return the bad value is sent anyway"
    # And it runs before the request rather than after it.
    assert save_stream_body.index("Number.isInteger(valuePrecision)") < save_stream_body.index("fetch(")


def test_the_edit_form_still_prefills_a_precision_of_zero(ui):
    """`??` and not `||`, because 0 is a legitimate precision — round to whole numbers.

    With `||` the box would come up EMPTY for a stream configured with 0, and saving the form
    without touching it would send null and reset that stream to the default two decimals. Pinned
    here because it is a one-character regression that no other test in this suite would notice.
    """
    assert "f.value_precision ?? ''" in ui
    assert "f.value_precision ||" not in ui
