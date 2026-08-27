#!/usr/bin/env python3
"""What the one-file web UI must not stop doing — asserted against its source text.

`static/index.html` is the whole UI, markup and script in one file, and CI runs this suite inside
`python:3.11-slim` — the same base the application image is built from — where there is no JS
runtime at all. So these tests read the file and pin the few mechanisms whose absence is SILENT: a
form that sends a precision the operator did not type saves without an error, closes the modal and
looks exactly like a form that worked.

They are string assertions, which is a weaker instrument than executing the function would be, and
the way they make up for it is by naming MECHANISMS rather than spellings. Every identifier they
work with — the raw box value, the parsed number, the fetch result — is discovered from the source
instead of being written down here, and every block they read is found by matching brackets rather
than by matching indentation. A rename or a reformat therefore leaves them green; removing one of
the checks they name turns them red.

`tests/test_static_ui_behaviour.py` runs the same script for real under `node` and asserts on what
`saveStream()` actually sends. That is the stronger instrument, and it is also the one CI cannot
run — so anything that must hold in CI is pinned here as well.
"""

import os
import re

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(BASE_DIR, "static", "index.html")

# Every way this file could go back to throwing away the fractional part of what the operator typed.
# `Number()` is the only reading of the box that keeps `0.5` a `0.5` for the guard to refuse; each of
# these turns it into a different, perfectly valid precision and saves without a word.
TRUNCATORS = ("Math.trunc", "Math.round", "Math.floor", "Math.ceil",
              "parseInt", "toFixed", "|0", "| 0", "~~", ">>0", ">> 0", ">>>0", ">>> 0")


def _strip_comments(js):
    """Blank out `//` comments, so an assertion about the CODE cannot be satisfied by prose.

    The comments in `saveStream()` discuss `parseInt`, 422 and closing the modal by name, and a test
    that greps the function as a whole would happily read a mechanism out of a paragraph explaining
    why that mechanism was removed. Quote-aware, because the same lines build URLs and template
    literals.
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


@pytest.fixture(scope="module")
def code(save_stream_body):
    """`saveStream()` with its commentary removed — the part that actually runs."""
    return _strip_comments(save_stream_body)


def precision_identifiers(code):
    """The two identifiers the precision travels through, read out of the source, not assumed.

    Every test below works through these, which is what makes renaming a local variable a refactor
    rather than a failure — and what stops the suite from passing because the WORD `precisionRaw`
    still appears somewhere in the file. `test_static_ui_behaviour.py` imports this so that the two
    files cannot drift into disagreeing about which statement is which.
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
    """`min="-1"` is what stops the spinner and the browser's own validation below -1.

    It is a hint and not a guarantee — nothing here calls `checkValidity()`, and a value can always
    be typed or pasted past it — which is why the guard in `saveStream()` exists as well. Both, not
    either: the attribute is what a browser shows the operator before they hit Save.
    """
    tag = re.search(r"""<input[^>]*id=['"]fValuePrecision['"][^>]*>""", ui)
    assert tag, "the value-precision input is gone from the form"
    for attr, value in (("type", "number"), ("step", "1"), ("min", "-1")):
        assert re.search(attr + r"""=['"]""" + value + r"""['"]""", tag.group(0)), \
            "the value-precision input lost {}={}".format(attr, value)


def test_the_precision_is_read_with_number_and_not_with_parseint(code, precision_names):
    """Nothing on the path from the box to the request may round, truncate or reinterpret.

    `parseInt` was the original trap: it answers a typed `0.5` with 0 and a typed `1e-5` with 1.
    Both are values the input accepts (`step="1"` is a stepMismatch, not a parse error, and the
    browser hands them back through `.value` unchanged), and both come out of `parseInt` as a
    DIFFERENT, perfectly valid precision — rounding to whole numbers, and to one decimal, on a
    stream whose operator asked for neither. `Number()` keeps the value as entered so the guard
    below has something real to refuse.

    Asserted as the ABSENCE of every truncating operator rather than as the presence of the string
    `Number(precisionRaw)`, because that string survives being wrapped: `Math.trunc(Number(raw))`
    still contains it, still passes the guard, and still saves a 0 for a 0.5.
    """
    raw, parsed = precision_names
    path = "\n".join([_statement_defining(code, raw),
                      _statement_defining(code, parsed),
                      _request_field(code, "value_precision")])
    for truncator in TRUNCATORS:
        assert truncator not in path, \
            "{} is back on the path from the precision box to the request".format(truncator)


def test_an_empty_precision_box_becomes_null_where_it_is_read(code, precision_names):
    """The first of the two empty-box checks — and the one whose loss is now catastrophic.

    `Number('')` is 0, not NaN. Before the switch to `Number()` this branch could be lost and the
    NaN check downstream would still land on null, i.e. on the default two decimals; today losing it
    sends `value_precision: 0`, which the API accepts without a warning and which strips the
    fractional part off every value the stream writes from then on.

    Nothing about the RUNTIME behaviour of the form depends on this branch any more — the request
    body checks the same raw string a second time, deliberately, so that neither check alone is
    load-bearing. That is exactly why it has to be pinned here: it is now the kind of code a reader
    can delete, run the whole suite, and see nothing happen.
    """
    raw, parsed = precision_names
    parse = _statement_defining(code, parsed)
    assert _tests_for_an_empty_string(parse, raw), \
        "the empty-box branch is gone from the read: Number('') is 0, and 0 is a precision"
    assert "null" in parse, "the empty box must become null, which is what means 'not configured'"


def test_an_empty_precision_box_becomes_null_again_in_the_request(code, precision_names):
    """The second check, and the reason it looks at the raw string rather than at the number.

    `Number('')` is 0, so no test applied AFTER the parse can tell an empty box from an operator who
    typed a zero — a `Number.isFinite()` guard here would pass the 0 straight through. The two
    checks are independent only as long as this one keeps reading the box's own text.
    """
    raw, parsed = precision_names
    sent = _request_field(code, "value_precision")
    assert _tests_for_an_empty_string(sent, raw), \
        "the request body no longer re-checks the empty box; the read is a single point of failure"
    assert "null" in sent, "the second check must land on null too, or it is not the same check"


def test_the_request_sends_the_precision_that_was_read(code, precision_names):
    """`value_precision` must carry the number this function parsed, not a constant.

    Pinned because the failure is invisible from the outside: the form saves, the modal closes, the
    list redraws, and every stream quietly reverts to the default two decimals on every edit.
    """
    raw, parsed = precision_names
    sent = _request_field(code, "value_precision")
    assert re.search(r"\b" + re.escape(parsed) + r"\b", sent), \
        "value_precision no longer carries the number saveStream() read from the form"


def test_a_precision_that_is_not_a_whole_number_never_reaches_the_request(code, precision_names):
    """The guard, and the three things that make it a guard: it alerts, it returns, it runs first.

    Located through the check it performs and read out by matching braces, so that moving the `{`,
    reindenting the block or reordering the condition is a refactor and not a failure.
    """
    raw, parsed = precision_names
    check = re.search(r"Number\.isSafeInteger\(\s*" + re.escape(parsed) + r"\s*\)", code)
    assert check, "the whole-number check on value_precision is gone from saveStream()"
    block = _balanced(code, check.end())
    assert "alert(" in block, "a refusal the operator cannot see is not a refusal"
    assert "return;" in block, "without the return the bad value is sent anyway"
    # And it runs before the request rather than after it.
    assert check.start() < code.index("fetch("), "the guard runs after the request it is guarding"


def test_the_guard_means_by_integer_what_the_api_means(code, precision_names):
    """`Number.isSafeInteger`, not `Number.isInteger` — above 2^53 the two ends disagree.

    `Number.isInteger(1e21)` is true, so `1e21` typed into the box used to pass the guard; but
    `JSON.stringify` writes it as `1e+21` and Python's `json.loads` reads that back as a float, so
    the API refuses it with 422. The operator saw a form that closed and a stream that had not been
    written. Narrowing the guard removes the disagreement at the only place where it can still be
    explained to the person who typed the number.
    """
    raw, parsed = precision_names
    assert re.search(r"Number\.isSafeInteger\(\s*" + re.escape(parsed) + r"\s*\)", code), \
        "the whole-number guard is not the safe-integer one"
    assert not re.search(r"Number\.isInteger\(\s*" + re.escape(parsed) + r"\s*\)", code), \
        "Number.isInteger accepts 1e21, which the API answers with 422"


def test_a_refused_save_keeps_the_modal_open(code):
    """A save the server rejected must not look like a save that worked.

    The response used to be discarded — `await fetch(...)` on its own line — and `closeModal()` ran
    unconditionally after it. A 422 (a precision the API reads as a float), a 404 (the stream was
    deleted in another tab) or a 500 therefore closed the form and redrew the list, taking with it
    every OTHER edit made in the same window and leaving nothing on screen to say the stream had
    never been written.
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


def test_the_edit_form_still_prefills_a_precision_of_zero(ui):
    """`??` and not `||`, because 0 is a legitimate precision — round to whole numbers.

    With `||` the box would come up EMPTY for a stream configured with 0, and saving the form
    without touching it would send null and reset that stream to the default two decimals. Pinned
    here because it is a one-character regression that no other test in this suite would notice.
    """
    assert "f.value_precision ?? ''" in ui
    assert "f.value_precision ||" not in ui
