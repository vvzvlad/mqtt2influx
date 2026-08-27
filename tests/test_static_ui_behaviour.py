#!/usr/bin/env python3
"""What `saveStream()` actually does — by running it.

`tests/test_static_ui.py` reads the UI's source and pins the mechanisms by name. That is the only
thing CI can do: the suite runs inside `python:3.11-slim`, which has no JS runtime, and adding one
to the test image to check a single form would be a poor trade. But a string assertion cannot tell
you what the form SENDS, and the two defects this file exists for were both invisible in the source
and obvious in the behaviour — an empty box travelling as `value_precision: 0`, and a 422 that
closed the modal and threw away the operator's other edits.

So: where `node` is present — a developer's machine, a pre-commit hook, any CI image that has it —
these tests load the real `<script>` block out of `static/index.html` into a stub DOM, call
`saveStream()` and assert on the request that comes out of it. Where `node` is absent they skip,
because a suite that fails for a missing interpreter teaches people to ignore it. Nothing here is
the last line of defence: every property below is ALSO pinned as source text in the sibling file.

The last test is the odd one out. It deliberately breaks the source in the two ways a future reader
might, and asserts that an empty box still travels as null through EITHER wound — the "two
independent checks" claim, stated as something that can fail rather than as a comment.
"""

import json
import os
import re
import shutil
import subprocess

import pytest

# Same directory, same source of truth: the wounds below have to find the same statements the text
# assertions pin, or they would be testing a mutation nobody could actually make.
from test_static_ui import _request_field, _strip_comments, precision_identifiers

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(BASE_DIR, "static", "index.html")

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(
    NODE is None,
    reason="no JS runtime here (CI runs on python:3.11-slim); test_static_ui.py covers this in text")

# A browser, to the extent that loading this one script and running one of its functions needs one.
# Everything the script touches at load time is here; everything it touches while saving is
# recorded, so a scenario can ask what was sent, what was said and whether the modal closed.
STUBS = r"""
'use strict';

const rec = { alerts: [], fetches: [], overlay: [], focused: null };
let nextResponse = { ok: true, status: 200, body: {} };

const EXISTING = {
  id: 'strm-1', name: 'kiln', mqtt_host: 'mqtt.local', mqtt_port: 1883, mqtt_topic: '#',
  influx_host: 'influx.local', influx_port: 8086, influx_database: 'wb', value_precision: 4,
  enabled: true, running: false,
};

function makeEl(id) {
  return {
    id, value: '', textContent: '', innerHTML: '', className: '',
    scrollTop: 0, scrollHeight: 0, clientHeight: 0,
    style: {}, children: [], firstChild: null,
    classList: {
      add(c) { rec.overlay.push(id + ':add:' + c); },
      remove(c) { rec.overlay.push(id + ':remove:' + c); },
      contains() { return false; },
      toggle() {},
    },
    addEventListener() {},
    focus() { rec.focused = id; },
    appendChild(child) { this.children.push(child); this.firstChild = this.children[0]; },
    removeChild(child) {
      const i = this.children.indexOf(child);
      if (i >= 0) this.children.splice(i, 1);
      this.firstChild = this.children[0] || null;
    },
  };
}

const elements = new Map();
globalThis.document = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, makeEl(id));
    return elements.get(id);
  },
  createElement() { return makeEl('created'); },
  addEventListener() {},
};
globalThis.location = { protocol: 'http:', host: 'localhost:8000' };
globalThis.WebSocket = function () { return {}; };
globalThis.alert = (m) => { rec.alerts.push(String(m)); };
globalThis.confirm = () => true;

globalThis.fetch = async (url, opts) => {
  const method = (opts && opts.method) || 'GET';
  rec.fetches.push({ url, method, body: opts && opts.body });
  if (method === 'GET') return { ok: true, status: 200, json: async () => [EXISTING] };
  const r = nextResponse;
  return {
    ok: r.ok,
    status: r.status,
    json: async () => {
      if (r.notJson) throw new SyntaxError('Unexpected token < in JSON at position 0');
      return r.body;
    },
  };
};
"""

DRIVER = r"""
// A form filled in with a stream that saves cleanly; each scenario overrides only what it is about.
const FORM = {
  fName: 'kiln', fMqttHost: 'mqtt.local', fMqttPort: '1883', fMqttUser: '', fMqttPass: '',
  fMqttTopic: '#', fTopicPrefix: '', fInfluxHost: 'influx.local', fInfluxPort: '8086',
  fInfluxUser: '', fInfluxPass: '', fInfluxDb: 'wb', fValuePrecision: '',
};

async function run(spec) {
  rec.alerts = []; rec.fetches = []; rec.overlay = []; rec.focused = null;
  nextResponse = spec.response || { ok: true, status: 200, body: {} };
  editingId = spec.editing ? EXISTING.id : null;
  for (const id of Object.keys(FORM)) document.getElementById(id).value = FORM[id];
  for (const id of Object.keys(spec.form || {})) document.getElementById(id).value = spec.form[id];

  await saveStream();

  const save = rec.fetches.filter(f => f.method === 'PUT' || f.method === 'POST')[0];
  return {
    name: spec.name,
    alerts: rec.alerts,
    focused: rec.focused,
    sentRaw: save ? save.body : null,
    sent: save ? JSON.parse(save.body) : null,
    method: save ? save.method : null,
    url: save ? save.url : null,
    modalClosed: rec.overlay.indexOf('overlay:remove:open') !== -1,
    reloadedAfterSave: save ? rec.fetches.indexOf(save) < rec.fetches.length - 1 : false,
  };
}

(async () => {
  // Let the script's own bootstrap (connectWS / loadStreams) finish before anything is recorded.
  await new Promise(r => setTimeout(r, 0));
  const out = [];
  for (const spec of SPECS) out.push(await run(spec));
  process.stdout.write(JSON.stringify(out));
})().catch(e => {
  process.stderr.write(String((e && e.stack) || e));
  process.exit(1);
});
"""


def _ui_script(html):
    """The contents of the single `<script>` block, which is the whole of the UI's behaviour."""
    match = re.search(r"<script>\n(.*)\n</script>", html, re.S)
    assert match, "static/index.html no longer has one script block"
    return match.group(1)


def _save_stream_span(script):
    start = script.index("async function saveStream()")
    following = re.compile(r"^(?:async )?function ", re.M).search(script, start + 1)
    return start, (following.start() if following else len(script))


def _precision_names(script):
    start, end = _save_stream_span(script)
    return precision_identifiers(_strip_comments(script[start:end]))


def _drop_the_read_check(script):
    """Remove the `raw === '' ? null :` branch — the reviewer's mutation, applied on purpose."""
    raw, parsed = _precision_names(script)
    start, end = _save_stream_span(script)
    region = script[start:end]
    stmt = re.search(r"(const|let|var)\s+" + re.escape(parsed) + r"\b\s*=\s*[^;]*;", region)
    assert stmt, "cannot find the statement to wound"
    wounded = "{} {} = Number({});".format(stmt.group(1), parsed, raw)
    return script[:start] + region[:stmt.start()] + wounded + region[stmt.end():] + script[end:]


def _drop_the_request_check(script):
    """Remove the second check, leaving the request body to send the parsed number as it is."""
    raw, parsed = _precision_names(script)
    start, end = _save_stream_span(script)
    region = script[start:end]
    at = region.index("value_precision:")
    # Read off the unstripped source so the offsets are the ones being spliced. Safe because the
    # expression itself is code from `:` to the `,` that ends it — the commentary is above it.
    expr = _request_field(region, "value_precision")
    return script[:start] + region[:at] + "value_precision: " + parsed \
        + region[at + len("value_precision:") + len(expr):] + script[end:]


def _run_scenarios(tmp_path, specs, script=None):
    html = open(INDEX_HTML, encoding="utf-8").read()
    source = script if script is not None else _ui_script(html)
    harness = tmp_path / "harness.js"
    harness.write_text(
        STUBS + "\n// ── the UI's own script, verbatim ──\n" + source + "\n"
        + "const SPECS = " + json.dumps(specs) + ";\n" + DRIVER,
        encoding="utf-8")
    done = subprocess.run([NODE, str(harness)], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, "the UI script did not run:\n" + done.stderr
    return {r["name"]: r for r in json.loads(done.stdout)}


@pytest.fixture(scope="module")
def saved(tmp_path_factory):
    """Every scenario, run in one node process, keyed by name."""
    if NODE is None:
        pytest.skip("no node")
    return _run_scenarios(tmp_path_factory.mktemp("ui"), [
        {"name": "empty", "form": {"fValuePrecision": ""}},
        {"name": "blank", "form": {"fValuePrecision": "   "}},
        {"name": "zero", "form": {"fValuePrecision": "0"}},
        {"name": "three", "form": {"fValuePrecision": "3"}},
        {"name": "unrounded", "form": {"fValuePrecision": "-1"}},
        {"name": "half", "form": {"fValuePrecision": "0.5"}},
        {"name": "tiny", "form": {"fValuePrecision": "1e-5"}},
        {"name": "huge", "form": {"fValuePrecision": "1e21"}},
        {"name": "letters", "form": {"fValuePrecision": "abc"}},
        {"name": "edit_ok", "editing": True,
         "form": {"fMqttHost": "mqtt-new.local", "fValuePrecision": "3"}},
        {"name": "edit_422", "editing": True,
         "form": {"fMqttHost": "mqtt-new.local", "fValuePrecision": "3"},
         "response": {"ok": False, "status": 422,
                      "body": {"detail": "value_precision must be an integer or null"}}},
        {"name": "edit_422_fields", "editing": True,
         "form": {"fMqttHost": "mqtt-new.local"},
         "response": {"ok": False, "status": 422,
                      "body": {"detail": [{"loc": ["body", "value_precision"], "msg": "bad"}]}}},
        {"name": "edit_404_html", "editing": True, "form": {"fMqttHost": "mqtt-new.local"},
         "response": {"ok": False, "status": 404, "notJson": True}},
        {"name": "edit_500", "editing": True,
         "response": {"ok": False, "status": 500, "body": {"detail": "boom"}}},
    ])


@needs_node
def test_an_empty_precision_box_is_sent_as_null_and_not_as_zero(saved):
    """The whole finding, in one assertion: `Number('')` is 0, and 0 is a precision the API takes.

    A stream saved with `value_precision: 0` rounds every value it writes to a whole number, forever
    and without a word in any log — on this deployment that is the Wirenboard telemetry stream.
    `null` means "not configured" and leaves the default two decimals in place, which is what an
    empty box has always meant.
    """
    for name in ("empty", "blank"):
        result = saved[name]
        assert result["sent"] is not None, "the form refused a save it should have made"
        assert result["sent"]["value_precision"] is None, \
            "an empty precision box was sent as {!r}".format(result["sent"]["value_precision"])
        assert '"value_precision":null' in result["sentRaw"]


@needs_node
def test_a_precision_the_operator_typed_reaches_the_request_unchanged(saved):
    """0 and -1 are both meaningful and both easy to lose: `||` blanks the first, `Math.abs` the
    second. And 3 is here so that "sends null always" cannot pass this file."""
    assert saved["zero"]["sent"]["value_precision"] == 0
    assert saved["three"]["sent"]["value_precision"] == 3
    assert saved["unrounded"]["sent"]["value_precision"] == -1


@needs_node
def test_a_precision_that_is_not_a_whole_number_is_refused_rather_than_rounded(saved):
    """`0.5` and `1e-5` can be typed into `<input type="number" step="1">` — step is a mismatch the
    browser reports through `checkValidity()`, which this form never calls. Rounding them silently
    is the defect the setting was added to remove; refusing them is the only honest answer."""
    for name in ("half", "tiny", "letters"):
        result = saved[name]
        assert result["sent"] is None, "{} was saved instead of refused".format(name)
        assert result["alerts"], "{} was refused without telling the operator".format(name)
        assert result["focused"] == "fValuePrecision", "the refusal did not point at the box"
        assert not result["modalClosed"]


@needs_node
def test_a_precision_the_api_would_read_as_a_float_is_refused_on_the_client(saved):
    """`1e21` passes `Number.isInteger`, and `JSON.stringify` writes it `1e+21`, which `json.loads`
    reads as a float and the API answers with 422. `Number.isSafeInteger` makes the two ends agree,
    so the operator is told what is wrong while the number is still on screen."""
    result = saved["huge"]
    assert result["sent"] is None, "1e21 was sent; the API answers that with 422"
    assert result["alerts"]


@needs_node
def test_a_refused_save_keeps_the_modal_open_and_says_why(saved):
    """422, 404 and 500 all used to look exactly like a save that worked.

    The modal closed, the list redrew, and the operator's other edits in that window — the changed
    `mqtt_host` here — went with it. Now the form stays open with the edits still in it, and the
    message carries the status and whatever the API said was wrong.
    """
    for name, status, expected in (("edit_422", "422", "value_precision must be an integer"),
                                   ("edit_422_fields", "422", "value_precision"),
                                   ("edit_404_html", "404", None),
                                   ("edit_500", "500", "boom")):
        result = saved[name]
        assert result["sent"] is not None, "{}: nothing was even sent".format(name)
        assert not result["modalClosed"], \
            "{}: the modal closed on a refused save, taking the other edits with it".format(name)
        assert not result["reloadedAfterSave"], \
            "{}: the list was redrawn as if the stream had been written".format(name)
        assert len(result["alerts"]) == 1, "{}: {}".format(name, result["alerts"])
        assert status in result["alerts"][0], \
            "{}: the message does not name the status: {}".format(name, result["alerts"][0])
        if expected:
            assert expected in result["alerts"][0], \
                "{}: the API said what was wrong and the message dropped it".format(name)
    # A 404 whose body is a proxy's HTML error page must still produce one usable message, not an
    # unhandled rejection from res.json().
    assert saved["edit_404_html"]["alerts"][0]


@needs_node
def test_a_save_that_worked_still_closes_the_modal_and_reloads(saved):
    """The other half of the same check: guarding the failure path must not break the happy one."""
    result = saved["edit_ok"]
    assert result["method"] == "PUT"
    assert result["url"] == "/api/streams/strm-1"
    assert result["sent"]["mqtt_host"] == "mqtt-new.local"
    assert result["alerts"] == []
    assert result["modalClosed"]
    assert result["reloadedAfterSave"]


@needs_node
def test_losing_either_empty_box_check_still_costs_only_the_default(tmp_path):
    """The "two independent checks" claim, run rather than asserted about.

    Both wounds below are things a reader could plausibly do — the first one is exactly what the
    reviewer did, and the whole suite stayed green. The point of having the check twice is that
    neither removal reaches the database: an empty box still travels as null, i.e. as the default
    two decimals, instead of as a 0 that silently truncates every value in the stream.

    This is also why `test_static_ui.py` pins BOTH checks as text. Behaviour cannot notice the loss
    of a redundancy — that is what redundancy is — so the source is where it has to be noticed.
    """
    html = open(INDEX_HTML, encoding="utf-8").read()
    script = _ui_script(html)
    specs = [{"name": "empty", "form": {"fValuePrecision": ""}},
             {"name": "three", "form": {"fValuePrecision": "3"}}]
    for wound, mutate in (("the read", _drop_the_read_check),
                          ("the request body", _drop_the_request_check)):
        saved = _run_scenarios(tmp_path, specs, script=mutate(script))
        assert saved["empty"]["sent"]["value_precision"] is None, \
            "with the empty-box check gone from {}, an empty box is sent as {!r}".format(
                wound, saved["empty"]["sent"]["value_precision"])
        assert saved["three"]["sent"]["value_precision"] == 3, \
            "the wound to {} broke an ordinary save; the mutation is not the one intended".format(
                wound)
