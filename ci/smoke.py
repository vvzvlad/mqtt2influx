"""Smoke gate for the mqtt2influx image, run on the CI runner against an already-built image.

It sits BETWEEN `docker build` and `docker push`, which is the only position in which it is worth
anything: nobody presses a button between the push and the rollout. The deployed stack carries
`io.portainer.update.enable` (see docker-compose.yml), so an updater polls `:latest` and redeploys
whatever lands on it. This gate is the last point at which a broken image can still be stopped.

WHY THE BAR IS HIGH HERE
------------------------
This service is a bridge, not a request/response app. It holds a subscription to an MQTT broker and
writes what arrives into InfluxDB — 612 million messages and 604 million points on the production
host so far. MQTT does not buffer for a subscriber that is not connected: an image that crash-loops
does not delay points, it loses them, permanently, for as long as it takes somebody to notice. So
"the container came up" is not the question this file asks. The question is whether the thing SERVES.

THE SPLIT
---------
The OUTER half is this file: a plain `python3 ci/smoke.py` on the runner, driving `docker`. It
answers the questions that are about the container as an OBJECT — what `docker inspect` says its CMD,
WORKDIR, ENTRYPOINT and DATA_DIR are, what the real command writes to its log, whether it is still
alive when everything else has had its turn at it. None of those can be answered from a process
already running inside it.

The two INNER halves are PROBES: programs that live here as string constants and are fed to
`docker exec -i <name> python -u -`. The IMAGE probe answers everything about the FILES and the
dependencies inside the image. The SERVICE probe answers whether the running process is really
serving — the whole REST surface, the config file it writes to the volume, the static UI and the
websocket.

They run inside for two reasons. The obvious one is that those objects only exist in there. The
other shapes this whole file: **this job and the docker daemon are not in the same network
namespace.** Gitea's act_runner executes the job inside its own job container while the `docker` CLI
it provides drives a daemon outside it. So:

* NO PORT IS PUBLISHED by anything here. Publishing 8000 would land it in the HOST daemon's network
  namespace, and this job's 127.0.0.1 is a different loopback entirely — the gate would be talking
  to its own empty socket and would have to be written as "connection refused, so probably fine",
  which is the kind of check that passes forever after the service stops working.
* NOTHING IS BIND-MOUNTED either. A `-v $(pwd):/w` would hand the daemon a path that exists in this
  container's filesystem and not in its own; it would mount an empty directory, and every file check
  would pass by looking at nothing.

Everything internal therefore goes through `docker exec`, which puts the probe in the container's
own namespace — where 127.0.0.1 means what the compose healthcheck means by it.

Two more properties of `docker exec` are worth stating because checks below depend on them: it does
NOT go through ENTRYPOINT, and it runs as the image's `Config.User`. Both cut the same way here. The
image declares `/entrypoint.sh` — which starts as root, repairs the ownership of /data and `exec`s
gosu to drop to uid 1000 — and declares no `USER`, so every probe below arrives as ROOT and bypasses
that drop entirely. Half of that is convenient: a root probe can read /proc/1/status and stat /data
whatever their ownership. The other half is a trap, and it is the one that matters — a probe that
asked for its OWN uid would answer 0 forever and prove nothing about the process that serves. So the
non-root check interrogates PID 1, the only process in the container that came through the
entrypoint, and the contract check pins the ENTRYPOINT to an exact value rather than reading it off
a process that never went through one.

WHY THE CRUD CYCLE IS THE CENTRE OF THIS FILE
---------------------------------------------
tests/ already proves the SOURCE is good. It says nothing about the artefact: whether pip resolved a
dependency set that actually imports, whether .dockerignore still keeps the developer's data/ out,
whether static/ was copied at all, whether the volume at /data is writable by the user the image
runs as. So the gate drives the real image through the whole lifecycle a user drives it through —
create a stream, see it listed, edit it, delete it — and then reads the file that lifecycle was
supposed to produce OFF THE CONTAINER'S DISK rather than believing the API's own echo. The API
answering 200 and the volume holding the stream are two different claims, and only the second one is
what survives a restart.

Every stream this gate creates carries `enabled: false`, and that is load-bearing rather than tidy:
`create_stream()` in src/api.py calls `manager.start_stream()` the instant `enabled` is true, which
schedules a task that connects to the broker named in the config and retries every five seconds
forever. With a made-up hostname that is a gate that hangs, logs errors and then reports them as the
image's fault.

THE CHECKS
----------
* (a) the image's declared contract: CMD, WORKDIR, ENTRYPOINT, DATA_DIR=/data. Outer half.
* (b) the real command starts and uvicorn reports itself listening on 8000. Outer half.
* (c) the empty state: GET /api/streams answers 200 with [] on a fresh volume. Service probe.
* (d) the full CRUD cycle over HTTP, including the second DELETE that must be a 404. Service probe.
* (e) persistence: /data/streams.json exists on the container's disk and holds the created stream.
      Read with open(), not asked of the API. Service probe.
* (f) GET /api/stats answers 200 with valid JSON — it is also the compose healthcheck's target, and
      Portainer's updater rolls the image back if it does not go healthy within 120 s. Service probe.
* (g) the static UI: / is 200 text/html carrying a real marker from static/index.html, and an
      unknown path is a 404 rather than the index page. Service probe.
* (h) the websocket: /ws accepts and delivers its {"type": "snapshot"} first frame. This is the only
      check of the channel the entire UI runs on — without it the dashboard connects and shows
      nothing. Service probe.
* (i) .dockerignore did its job: no .venv, .git, data/ or tests/ inside the image. Image probe.
* (j) the runtime dependencies import: fastapi, uvicorn, aiomqtt, aiohttp, websockets. Image probe.
* (k) the container is still running at the end, and the log it has BY THEN carries no traceback.
      Outer half.
* (l) PID 1 — the process the ENTRYPOINT actually started — runs as uid 1000 and not as root. This
      is the artefact, not the intention: a Dockerfile can carry every non-root instruction there is
      and still serve as root if the entrypoint stopped `exec`ing gosu. Service probe.
* (m) /data is writable by the uid PID 1 really runs as. The privilege drop and the volume's
      ownership are set in two different places — a chown at build time and a chown at start — and
      an image that dropped privileges but left the volume owned by root comes up perfectly, serves
      every read, and fails the first time somebody saves a stream. Service probe.

Two properties matter and are easy to lose, so they are stated where they can be checked:

* Failures leave through SystemExit, never `assert` — on all three sides of the split. Asserts vanish
  under PYTHONOPTIMIZE=1, which would silently turn this gate permanently green.
* Every check runs before the run is judged, so one run shows the full extent of the breakage
  instead of only the first broken thing. A check that CANNOT run reports itself as FAILED; it is
  never quietly skipped, which is the classic way a gate keeps reporting success while proving less
  and less. Each probe's own report lines are parsed back into this gate's report, so a failure
  inside the container is one row in the same list as a failure outside it.

Nothing here holds a credential and nothing it starts reaches a service outside the runner: the only
hosts the created stream names are `mqtt.invalid` and `influx.invalid` — reserved by RFC 2606 to
never resolve — and it is never started. That is what lets this same gate run on a pull request.
"""

import json
import os
import subprocess
import time

# The tag to test. Required rather than defaulted: a default would let a mistyped `env:` block in a
# workflow silently gate some other image that happens to be on the daemon.
IMAGE_ENV = "SMOKE_IMAGE"
# Base name for every container this gate starts. The workflows put the run id in it, because the
# runner has ONE docker daemon shared by every repository in the fleet and two concurrent runs must
# not collide on a name — remove_container() below would otherwise delete another run's live
# container out from under it. Required for a second reason too: the workflow's `if: always()`
# cleanup step builds the same names from the same variable, so a default here would leave this
# script naming its container one way while the cleanup went looking for another, found nothing, and
# swallowed the miss in its `|| true`.
NAME_ENV = "SMOKE_NAME"

SERVE_SUFFIX = "-serve"
# Kept in step with the cleanup step in both workflows, which removes the same suffixes.
ALL_SUFFIXES = (SERVE_SUFFIX,)

# ── what the image is supposed to declare ─────────────────────────────────────────────────────────
APP_DIR = "/app"
EXPECTED_CMD = ["python", "main.py"]
# Pinned to an exact value, not merely "there is one". This is the ONLY evidence this gate has that
# the privilege-dropping wrapper is still on the image: every in-container check below enters through
# `docker exec`, which walks straight past the entrypoint, so an image that lost it would answer all
# of them exactly as it does now while quietly going back to serving as root.
EXPECTED_ENTRYPOINT = ["/entrypoint.sh"]
# `DATA_DIR=/data` is not decoration: src/config.py reads it once at import and every stream this
# service knows about lives in one file under it. An image that lost this variable would default to
# the same "/data" today — and would silently start writing into the container's writable layer the
# day that default in the source changes, where the next image update takes the configuration with
# it.
EXPECTED_DATA_DIR = "DATA_DIR=/data"

# ── what a healthy boot looks like ────────────────────────────────────────────────────────────────
# Both are uvicorn's own lines, observed in this exact order: startup completes first, the listening
# line follows. BOTH are waited for, not the first — see wait_for_markers(). The port is spelled out
# because it is the contract the compose file publishes 8111 onto.
STARTUP_COMPLETE_MARKER = "Application startup complete."
LISTENING_MARKER = "Uvicorn running on http://0.0.0.0:8000"
STARTUP_MARKERS = (STARTUP_COMPLETE_MARKER, LISTENING_MARKER)

TRACEBACK_MARKER = "Traceback (most recent call last)"

# ── the probes ────────────────────────────────────────────────────────────────────────────────────
IMAGE_PROBE_MARKER = "mqtt2influx image probe ok"
SERVICE_PROBE_MARKER = "mqtt2influx service probe ok"
# The number of verdicts each probe is supposed to print. Compared EXACTLY rather than "at least",
# because the failure this catches is a probe that quietly stopped checking things: every line it
# does print says ok and it exits 0, so nothing else in this file would notice.
# 15 = 5 files that must be in the image + 5 that must not + 5 runtime imports.
EXPECTED_IMAGE_PROBE_TARGETS = 15
# 24 = 2 process identity + 2 empty state + 12 CRUD + 2 stats + 4 static UI + 2 websocket.
EXPECTED_SERVICE_PROBE_TARGETS = 24

IMAGE_PROBE_ROW_PREFIX = "[in-image] "
SERVICE_PROBE_ROW_PREFIX = "[in-container] "

# Rows this file produces itself: 4 from the contract check, 1 for the container starting, 2 startup
# markers, 1 for it still running at the end, 1 for the final log.
EXPECTED_OUTER_TARGETS = 9
# Each probe contributes its own targets plus the two consistency rows run_probe() adds.
EXPECTED_TOTAL_TARGETS = (
    EXPECTED_OUTER_TARGETS
    + EXPECTED_IMAGE_PROBE_TARGETS + 2
    + EXPECTED_SERVICE_PROBE_TARGETS + 2)

# ── bounds ────────────────────────────────────────────────────────────────────────────────────────
# Every docker call is bounded. The worst case adds up to roughly eight minutes, against the step's
# own 15 — the margin is there so that a gate which is merely slow fails with ITS OWN diagnosis
# rather than being killed by act_runner, which would skip the cleanup in the `finally` below.
INSPECT_TIMEOUT = 30
REMOVE_TIMEOUT = 30
START_TIMEOUT = 60
LOGS_TIMEOUT = 30
IMAGE_PROBE_TIMEOUT = 120
SERVICE_PROBE_TIMEOUT = 120

# How long the container gets to print both startup markers, and how often the log is re-read while
# waiting. Generous: the image installs nothing at start, but a cold runner reading a fresh image's
# layers off disk is slower than a warm one.
BOOT_BUDGET = 60
BOOT_PAUSE = 0.5

EXCERPT_CHARS = 4000


# ══ THE IMAGE PROBE ═══════════════════════════════════════════════════════════════════════════════
# Runs inside the container, as the image's own interpreter, fed on stdin. It answers what is IN the
# image: the files that must be there, the files that must not, and whether the dependency set pip
# resolved at build time actually imports.
IMAGE_PROBE = r'''
import importlib
import os

PROBE_MARKER = "mqtt2influx image probe ok"

APP_DIR = "/app"

# The three paths the Dockerfile copies, spelled out one level deeper than the COPY lines so that a
# COPY which created an empty directory is not mistaken for a COPY that worked. static/index.html in
# particular is the ENTIRE user interface — one file — and `StaticFiles` mounted on a directory that
# exists but is empty raises nothing at startup: the service comes up perfectly and serves 404 at /.
PRESENT = [
    (APP_DIR + "/main.py", "the entrypoint the image's CMD runs"),
    (APP_DIR + "/src/api.py", "the ASGI app uvicorn is told to import"),
    (APP_DIR + "/src/config.py", "the stream store"),
    (APP_DIR + "/src/mqtt_processor.py", "the MQTT subscriber and batcher"),
    (APP_DIR + "/static/index.html", "the whole web UI, in one file"),
]

# What .dockerignore and the Dockerfile's three explicit COPY lines are supposed to keep out. Two of
# these are size (.venv is ~65 MB of macOS wheels for an interpreter that is not in this image), and
# two are disclosure: .git carries the full history into a public registry, and data/ on a developer
# machine holds a real streams.json with real MQTT and InfluxDB passwords in it.
ABSENT = [
    (APP_DIR + "/.venv", "a host-built virtualenv: ~65 MB of wheels for the wrong interpreter"),
    (APP_DIR + "/.git", "the repository history, which would ship to a public registry"),
    (APP_DIR + "/data", "a developer's runtime state, which holds real broker credentials"),
    (APP_DIR + "/tests", "the test suite, which is run from the source tree and not from the image"),
    (APP_DIR + "/ci", "this gate itself"),
]

# Imported rather than pip-listed: a wheel can be present and still fail to import (wrong platform
# tag, a missing shared library after a base-image bump). `websockets` is the one most likely to go
# missing unnoticed, because nothing in src/ imports it — uvicorn picks it up at runtime, and
# without it /ws is refused while every HTTP route keeps answering.
IMPORTS = ["fastapi", "uvicorn", "aiomqtt", "aiohttp", "websockets"]


def describe(error):
    return "{}: {}".format(type(error).__name__, error)


def check_present(path, what):
    target = "{} is in the image ({})".format(path, what)
    try:
        if os.path.exists(path):
            return (target, None)
        return (target, "it is not there at all")
    except Exception as error:
        return (target, "could not be checked: {}".format(describe(error)))


def check_absent(path, what):
    target = "{} is NOT in the image".format(path)
    try:
        if not os.path.exists(path):
            return (target, None)
        return (target, (
            "it IS in the image, and it should not be — {}. Either .dockerignore stopped covering "
            "it or the Dockerfile grew a `COPY . .`".format(what)))
    except Exception as error:
        return (target, "could not be checked: {}".format(describe(error)))


def check_import(module):
    target = "`import {}` works inside the image".format(module)
    try:
        importlib.import_module(module)
        return (target, None)
    except Exception as error:
        return (target, (
            "it does not import: {}. pip installed something at build time that cannot actually be "
            "used here".format(describe(error))))


def main():
    rows = []
    for path, what in PRESENT:
        rows.append(check_present(path, what))
    for path, what in ABSENT:
        rows.append(check_absent(path, what))
    for module in IMPORTS:
        rows.append(check_import(module))

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("{}: FAILED {}/{} targets".format(PROBE_MARKER, len(failures), len(rows)))
        # SystemExit and not `assert`: asserts disappear under PYTHONOPTIMIZE=1, and a probe whose
        # failures disappear with them is a probe that reports success forever.
        raise SystemExit(1)

    print("{}: {}/{} targets".format(PROBE_MARKER, len(rows), len(rows)))


if __name__ == "__main__":
    main()
'''


# ══ THE SERVICE PROBE ═════════════════════════════════════════════════════════════════════════════
# Runs inside the container, against the process the image's own CMD started. This is the half that
# proves the thing SERVES: the whole REST lifecycle, the file that lifecycle writes to the volume,
# the static UI and the websocket the dashboard lives on.
SERVICE_PROBE = r'''
import json
import os
import stat
import urllib.error
import urllib.request

PROBE_MARKER = "mqtt2influx service probe ok"

# 127.0.0.1 inside the container, which is where uvicorn's 0.0.0.0:8000 is reachable. See the module
# docstring in ci/smoke.py for why nothing here talks to a published port.
BASE_URL = "http://127.0.0.1:8000"
HTTP_TIMEOUT = 15

# Written by src/config.py under DATA_DIR, which the image sets to /data.
CONFIG_PATH = "/data/streams.json"
STATE_DIR = "/data"

# The uid the Dockerfile creates and /entrypoint.sh drops to with gosu. Checked against PID 1 and
# NOT against this probe's own uid: `docker exec` does not go through ENTRYPOINT and runs as the
# image's Config.User, which the Dockerfile deliberately leaves unset — so os.getuid() here is 0 on
# a perfectly good image and would be 0 just the same on one that never drops privileges at all.
# PID 1 is the only process in this container that came through the entrypoint.
APP_UID = 1000

# A real line out of static/index.html rather than a generic "<html". A directory served with the
# wrong contents, or an index.html replaced by a placeholder, would sail through a check for "<html".
UI_MARKER = "<h1>mqtt2influx</h1>"

# `.invalid` is reserved by RFC 2606 and never resolves, so this configuration cannot reach anything
# even if something did try to start it. `enabled` is FALSE on purpose — see the docstring.
STREAM_BODY = {
    "name": "smoke stream",
    "mqtt_host": "mqtt.invalid",
    "mqtt_port": 1883,
    "mqtt_user": "smoke-user",
    "mqtt_password": "smoke-password",
    "mqtt_topic": "/devices/#",
    "topic_prefix": "smoke",
    "influx_host": "influx.invalid",
    "influx_port": 8086,
    "influx_user": "smoke-influx-user",
    "influx_password": "smoke-influx-password",
    "influx_database": "smokedb",
    "enabled": False,
}

UNKNOWN_PATH = "/no-such-page-smoke-gate"

rows = []


def record(target, reason):
    rows.append((target, reason))


def describe(error):
    return "{}: {}".format(type(error).__name__, error)


def call(method, path, payload=None):
    """One HTTP call. Returns (status, headers, body) — headers is the raw HTTPMessage.

    Deliberately NOT `dict(response.headers)`: an HTTPMessage looks up header names
    case-insensitively and a plain dict does not, so a `headers["Content-Type"]` against the dict
    would miss the `content-type` starlette actually sends and report a perfectly good image as
    broken. That exact mistake was made while developing this gate.

    A non-2xx status arrives as HTTPError, which IS a response — 404 is an expected answer twice
    below — so it is unwrapped rather than raised.
    """
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, response.headers, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read().decode("utf-8", "replace")


def pid1_ids():
    """The REAL uid and gid of PID 1, read out of procfs.

    The `Uid:`/`Gid:` lines in /proc/<pid>/status are tab-separated as real, effective, saved, fs.
    The FIRST column is the one taken: an effective uid of 1000 sitting on a real uid of 0 is a
    process that can climb straight back, and reading the second column would call that a pass.

    procfs and not `ps`: python:3.11-slim ships no procps, so a `ps` here would fail on the image
    being tested rather than on anything it is testing.
    """
    uid = None
    gid = None
    with open("/proc/1/status") as handle:
        for line in handle:
            if line.startswith("Uid:"):
                uid = int(line.split()[1])
            elif line.startswith("Gid:"):
                gid = int(line.split()[1])
    return uid, gid


def check_process_identity():
    """(l) + (m) Who is serving, and whether the state directory is writable BY THEM.

    Both rows interrogate the running process rather than the image's declarations, and that is the
    entire point: a Dockerfile can carry every non-root instruction there is while the entrypoint
    has quietly stopped `exec`ing gosu, and no other check in this gate would see the difference —
    they all arrive through `docker exec`, which never goes through the entrypoint at all.

    Runs BEFORE the CRUD cycle so that an unwritable /data is diagnosed as an unwritable /data. The
    CRUD cycle would otherwise report the same fault only as a missing streams.json, which reads
    like a store bug and sends whoever is looking into src/config.py.
    """
    uid_target = "PID 1 serves as uid {} rather than as root".format(APP_UID)
    write_target = "{} is writable by the uid PID 1 actually runs as".format(STATE_DIR)
    try:
        uid, gid = pid1_ids()
    except Exception as error:
        reason = "/proc/1/status could not be read: {}".format(describe(error))
        record(uid_target, reason)
        record(write_target, "not attempted: " + reason)
        return
    if uid is None or gid is None:
        reason = (
            "/proc/1/status carried no Uid:/Gid: line, so the serving process cannot be identified "
            "and neither of these two claims can be made about it")
        record(uid_target, reason)
        record(write_target, "not attempted: " + reason)
        return

    record(uid_target, None if uid == APP_UID else (
        "it runs as uid {}. {}".format(uid, (
            "That is root: the privilege drop did not happen. Either /entrypoint.sh stopped "
            "`exec`ing gosu, or the ENTRYPOINT is off the image and CMD ran on its own"
        ) if uid == 0 else (
            "That is neither root nor the `app` account the image creates, so the ownership the "
            "Dockerfile and the entrypoint put on {} belongs to somebody else".format(STATE_DIR)))))

    try:
        info = os.stat(STATE_DIR)
    except Exception as error:
        record(write_target, "{} could not be stat'ed: {}".format(STATE_DIR, describe(error)))
        return
    # Worked out against PID 1's ids by hand rather than with os.access(), which answers for the
    # CALLER — and the caller here is root, for whom every directory is writable and this check
    # would be a permanent pass. What it exists to catch is the production case the entrypoint's
    # chown was written for: a volume created by the old root-based image, whose ownership the
    # Dockerfile's build-time chown never touched because docker only seeds an EMPTY volume.
    mode = info.st_mode
    writable = (
        (info.st_uid == uid and mode & stat.S_IWUSR)
        or (info.st_gid == gid and mode & stat.S_IWGRP)
        or mode & stat.S_IWOTH)
    record(write_target, None if writable else (
        "it is owned by {}:{} with mode {}, and PID 1 is {}:{}. A container like this comes up, "
        "reports healthy and answers every read — it fails the first time somebody saves a stream, "
        "which is the one moment nobody is watching the log".format(
            info.st_uid, info.st_gid, oct(stat.S_IMODE(mode)), uid, gid)))


def check_empty_state():
    """(c) A fresh /data volume means no streams — the state right after a deploy."""
    status_target = "GET /api/streams answers 200 on a fresh volume"
    body_target = "GET /api/streams is an empty list on a fresh volume"
    try:
        status, _headers, body = call("GET", "/api/streams")
    except Exception as error:
        reason = "the request itself failed: {}".format(describe(error))
        record(status_target, reason)
        record(body_target, "not attempted: " + reason)
        return
    record(status_target, None if status == 200 else "it answered {}: {}".format(status, body[:400]))
    try:
        parsed = json.loads(body)
    except Exception as error:
        record(body_target, "its body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    record(body_target, None if parsed == [] else (
        "it returned {!r}. Either the volume is not fresh — a leftover streams.json from a previous "
        "run — or the store is reading from somewhere other than DATA_DIR".format(parsed)))


def check_crud_cycle():
    """(d) + (e) The whole lifecycle a user drives through the UI, plus what it leaves on disk.

    Written as one function on purpose: every step depends on the id the previous one produced, and
    splitting it would mean either re-creating a stream per check or passing a half-built state
    around. When a step cannot run, the remaining targets are recorded as FAILED with the reason —
    never skipped.
    """
    targets = [
        "POST /api/streams answers 200 and returns a generated id",
        "POST /api/streams echoes the fields it was given",
        "/data/streams.json exists on the container's disk after the POST",
        "/data/streams.json holds exactly the stream that was created",
        "GET /api/streams lists the created stream",
        "the listed stream reports running=false",
        "PUT /api/streams/<id> answers 200 and carries the new value",
        "PUT leaves the fields it was not given alone",
        "GET /api/streams reflects the PUT",
        "DELETE /api/streams/<id> answers 200 with {'ok': true}",
        "GET /api/streams is empty again after the DELETE",
        "a second DELETE of the same id answers 404",
    ]

    def abandon(done, reason):
        for target in targets[done:]:
            record(target, "not attempted: " + reason)

    # --- create -----------------------------------------------------------------------------------
    try:
        status, _headers, body = call("POST", "/api/streams", STREAM_BODY)
        created = json.loads(body)
    except Exception as error:
        abandon(0, "the POST failed: {}".format(describe(error)))
        return
    stream_id = created.get("id") if isinstance(created, dict) else None
    if status != 200 or not stream_id:
        record(targets[0], "it answered {} with {}".format(status, body[:400]))
        abandon(1, "the POST did not produce a stream id")
        return
    record(targets[0], None)

    mismatched = [
        key for key, value in STREAM_BODY.items() if created.get(key) != value]
    record(targets[1], None if not mismatched else (
        "these fields came back different from what was sent: {}. Sent {!r}, got {!r}".format(
            sorted(mismatched),
            {k: STREAM_BODY[k] for k in sorted(mismatched)},
            {k: created.get(k) for k in sorted(mismatched)})))

    # --- persistence: read the volume, do not take the API's word for it --------------------------
    if os.path.isfile(CONFIG_PATH):
        record(targets[2], None)
        try:
            with open(CONFIG_PATH) as handle:
                on_disk = json.load(handle)
        except Exception as error:
            on_disk = None
            record(targets[3], "it is there but could not be read as JSON: {}".format(
                describe(error)))
        if on_disk is not None:
            if (isinstance(on_disk, list) and len(on_disk) == 1
                    and on_disk[0].get("id") == stream_id
                    and on_disk[0].get("name") == STREAM_BODY["name"]
                    and on_disk[0].get("influx_database") == STREAM_BODY["influx_database"]):
                record(targets[3], None)
            else:
                record(targets[3], (
                    "it does not hold the stream that was just created. The API reported success, "
                    "so this is a store that answers from memory and loses the configuration on "
                    "the next restart. On disk: {!r}".format(on_disk)))
    else:
        record(targets[2], (
            "it is not there. The API answered 200, so the stream exists as far as the UI is "
            "concerned — and would be gone the moment the container restarts. Either /data is not "
            "writable by the user this image runs as, or DATA_DIR points somewhere else"))
        record(targets[3], "not attempted: the file does not exist")

    # --- read ------------------------------------------------------------------------------------
    try:
        status, _headers, body = call("GET", "/api/streams")
        listed = json.loads(body)
    except Exception as error:
        abandon(4, "the GET after the POST failed: {}".format(describe(error)))
        return
    # Guarded rather than indexed straight away: an error response parses as a dict, and `listed[0]`
    # on one raises a KeyError that would surface as "the gate crashed" instead of "the API answered
    # something unexpected".
    first = listed[0] if isinstance(listed, list) and listed else None
    if status == 200 and isinstance(listed, list) and len(listed) == 1 \
            and first is not None and first.get("id") == stream_id:
        record(targets[4], None)
    else:
        record(targets[4], "it answered {} with {}".format(status, body[:400]))
    record(targets[5], None if first is not None and first.get("running") is False else (
        "it reports running={!r}. The stream was created with enabled=false, so nothing should have "
        "started it — a running processor here is a connection attempt to a hostname that does not "
        "resolve".format(first.get("running") if first is not None else "(no stream listed)")))

    # --- update ----------------------------------------------------------------------------------
    new_name = "renamed by the smoke gate"
    try:
        status, _headers, body = call(
            "PUT", "/api/streams/" + stream_id, {"name": new_name})
        updated = json.loads(body)
    except Exception as error:
        abandon(6, "the PUT failed: {}".format(describe(error)))
        return
    if status == 200 and updated.get("name") == new_name:
        record(targets[6], None)
    else:
        record(targets[6], "it answered {} with {}".format(status, body[:400]))
    record(targets[7], None if updated.get("influx_database") == STREAM_BODY["influx_database"] else (
        "influx_database came back as {!r} instead of {!r}. A PUT that carries one field must not "
        "blank the rest — in production that silently repoints a stream at nothing".format(
            updated.get("influx_database"), STREAM_BODY["influx_database"])))

    try:
        status, _headers, body = call("GET", "/api/streams")
        relisted = json.loads(body)
    except Exception as error:
        abandon(8, "the GET after the PUT failed: {}".format(describe(error)))
        return
    relisted_first = relisted[0] if isinstance(relisted, list) and relisted else None
    if status == 200 and relisted_first is not None and relisted_first.get("name") == new_name:
        record(targets[8], None)
    else:
        record(targets[8], (
            "the PUT reported success but the list still shows {!r}. The change did not reach the "
            "store".format(
                relisted_first.get("name") if relisted_first is not None else "(nothing)")))

    # --- delete ----------------------------------------------------------------------------------
    try:
        status, _headers, body = call("DELETE", "/api/streams/" + stream_id)
        deleted = json.loads(body)
    except Exception as error:
        abandon(9, "the DELETE failed: {}".format(describe(error)))
        return
    if status == 200 and deleted == {"ok": True}:
        record(targets[9], None)
    else:
        record(targets[9], "it answered {} with {}".format(status, body[:400]))

    try:
        status, _headers, body = call("GET", "/api/streams")
        remaining = json.loads(body)
    except Exception as error:
        abandon(10, "the GET after the DELETE failed: {}".format(describe(error)))
        return
    record(targets[10], None if remaining == [] else (
        "it still lists {!r}. The DELETE reported success without removing anything".format(
            remaining)))

    try:
        status, _headers, body = call("DELETE", "/api/streams/" + stream_id)
    except Exception as error:
        record(targets[11], "the second DELETE failed: {}".format(describe(error)))
        return
    record(targets[11], None if status == 404 else (
        "it answered {} instead of 404. Deleting something that is not there has to be a miss, not "
        "a success — the UI reads that 200 as 'it was removed'".format(status)))


def check_stats():
    """(f) Also the compose healthcheck's target, on a bridge with no streams running."""
    status_target = "GET /api/stats answers 200"
    json_target = "GET /api/stats returns a JSON list"
    try:
        status, _headers, body = call("GET", "/api/stats")
    except Exception as error:
        reason = "the request itself failed: {}".format(describe(error))
        record(status_target, reason)
        record(json_target, "not attempted: " + reason)
        return
    record(status_target, None if status == 200 else (
        "it answered {}. This endpoint is what the compose healthcheck probes, and Portainer's "
        "updater rolls the image back if the container does not report healthy within 120 s: {}"
        .format(status, body[:400])))
    try:
        parsed = json.loads(body)
    except Exception as error:
        record(json_target, "its body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    record(json_target, None if isinstance(parsed, list) else (
        "it returned a {} rather than a list: {!r}".format(type(parsed).__name__, parsed)))


def check_static_ui():
    """(g) The UI is one file. Serving it is the whole of the front end."""
    status_target = "GET / answers 200"
    type_target = "GET / is served as text/html"
    marker_target = "GET / carries {!r} from static/index.html".format(UI_MARKER)
    missing_target = "GET {} answers 404 rather than the index page".format(UNKNOWN_PATH)
    try:
        status, headers, body = call("GET", "/")
    except Exception as error:
        reason = "the request itself failed: {}".format(describe(error))
        for target in (status_target, type_target, marker_target):
            record(target, reason)
    else:
        record(status_target, None if status == 200 else (
            "it answered {}. StaticFiles is mounted on / and serves index.html from static/ — a 404 "
            "here means the directory reached the image empty or not at all: {}".format(
                status, body[:400])))
        content_type = headers.get_content_type()
        record(type_target, None if content_type == "text/html" else (
            "its Content-Type is {!r}. A browser will not render the dashboard".format(
                content_type)))
        record(marker_target, None if UI_MARKER in body else (
            "the marker is not in the {} bytes it returned. Something is being served at / but it "
            "is not this project's UI".format(len(body))))

    try:
        status, _headers, body = call("GET", UNKNOWN_PATH)
    except Exception as error:
        record(missing_target, "the request itself failed: {}".format(describe(error)))
        return
    record(missing_target, None if status == 404 and UI_MARKER not in body else (
        "it answered {} and {} the index marker. `StaticFiles(html=True)` is NOT an SPA "
        "catch-all — it looks for a 404.html, finds none in static/, and 404s. A fallback here "
        "would turn every mistyped API path into a 200 carrying a page of HTML".format(
            status, "carries" if UI_MARKER in body else "does not carry")))


def check_websocket():
    """(h) The channel the whole dashboard runs on.

    src/api.py accepts the connection and sends {"type": "snapshot"} straight away, then a "stats"
    frame every two seconds. Only the first frame is waited for: it is the one that proves the
    endpoint is wired up, and waiting for the periodic one would add two seconds to the gate to
    prove the same thing twice.

    `proxy=None` is explicit because websockets honours proxy environment variables by default, and
    a proxy variable inherited into this container would send a loopback connection somewhere else.
    """
    connect_target = "the /ws websocket accepts the connection"
    frame_target = "the first /ws frame is a {'type': 'snapshot'} message"
    try:
        from websockets.sync.client import connect
    except Exception as error:
        reason = "`websockets` does not import inside the image: {}".format(describe(error))
        record(connect_target, reason)
        record(frame_target, "not attempted: " + reason)
        return
    try:
        with connect("ws://127.0.0.1:8000/ws", open_timeout=15, close_timeout=5,
                     proxy=None) as websocket:
            record(connect_target, None)
            raw = websocket.recv(timeout=15)
    except Exception as error:
        reason = (
            "{}. Without /ws the dashboard connects and then shows nothing at all — every number on "
            "it comes from this channel".format(describe(error)))
        record(connect_target, reason)
        record(frame_target, "not attempted: " + reason)
        return
    try:
        frame = json.loads(raw)
    except Exception as error:
        record(frame_target, "the first frame is not JSON ({}): {!r}".format(
            describe(error), raw[:400]))
        return
    record(frame_target, None if frame.get("type") == "snapshot" else (
        "the first frame is {!r}. The UI keys its initial render off the snapshot and renders "
        "nothing until it arrives".format(frame)))


def main():
    # First, and deliberately: it is the cheapest check here and it is the one that explains the
    # others. A /data the serving uid cannot write turns the persistence rows below into a
    # confusing report about a store that answers 200 and saves nothing.
    check_process_identity()
    check_empty_state()
    check_crud_cycle()
    check_stats()
    check_static_ui()
    check_websocket()

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("{}: FAILED {}/{} targets".format(PROBE_MARKER, len(failures), len(rows)))
        # SystemExit and not `assert`, for the same reason as in the image probe.
        raise SystemExit(1)

    print("{}: {}/{} targets".format(PROBE_MARKER, len(rows), len(rows)))


if __name__ == "__main__":
    main()
'''


# ══ THE OUTER HALF ════════════════════════════════════════════════════════════════════════════════

def excerpt(text):
    """Bound what reaches the log, and say so when something was cut."""
    if text is None:
        return ""
    if len(text) <= EXCERPT_CHARS:
        return text
    return text[:EXCERPT_CHARS] + "\n[... truncated at {} characters]".format(EXCERPT_CHARS)


def docker(args, timeout, stdin_text=None):
    """Run a docker command.

    Returns (status, output) with stderr folded into stdout, because everything here is read by a
    human out of a CI log where the interleaving is the useful part. A status of None means the
    command produced no exit code at all — it timed out, or docker is not there — and `output` then
    explains which. Callers must keep that case apart from a non-zero exit: they mean different
    things and only one of them is a verdict about the image.
    """
    argv = ["docker"] + args
    try:
        completed = subprocess.run(
            argv,
            input=stdin_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            text=True)
    except FileNotFoundError:
        return None, (
            "`docker` is not on PATH. This gate drives the daemon from the runner, so it cannot run "
            "anywhere the docker CLI is missing")
    except subprocess.TimeoutExpired as error:
        return None, "`{}` did not finish within {} s. Output so far:\n{}".format(
            " ".join(argv), timeout, excerpt(error.output))
    return completed.returncode, completed.stdout or ""


def remove_container(name):
    """Best effort. Never the reason a check fails; the workflow cleans up too.

    `-v` matters here and is not the usual boilerplate: the Dockerfile declares VOLUME ["/data"], so
    every `docker run` of this image creates an ANONYMOUS volume. Removing the container without it
    leaves that volume behind on a daemon shared by the whole fleet, one per run, forever.
    """
    docker(["rm", "-f", "-v", name], REMOVE_TIMEOUT)


def probe_report_rows(output, prefix):
    """Parse a probe's own report lines back into rows of this gate's report.

    Both probes print the same `ok   <target>` / `FAIL <target> -> <reason>` shape this file does, so
    their verdicts merge into one list instead of arriving as a single opaque "the probe failed".
    That matters for the same reason every other check here has one row per claim: a run that breaks
    four things should say four things.
    """
    rows = []
    for line in output.splitlines():
        if line.startswith("ok   "):
            rows.append((prefix + line[5:], None))
        elif line.startswith("FAIL "):
            target, _, reason = line[5:].partition(" -> ")
            rows.append((prefix + target, reason or "the probe reported FAIL with no reason"))
    return rows


def wait_for_markers(name, markers, budget):
    """Poll `docker logs` until EVERY marker has appeared, or the budget runs out.

    All of them, not the first. Returning on whichever arrived first would leave the later ones
    judged against a snapshot taken BEFORE they could have been written — a check that reports the
    absence of something it never gave a chance to appear. Bounded in WALL CLOCK rather than in
    attempts, because each attempt shells out to `docker logs` with its own bound and an
    attempt-counted loop would multiply into minutes on a slow daemon.

    Returns (status, logs) from the last poll.
    """
    deadline = time.monotonic() + budget
    while True:
        status, logs = docker(["logs", name], LOGS_TIMEOUT)
        if status == 0 and all(marker in logs for marker in markers):
            return status, logs
        if time.monotonic() >= deadline:
            return status, logs
        time.sleep(BOOT_PAUSE)


def check_image_contract(image):
    """(a) What the image DECLARES: its command, where it runs, what wraps it, and DATA_DIR.

    Four claims, four rows. The ENTRYPOINT row carries the weight here: `docker exec` does not go
    through an entrypoint, so every in-container check in this gate enters PAST the privilege drop
    and `docker inspect` is the only witness that the wrapper is still on the image. An image that
    lost it would answer all of them exactly as it does now, while its real startup path went back
    to running the CMD as root.
    """
    targets = [
        "the image declares CMD {}".format(EXPECTED_CMD),
        "the image declares WORKDIR {}".format(APP_DIR),
        "the image declares ENTRYPOINT {}".format(EXPECTED_ENTRYPOINT),
        "the image declares {} in its environment".format(EXPECTED_DATA_DIR),
    ]
    status, output = docker(["inspect", "--format", "{{json .Config}}", image], INSPECT_TIMEOUT)
    if status is None:
        # No exit code at all: the docker CLI is missing or the call ran out its bound. The message
        # from docker() already says which, and neither means what a non-zero exit means.
        return [(target, output) for target in targets]
    if status != 0:
        reason = (
            "`docker inspect` exited {} — the tag does not exist on this daemon, so the build step "
            "and this step disagree about what was built. Output:\n{}".format(
                status, excerpt(output)))
        return [(target, reason) for target in targets]
    try:
        config = json.loads(output)
    except Exception as error:
        reason = "`docker inspect` returned something that is not JSON ({}): {}".format(
            type(error).__name__, excerpt(output))
        return [(target, reason) for target in targets]

    rows = []
    cmd = config.get("Cmd")
    rows.append((targets[0], None if cmd == EXPECTED_CMD else (
        "it declares {!r}. ci/smoke.py starts the container with its own command and reads the log "
        "uvicorn writes; a different CMD means the artefact that ships is not the one checked "
        "here".format(cmd))))

    workdir = config.get("WorkingDir")
    rows.append((targets[1], None if workdir == APP_DIR else (
        "it declares {!r}. main.py runs `uvicorn.run('src.api:app')`, which imports by relative "
        "path — from the wrong directory the process dies on ModuleNotFoundError".format(workdir))))

    entrypoint = config.get("Entrypoint")
    rows.append((targets[2], None if entrypoint == EXPECTED_ENTRYPOINT else (
        "it declares {!r}. /entrypoint.sh is what starts as root, chowns /data so a volume written "
        "by the older root-based image stays usable, and `exec`s gosu to drop to uid 1000. "
        "`docker exec` bypasses ENTRYPOINT, so every in-container check in this gate would keep "
        "passing while the container went back to serving as root".format(entrypoint))))

    env = config.get("Env") or []
    rows.append((targets[3], None if EXPECTED_DATA_DIR in env else (
        "its environment is {!r}. src/config.py reads DATA_DIR once at import and puts every stream "
        "this service knows about in one file under it; the wrong value writes the configuration "
        "into the container layer, where the next image update takes it away".format(env))))
    return rows


def start_serving_container(image, name):
    """(b) Start the image with its REAL command and nothing else.

    No environment overrides, no published port, no bind mount — see the module docstring. /data is
    the anonymous volume the Dockerfile's VOLUME line creates, which is exactly the fresh, empty
    volume the empty-state check needs.
    """
    target = "`docker run -d` starts the image with its own command"
    remove_container(name)
    status, output = docker(["run", "-d", "--name", name, image], START_TIMEOUT)
    if status is None:
        return [(target, output)], False
    if status != 0:
        return [(target, "`docker run -d` exited {}:\n{}".format(status, excerpt(output)))], False
    return [(target, None)], True


def check_startup_markers(name, started):
    """(b) uvicorn reports startup complete AND reports itself listening on 8000."""
    if not started:
        reason = "not attempted: the serving container never started"
        return [("the log carries {!r}".format(marker), reason) for marker in STARTUP_MARKERS], ""

    status, logs = wait_for_markers(name, STARTUP_MARKERS, BOOT_BUDGET)
    if status is None:
        reason = "not attempted: `docker logs` produced no exit code — {}".format(logs)
        return [("the log carries {!r}".format(marker), reason) for marker in STARTUP_MARKERS], ""

    rows = []
    for marker in STARTUP_MARKERS:
        rows.append((
            "the log carries {!r}".format(marker),
            None if marker in logs else (
                "it never appeared within {} s. The process did not get this far, which for a "
                "bridge means it is subscribed to nothing and every message published in the "
                "meantime is gone — MQTT does not replay them".format(BOOT_BUDGET))))
    return rows, logs


def run_probe(name, started, program, timeout, marker, expected_targets, prefix, label):
    """Feed one probe to `docker exec -i <name> python -u -` and merge its verdicts into ours.

    `-i` attaches stdin without asking for a tty (none is needed and none is available on a runner),
    the image's own interpreter runs the program, and `docker exec` propagates the command's exit
    status — which is what lets the two consistency rows below tell "the probe reported failures"
    apart from "the probe died before it could report anything".
    """
    exit_target = "the {} probe's exit status agrees with its own report".format(label)
    marker_target = "the {} probe ran to its end, reporting all {} targets".format(
        label, expected_targets)
    if not started:
        return [("the {} probe".format(label),
                 "not attempted: its container never started")], ""

    status, output = docker(["exec", "-i", name, "python", "-u", "-"], timeout, stdin_text=program)
    if status is None:
        # Timed out, or docker is missing. Either way there are no verdicts to merge.
        return [("the {} probe".format(label), output)], output

    rows = probe_report_rows(output, prefix)
    if not rows:
        # Exit status alone cannot be read here: a probe that printed nothing has told us nothing,
        # whatever it exited with. This is what a `docker exec` that could not start the interpreter
        # looks like, and what a truncated stdin looks like.
        return [("the {} probe".format(label), (
            "it produced no report lines at all, so none of its checks ran. `docker exec` exited "
            "{}. Output:\n{}".format(status, excerpt(output))))], output

    reported_failures = any(reason is not None for _, reason in rows)
    if status != 0 and not reported_failures:
        rows.append((exit_target, (
            "`docker exec` exited {} but every line the probe printed says ok — so it died after "
            "reporting and before finishing, and the report above is incomplete".format(status))))
    elif status == 0 and reported_failures:
        rows.append((exit_target, (
            "the probe printed FAIL lines yet exited 0. Its failures are supposed to leave through "
            "SystemExit(1); an exit of 0 here means they no longer do, and this gate would have "
            "gone green on them")))
    else:
        rows.append((exit_target, None))

    # Exact, not a substring of the marker alone: see the note on the EXPECTED_*_TARGETS constants.
    expected_line = "{}: {}/{} targets".format(marker, expected_targets, expected_targets)
    if expected_line in output:
        rows.append((marker_target, None))
    elif marker not in output:
        rows.append((marker_target, (
            "it never printed {!r}. The marker is on the probe's last line, so its absence means "
            "the program did not run to the end — a truncated stdin, or something that killed the "
            "interpreter mid-report".format(marker))))
    else:
        # It finished, but with a different number of verdicts than this gate expects. That is a gate
        # that has quietly started proving less, which is the failure mode nothing else here can see:
        # every row it DID print says ok, and the exit status is 0.
        summary = [line for line in output.splitlines() if line.startswith(marker)]
        rows.append((marker_target, (
            "it ran to the end but reported {!r} instead of {} targets. Either a check stopped "
            "emitting rows — in which case this gate is now proving less than it says it does and "
            "nothing else would have noticed — or one was added and the EXPECTED_*_TARGETS constant "
            "in this file needs updating".format(
                summary[0] if summary else "(unparseable)", expected_targets))))
    return rows, output


def check_container_alive(name, started):
    """(k) The serving container is still up after everything else has had its turn at it."""
    target = "the serving container is still running at the end of the gate"
    if not started:
        return [(target, "not attempted: it never started")]
    status, output = docker(
        ["inspect", "--format", "{{.State.Running}} {{.State.ExitCode}}", name], INSPECT_TIMEOUT)
    if status is None:
        return [(target, output)]
    if status != 0:
        return [(target, "`docker inspect` exited {}: {}".format(status, excerpt(output)))]
    state = output.strip()
    if state.startswith("true"):
        return [(target, None)]
    return [(target, (
        "it is not running any more (`Running ExitCode` = {!r}). It answered the probes and then "
        "stopped — in production, with `restart: always`, that is a container that comes back, "
        "drops its subscription, and loses whatever was published in between".format(state)))]


def check_final_log(name, started):
    """(k) The log the container has BY NOW is free of tracebacks.

    Read fresh, at the end, rather than reusing the snapshot the boot wait returned: everything else
    in this gate has run against the container since then — a full CRUD cycle, a websocket
    connection — and a crash provoked by one of those would be invisible in the older copy.
    """
    target = "the serving container's final log is free of tracebacks"
    if not started:
        return [(target, "not attempted: it never started")], ""
    status, logs = docker(["logs", name], LOGS_TIMEOUT)
    if status is None:
        return [(target, logs)], ""
    if status != 0:
        return [(target, "`docker logs` exited {}: {}".format(status, excerpt(logs)))], logs
    if TRACEBACK_MARKER in logs:
        return [(target, (
            "it contains {!r}. Something raised past a handler that was supposed to contain it; the "
            "transcript above has the whole log".format(TRACEBACK_MARKER)))], logs
    return [(target, None)], logs


def main():
    image = os.environ.get(IMAGE_ENV)
    if not image:
        print("{} is not set: this gate tests the image it is given and has no default, because a "
              "default would silently gate whichever image happened to be on the daemon".format(
                  IMAGE_ENV))
        raise SystemExit(1)
    name = os.environ.get(NAME_ENV)
    if not name:
        print("{} is not set: this gate names every container it starts after it, and a default "
              "would both hide containers from the workflow's cleanup step and make two concurrent "
              "runs collide on one name".format(NAME_ENV))
        raise SystemExit(1)

    rows = []
    transcripts = []

    rows.extend(check_image_contract(image))

    try:
        serve_rows, serve_started = start_serving_container(image, name + SERVE_SUFFIX)
        rows.extend(serve_rows)

        marker_rows, _boot_logs = check_startup_markers(name + SERVE_SUFFIX, serve_started)
        rows.extend(marker_rows)

        image_probe_rows, image_probe_output = run_probe(
            name + SERVE_SUFFIX, serve_started, IMAGE_PROBE, IMAGE_PROBE_TIMEOUT,
            IMAGE_PROBE_MARKER, EXPECTED_IMAGE_PROBE_TARGETS, IMAGE_PROBE_ROW_PREFIX, "image")
        transcripts.append(("the image probe, from inside the container", image_probe_output))
        rows.extend(image_probe_rows)

        service_probe_rows, service_probe_output = run_probe(
            name + SERVE_SUFFIX, serve_started, SERVICE_PROBE, SERVICE_PROBE_TIMEOUT,
            SERVICE_PROBE_MARKER, EXPECTED_SERVICE_PROBE_TARGETS, SERVICE_PROBE_ROW_PREFIX,
            "service")
        transcripts.append(("the service probe, from inside the container", service_probe_output))
        rows.extend(service_probe_rows)

        # After the probes on purpose: "the container is still up" is only worth anything once
        # everything else has had its turn at it.
        rows.extend(check_container_alive(name + SERVE_SUFFIX, serve_started))

        # LAST, and that is the reason it exists: the log the container has at this point covers the
        # whole run rather than the snapshot the boot wait returned.
        final_rows, final_logs = check_final_log(name + SERVE_SUFFIX, serve_started)
        rows.extend(final_rows)
        transcripts.append(("the serving container", final_logs))
    finally:
        # Unconditionally. When a bound fires inside docker() it kills the docker CLIENT on the
        # runner, not the container on the daemon, so a hung run would otherwise sit here pinning
        # the image until somebody noticed. The workflow removes the same name again under
        # `if: always()`, which covers this whole script being killed by the step timeout.
        for suffix in ALL_SUFFIXES:
            remove_container(name + suffix)

    # The transcripts first, the verdicts last: in a CI log the verdicts are what somebody scrolls to
    # the bottom for, and the containers' own output is what turns a one-line verdict into a
    # diagnosis.
    for label, text in transcripts:
        print("")
        print("--- {} ---".format(label))
        print(excerpt(text).rstrip() or "(no output)")
    print("")
    print("--- results ---")

    # Before a single verdict is printed: the NUMBER of verdicts is itself one. A check_* that
    # stopped emitting rows takes its own verdicts out of the report and takes nothing red with them,
    # so the run would end `smoke ok: 48/48`, exit 0, and have every printed row saying ok with two
    # claims silently no longer made.
    #
    # ONLY on a run where nothing else failed, and that is not laziness about the arithmetic. A
    # failing check_* legitimately reports fewer rows than its happy path — run_probe emits 1 instead
    # of 15 when its exec timed out — so on an already-red run this row would fire too, on top of the
    # real failure, and read as though the GATE were broken. The fault it exists to catch is
    # invisible on a red run and decisive on a green one, which is exactly where it is still
    # reported.
    if len(rows) != EXPECTED_TOTAL_TARGETS and not any(reason is not None for _, reason in rows):
        rows.append((
            "this gate produced all {} of the verdicts it is supposed to".format(
                EXPECTED_TOTAL_TARGETS),
            "it produced {}, and every one of them says ok. Either a check stopped emitting rows — "
            "in which case this gate is now proving less than it says it does and nothing else here "
            "would have noticed — or one was added or removed and EXPECTED_TOTAL_TARGETS needs "
            "updating".format(len(rows))))

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("")
        print("smoke FAILED: {}/{} targets broken:".format(len(failures), len(rows)))
        for target in failures:
            print("  - {}".format(target))
        raise SystemExit(1)

    print("")
    print("smoke ok: {}/{} targets".format(len(rows), len(rows)))


if __name__ == "__main__":
    main()
