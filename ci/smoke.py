"""Smoke gate for the mqtt2influx image, run on the CI runner against an already-built image.

It sits between `docker build` and `docker push`. The deployed stack carries
`io.portainer.update.enable` (see docker-compose.yml), so whatever lands on `:latest` is rolled out
and this gate is the last point at which a broken image can be stopped. The service bridges MQTT
into InfluxDB and MQTT does not buffer for a disconnected subscriber, so the gate asks whether the
image serves, not merely whether the container starts.

THE SPLIT
---------
The OUTER half is this file, run as `python3 ci/smoke.py` on the runner. It drives `docker` and
checks what `docker inspect` declares, what the real command logs and whether it stays up.

The INNER halves are probes kept here as strings and fed to `docker exec -i <name> python -u -`.
The IMAGE probe checks the files and imports in the image; the SERVICE probe drives the REST API,
the config file on disk, the static UI and the websocket; the REPAIR probe checks that the
entrypoint makes a root-owned /app/data writable again after a restart.

act_runner runs this job in its own container while `docker` drives a daemon outside it, so the two
share neither a network namespace nor a filesystem. Nothing here publishes a port or bind-mounts a
host path; everything inside the container goes through `docker exec`.

`docker exec` bypasses ENTRYPOINT and runs as the image's Config.User, which is unset, so probes
arrive as root. Identity checks therefore read PID 1, the only process that came through the
entrypoint, the contract check pins ENTRYPOINT, and the repair probe runs as `-u 1000:1000`.

Every stream the gate creates has `enabled: false`: create_stream() starts an enabled stream at
once, and it would retry its made-up broker forever.

THE CHECKS
----------
* (a) the image declares the expected CMD, WORKDIR and ENTRYPOINT. Outer half.
* (b) the real command starts and uvicorn reports listening on 8000. Outer half.
* (c) GET /api/streams answers 200 with [] on a fresh /app/data. Service probe.
* (d) the CRUD cycle over HTTP, including a second DELETE that must be a 404. Service probe.
* (e) /app/data/streams.json on the container's disk holds the created stream. Service probe.
* (f) GET /api/stats, the compose healthcheck's target, answers 200 with JSON. Service probe.
* (g) / serves static/index.html as text/html, and an unknown path is a 404. Service probe.
* (h) /ws accepts and sends its {"type": "snapshot"} first frame. Service probe.
* (i) .venv, .git, tests/ and ci/ are not in the image, and /app/data is empty. Image probe.
* (j) fastapi, uvicorn, aiomqtt, aiohttp and websockets import. Image probe.
* (k) the container is still running after the probes, and its final log has no traceback.
      Outer half.
* (l) PID 1 runs as uid 1000, not as root. Service probe.
* (m) /app/data is writable by the uid PID 1 runs as. Service probe.
* (n) with /app/data chowned to root:root and the container restarted, the entrypoint makes it
      writable by uid 1000 again. Outer half + repair probe.
* (o) SIGTERM makes PID 1 exit 0 instead of being killed. Last on the container, because it ends
      it. Outer half.

Failures leave through SystemExit, never `assert`, which PYTHONOPTIMIZE=1 strips. Every check runs
before the run is judged, a check that cannot run reports FAILED, and each probe's rows are merged
into this report. Nothing here holds a credential: the created stream names only `mqtt.invalid` and
`influx.invalid` (RFC 2606) and is never started, so the gate also runs on pull requests.
"""

import json
import os
import subprocess
import time

# The tag to test. Required rather than defaulted: a default would let a mistyped `env:` block in a
# workflow silently gate some other image that happens to be on the daemon.
IMAGE_ENV = "SMOKE_IMAGE"
# Base name for every container this gate starts. The workflows put the run id in it, because the
# runner's one daemon is shared by the whole fleet and concurrent runs must not collide on a name;
# their `if: always()` cleanup builds the same name from the same variable.
NAME_ENV = "SMOKE_NAME"

SERVE_SUFFIX = "-serve"

# ── what the image is supposed to declare ─────────────────────────────────────────────────────────
APP_DIR = "/app"
# The uid the Dockerfile creates and /entrypoint.sh drops to. Spelled out on this side too, and not
# only inside the probes, because the repair check below has to `docker exec` AS that uid.
APP_UID = 1000
STATE_DIR = "/app/data"
EXPECTED_CMD = ["python", "main.py"]
# Pinned exactly: `docker exec` bypasses the entrypoint, so this is the only evidence the gate has
# that the privilege drop is still on the image.
EXPECTED_ENTRYPOINT = ["/entrypoint.sh"]

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
REPAIR_PROBE_MARKER = "mqtt2influx repair probe ok"
# The number of verdicts each probe is supposed to print. Compared EXACTLY rather than "at least",
# because the failure this catches is a probe that quietly stopped checking things: every line it
# does print says ok and it exits 0, so nothing else in this file would notice.
# 15 = 5 files that must be in the image + 4 that must not + /app/data is empty + 5 runtime imports.
EXPECTED_IMAGE_PROBE_TARGETS = 15
# 24 = 2 process identity + 2 empty state + 12 CRUD + 2 stats + 4 static UI + 2 websocket.
EXPECTED_SERVICE_PROBE_TARGETS = 24
# 2 = PID 1 is still uid 1000 after the restart + it can write /app/data again.
EXPECTED_REPAIR_PROBE_TARGETS = 2

IMAGE_PROBE_ROW_PREFIX = "[in-image] "
SERVICE_PROBE_ROW_PREFIX = "[in-container] "
# Says which uid asked, because that is the whole difference between this probe and the other two.
REPAIR_PROBE_ROW_PREFIX = "[in-container, as uid {}] ".format(APP_UID)

# Rows this file produces itself: 3 from the contract check, 1 for the container starting, 2 startup
# markers, 1 for it still running after the probes, 2 for staging and restarting on a root-owned
# /app/data, 2 for the SIGTERM shutdown, 1 for the final log.
EXPECTED_OUTER_TARGETS = 12
# Each probe contributes its own targets plus the two consistency rows run_probe() adds.
EXPECTED_TOTAL_TARGETS = (
    EXPECTED_OUTER_TARGETS
    + EXPECTED_IMAGE_PROBE_TARGETS + 2
    + EXPECTED_SERVICE_PROBE_TARGETS + 2
    + EXPECTED_REPAIR_PROBE_TARGETS + 2)

# ── bounds ────────────────────────────────────────────────────────────────────────────────────────
# Every docker call is bounded, and the sum has to stay under the smoke step's `timeout-minutes` in
# both workflows, so that a slow gate fails with its own diagnosis and the `finally` still runs.
# The longest path, counting a wait_for_markers() as BOOT_BUDGET plus one LOGS_TIMEOUT:
#
#   the -serve container  840 s — inspect 30, rm 30, run 60, boot wait 90, image probe 120,
#                                 service probe 120, inspect 30, chown 30, restart 60, boot wait
#                                 90, repair probe 60, stop 60, inspect 30, final logs 30
#   the `finally`          30 s — one container removal
#
# 870 s, about 15 minutes.
INSPECT_TIMEOUT = 30
REMOVE_TIMEOUT = 30
START_TIMEOUT = 60
LOGS_TIMEOUT = 30
IMAGE_PROBE_TIMEOUT = 120
SERVICE_PROBE_TIMEOUT = 120
CHOWN_TIMEOUT = 30
RESTART_TIMEOUT = 60
REPAIR_PROBE_TIMEOUT = 60
# What `docker stop` gives the process before SIGKILL. A bound short enough to kill a process that
# was shutting down would fail a working image. Production gets 30 s in docker-compose.yml; 20 is
# enough here, since this container serves no streams and has no batch to flush.
SIGTERM_GRACE = 20
# Must exceed SIGTERM_GRACE, or the client would be killed on the runner while the daemon was still
# waiting out the grace period — and the container would then be judged by an inspect that ran mid
# shutdown.
STOP_TIMEOUT = 60

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

# One level deeper than the COPY lines, so a COPY that produced an empty directory fails here: with
# an empty static/ the service still starts and serves 404 at /.
PRESENT = [
    (APP_DIR + "/main.py", "the entrypoint the image's CMD runs"),
    (APP_DIR + "/src/api.py", "the ASGI app uvicorn is told to import"),
    (APP_DIR + "/src/config.py", "the stream store"),
    (APP_DIR + "/src/mqtt_processor.py", "the MQTT subscriber and batcher"),
    (APP_DIR + "/static/index.html", "the whole web UI, in one file"),
]

# Kept out by .dockerignore and the explicit COPY lines: .venv is ~65 MB of host-built wheels, .git
# would ship the history to a public registry, tests/ and ci/ run from the source tree. data/ is
# created by the Dockerfile, so it is checked for emptiness in EMPTY_DIRS instead.
ABSENT = [
    (APP_DIR + "/.venv", "a host-built virtualenv: ~65 MB of wheels for the wrong interpreter"),
    (APP_DIR + "/.git", "the repository history, which would ship to a public registry"),
    (APP_DIR + "/tests", "the test suite, which is run from the source tree and not from the image"),
    (APP_DIR + "/ci", "this gate itself"),
]

# Emptiness rather than one forbidden filename: any file in it (streams.json, a leftover .tmp, a
# backup) holds a developer's real broker credentials.
EMPTY_DIRS = [
    (APP_DIR + "/data", (
        "a developer's runtime state lives there and every file in it holds real broker "
        "credentials; the directory belongs in the image, its contents never do")),
]

# Imported rather than pip-listed: an installed wheel can still fail to import. Nothing in src/
# imports `websockets`; uvicorn loads it at runtime, and without it /ws is refused.
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


def check_empty(path, what):
    target = "{} is in the image and is EMPTY".format(path)
    try:
        if not os.path.isdir(path):
            return (target, (
                "it is not a directory in the image at all. The Dockerfile creates and chowns it so "
                "that a volume mounted there arrives owned by uid 1000; without it the mount point "
                "is created by the daemon at start, owned by root"))
        listed = sorted(os.listdir(path))
        if not listed:
            return (target, None)
        return (target, (
            "it holds {!r}, and it should hold nothing — {}. Either .dockerignore stopped covering "
            "the directory's contents or the Dockerfile grew a `COPY . .`".format(listed, what)))
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
    for path, what in EMPTY_DIRS:
        rows.append(check_empty(path, what))
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

# Where src/config.py saves under the default DATA_DIR, which this container runs with.
CONFIG_PATH = "/app/data/streams.json"
STATE_DIR = "/app/data"

# The uid /entrypoint.sh drops to. Compared with PID 1's, not this probe's: `docker exec` skips
# the entrypoint and arrives as root on any image.
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

    Not `dict(response.headers)`: an HTTPMessage looks names up case-insensitively, a dict does not.
    A non-2xx status arrives as HTTPError, which is a response too (404 is expected twice below), so
    it is unwrapped.
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

    The first column of `Uid:`/`Gid:` is the real id: an effective 1000 over a real 0 can climb
    back to root. procfs, because python:3.11-slim ships no `ps`.
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

    Asked of PID 1, the only process that came through the entrypoint. Runs before the CRUD cycle so
    an unwritable /app/data is reported as that, not as a missing streams.json.
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
    # Worked out from PID 1's ids, not with os.access(): that answers for the caller, root, for whom
    # every directory is writable.
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
    """(c) A fresh /app/data volume means no streams — the state right after a deploy."""
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

    One function, because every step needs the id the previous one produced. A step that cannot run
    records the remaining targets as FAILED, never skipped.
    """
    targets = [
        "POST /api/streams answers 200 and returns a generated id",
        "POST /api/streams echoes the fields it was given",
        "/app/data/streams.json exists on the container's disk after the POST",
        "/app/data/streams.json holds exactly the stream that was created",
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
        # A sentinel, not None, for "could not be read": a file holding `null` loads as None and
        # still needs a verdict.
        unread = object()
        on_disk = unread
        try:
            with open(CONFIG_PATH) as handle:
                on_disk = json.load(handle)
        except Exception as error:
            record(targets[3], "it is there but could not be read as JSON: {}".format(
                describe(error)))
        if on_disk is not unread:
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
            "concerned — and would be gone the moment the container restarts. Either /app/data is not "
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

    src/api.py sends {"type": "snapshot"} on accept and a "stats" frame every two seconds; only the
    first frame is waited for. `proxy=None` because websockets honours proxy variables by default.
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
    # others. A /app/data the serving uid cannot write turns the persistence rows below into a
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


# ══ THE REPAIR PROBE ══════════════════════════════════════════════════════════════════════════════
# Runs after /app/data was handed back to root:root and the container restarted; see
# check_volume_repair(). It runs as `-u 1000:1000`, the serving uid, so it simply tries to write.
REPAIR_PROBE = r'''
import os

PROBE_MARKER = "mqtt2influx repair probe ok"

APP_UID = 1000
STATE_DIR = "/app/data"
# Created and removed again. The unlink is part of the claim rather than tidiness: src/config.py
# saves by writing a temp file and renaming it over streams.json, so a directory that grants create
# and refuses unlink is one this service still cannot save into.
PROBE_FILE = STATE_DIR + "/.smoke-repair-probe"

rows = []


def describe(error):
    return "{}: {}".format(type(error).__name__, error)


def pid1_uid():
    """The REAL uid of PID 1, first column of the `Uid:` line — see the service probe for why.

    Readable from here despite this program running unprivileged: /proc/<pid>/status is world
    readable, and it is the process's own identity that is being asked for, not its memory.
    """
    with open("/proc/1/status") as handle:
        for line in handle:
            if line.startswith("Uid:"):
                return int(line.split()[1])
    return None


def main():
    uid_target = "PID 1 still serves as uid {} after the restart".format(APP_UID)
    write_target = "{} is writable by uid {} again once the entrypoint has repaired it".format(
        STATE_DIR, APP_UID)

    try:
        uid = pid1_uid()
    except Exception as error:
        rows.append((uid_target, "/proc/1/status could not be read: {}".format(describe(error))))
    else:
        rows.append((uid_target, None if uid == APP_UID else (
            "it runs as uid {!r}. The restart was supposed to bring the same process identity back, "
            "so whatever the next row proves about {} is about the wrong uid".format(
                uid, STATE_DIR))))

    try:
        with open(PROBE_FILE, "w") as handle:
            handle.write("smoke")
        os.remove(PROBE_FILE)
        rows.append((write_target, None))
    except Exception as error:
        rows.append((write_target, (
            "it is not: {}. /app/data was handed back to root:root and the container restarted: a "
            "volume with root-owned contents, which docker never re-seeds, because it seeds a "
            "volume only while it is empty. Repairing that is the ONLY thing `chown -R app:app /app/data` in /entrypoint.sh does, and this row is the "
            "only one in this gate that can tell whether it still happens. Note what does NOT break "
            "without it: the container starts, _config_path() makedirs(exist_ok=True) succeeds on a "
            "directory that already exists, load_streams() swallows the PermissionError and returns "
            "[], /api/stats answers 200 with an empty list and the healthcheck goes green — while "
            "the bridge subscribes to nothing and writes nothing".format(describe(error)))))

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("{}: FAILED {}/{} targets".format(PROBE_MARKER, len(failures), len(rows)))
        # SystemExit and not `assert`, for the same reason as in the other two probes.
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
    """Run a docker command; returns (status, output) with stderr folded into stdout.

    A status of None means there is no exit code at all (a timeout, or no docker CLI) and `output`
    says which. Callers keep it apart from a non-zero exit: only the latter can be a verdict about
    the image.
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

    `-v` also removes any anonymous volume the container has, so none piles up on the shared daemon.
    """
    docker(["rm", "-f", "-v", name], REMOVE_TIMEOUT)


def probe_report_rows(output, prefix):
    """Parse a probe's own report lines back into rows of this gate's report.

    Probes print the same `ok   <target>` / `FAIL <target> -> <reason>` lines as this file, so every
    claim made inside the container gets its own row.
    """
    rows = []
    for line in output.splitlines():
        if line.startswith("ok   "):
            rows.append((prefix + line[5:], None))
        elif line.startswith("FAIL "):
            target, _, reason = line[5:].partition(" -> ")
            rows.append((prefix + target, reason or "the probe reported FAIL with no reason"))
    return rows


def wait_for_markers(name, markers, budget, minimum=1):
    """Poll `docker logs` until every marker has appeared `minimum` times, or the budget runs out.

    Bounded in wall-clock time, because every poll carries its own bound. Counted, because `docker
    logs` returns the whole history: after a restart the first boot's markers are still in it.

    Returns (status, logs) from the last poll.
    """
    deadline = time.monotonic() + budget
    while True:
        status, logs = docker(["logs", name], LOGS_TIMEOUT)
        if status == 0 and all(logs.count(marker) >= minimum for marker in markers):
            return status, logs
        if time.monotonic() >= deadline:
            return status, logs
        time.sleep(BOOT_PAUSE)


def check_image_contract(image):
    """(a) What the image declares: its command, where it runs, and what wraps it.

    The ENTRYPOINT row matters most: every in-container check enters through `docker exec`, past the
    privilege drop, so `docker inspect` is the only witness that the wrapper is still on the image.
    """
    targets = [
        "the image declares CMD {}".format(EXPECTED_CMD),
        "the image declares WORKDIR {}".format(APP_DIR),
        "the image declares ENTRYPOINT {}".format(EXPECTED_ENTRYPOINT),
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
        "it declares {!r}. /entrypoint.sh is what starts as root, chowns /app/data so a root-owned "
        "volume stays writable, and `exec`s gosu to drop to uid 1000. "
        "`docker exec` bypasses ENTRYPOINT, so every in-container check in this gate would keep "
        "passing while the container went back to serving as root".format(entrypoint))))
    return rows


def start_serving_container(image, name):
    """(b) Start the image with its REAL command and nothing else.

    No environment, no published port, no mount: /app/data is the image's own empty directory owned
    by uid 1000, which is also what a new named volume holds, since docker seeds it from the image.
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
    if status != 0:
        # On a non-zero exit `logs` is docker's error, not the container's output, so it is neither
        # searched for markers nor returned as the transcript.
        reason = "not attempted: `docker logs` exited {}:\n{}".format(status, excerpt(logs))
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


def run_probe(name, ready, program, timeout, marker, expected_targets, prefix, label, user=None):
    """Feed one probe to `docker exec -i <name> python -u -` and merge its verdicts into ours.

    `docker exec` passes the exit status through, which lets the consistency rows below tell "the
    probe reported failures" from "the probe died". `user` becomes `-u`; without it the exec runs as
    root, since the image declares no USER.
    """
    exit_target = "the {} probe's exit status agrees with its own report".format(label)
    marker_target = "the {} probe ran to its end, reporting all {} targets".format(
        label, expected_targets)
    if not ready:
        return [("the {} probe".format(label),
                 "not attempted: the serving container was not in a state to be probed")], ""

    argv = ["exec", "-i"]
    if user is not None:
        argv += ["-u", user]
    argv += [name, "python", "-u", "-"]
    status, output = docker(argv, timeout, stdin_text=program)
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
    """(k) The serving container is still up after both probes have had their turn at it.

    Runs before the repair check: `docker restart` revives an exited container as well, which would
    hide a crash during the probes.
    """
    target = "the serving container is still running once both probes are done with it"
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


def check_volume_repair(name, started):
    """(n) Hand /app/data back to root, restart, and make the entrypoint repair it.

    Without this, (m) cannot fail: the container runs with no volume, so /app/data is the image's
    own directory, already owned by uid 1000, and a missing chown in entrypoint.sh goes unnoticed.
    A named volume that already holds root-owned files is never re-seeded from the image, so the
    entrypoint's chown is what makes it writable; `docker exec` arrives as root and can stage that.

    Returns (rows, ready): `ready` gates the repair probe.
    """
    stage_target = "{} can be handed back to root:root, like a volume with root-owned files".format(
        STATE_DIR)
    restart_target = "the serving container comes back up with {} owned by root".format(STATE_DIR)
    if not started:
        reason = "not attempted: the serving container never started"
        return [(stage_target, reason), (restart_target, reason)], False

    # `-u 0:0` explicitly: this chown must run as root whatever USER the image declares.
    status, output = docker(
        ["exec", "-u", "0:0", name, "chown", "-R", "0:0", STATE_DIR], CHOWN_TIMEOUT)
    if status is None:
        return [(stage_target, output), (restart_target, "not attempted: " + output)], False
    if status != 0:
        reason = "`docker exec chown -R 0:0 {}` exited {}:\n{}".format(
            STATE_DIR, status, excerpt(output))
        return [(stage_target, reason), (restart_target, "not attempted: " + reason)], False
    rows = [(stage_target, None)]

    status, output = docker(["restart", name], RESTART_TIMEOUT)
    if status is None:
        rows.append((restart_target, output))
        return rows, False
    if status != 0:
        rows.append((restart_target, "`docker restart` exited {}:\n{}".format(
            status, excerpt(output))))
        return rows, False

    # minimum=2: the first boot already put one of each marker in this log and `docker logs` returns
    # the whole of it, so anything less would pass on the container's history instead of on its
    # restart. See wait_for_markers().
    log_status, logs = wait_for_markers(name, STARTUP_MARKERS, BOOT_BUDGET, minimum=2)
    if log_status is None:
        rows.append((restart_target, "`docker logs` produced no exit code — {}".format(logs)))
        return rows, False
    if log_status != 0:
        # Its own reason: `logs` is docker's error text here, not the container's output, and
        # counting markers in it would blame the entrypoint for a daemon that did not answer.
        rows.append((restart_target, "`docker logs` exited {}:\n{}".format(
            log_status, excerpt(logs))))
        return rows, False
    missing = [marker for marker in STARTUP_MARKERS if logs.count(marker) < 2]
    if missing:
        rows.append((restart_target, (
            "it never printed {} a second time within {} s. The entrypoint runs again on every "
            "start, and a container that does not come back from an /app/data it cannot chown is one "
            "that now EXITS on it — which under `restart: always` is not a failed deploy but an "
            "unbounded restart loop, and a bridge in a restart loop is losing every message "
            "published while it is down".format(missing, BOOT_BUDGET))))
        return rows, False
    rows.append((restart_target, None))
    return rows, True


def check_sigterm_shutdown(name, started):
    """(o) SIGTERM reaches PID 1 and it shuts itself down instead of being killed.

    This is what the gosu exec is for: behind `su`, `sudo` or a `sh -c` wrapper, PID 1 is not python
    and the signal does not reach it. `docker stop` is what a redeploy does: SIGTERM, wait, SIGKILL.
    Exit 0 means uvicorn ran the lifespan shutdown, where manager.stop_all() flushes each stream's
    batch; 137 (128+9) means the grace period ran out; 143 (128+15) means nothing handled it.

    Last of everything that touches the container, because it ends it.
    """
    stop_target = "`docker stop` completes within the {} s grace period".format(SIGTERM_GRACE)
    code_target = "the serving container exits 0 on SIGTERM rather than being killed"
    if not started:
        reason = "not attempted: the serving container never started"
        return [(stop_target, reason), (code_target, reason)]

    status, output = docker(["stop", "-t", str(SIGTERM_GRACE), name], STOP_TIMEOUT)
    if status is None:
        return [(stop_target, output), (code_target, "not attempted: " + output)]
    if status != 0:
        reason = "`docker stop` exited {}:\n{}".format(status, excerpt(output))
        return [(stop_target, reason), (code_target, "not attempted: " + reason)]
    rows = [(stop_target, None)]

    status, output = docker(
        ["inspect", "--format", "{{.State.ExitCode}}", name], INSPECT_TIMEOUT)
    if status is None:
        rows.append((code_target, output))
        return rows
    if status != 0:
        rows.append((code_target, "`docker inspect` exited {}: {}".format(
            status, excerpt(output))))
        return rows
    code = output.strip()
    rows.append((code_target, None if code == "0" else (
        "it exited {!r}. {} A signal that does not reach the python process is the one failure this "
        "image's ENTRYPOINT is built to prevent — `su` and `sudo` fork and wait, gosu execs — and it "
        "costs whatever was batched and unwritten at the moment of every deploy".format(
            code, {
                "137": "That is 128+9: SIGKILL, sent because the {} s grace period ran out with the "
                       "process still alive.".format(SIGTERM_GRACE),
                "143": "That is 128+15: SIGTERM arrived and killed it outright, so no handler ran "
                       "and the shutdown path that flushes never executed.",
            }.get(code, "That is neither a clean exit nor a signal this gate recognises.")))))
    return rows


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
        # everything else has had its turn at it. Also BEFORE the restart below — see its docstring.
        rows.extend(check_container_alive(name + SERVE_SUFFIX, serve_started))

        # The container is deliberately damaged from here on: /app/data goes back to root and the
        # container is restarted onto it, so nothing above this line could have run afterwards.
        repair_rows, repair_ready = check_volume_repair(name + SERVE_SUFFIX, serve_started)
        rows.extend(repair_rows)

        # Numeric uid and gid: `-u 1000` alone leaves the group at 0, which is not what `gosu app`
        # gives the service, and `-u app` would trust the image's /etc/passwd.
        repair_probe_rows, repair_probe_output = run_probe(
            name + SERVE_SUFFIX, repair_ready, REPAIR_PROBE, REPAIR_PROBE_TIMEOUT,
            REPAIR_PROBE_MARKER, EXPECTED_REPAIR_PROBE_TARGETS, REPAIR_PROBE_ROW_PREFIX, "repair",
            user="{0}:{0}".format(APP_UID))
        transcripts.append(
            ("the repair probe, from inside the restarted container", repair_probe_output))
        rows.extend(repair_probe_rows)

        # Ends the container, so it goes after everything that needed it alive and before the two
        # steps that do not: the final log and the cleanup both work on a stopped container.
        rows.extend(check_sigterm_shutdown(name + SERVE_SUFFIX, serve_started))

        # LAST of everything about the -serve container, and that is the reason it exists: the log
        # it has at this point covers the whole run rather than the snapshot the boot wait returned.
        final_rows, final_logs = check_final_log(name + SERVE_SUFFIX, serve_started)
        rows.extend(final_rows)
        transcripts.append(("the serving container", final_logs))
    finally:
        # A bound firing in docker() kills the client, not the container, so the container is
        # removed here unconditionally; the workflow removes it again under `if: always()`.
        remove_container(name + SERVE_SUFFIX)

    # The transcripts first, the verdicts last: in a CI log the verdicts are what somebody scrolls to
    # the bottom for, and the containers' own output is what turns a one-line verdict into a
    # diagnosis.
    for label, text in transcripts:
        print("")
        print("--- {} ---".format(label))
        print(excerpt(text).rstrip() or "(no output)")
    print("")
    print("--- results ---")

    # The number of verdicts is itself a verdict: a check that stops emitting rows leaves an all-ok
    # report behind. Only judged on an otherwise green run: a failing check emits fewer rows.
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
