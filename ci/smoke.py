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

The three INNER halves are PROBES: programs that live here as string constants and are fed to
`docker exec -i <name> python -u -`. The IMAGE probe answers everything about the FILES and the
dependencies inside the image. The SERVICE probe answers whether the running process is really
serving — the whole REST surface, the config file it writes to the volume, the static UI and the
websocket. The REPAIR probe runs once more at the end, against a container that has been restarted
on an /app/data deliberately handed back to root, and asks whether the entrypoint put it right again.

They run inside for two reasons. The obvious one is that those objects only exist in there. The
other shapes this whole file: **this job and the docker daemon are not in the same network
namespace.** Gitea's act_runner executes the job inside its own job container while the `docker` CLI
it provides drives a daemon outside it. So:

* NO PORT IS PUBLISHED by anything here. Publishing 8000 would land it in the HOST daemon's network
  namespace, and this job's 127.0.0.1 is a different loopback entirely — the gate would be talking
  to its own empty socket and would have to be written as "connection refused, so probably fine",
  which is the kind of check that passes forever after the service stops working.
* NO HOST PATH IS BIND-MOUNTED either. A `-v $(pwd):/w` would hand the daemon a path that exists in
  this container's filesystem and not in its own; it would mount an empty directory, and every file
  check would pass by looking at nothing. NAMED volumes are a different thing entirely — they live
  on the daemon's side and mean the same to both — which is why check_legacy_layout() and
  check_empty_legacy_layout() below are allowed to create one each, seed the first through a
  throwaway container, and mount both.

Everything internal therefore goes through `docker exec`, which puts the probe in the container's
own namespace — where 127.0.0.1 means what the compose healthcheck means by it.

Two more properties of `docker exec` are worth stating because checks below depend on them: it does
NOT go through ENTRYPOINT, and it runs as the image's `Config.User`. Both cut the same way here. The
image declares `/entrypoint.sh` — which starts as root, repairs the ownership of /app/data and `exec`s
gosu to drop to uid 1000 — and declares no `USER`, so every probe below arrives as ROOT and bypasses
that drop entirely. Half of that is convenient: a root probe can read /proc/1/status and stat /app/data
whatever their ownership. The other half is a trap, and it is the one that matters — a probe that
asked for its OWN uid would answer 0 forever and prove nothing about the process that serves. So the
non-root check interrogates PID 1, the only process in the container that came through the
entrypoint, and the contract check pins the ENTRYPOINT to an exact value rather than reading it off
a process that never went through one.

The repair probe is the one exception and passes `-u 1000:1000` for exactly that reason: its question
is whether the SERVING uid can write to /app/data, and root would answer yes to it on every image forever.
Both cuts are used deliberately — the same `docker exec`-arrives-as-root property is what lets this
gate stage a root-owned /app/data in the first place, which is a thing no process inside the container
can do to itself once the entrypoint has dropped privileges.

WHY THE CRUD CYCLE IS THE CENTRE OF THIS FILE
---------------------------------------------
tests/ already proves the SOURCE is good. It says nothing about the artefact: whether pip resolved a
dependency set that actually imports, whether .dockerignore still keeps the developer's data/ out,
whether static/ was copied at all, whether the volume at /app/data is writable by the user the image
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
* (a) the image's declared contract: CMD, WORKDIR, ENTRYPOINT, and the deliberate ABSENCE of a
      DATA_DIR in its environment. Outer half.
* (b) the real command starts and uvicorn reports itself listening on 8000. Outer half.
* (c) the empty state: GET /api/streams answers 200 with [] on a fresh volume. Service probe.
* (d) the full CRUD cycle over HTTP, including the second DELETE that must be a 404. Service probe.
* (e) persistence: /app/data/streams.json exists on the container's disk and holds the created stream.
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
* (k) the container is still running once both probes are done with it — asked BEFORE (n) restarts
      it, because `docker restart` revives a container that had exited just as readily as one that
      is up — and the log it has at the very end, after (o) has stopped it, carries no traceback.
      Outer half.
* (l) PID 1 — the process the ENTRYPOINT actually started — runs as uid 1000 and not as root. This
      is the artefact, not the intention: a Dockerfile can carry every non-root instruction there is
      and still serve as root if the entrypoint stopped `exec`ing gosu. Service probe.
* (m) /app/data is writable by the uid PID 1 really runs as. The privilege drop and the volume's
      ownership are set in two different places — a chown at build time and a chown at start — and
      an image that dropped privileges but left the volume owned by root comes up perfectly, serves
      every read, and fails the first time somebody saves a stream. Service probe.
* (n) the entrypoint really REPAIRS an /app/data it did not create. On its own (m) cannot fail: the
      container below runs with no `-v` and the image declares no VOLUME, so /app/data is simply the
      directory the IMAGE LAYER carries — created and `chown app:app`ed by the Dockerfile at build
      time — read and written through the container's writable layer, which `docker restart`
      preserves. It therefore arrives owned by uid 1000 whether the entrypoint chowns anything or
      not, and stays that way across the restart below. The case that is left uncovered is the only
      one the entrypoint's chown was ever written for: production's NAMED volume, created by the
      older ROOT-based image, which docker never re-seeds from the image because it seeds a volume
      only while it is empty — so it keeps root's ownership forever. So this check stages that
      state — hand /app/data back to root:root through `docker exec`, restart the container, ask (l)
      and (m) again of a directory nothing put right for us. Outer half + repair probe.
* (o) SIGTERM reaches PID 1 and it exits 0, rather than sitting there until the grace period runs
      out and the kernel kills it. The whole gosu-and-not-su paragraph in the Dockerfile is about
      this one property, and (l) catches only its crudest loss: under `su` PID 1 would run as uid 0,
      but a `sh -c "python main.py"` wrapper keeps the uid and drops the signal all the same. Runs
      LAST of everything that touches the -serve container, because it ENDS it. Outer half.
* (p) THE LEGACY LAYOUT, in a SECOND container shaped the way the older deployments really are: a
      named volume already holding a streams.json mounted at /data, and NO DATA_DIR in the
      environment. Everything above runs in the consistent layout — no environment overrides, no
      mount at all — so no check above this one can even reach the combination that costs the most.
      This one asserts that the entrypoint says in the log which path it fell back to and why, that
      the service SERVES the streams the volume really holds rather than an empty list, and that a
      stream saved through the API is written back onto that same volume. Outer half + legacy probe.
      It runs in its own container and its own volume, both removed in the `finally`.
* (q) THE SAME LEGACY SHAPE WITH AN EMPTY VOLUME, in a THIRD container: a named volume holding NO
      streams.json, mounted at /data, and again no environment at all. That is a legacy-shaped stack
      which has simply not saved anything yet — a deployment of the old shape just created, a volume
      restored empty, a file removed by hand. (p) CANNOT SEE THIS CASE, because (p) seeds a
      streams.json onto its volume BEFORE mounting it: with that file already there, an entrypoint
      that tests for the legacy FILE and one that tests for the legacy MOUNT both fire, and (p)
      comes out green on either. So this check asserts where the FIRST SAVE actually lands —
      `docker diff` says nothing was written into the container's writable layer at /app/data, and
      the volume, read back from a throwaway container, holds the stream the API accepted. Outer
      half + empty-legacy-volume probe. Its own container and its own volume, both removed in the
      `finally`.

WHY (p) AND (q) ARE HERE AT ALL, AND WHY NEITHER REPLACES THE OTHER
-------------------------------------------------------------------
An empty answer is a VALID answer everywhere else in this service. `load_streams()` returns [] for a
file that is not there, /api/streams answers 200 with it, /api/stats answers 200 with an empty list,
and the compose healthcheck is satisfied — so a container that is looking at the wrong directory is
indistinguishable, from the outside, from a container that has not been configured yet. Portainer's
updater then records a successful deploy and rolls nothing back. There is no other copy of
streams.json: it holds every stream with its MQTT and InfluxDB credentials. So the one shape this
gate must be able to reproduce is the one where the deployment and the image disagree about where
that file is, and (p) is that shape.

(q) is that same shape with the file NOT WRITTEN YET, and it is a check of its own because the
detection has already been wrong twice in exactly that gap. First it did not exist at all and the
image defaulted to /app/data; then it asked for the legacy FILE instead of the legacy MOUNT — which
(p) is structurally blind to, since it puts a streams.json on the volume before mounting it and both
tests are then true. A legacy-shaped deployment whose volume was still empty therefore resolved to
/app/data, and with the Dockerfile's VOLUME line gone that path is the container's WRITABLE LAYER:
the first stream saved through the UI survived a restart and was destroyed by the next automatic
image update, while the container stayed healthy and /api/stats went on answering 200. So (q) mounts
an EMPTY volume in the legacy shape and asks where the first save lands. Neither check subsumes the
other — (p) is the volume that already holds a configuration, (q) is the volume that does not hold
one yet, and only (q) can go red on a detection that looks at the file instead of at the mount. See
the docstring of check_empty_legacy_layout() before deciding one of them is redundant.

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
# The second container: the image started the way the OLDER deployments really start it. See
# check_legacy_layout().
LEGACY_SUFFIX = "-legacy"
# The third container: the SAME older shape as the second, but with a volume that holds nothing yet.
# A name of its own rather than a reuse of the one above — both containers exist at the same time by
# the end of the run, and the cleanup has to be able to name each of them. See
# check_empty_legacy_layout().
EMPTY_LEGACY_SUFFIX = "-legacy-empty"
# The two SHORT-LIVED containers: the one that seeds check (p)'s volume and the one that reads check
# (q)'s volume back. Both run `--rm` and both live for a second, which is exactly why they still need
# names. `--rm` is the DAEMON removing the container when the process exits; when a bound in docker()
# fires it kills the docker CLIENT on the runner and leaves the container running on the daemon,
# which nothing then ever removes — this runner's daemon is shared by the whole fleet and outlives
# every job. The read-back one is the worse of the two: it holds the named volume open, so
# remove_volume() below would fail as well and the volume would leak with it.
#
# Appended to the ALREADY-SUFFIXED name each check was given, so the container reads as what it is —
# `<run>-legacy-seed`, `<run>-legacy-empty-readback` — and the concatenated suffixes below are what
# the cleanup has to know.
SEED_SUFFIX = "-seed"
READBACK_SUFFIX = "-readback"
# Kept in step with the cleanup step in both workflows, which removes the same suffixes.
ALL_SUFFIXES = (SERVE_SUFFIX, LEGACY_SUFFIX, EMPTY_LEGACY_SUFFIX,
                LEGACY_SUFFIX + SEED_SUFFIX, EMPTY_LEGACY_SUFFIX + READBACK_SUFFIX)
# The NAMED volumes this gate creates. Named after the run for the same reason the containers are:
# one docker daemon serves every repository on this runner, and two concurrent runs must not collide
# on them. Both workflows remove these names too, under `if: always()`.
#
# TWO volumes and not one reused twice, because the ONLY difference between check (p) and check (q)
# is that one of them holds a streams.json and the other holds nothing: sharing a volume would have
# the second check run against the first one's fixture, which is precisely the blindness (q) exists
# to remove.
LEGACY_VOLUME_SUFFIX = "-legacy-data"
EMPTY_LEGACY_VOLUME_SUFFIX = "-legacy-empty-data"
ALL_VOLUME_SUFFIXES = (LEGACY_VOLUME_SUFFIX, EMPTY_LEGACY_VOLUME_SUFFIX)

# ── what the image is supposed to declare ─────────────────────────────────────────────────────────
APP_DIR = "/app"
# The uid the Dockerfile creates and /entrypoint.sh drops to. Spelled out on this side too, and not
# only inside the probes, because the repair check below has to `docker exec` AS that uid.
APP_UID = 1000
STATE_DIR = "/app/data"
# Where the OLDER stacks bind their volume. They pass no environment at all, so this path is not a
# setting anywhere — it is a fact about deployments that already exist, and /entrypoint.sh detects
# it. check_legacy_layout() reproduces exactly that shape.
LEGACY_STATE_DIR = "/data"
EXPECTED_CMD = ["python", "main.py"]
# Pinned to an exact value, not merely "there is one". This is the ONLY evidence this gate has that
# the privilege-dropping wrapper is still on the image: every in-container check below enters through
# `docker exec`, which walks straight past the entrypoint, so an image that lost it would answer all
# of them exactly as it does now while quietly going back to serving as root.
EXPECTED_ENTRYPOINT = ["/entrypoint.sh"]
# THE IMAGE MUST NOT DECLARE DATA_DIR AT ALL, and this is an assertion about an ABSENCE on purpose.
#
# It used to be `ENV DATA_DIR=/app/data`, pinned here as the image's declared contract. That was
# wrong, and the way it was wrong is the reason this row now reads backwards. An always-set DATA_DIR
# makes the two layouts in this fleet indistinguishable from inside the container: /entrypoint.sh
# can no longer tell a deployment that CHOSE /app/data from one of the older stacks that binds its
# volume at /data and passes nothing, so it defaults to /app/data for both — and the older stack
# comes up on an empty directory, answers every read 200, reports healthy, and bridges nothing while
# its real configuration sits unread at /data.
#
# Unset is therefore the signal the entrypoint's detection depends on, which makes re-adding the ENV
# line a silent regression of check (p) below rather than a cosmetic change. Hence a row of its own,
# here, in the cheapest check in the gate.
DATA_DIR_ENV_PREFIX = "DATA_DIR="

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
LEGACY_PROBE_MARKER = "mqtt2influx legacy-layout probe ok"
EMPTY_LEGACY_PROBE_MARKER = "mqtt2influx empty-legacy-volume probe ok"
# The number of verdicts each probe is supposed to print. Compared EXACTLY rather than "at least",
# because the failure this catches is a probe that quietly stopped checking things: every line it
# does print says ok and it exits 0, so nothing else in this file would notice.
# 15 = 5 files that must be in the image + 4 that must not + /app/data is empty + 5 runtime imports.
EXPECTED_IMAGE_PROBE_TARGETS = 15
# 24 = 2 process identity + 2 empty state + 12 CRUD + 2 stats + 4 static UI + 2 websocket.
EXPECTED_SERVICE_PROBE_TARGETS = 24
# 2 = PID 1 is still uid 1000 after the restart + it can write /app/data again.
EXPECTED_REPAIR_PROBE_TARGETS = 2
# 3 = the API serves the seeded stream + a stream created through the API is written back onto that
# same volume + nothing was written to the path the deployment did not mount.
EXPECTED_LEGACY_PROBE_TARGETS = 3
# 2 = the POST is accepted + the service then lists what it accepted. Both of these pass on a BROKEN
# entrypoint as well, and that is deliberate: this probe's job is only to make the service save
# something. WHERE it landed is asked from outside, by check_empty_legacy_writes_landed(), because
# neither `docker diff` nor a read of the volume from a throwaway container can be performed by a
# process living inside the container under test.
EXPECTED_EMPTY_LEGACY_PROBE_TARGETS = 2

IMAGE_PROBE_ROW_PREFIX = "[in-image] "
SERVICE_PROBE_ROW_PREFIX = "[in-container] "
# Says which uid asked, because that is the whole difference between this probe and the other two.
REPAIR_PROBE_ROW_PREFIX = "[in-container, as uid {}] ".format(APP_UID)
# Says which CONTAINER asked: this one is not the -serve container every other probe runs in.
LEGACY_PROBE_ROW_PREFIX = "[in the legacy-layout container] "
# Says which container again, for the same reason: this is the third one, and its whole distinction
# from the second is the state of the volume it was handed.
EMPTY_LEGACY_PROBE_ROW_PREFIX = "[in the empty-legacy-volume container] "

# Rows this file produces itself: 4 from the contract check, 1 for the container starting, 2 startup
# markers, 1 for it still running at the end, 2 for staging and restarting on a root-owned /app/data,
# 2 for the SIGTERM shutdown, 1 for the final log, 3 for the legacy-layout container (seeding its
# volume, booting it, and the warning it has to log), 5 for the empty-legacy-volume container
# (creating its volume, booting it, the warning it has to log, `docker diff` finding nothing under
# /app/data, and the stream read back off the volume).
EXPECTED_OUTER_TARGETS = 21
# Each probe contributes its own targets plus the two consistency rows run_probe() adds.
EXPECTED_TOTAL_TARGETS = (
    EXPECTED_OUTER_TARGETS
    + EXPECTED_IMAGE_PROBE_TARGETS + 2
    + EXPECTED_SERVICE_PROBE_TARGETS + 2
    + EXPECTED_REPAIR_PROBE_TARGETS + 2
    + EXPECTED_LEGACY_PROBE_TARGETS + 2
    + EXPECTED_EMPTY_LEGACY_PROBE_TARGETS + 2)

# ── bounds ────────────────────────────────────────────────────────────────────────────────────────
# Every docker call is bounded, and the SUM of those bounds is what the smoke step's own
# `timeout-minutes` in BOTH workflows is set against — the margin is there so that a gate which is
# merely slow fails with ITS OWN diagnosis rather than being killed by act_runner, which would skip
# the cleanup in the `finally` below. Adding a bounded call here without revisiting that number is
# how a gate starts being killed instead of reporting.
#
# THE ARITHMETIC, so the next person can re-derive it instead of trusting it. Every bounded call on
# the longest path, counting a wait_for_markers() as BOOT_BUDGET plus one LOGS_TIMEOUT of overshoot,
# and counting the removals in the `finally` too:
#
#   the -serve container    840 s — inspect 30, rm 30, run 60, boot wait 90, image probe 120,
#                                   service probe 120, inspect 30, chown 30, restart 60, boot wait
#                                   90, repair probe 60, stop 60, inspect 30, final logs 30
#   the -legacy one         450 s — volume create 30, rm of the -seed container 30, seed 60,
#                                   rm 30, run 60, boot wait 90, probe 120, logs 30
#   the -legacy-empty one   480 s — volume create 30, rm 30, run 60, boot wait 90, probe 120,
#                                   diff 30, rm of the -readback container 30, read-back 60,
#                                   logs 30
#   the `finally`           210 s — FIVE container removals at 30 (ALL_SUFFIXES: the three long-
#                                   lived containers plus the -seed and -readback ones), two
#                                   volume removals at 30
#
# which comes to 1980 s, i.e. 33 minutes, against the step's own 40 — seven minutes of headroom,
# not nine. The two `remove_container()` calls that precede the -seed and -readback runs, and the
# two extra entries they added to ALL_SUFFIXES, are the difference between this sum and the 1860 s
# that stood here before those containers were named; a removal is a bounded docker call like any
# other and is counted like one. The figure quoted before check (q) existed was "roughly nineteen",
# and it left the cleanup calls out of the sum entirely; everything is counted now, which is why
# the step bound moved from 25 to 40 rather than to a number a shade over the sum.
INSPECT_TIMEOUT = 30
REMOVE_TIMEOUT = 30
# Creating and destroying a named volume, and the throwaway container that seeds it.
VOLUME_TIMEOUT = 30
SEED_TIMEOUT = 60
LEGACY_PROBE_TIMEOUT = 120
EMPTY_LEGACY_PROBE_TIMEOUT = 120
# `docker diff` on a stopped-or-running container: a single read of the container's writable layer,
# cheap, but bounded like everything else here.
DIFF_TIMEOUT = 30
# The throwaway container that reads the named volume back after the probe has saved to it. Same
# shape and the same bound as the seeding run above — a `docker run --rm` of the image under test.
READBACK_TIMEOUT = 60
START_TIMEOUT = 60
LOGS_TIMEOUT = 30
IMAGE_PROBE_TIMEOUT = 120
SERVICE_PROBE_TIMEOUT = 120
CHOWN_TIMEOUT = 30
RESTART_TIMEOUT = 60
REPAIR_PROBE_TIMEOUT = 60
# What `docker stop` gives the process before the kernel takes over, and it has to be generous for
# the check to mean anything: a bound short enough to kill a process that WAS shutting down would
# report a working image as one that never got the signal. docker-compose.yml grants production 30 s
# for the same shutdown; 20 is enough here because this container serves no streams and therefore
# has no final flush to wait on.
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

# What .dockerignore and the Dockerfile's three explicit COPY lines are supposed to keep out. One of
# these four is size (.venv is ~65 MB of macOS wheels for an interpreter that is not in this image);
# one is disclosure (.git carries the full history into a public registry); the other two are
# development scaffolding that runs from the source tree — tests/ in python:3.11-slim before the
# image exists at all (build declares `needs: test`), ci/ against the image once it does.
# data/ is NOT on this list: the Dockerfile creates that directory on purpose, so the claim about it
# is emptiness rather than absence and it is asserted in EMPTY_DIRS below.
ABSENT = [
    (APP_DIR + "/.venv", "a host-built virtualenv: ~65 MB of wheels for the wrong interpreter"),
    (APP_DIR + "/.git", "the repository history, which would ship to a public registry"),
    (APP_DIR + "/tests", "the test suite, which is run from the source tree and not from the image"),
    (APP_DIR + "/ci", "this gate itself"),
]

# The state directory itself DOES exist in the image — the Dockerfile creates and chowns it so that
# a volume mounted there arrives owned by uid 1000 — so the claim about it is not absence but
# EMPTINESS, and it is asserted that way rather than as "one particular filename is not there".
#
# The narrow version of this check was `/app/data/streams.json is NOT in the image`, and it would
# have passed on a build that shipped `streams.json.tmp` (which src/config.py leaves behind when a
# save is interrupted), or `streams.json.bak`, or a developer's `streams-prod.json`. Any of those is
# a file full of real MQTT and InfluxDB passwords going to a registry, and the whole point of
# checking here rather than trusting .dockerignore is that the check does not get to assume which
# names a developer's data/ happens to contain.
EMPTY_DIRS = [
    (APP_DIR + "/data", (
        "a developer's runtime state lives there and every file in it holds real broker "
        "credentials; the directory belongs in the image, its contents never do")),
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

# Written by src/config.py under DATA_DIR, which /entrypoint.sh resolves to /app/data for a
# container started with no volume at /data — which is how the -serve container this probe runs in
# was started. The IMAGE itself declares no DATA_DIR at all, deliberately: see DATA_DIR_ENV_PREFIX
# in ci/smoke.py for why that absence is load-bearing and is asserted as a row of its own.
CONFIG_PATH = "/app/data/streams.json"
STATE_DIR = "/app/data"

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

    Runs BEFORE the CRUD cycle so that an unwritable /app/data is diagnosed as an unwritable /app/data. The
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

    Written as one function on purpose: every step depends on the id the previous one produced, and
    splitting it would mean either re-creating a stream per check or passing a half-built state
    around. When a step cannot run, the remaining targets are recorded as FAILED with the reason —
    never skipped.
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
        # A sentinel rather than None for "the file could not be read". `json.load` returns None for
        # a file holding the literal `null` — which is exactly what a store that wrote the wrong
        # thing produces — and with None meaning both, that case recorded NO verdict for targets[3]
        # at all: the probe would print 23 rows instead of 24 and the gate's exact-count check would
        # report "a check stopped emitting rows" for what is really "the volume holds null".
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
# Runs LAST, inside a container that has been restarted after /app/data was deliberately handed back to
# root:root — the state production's volume is in, and the only state in which the entrypoint's
# `chown -R app:app /app/data` is load-bearing. See check_volume_repair() for why this cannot be asked
# of the container the rest of the gate uses.
#
# It is the one probe fed to `docker exec -u 1000:1000`, and that is what lets it answer by DOING rather
# than by inferring: it is the serving uid, so it can simply try to write. The service probe cannot
# — it arrives as root, for whom every directory is writable, which is why check_process_identity()
# there has to work the permission out of st_uid/st_gid and the mode bits by hand.
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
            "it is not: {}. /app/data was handed back to root:root and the container restarted, which "
            "is the state production's volume is in — written by the old root-based image, and "
            "never re-seeded, because docker seeds a volume only while it is empty. Repairing that "
            "is the ONLY thing `chown -R app:app /app/data` in /entrypoint.sh does, and this row is the "
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


# ══ THE LEGACY-LAYOUT PROBE ═══════════════════════════════════════════════════════════════════════
# Runs inside the SECOND container — the one started the way the older deployments really start this
# image: a named volume that already holds a streams.json mounted at /data, and no DATA_DIR anywhere
# in the environment. See check_legacy_layout() for how that container is built.
#
# Its question is the one no other probe here can ask, because every other container in this gate is
# started in the consistent layout: does the service read the file the deployment actually mounted?
# An empty answer is a VALID answer from every endpoint this service has, so nothing else in this
# gate — not the healthcheck's target, not the CRUD cycle, not the log — can tell "no streams
# configured yet" apart from "every stream is in a directory nobody is reading".
#
# The two identifiers below are substituted in by check_legacy_layout() rather than written twice:
# the same values are what it seeds the volume with, and a probe carrying its own copy could drift
# from the seed and then assert nothing at all.
LEGACY_PROBE = r'''
import json
import os
import urllib.error
import urllib.request

PROBE_MARKER = "mqtt2influx legacy-layout probe ok"

BASE_URL = "http://127.0.0.1:8000"
HTTP_TIMEOUT = 15

LEGACY_STATE_DIR = "/data"
LEGACY_CONFIG_PATH = LEGACY_STATE_DIR + "/streams.json"
STATE_DIR = "/app/data"

EXPECTED_STREAM_ID = "@@STREAM_ID@@"
EXPECTED_STREAM_NAME = "@@STREAM_NAME@@"

# What the write half of this probe creates through the API. `.invalid` never resolves and
# `enabled` is false, for the same reasons as everywhere else in this gate.
NEW_STREAM = {
    "name": "written back onto the legacy volume",
    "mqtt_host": "mqtt.invalid",
    "mqtt_port": 1883,
    "mqtt_topic": "/devices/#",
    "influx_host": "influx.invalid",
    "influx_port": 8086,
    "influx_database": "legacydb",
    "enabled": False,
}

# WHY THE RESOLVED DATA_DIR IS NOT READ OUT OF PID 1's ENVIRONMENT, which would be the obvious way
# to ask this question: /proc/<pid>/environ is not world-readable like /proc/<pid>/status is —
# reading another process's requires PTRACE_MODE_READ, i.e. CAP_SYS_PTRACE, which docker DROPS from
# a container's default capability set. Even the root that `docker exec` arrives as gets
# PermissionError. Adding `--cap-add=SYS_PTRACE` to the container would answer it, at the cost of
# no longer running the image in the shape this whole check exists to reproduce.
#
# So the two rows below ask by OBSERVING instead, which is the better question anyway: the service
# serves what the mounted volume holds, and a stream created through the API is written back to that
# same file. Between them they pin both directions of the thing that was broken.

rows = []


def record(target, reason):
    rows.append((target, reason))


def describe(error):
    return "{}: {}".format(type(error).__name__, error)


def check_seeded_stream_is_served():
    """The API serves the stream that is really on the volume, not an empty list.

    This is the whole finding in one row. `load_streams()` returns [] for a file it cannot find,
    /api/streams answers 200 with it, /api/stats answers 200 with an empty list and the compose
    healthcheck goes green — so an image pointed at the wrong directory is, from every other angle,
    a healthy container. There is no second copy of streams.json.
    """
    target = "GET /api/streams serves the stream seeded onto the volume at {}".format(
        LEGACY_STATE_DIR)
    request = urllib.request.Request(BASE_URL + "/api/streams", method="GET")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            status = response.status
            body = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        status = error.code
        body = error.read().decode("utf-8", "replace")
    except Exception as error:
        record(target, "the request itself failed: {}".format(describe(error)))
        return
    if status != 200:
        record(target, "it answered {}: {}".format(status, body[:400]))
        return
    try:
        listed = json.loads(body)
    except Exception as error:
        record(target, "its body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    if (isinstance(listed, list) and len(listed) == 1
            and listed[0].get("id") == EXPECTED_STREAM_ID
            and listed[0].get("name") == EXPECTED_STREAM_NAME):
        record(target, None)
        return
    record(target, (
        "it returned {!r}. {}".format(listed, (
            "AN EMPTY LIST IS THE FAILURE THIS ROW EXISTS FOR: the volume mounted at {} holds a "
            "streams.json with this stream in it, and the service is answering as though there "
            "were no configuration at all. Every endpoint still answers 200, the healthcheck still "
            "goes green, and an updater would record this deploy as a success while the bridge "
            "subscribed to nothing".format(LEGACY_STATE_DIR)
        ) if listed == [] else (
            "the volume at {} holds exactly one stream, id {!r} named {!r}".format(
                LEGACY_STATE_DIR, EXPECTED_STREAM_ID, EXPECTED_STREAM_NAME)))))


def check_writes_land_on_the_mounted_volume():
    """A stream created through the API is written back to the volume the deployment mounted.

    The other direction of the same fault, and it is the one that makes a wrong DATA_DIR permanent
    rather than merely useless: an operator who finds an empty UI and re-creates a stream by hand
    gets a 200, sees it listed, and has written it into a directory the next image update discards.
    So this asks the file — not the API's echo of its own success.
    """
    target = "a stream created through the API is written into {}".format(LEGACY_CONFIG_PATH)
    payload = json.dumps(NEW_STREAM).encode("utf-8")
    request = urllib.request.Request(
        BASE_URL + "/api/streams", data=payload,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            status = response.status
            body = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        status = error.code
        body = error.read().decode("utf-8", "replace")
    except Exception as error:
        record(target, "the POST itself failed: {}".format(describe(error)))
        return
    if status != 200:
        record(target, "the POST answered {}: {}".format(status, body[:400]))
        return
    try:
        created_id = json.loads(body).get("id")
    except Exception as error:
        record(target, "the POST's body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    if not created_id:
        record(target, "the POST answered 200 but returned no id: {}".format(body[:400]))
        return
    try:
        with open(LEGACY_CONFIG_PATH) as handle:
            on_disk = json.load(handle)
    except Exception as error:
        record(target, (
            "{} could not be read back after the POST: {}. The API reported success, so the stream "
            "exists as far as the UI is concerned — and is not on the volume".format(
                LEGACY_CONFIG_PATH, describe(error))))
        return
    ids = [record_.get("id") for record_ in on_disk] if isinstance(on_disk, list) else None
    if ids is not None and created_id in ids and EXPECTED_STREAM_ID in ids:
        record(target, None)
        return
    record(target, (
        "after the POST the file holds ids {!r}; it must hold both the seeded {!r} and the new {!r}. "
        "A save that goes anywhere else lands in the container's writable layer, where the next "
        "image update deletes it — and an operator who found an empty UI and re-entered a stream "
        "would have no way of knowing".format(ids, EXPECTED_STREAM_ID, created_id)))


def check_nothing_written_to_the_unmounted_path():
    """Nothing was created at the path the deployment did NOT mount.

    A container that resolved DATA_DIR correctly never touches /app/data. One that did not would
    leave a streams.json there — in the container's writable layer, which the next image update
    discards — and this row names that directly instead of leaving it to be inferred from the row
    above.
    """
    target = "{} was left untouched: nothing was written to the path nobody mounted".format(
        STATE_DIR)
    try:
        listed = sorted(os.listdir(STATE_DIR))
    except FileNotFoundError:
        record(target, None)
        return
    except Exception as error:
        record(target, "{} could not be listed: {}".format(STATE_DIR, describe(error)))
        return
    record(target, None if not listed else (
        "it holds {!r}. The app is writing to the image's own directory rather than to the volume "
        "the deployment mounted, so everything saved through the UI lives in the container's "
        "writable layer and is discarded by the next image update".format(listed)))


def main():
    # Read first, then write, then the negative: a failing read explains a failing write, and the
    # "nothing at /app/data" row is only interesting once something has actually been saved.
    check_seeded_stream_is_served()
    check_writes_land_on_the_mounted_volume()
    check_nothing_written_to_the_unmounted_path()

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("{}: FAILED {}/{} targets".format(PROBE_MARKER, len(failures), len(rows)))
        # SystemExit and not `assert`, for the same reason as in the other three probes.
        raise SystemExit(1)

    print("{}: {}/{} targets".format(PROBE_MARKER, len(rows), len(rows)))


if __name__ == "__main__":
    main()
'''

# What check_legacy_layout() writes into the volume, and what LEGACY_PROBE looks for. `.invalid` is
# reserved by RFC 2606 and never resolves; `enabled` is FALSE for the same reason it is in the
# service probe — a true one would have the manager dial a hostname that does not exist, forever.
#
# The id is a fixed, recognisable literal rather than a uuid4: this stream has to be identifiable in
# the probe's output at a glance, and "the service returned the stream that was really on the disk"
# is a claim worth being able to read straight out of a CI log.
LEGACY_STREAM = {
    "id": "smoke-legacy-layout-stream",
    "name": "seeded onto the volume at /data",
    "mqtt_host": "mqtt.invalid",
    "mqtt_port": 1883,
    "mqtt_user": "legacy-user",
    "mqtt_password": "legacy-password",
    "mqtt_topic": "/devices/#",
    "topic_prefix": "legacy",
    "influx_host": "influx.invalid",
    "influx_port": 8086,
    "influx_user": "legacy-influx-user",
    "influx_password": "legacy-influx-password",
    "influx_database": "legacydb",
    "enabled": False,
}

# The two lines /entrypoint.sh must print when it takes the fallback. Checked as a row of its own,
# because a fallback that happens SILENTLY is a deployment nobody ever fixes: the operator has no
# way to learn that the container is running on a guess rather than on a decision.
#
# BOTH legacy checks look for these, and in (q) the row is a discriminator rather than a courtesy: on
# a detection that tests for the legacy FILE, a volume with no streams.json on it does not look
# legacy at all, so DATA_DIR silently becomes /app/data and NOTHING is printed. In (p) the same row
# can only catch a fallback that was taken quietly.
LEGACY_WARN_MARKERS = (
    "WARN: DATA_DIR is not set",
    "DATA_DIR={}".format(LEGACY_STATE_DIR),
)

# The name the empty-legacy-volume probe gives the one stream it creates. Declared out here because
# it is used on BOTH sides of that check: the probe POSTs it from inside the container, and
# check_empty_legacy_writes_landed() looks for it in the file it reads back off the volume from a
# throwaway container. Two copies could drift, and a drifted copy would make the read-back row
# assert nothing.
EMPTY_LEGACY_STREAM_NAME = "saved onto a legacy volume that started out empty"


# ══ THE EMPTY-LEGACY-VOLUME PROBE ═════════════════════════════════════════════════════════════════
# Runs inside the THIRD container: the same shape the older deployments have — a named volume mounted
# at /data and no DATA_DIR anywhere in the environment — except that the volume arrived EMPTY.
#
# It asserts almost nothing, and that is deliberate. Both rows below pass on a BROKEN entrypoint
# exactly as they do on a working one: the API answers 200 and then lists what it accepted whichever
# directory it is really writing to, and that indistinguishability IS the disease. This probe's only
# job is to make the service perform one save. WHERE that save landed is asked from outside by
# check_empty_legacy_writes_landed(), with `docker diff` and a read of the volume — neither of which
# a process living inside this container can perform about itself.
EMPTY_LEGACY_PROBE = r'''
import json
import urllib.error
import urllib.request

PROBE_MARKER = "mqtt2influx empty-legacy-volume probe ok"

BASE_URL = "http://127.0.0.1:8000"
HTTP_TIMEOUT = 15

# Substituted in by check_empty_legacy_layout() rather than written twice: the outer half looks for
# this same name in the file it reads back off the volume.
STREAM_NAME = "@@STREAM_NAME@@"

# `.invalid` is reserved by RFC 2606 and never resolves; `enabled` is false so that src/api.py does
# not schedule a connection to it. Same reasons as everywhere else in this gate.
NEW_STREAM = {
    "name": STREAM_NAME,
    "mqtt_host": "mqtt.invalid",
    "mqtt_port": 1883,
    "mqtt_topic": "/devices/#",
    "influx_host": "influx.invalid",
    "influx_port": 8086,
    "influx_database": "emptylegacydb",
    "enabled": False,
}

rows = []
# The id the POST hands back. src/api.py's create_stream() pops any client-supplied id and generates
# its own, so it cannot be pinned from here — it is carried between the two checks instead.
created = {}


def record(target, reason):
    rows.append((target, reason))


def describe(error):
    return "{}: {}".format(type(error).__name__, error)


def call(method, path, payload=None):
    """(status, body), or (None, description) when the request could not be made at all."""
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {} if data is None else {"Content-Type": "application/json"}
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace")
    except Exception as error:
        return None, describe(error)


def check_the_save_is_accepted():
    """The service accepts a stream on a deployment whose volume arrived empty.

    A PRECONDITION for the two outer rows, not the finding itself: a 200 here says nothing about
    which directory was written, and this row is green on the broken entrypoint too. It is worth a
    row of its own for the failure it separates out — if the POST were refused, `docker diff` and
    the volume read-back would both report "nothing was written anywhere" and read as a
    wrong-directory bug rather than as a service that never saved anything.
    """
    target = "POST /api/streams is accepted on a deployment whose volume arrived EMPTY"
    status, body = call("POST", "/api/streams", NEW_STREAM)
    if status is None:
        record(target, "the POST itself failed: {}".format(body))
        return
    if status != 200:
        record(target, "it answered {}: {}".format(status, body[:400]))
        return
    try:
        created["id"] = json.loads(body).get("id")
    except Exception as error:
        record(target, "its body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    if not created.get("id"):
        record(target, "it answered 200 but returned no id: {}".format(body[:400]))
        return
    record(target, None)


def check_the_service_lists_what_it_saved():
    """The stream comes back from GET /api/streams — i.e. the service can re-read its own write.

    Also green on the broken entrypoint, and for the same reason: the app reads back from whatever
    directory it wrote to, so this proves round-tripping and not location. It is here so that the
    outer rows have something unambiguous to contrast with — "the API accepted it, serves it, and it
    is nowhere on the volume the deployment mounted" is the sentence check (q) exists to be able to
    say.
    """
    target = "GET /api/streams then lists the stream that was just created"
    if not created.get("id"):
        record(target, "not attempted: the POST above did not produce a stream")
        return
    status, body = call("GET", "/api/streams")
    if status is None:
        record(target, "the request itself failed: {}".format(body))
        return
    if status != 200:
        record(target, "it answered {}: {}".format(status, body[:400]))
        return
    try:
        listed = json.loads(body)
    except Exception as error:
        record(target, "its body is not JSON ({}): {}".format(describe(error), body[:400]))
        return
    ids = [entry.get("id") for entry in listed] if isinstance(listed, list) else None
    if ids is not None and created["id"] in ids:
        record(target, None)
        return
    record(target, (
        "it returned {!r}, which does not contain the stream {!r} the POST just reported creating. "
        "The save was accepted and then lost between the write and the read".format(
            listed, created["id"])))


def main():
    check_the_save_is_accepted()
    check_the_service_lists_what_it_saved()

    failures = []
    for target, reason in rows:
        if reason is None:
            print("ok   {}".format(target))
        else:
            print("FAIL {} -> {}".format(target, reason))
            failures.append(target)

    if failures:
        print("{}: FAILED {}/{} targets".format(PROBE_MARKER, len(failures), len(rows)))
        # SystemExit and not `assert`, for the same reason as in the other four probes.
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

    `-v` removes the container's ANONYMOUS volumes. The image no longer declares a VOLUME — that
    line was dropped precisely because it made every container of it create one — so today there are
    none to remove and the flag is insurance rather than necessity. It is kept for the day something
    adds a VOLUME back: this daemon is shared by the whole fleet, and a leaked volume per CI run
    accumulates silently and forever. It does NOT touch the named volume this gate creates; that one
    is removed by name in remove_volume().
    """
    docker(["rm", "-f", "-v", name], REMOVE_TIMEOUT)


def remove_volume(name):
    """Best effort, for the one NAMED volume this gate creates. The workflows remove it too.

    Separate from remove_container() because a named volume outlives every container that mounted
    it: `docker rm -v` deliberately never removes one, since by design somebody else may still want
    it. Only an explicit removal by name gets rid of it, and without that this runner would collect
    one dead volume per CI run.

    Ordering matters and is the caller's responsibility: the container that mounts it has to be gone
    first, or the daemon refuses with "volume is in use".
    """
    docker(["volume", "rm", name], VOLUME_TIMEOUT)


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


def wait_for_markers(name, markers, budget, minimum=1):
    """Poll `docker logs` until EVERY marker has appeared `minimum` times, or the budget runs out.

    All of them, not the first. Returning on whichever arrived first would leave the later ones
    judged against a snapshot taken BEFORE they could have been written — a check that reports the
    absence of something it never gave a chance to appear. Bounded in WALL CLOCK rather than in
    attempts, because each attempt shells out to `docker logs` with its own bound and an
    attempt-counted loop would multiply into minutes on a slow daemon.

    Counted rather than merely looked for, and `minimum` is what makes this reusable after a
    restart: `docker logs` returns the container's WHOLE history, so the first boot's markers are
    still in it and a membership test would report the SECOND boot complete before it had begun —
    then hand the repair probe a container that had not finished starting.

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
        "the image declares NO {} in its environment".format(DATA_DIR_ENV_PREFIX),
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
        "it declares {!r}. /entrypoint.sh is what starts as root, chowns /app/data so a volume written "
        "by the older root-based image stays usable, and `exec`s gosu to drop to uid 1000. "
        "`docker exec` bypasses ENTRYPOINT, so every in-container check in this gate would keep "
        "passing while the container went back to serving as root".format(entrypoint))))

    env = config.get("Env") or []
    declared = [entry for entry in env if entry.startswith(DATA_DIR_ENV_PREFIX)]
    rows.append((targets[3], None if not declared else (
        "it declares {!r}. That variable must be UNSET on the image: /entrypoint.sh uses 'DATA_DIR "
        "is not set' as the signal that a deployment did not choose a path, and only then falls "
        "back to detecting the older layout — a volume bound at {} with no environment at all. "
        "Declaring it here makes that case unobservable from inside the container, so those stacks "
        "would come up on an empty {}, answer every read 200, report healthy, and bridge nothing "
        "while their real configuration sat unread. The default lives in /entrypoint.sh and "
        "src/settings.py, not in an ENV line".format(declared, LEGACY_STATE_DIR, STATE_DIR))))
    return rows


def start_serving_container(image, name):
    """(b) Start the image with its REAL command and nothing else.

    No environment overrides, no published port, no mount at all — see the module docstring. /app/data
    is therefore the image's own directory, created and chowned to uid 1000 at build time, which is
    exactly the fresh and empty state the empty-state check needs. It is also the state a deployment
    that mounts a brand-new named volume there arrives in, since docker seeds an empty volume from
    the image directory, ownership included.

    This is the CONSISTENT layout, and everything except check_legacy_layout() runs in it.
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
        # Same distinction the repair check makes: on a non-zero exit `logs` is docker's complaint,
        # not the container's output, and searching it for uvicorn's lines would report a boot that
        # never printed them — pointing at the image rather than at the daemon that refused to say.
        # The transcript is returned empty for the same reason: it is not a log.
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

    `-i` attaches stdin without asking for a tty (none is needed and none is available on a runner),
    the image's own interpreter runs the program, and `docker exec` propagates the command's exit
    status — which is what lets the two consistency rows below tell "the probe reported failures"
    apart from "the probe died before it could report anything".

    `user` is passed on as `-u` and only the repair probe uses it. Left None the exec arrives as the
    image's Config.User, which this Dockerfile deliberately leaves unset — i.e. as root, which is
    what the image and service probes need and what the repair probe must NOT have.
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

    Runs BEFORE the repair check below and not after, even though "at the end" would read better:
    `docker restart` starts a container that had EXITED exactly as happily as one that is running,
    so a liveness question asked on the far side of it would be answered about the restart rather
    than about the run that preceded it — and the crash this row exists to catch, a container that
    served the whole CRUD cycle and then died, would be repaired out of the report.
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
    """(n) Stage the volume production actually has, and make the entrypoint repair it.

    WHY THIS EXISTS AT ALL. Check (m) — "/app/data is writable by the uid PID 1 runs as" — cannot fail
    on the container the rest of this gate uses, and a check that cannot fail is a comment. The
    container is started with no `-v` and the image declares no VOLUME, so /app/data is not a volume
    at all: it is the directory baked into the IMAGE LAYER, which the image built with
    `mkdir -p /app/data && chown app:app /app/data`, reached through the container's writable layer.
    It therefore starts out owned by uid 1000 no matter what the entrypoint does, and `docker
    restart` — which reuses the same container and the same writable layer — does not reset it.
    Delete `chown -R app:app /app/data` from entrypoint.sh and every row in this gate stays green.

    Production is the case that is left over, and it is the only case the line was written for: its
    NAMED volume was created by the ROOT-based version of this image, holds files, and is therefore
    never re-seeded — docker seeds a volume only while it is empty. On that volume the entrypoint's
    chown is the single thing standing between the deploy and a container that comes up healthy and
    silently persists nothing.

    So the state is staged rather than hoped for. `docker exec` arrives as root precisely because it
    does not go through the ENTRYPOINT, which makes it the one way to put /app/data back under root from
    outside; the restart then runs the entrypoint again over it. A working entrypoint chowns it back
    before gosu drops privileges, and the repair probe writes its file. An entrypoint that lost the
    chown leaves /app/data at 0:0 and that write raises PermissionError — while everything else about
    the container looks exactly as it did a moment ago.

    Returns (rows, ready): `ready` gates the repair probe, because a probe run against a container
    that never came back would report a permission failure as though it were a verdict about
    ownership.
    """
    stage_target = "{} can be handed back to root:root, the way production's volume arrives".format(
        STATE_DIR)
    restart_target = "the serving container comes back up with {} owned by root".format(STATE_DIR)
    if not started:
        reason = "not attempted: the serving container never started"
        return [(stage_target, reason), (restart_target, reason)], False

    # `-u 0:0` is redundant today — the image declares no USER, so an exec arrives as root anyway —
    # and it is spelled out because the alternative failure is silent: an image that grew a `USER`
    # line would run this chown unprivileged, it would be refused, and the row would read as though
    # the daemon were at fault rather than the image.
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
        # Its own reason, because `logs` here holds docker's error text and not the container's
        # output: counting markers in it would find none and the row below would blame the
        # entrypoint for a restart loop, sending the reader into entrypoint.sh over a daemon that
        # simply would not answer. "The check could not run" and "the check failed" are different
        # sentences everywhere else in this file — see check_container_alive and check_final_log.
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

    This is the property the whole gosu construction in the Dockerfile exists for, and until now
    nothing in this gate sent the container a signal at all. Check (l) sees only the crudest way to
    lose it — under `su` or `sudo` PID 1 would be a shell running as uid 0 — while a wrapper like
    `sh -c "python main.py"` keeps the uid, keeps every other row here green, and still leaves PID 1
    a shell that forwards nothing.

    `docker stop` is exactly what a redeploy does: SIGTERM, wait, then SIGKILL. An exit code of 0
    means uvicorn's own handler ran and took the app down through its lifespan, which is where
    manager.stop_all() flushes each stream's half-built batch. 137 is 128+9 — the grace period ran
    out and the kernel killed it, i.e. nothing acted on the signal and every point batched since the
    last flush is gone, permanently, because MQTT does not replay them. 143 (128+15) is the third
    case and fails here too: the signal arrived but nothing handled it, so the process died at
    exactly the moment it was supposed to start flushing.

    LAST of everything that touches the container, because it ENDS it. check_final_log() below still
    reads the log of a stopped container, and `docker rm -f -v` still removes one.
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


def check_legacy_layout(image, name, volume):
    """(p) Start the image the way the OLDER deployments really start it, and see what it serves.

    WHY NOTHING ELSE IN THIS GATE COVERS THIS. Every other container here is started with no `-v`,
    no environment and no port — the CONSISTENT layout, in which the app's default and the mount
    agree by construction. The deployments that exist in this fleet are not all shaped that way:
    the older stacks bind the volume at /data and set NO environment variables at all, and they
    carry `io.portainer.update.enable`, so they take a new :latest by themselves. That combination —
    a volume somewhere the image does not default to, and nothing in the environment saying so — is
    the one this gate could never reproduce, and it is also the one that costs the most.

    WHAT GOING WRONG LOOKS LIKE, WHICH IS WHY THE CHECK HAS TO BE THIS SHAPE. Not a crash. The
    container starts, the entrypoint chowns whatever it decided on, load_streams() finds no file and
    returns [], /api/streams answers 200 with an empty list, /api/stats — the compose healthcheck's
    target — answers 200 as well, the container reports HEALTHY, and the updater records a
    successful deploy and rolls nothing back. The volume holding every stream, with its MQTT and
    InfluxDB credentials, stays mounted at /data and is read by nobody, and the bridge writes
    nothing while logging no error. So the assertion cannot be "it came up" or "it answers": it has
    to be that the streams which are REALLY on the volume are the streams it serves.

    HOW THE STATE IS BUILT WITHOUT A BIND MOUNT. A host path cannot be handed to the daemon from
    this job (see the module docstring), but a NAMED volume is daemon-side and means the same to
    both. So: create one, seed it through a throwaway container of the image itself with
    `--entrypoint python` — which both skips the entrypoint under test and writes the file as ROOT,
    which is exactly how production's volume was written by the old root-based image — and then
    mount it at /data on a second long-running container with no environment at all.

    Returns (rows, ready): `ready` gates the probe, so a probe run against a container that never
    came up cannot report its own failure to connect as a verdict about where the app is looking.
    """
    seed_target = "a volume holding a streams.json can be seeded and mounted at {}".format(
        LEGACY_STATE_DIR)
    start_target = (
        "the image starts with that volume at {} and NO DATA_DIR, and reaches uvicorn's startup "
        "markers".format(LEGACY_STATE_DIR))
    warn_target = "its log warns that DATA_DIR is unset and names the path it fell back to"

    # --- create and seed the volume ---------------------------------------------------------------
    status, output = docker(["volume", "create", volume], VOLUME_TIMEOUT)
    if status is None:
        return [(seed_target, output),
                (start_target, "not attempted: " + output),
                (warn_target, "not attempted: " + output)], False
    if status != 0:
        reason = "`docker volume create {}` exited {}:\n{}".format(
            volume, status, excerpt(output))
        return [(seed_target, reason),
                (start_target, "not attempted: " + reason),
                (warn_target, "not attempted: " + reason)], False

    # Built here and handed to the interpreter as one argv element, so nothing goes through a shell
    # and the JSON needs no quoting beyond repr(). json.dumps first, so what lands on the volume is
    # exactly the shape src/config.py writes: a list of objects.
    payload = json.dumps([LEGACY_STREAM])
    seed_program = (
        "import os\n"
        "os.makedirs({directory!r}, exist_ok=True)\n"
        "with open(os.path.join({directory!r}, 'streams.json'), 'w') as handle:\n"
        "    handle.write({payload!r})\n"
        "print('seeded ' + os.path.join({directory!r}, 'streams.json'))\n"
    ).format(directory=LEGACY_STATE_DIR, payload=payload)

    # `--entrypoint python` on purpose, twice over: it keeps the entrypoint under test out of the
    # SEEDING step, and it leaves the file owned by root — the state production's volume is really
    # in, since it was written by the version of this image that still ran as root. The entrypoint's
    # chown then has to repair it on the real container below, which is a second thing this check
    # exercises for free.
    # NAMED even though it is `--rm` and lives for a second: see SEED_SUFFIX. The removal beforehand
    # is what makes the name safe to reuse if a previous run was killed before its own cleanup.
    remove_container(name + SEED_SUFFIX)
    status, output = docker(
        ["run", "--rm", "--name", name + SEED_SUFFIX,
         "-v", "{}:{}".format(volume, LEGACY_STATE_DIR),
         "--entrypoint", "python", image, "-c", seed_program],
        SEED_TIMEOUT)
    if status is None:
        return [(seed_target, output),
                (start_target, "not attempted: " + output),
                (warn_target, "not attempted: " + output)], False
    if status != 0:
        reason = "seeding the volume exited {}:\n{}".format(status, excerpt(output))
        return [(seed_target, reason),
                (start_target, "not attempted: " + reason),
                (warn_target, "not attempted: " + reason)], False
    rows = [(seed_target, None)]

    # --- start the container the way the old stacks do --------------------------------------------
    # No `-e`, no `--env`, no port: this argv IS the claim. Anything added here would make the check
    # test some other deployment than the one that exists.
    remove_container(name)
    status, output = docker(
        ["run", "-d", "--name", name, "-v", "{}:{}".format(volume, LEGACY_STATE_DIR), image],
        START_TIMEOUT)
    if status is None:
        rows.append((start_target, output))
        rows.append((warn_target, "not attempted: " + output))
        return rows, False
    if status != 0:
        reason = "`docker run -d` exited {}:\n{}".format(status, excerpt(output))
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False

    log_status, logs = wait_for_markers(name, STARTUP_MARKERS, BOOT_BUDGET)
    if log_status is None:
        reason = "`docker logs` produced no exit code — {}".format(logs)
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False
    if log_status != 0:
        # Same distinction the other log-reading checks make: on a non-zero exit `logs` is docker's
        # complaint and not the container's output, so searching it for markers would blame the
        # image for a daemon that would not answer.
        reason = "`docker logs` exited {}:\n{}".format(log_status, excerpt(logs))
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False

    missing = [marker for marker in STARTUP_MARKERS if marker not in logs]
    if missing:
        rows.append((start_target, (
            "it never printed {} within {} s. Under the legacy layout the entrypoint has more to do "
            "than usual — resolve DATA_DIR, then chown a volume written by root — and a failure in "
            "either is an exit before `exec`, which under `restart: always` is an unbounded restart "
            "loop rather than a failed deploy.\nLog:\n{}".format(
                missing, BOOT_BUDGET, excerpt(logs)))))
        rows.append((warn_target, "not attempted: the container did not finish starting"))
        return rows, False
    rows.append((start_target, None))

    absent = [marker for marker in LEGACY_WARN_MARKERS if marker not in logs]
    rows.append((warn_target, None if not absent else (
        "the log is missing {}. The fallback is a compatibility measure, not a configuration, and a "
        "silent one is a deployment nobody ever fixes: the operator has no way to learn that this "
        "container found its configuration by guessing rather than by being told. The warning has "
        "to name both paths and say that DATA_DIR should be set explicitly".format(absent))))
    return rows, True


def check_empty_legacy_layout(image, name, volume):
    """(q) The legacy SHAPE with an EMPTY volume — the case check (p) is structurally blind to.

    WHAT THIS DEFENDS. /entrypoint.sh has to recognise a deployment of the OLDER shape — volume bound
    at /data, no environment at all — and resolve DATA_DIR to /data for it. That recognition has been
    wrong twice, and both times it failed SILENTLY: a healthy container serving an empty stream list
    while the real configuration sat unread on the volume. First there was no detection at all and
    the image defaulted to /app/data. Then the detection asked for the legacy FILE
    (`[ -e /data/streams.json ]`) rather than for the legacy MOUNT (`[ -d /data ]`), so a
    legacy-shaped deployment whose volume held no streams.json YET — one just created, one restored
    empty, one whose file was removed by hand — fell through to /app/data, which since the Dockerfile
    dropped its VOLUME line is the container's WRITABLE LAYER. The first stream the operator saved
    through the UI survived a `restart` and was destroyed by the next automatic image update, and
    since that update never created /data/streams.json either, the same hole reopened on every cycle.

    WHY CHECK (p) CANNOT CATCH THIS, WHICH IS THE WHOLE REASON BOTH CHECKS EXIST. (p) SEEDS a
    streams.json onto its volume BEFORE mounting it. Under that fixture `[ -e /data/streams.json ]`
    and `[ -d /data ]` are BOTH true, so (p) passes identically on the correct detection and on the
    incorrect one — it cannot go red on the regression that has now happened twice. This check
    removes the seeding, which is the only difference between the two, and that difference is exactly
    the bug. NEITHER CHECK IS REDUNDANT: (p) is the volume that already holds a configuration and
    asserts the service reads it rather than an empty list; (q) is the volume that holds nothing yet
    and asserts the FIRST SAVE lands on it. Deleting either one leaves a silent failure unguarded.

    WHAT IS ASSERTED, AND WHERE. Three rows here — the volume exists and the container boots on it,
    and the entrypoint SAYS in the log that it fell back to /data (on the file-based detection there
    is no fallback and therefore no warning at all, so that row alone already goes red). Then the
    probe makes the service save one stream, and check_empty_legacy_writes_landed() asks the two
    questions that cannot be asked from inside: `docker diff` for the writable layer, and a read of
    the volume itself from a throwaway container.

    Returns (rows, ready): `ready` gates the probe, so a probe run against a container that never
    came up cannot report its own failure to connect as a verdict about where the app is looking.
    """
    create_target = "an EMPTY named volume can be created and mounted at {}".format(
        LEGACY_STATE_DIR)
    start_target = (
        "the image starts with an EMPTY volume at {} and NO DATA_DIR, and reaches uvicorn's startup "
        "markers".format(LEGACY_STATE_DIR))
    warn_target = (
        "its log warns that DATA_DIR is unset and names {} as the path it fell back to, on a volume "
        "that holds no streams.json".format(LEGACY_STATE_DIR))

    # --- create the volume, and DO NOT SEED IT -----------------------------------------------------
    # The absence of a seeding step is the entire check. Anything written here before the mount would
    # make this a second copy of (p).
    status, output = docker(["volume", "create", volume], VOLUME_TIMEOUT)
    if status is None:
        return [(create_target, output),
                (start_target, "not attempted: " + output),
                (warn_target, "not attempted: " + output)], False
    if status != 0:
        reason = "`docker volume create {}` exited {}:\n{}".format(volume, status, excerpt(output))
        return [(create_target, reason),
                (start_target, "not attempted: " + reason),
                (warn_target, "not attempted: " + reason)], False
    rows = [(create_target, None)]

    # --- start the container the way the old stacks do, on that empty volume -----------------------
    # No `-e`, no `--env`, no port — this argv IS the claim, exactly as in check_legacy_layout(). The
    # only difference between the two containers is what is on the volume.
    remove_container(name)
    status, output = docker(
        ["run", "-d", "--name", name, "-v", "{}:{}".format(volume, LEGACY_STATE_DIR), image],
        START_TIMEOUT)
    if status is None:
        rows.append((start_target, output))
        rows.append((warn_target, "not attempted: " + output))
        return rows, False
    if status != 0:
        reason = "`docker run -d` exited {}:\n{}".format(status, excerpt(output))
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False

    log_status, logs = wait_for_markers(name, STARTUP_MARKERS, BOOT_BUDGET)
    if log_status is None:
        reason = "`docker logs` produced no exit code — {}".format(logs)
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False
    if log_status != 0:
        # Same distinction every other log-reading check here makes: on a non-zero exit `logs` holds
        # docker's complaint rather than the container's output, so searching it for markers would
        # blame the image for a daemon that would not answer.
        reason = "`docker logs` exited {}:\n{}".format(log_status, excerpt(logs))
        rows.append((start_target, reason))
        rows.append((warn_target, "not attempted: " + reason))
        return rows, False

    missing = [marker for marker in STARTUP_MARKERS if marker not in logs]
    if missing:
        rows.append((start_target, (
            "it never printed {} within {} s. On an empty volume the entrypoint has to create the "
            "directory it resolved and chown it before `exec`ing gosu, and a failure in either is an "
            "exit before the app ever starts — which under `restart: always` is an unbounded restart "
            "loop rather than a failed deploy.\nLog:\n{}".format(
                missing, BOOT_BUDGET, excerpt(logs)))))
        rows.append((warn_target, "not attempted: the container did not finish starting"))
        return rows, False
    rows.append((start_target, None))

    absent = [marker for marker in LEGACY_WARN_MARKERS if marker not in logs]
    rows.append((warn_target, None if not absent else (
        "the log is missing {}. On a volume that holds no streams.json this is not merely a missing "
        "courtesy — it is the detection itself reporting that it did NOT recognise the legacy "
        "layout, so DATA_DIR resolved to {} instead: the container's own writable layer, which the "
        "next image update discards along with every stream saved into it. A detection that tests "
        "for {}/streams.json rather than for the {} MOUNT fails exactly here and nowhere else in "
        "this gate.\nLog:\n{}".format(
            absent, STATE_DIR, LEGACY_STATE_DIR, LEGACY_STATE_DIR, excerpt(logs)))))
    return rows, True


def check_empty_legacy_writes_landed(image, name, volume, ready):
    """(q) After the probe has saved a stream: where did it actually go?

    THE DISCRIMINATING HALF OF CHECK (q). The probe's own rows are green on a broken entrypoint and a
    working one alike — a 200 and a listing prove only that the service round-tripped its own write.
    These two rows ask the question the service cannot be asked to answer about itself, and they
    answer it from two sides:

      * `docker diff` reads the container's WRITABLE LAYER. On the file-based detection the save
        lands in the image's own directory and the diff carries `A /app/data/streams.json` — direct
        evidence, from the daemon, of the exact failure. On the mount-based one the write goes to the
        volume, which `docker diff` does not report at all, so nothing under /app/data appears.
      * the volume is then read back through a THROWAWAY container that mounts it. That is the other
        half of the same claim: not "the app wrote to the path /data" but "the bytes are on the
        volume the deployment mounted", which is the only thing that survives the container.

    Both are outer-half by necessity. A process inside the container cannot see its own writable
    layer as a layer, and reading /data from in there would prove the path rather than the volume.

    Returns (rows, transcripts): the transcripts are the raw `docker diff` output and the read-back
    container's output, printed with the others so that a red row is readable without a re-run.
    """
    diff_target = "nothing was written under {} in the container's writable layer".format(STATE_DIR)
    volume_target = (
        "the stream created through the API is really on the volume mounted at {}".format(
            LEGACY_STATE_DIR))
    if not ready:
        reason = "not attempted: the empty-legacy-volume container was not in a state to be probed"
        return [(diff_target, reason), (volume_target, reason)], []

    rows = []
    transcripts = []

    # --- the writable layer -----------------------------------------------------------------------
    status, diff_output = docker(["diff", name], DIFF_TIMEOUT)
    transcripts.append(("`docker diff` of the empty-legacy-volume container", diff_output))
    if status is None:
        rows.append((diff_target, diff_output))
    elif status != 0:
        rows.append((diff_target, "`docker diff` exited {}:\n{}".format(
            status, excerpt(diff_output))))
    else:
        # Only paths at or under /app/data are read. The rest of the diff is noise this check has no
        # opinion about — __pycache__ under /app/src, the /data mount point the daemon had to create
        # — and matching on the whole diff would make the row fail for reasons that have nothing to
        # do with where the configuration went.
        touched = []
        for line in diff_output.splitlines():
            _verb, _, path = line.partition(" ")
            if path == STATE_DIR or path.startswith(STATE_DIR + "/"):
                touched.append(line.strip())
        rows.append((diff_target, None if not touched else (
            "it carries {!r}. This deployment mounted its volume at {} and set no DATA_DIR, so "
            "{} must never be touched — a write there means the entrypoint did not recognise the "
            "legacy layout and resolved DATA_DIR to the image's own directory. That directory is "
            "the container's WRITABLE LAYER (the Dockerfile declares no VOLUME), so the stream an "
            "operator just saved through the UI is destroyed by the next automatic image update, "
            "with every endpoint answering 200 and the healthcheck green the whole time".format(
                touched, LEGACY_STATE_DIR, STATE_DIR))))

    # --- the volume itself ------------------------------------------------------------------------
    # Read through a throwaway container of the image under test, with `--entrypoint python` so that
    # nothing about the entrypoint being checked can influence the answer. Only the stream NAMES are
    # printed and never the file: streams.json is the one file in this service that holds MQTT and
    # InfluxDB credentials, and a gate that dumps it into a CI log is a habit worth not forming, even
    # on a synthetic stream.
    read_program = (
        "import json\n"
        "import os\n"
        "DIRECTORY = {directory!r}\n"
        "PATH = os.path.join(DIRECTORY, 'streams.json')\n"
        "try:\n"
        "    listed = sorted(os.listdir(DIRECTORY))\n"
        "except Exception as error:\n"
        "    print('UNLISTABLE ' + type(error).__name__ + ': ' + str(error))\n"
        "    raise SystemExit(0)\n"
        "print('LISTING ' + json.dumps(listed))\n"
        "if not os.path.exists(PATH):\n"
        "    print('NO-FILE')\n"
        "    raise SystemExit(0)\n"
        "try:\n"
        "    with open(PATH) as handle:\n"
        "        loaded = json.load(handle)\n"
        "except Exception as error:\n"
        "    print('UNREADABLE ' + type(error).__name__ + ': ' + str(error))\n"
        "    raise SystemExit(0)\n"
        "names = [entry.get('name') for entry in loaded] if isinstance(loaded, list) else None\n"
        "print('NAMES ' + json.dumps(names))\n"
    ).format(directory=LEGACY_STATE_DIR)

    # NAMED even though it is `--rm`: see READBACK_SUFFIX. This is the container that matters most
    # for cleanup — it mounts the named volume, so one left behind by a killed client would also make
    # the remove_volume() in main()'s `finally` fail and leak the volume too.
    remove_container(name + READBACK_SUFFIX)
    status, read_output = docker(
        ["run", "--rm", "--name", name + READBACK_SUFFIX,
         "-v", "{}:{}".format(volume, LEGACY_STATE_DIR),
         "--entrypoint", "python", image, "-c", read_program],
        READBACK_TIMEOUT)
    transcripts.append(
        ("the volume mounted at {}, read back from a throwaway container".format(LEGACY_STATE_DIR),
         read_output))
    if status is None:
        rows.append((volume_target, read_output))
        return rows, transcripts
    if status != 0:
        rows.append((volume_target, "reading the volume back exited {}:\n{}".format(
            status, excerpt(read_output))))
        return rows, transcripts

    names_line = next(
        (line for line in read_output.splitlines() if line.startswith("NAMES ")), None)
    if names_line is None:
        rows.append((volume_target, (
            "the volume holds no usable streams.json after a save the API reported as successful. "
            "The read-back container said:\n{}\nA `NO-FILE` there is the finding itself: the "
            "configuration went into the container's writable layer at {} instead, where the next "
            "image update deletes it — and the operator who saved it has a 200, a populated UI and "
            "no way to know".format(excerpt(read_output), STATE_DIR))))
        return rows, transcripts
    try:
        names = json.loads(names_line[len("NAMES "):])
    except Exception as error:
        rows.append((volume_target, (
            "the read-back container's NAMES line is not JSON ({}): {}".format(
                type(error).__name__, names_line[:400]))))
        return rows, transcripts
    if isinstance(names, list) and EMPTY_LEGACY_STREAM_NAME in names:
        rows.append((volume_target, None))
        return rows, transcripts
    rows.append((volume_target, (
        "the file on the volume holds streams named {!r}, and {!r} — the one the API accepted and "
        "then listed back — is not among them. What the service saved is not on the volume the "
        "deployment mounted, so it does not survive the next image update".format(
            names, EMPTY_LEGACY_STREAM_NAME))))
    return rows, transcripts


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

        # The GID is spelled out as well as the uid, and not out of symmetry: `docker exec -u 1000`
        # on its own leaves the group at 0, which is not the pair `gosu app` hands the serving
        # process — and an /app/data that was group-writable rather than owner-writable would then be
        # judged against a group the service never has. Numeric on both sides rather than `-u app`,
        # so that an /etc/passwd in which `app` had quietly become uid 0 could not answer this
        # probe's question as root.
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

        # (p) A SECOND container, in the shape the older deployments really have. Placed after
        # everything above so that the -serve container's story is complete and uninterrupted in the
        # report; it shares nothing with it — its own name, its own volume, its own probe.
        legacy_rows, legacy_ready = check_legacy_layout(
            image, name + LEGACY_SUFFIX, name + LEGACY_VOLUME_SUFFIX)
        rows.extend(legacy_rows)

        legacy_probe_rows, legacy_probe_output = run_probe(
            name + LEGACY_SUFFIX, legacy_ready,
            LEGACY_PROBE
            .replace("@@STREAM_ID@@", LEGACY_STREAM["id"])
            .replace("@@STREAM_NAME@@", LEGACY_STREAM["name"]),
            LEGACY_PROBE_TIMEOUT, LEGACY_PROBE_MARKER, EXPECTED_LEGACY_PROBE_TARGETS,
            LEGACY_PROBE_ROW_PREFIX, "legacy-layout")
        transcripts.append(
            ("the legacy-layout probe, from inside the second container", legacy_probe_output))
        rows.extend(legacy_probe_rows)

        legacy_log_status, legacy_logs = docker(
            ["logs", name + LEGACY_SUFFIX], LOGS_TIMEOUT)
        transcripts.append((
            "the legacy-layout container",
            legacy_logs if legacy_log_status == 0 else ""))

        # (q) A THIRD container, in that same older shape but on a volume that holds nothing yet —
        # the one case (p) above cannot reach, because (p) seeds its volume before mounting it and is
        # therefore satisfied by a detection that looks at the legacy FILE just as readily as by one
        # that looks at the legacy MOUNT. Its own name, its own volume, its own probe; it shares
        # nothing with either container above.
        empty_legacy_rows, empty_legacy_ready = check_empty_legacy_layout(
            image, name + EMPTY_LEGACY_SUFFIX, name + EMPTY_LEGACY_VOLUME_SUFFIX)
        rows.extend(empty_legacy_rows)

        empty_legacy_probe_rows, empty_legacy_probe_output = run_probe(
            name + EMPTY_LEGACY_SUFFIX, empty_legacy_ready,
            EMPTY_LEGACY_PROBE.replace("@@STREAM_NAME@@", EMPTY_LEGACY_STREAM_NAME),
            EMPTY_LEGACY_PROBE_TIMEOUT, EMPTY_LEGACY_PROBE_MARKER,
            EXPECTED_EMPTY_LEGACY_PROBE_TARGETS, EMPTY_LEGACY_PROBE_ROW_PREFIX,
            "empty-legacy-volume")
        transcripts.append(
            ("the empty-legacy-volume probe, from inside the third container",
             empty_legacy_probe_output))
        rows.extend(empty_legacy_probe_rows)

        # AFTER that probe, necessarily: it is the probe that makes the service save something, and
        # these two rows are the ones that ask where the save went.
        landed_rows, landed_transcripts = check_empty_legacy_writes_landed(
            image, name + EMPTY_LEGACY_SUFFIX, name + EMPTY_LEGACY_VOLUME_SUFFIX,
            empty_legacy_ready)
        rows.extend(landed_rows)
        transcripts.extend(landed_transcripts)

        empty_legacy_log_status, empty_legacy_logs = docker(
            ["logs", name + EMPTY_LEGACY_SUFFIX], LOGS_TIMEOUT)
        transcripts.append((
            "the empty-legacy-volume container",
            empty_legacy_logs if empty_legacy_log_status == 0 else ""))
    finally:
        # Unconditionally. When a bound fires inside docker() it kills the docker CLIENT on the
        # runner, not the container on the daemon, so a hung run would otherwise sit here pinning
        # the image until somebody noticed. The workflow removes the same names again under
        # `if: always()`, which covers this whole script being killed by the step timeout.
        for suffix in ALL_SUFFIXES:
            remove_container(name + suffix)
        # AFTER the containers, never before: the daemon refuses to remove a volume that is still
        # mounted by one, and each of these is mounted by a container removed in the loop above.
        for suffix in ALL_VOLUME_SUFFIXES:
            remove_volume(name + suffix)

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
    # so the run would end `smoke ok: 58/58`, exit 0, and have every printed row saying ok with two
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
