FROM python:3.11-slim

WORKDIR /app

# Unbuffered stdout/stderr. On a service whose only window into what it is doing is `docker logs`,
# a full 8 KB block buffer means the log appears in bursts minutes apart — and, worse, that whatever
# was written before a crash is lost with the process. It is also what lets ci/smoke.py wait for the
# uvicorn startup markers instead of guessing at a sleep.
ENV PYTHONUNBUFFERED=1

# gosu, and nothing else. This image serves as an unprivileged user, but /entrypoint.sh has to start
# as root to repair the ownership of a state volume that an older root-based image wrote — so
# something has to step DOWN afterwards. `su` and `sudo` both fork and wait, which leaves PID 1 a
# shell that does not forward SIGTERM; gosu execs, so the python process itself is PID 1 and gets the
# signal. That is not a nicety here: on a clean stop mqtt_processor catches the CancelledError and
# flushes the half-built batch, and a signal that never arrives loses those points for good.
#
# curl is deliberately NOT installed. The compose healthcheck probes with `python -c` precisely
# because there is no curl in this image, and the comment there that says so has to stay true.
RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*

# The account the service actually runs as. uid 1000 is spelled out rather than left to useradd's
# next-free pick because two other places are pinned to it: the chown below, and the assertion in
# ci/smoke.py that PID 1 is 1000 and not root.
RUN useradd -m -u 1000 app

# /app/data is created and handed to `app` HERE so that a FRESH volume inherits that ownership when
# docker seeds it from the image. That covers only half the fleet: a volume that already holds files
# — production's, written when this image still ran as root — is never re-seeded and keeps its own
# ownership, which is why /entrypoint.sh chowns again at run time.
#
# The directory moved out of the filesystem root and under WORKDIR: /data was a top-level name this
# image invented, and everything else this service owns already lives under /app. This is the path
# for a deployment that mounts its volume here, or names its own with DATA_DIR. A deployment that
# still binds at the old /data and passes NOTHING is handled at RUN time rather than here:
# /entrypoint.sh sees a /data DIRECTORY with no streams.json beside it at /app/data and resolves
# DATA_DIR to /data itself. See the DATA_DIR block at the top of that script — the detection lives
# there and nowhere else.
#
# THAT DETECTION DEPENDS ON THIS LINE CREATING ONLY /app/data. `/data` is deliberately absent from
# this image — nothing here creates it and the python:3.11-slim base has none — which is what makes
# its existence at run time mean "docker mounted something there" and nothing else. Adding a
# `mkdir -p /data` here, for any reason, would make every container look like the legacy layout.
RUN mkdir -p /app/data && chown app:app /app/data

# requirements.txt is copied on its own, before the source, so the pip layer is only rebuilt when
# the dependencies really change rather than on every edit to src/.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Three explicit paths instead of `COPY . .`: nothing else in the repository belongs in a published
# image. .dockerignore is the second line of defence for the day this becomes a `COPY . .`, and
# ci/smoke.py asserts that .venv, .git, data/ and tests/ are absent from the built artefact.
COPY main.py .
COPY src/ ./src/
COPY static/ ./static/

# THERE IS DELIBERATELY NO `ENV DATA_DIR` LINE HERE, AND ITS ABSENCE IS LOAD-BEARING.
#
# Everything this service is configured with — the MQTT and InfluxDB hosts, their credentials, the
# topic filters — is one file, streams.json, in the directory DATA_DIR names. There is no database
# and no environment fallback for any of it, so a container that reads the wrong directory is a
# bridge with no streams rather than a bridge with a wrong setting.
#
# Two layouts exist in this fleet. The current one mounts the volume at /app/data (and says so). The
# older one binds it at /data and passes NO environment at all — and those stacks carry
# `io.portainer.update.enable`, so they take a new :latest by themselves, with nobody editing
# anything and nobody watching.
#
# An `ENV DATA_DIR=/app/data` here would make the two indistinguishable from inside the container:
# the variable would ALWAYS be set, so "this deployment did not choose a path" — the one signal that
# says the legacy layout may be in play — could never be observed. The old stack would then come up
# on an empty directory, load_streams() would return [], /api/stats would answer 200, the container
# would report healthy, the updater would record a successful deploy and roll nothing back, and the
# volume holding every stream would sit unread at /data while the bridge wrote nothing.
#
# So the variable is left UNSET and /entrypoint.sh decides it once at start — detecting the legacy
# layout when it has to — and `export`s the result, so the python process inherits a value that is
# already decided. The DEFAULT therefore lives in exactly two places that must agree, /entrypoint.sh
# and src/settings.py, and is /app/data in both. What this image gives up by not declaring the
# variable is a line in `docker inspect`; ci/smoke.py now asserts the ABSENCE instead, because the
# absence is what the run-time detection depends on.

# Defaults for the write batching, declared in src/settings.py and read by src/mqtt_processor.py at
# import time: flush after BATCH_SIZE points or after BATCH_INTERVAL seconds, whichever comes first.
# Production overrides them in docker-compose.yml (220/3).
ENV BATCH_SIZE=100
ENV BATCH_INTERVAL=1.0

# THERE IS DELIBERATELY NO `VOLUME` LINE EITHER. This one is a judgement call rather than a
# requirement, so the reasoning is written down.
#
# What `VOLUME ["/app/data"]` bought: a `docker run` with no `-v` got an anonymous volume there
# instead of writing the configuration into the container's writable layer. That is worth very
# little. An anonymous volume is unnamed and unlabelled; nobody goes looking in one, no deployment
# in this fleet reads one, and after the next image update the configuration in it is exactly as
# unreachable as it would have been in the writable layer. It was never a safety net, only the
# appearance of one.
#
# What it cost is concrete. Under the legacy layout — volume bound at /data, no DATA_DIR set — the
# entrypoint now resolves DATA_DIR to /data, so /app/data is read and written by nobody; a VOLUME
# line would still have docker create an EMPTY anonymous volume there on every container, chown it,
# and leave it behind on the host after every one of the automatic redeploys that stack performs on
# its own. That is unbounded garbage on a daemon shared by the whole fleet. And before the
# entrypoint learned to detect that layout, this same empty, freshly-chowned directory was precisely
# what made the failure SILENT: it looks exactly like a healthy first boot.
#
# Nothing that matters loses anything: every real deployment mounts a NAMED volume, and a named
# mount overrides a VOLUME declaration in any case.

# --chmod is not decoration: ENTRYPOINT in exec form does not go through a shell, so a script that
# arrives without its executable bit does not fail the build — it fails every container at start
# with `permission denied`. The bit does not survive every build context (a CI checkout on a
# filesystem with no POSIX permissions drops it), so it is set here rather than hoped for.
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# There is deliberately NO `USER` line. root is what /entrypoint.sh needs to chown the volume, and it
# drops to uid 1000 itself with gosu; a static USER here would take that root away and leave a
# container that cannot write to a volume an earlier version of this image created.
#
# `docker exec` does NOT go through ENTRYPOINT, which is why ci/smoke.py's in-container probes still
# arrive as root and can read whatever they need — and why the one thing they must never ask is their
# OWN uid. They read PID 1's instead, which is the only process here that came through this line.
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "main.py"]
