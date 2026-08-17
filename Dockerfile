FROM python:3.11-slim

WORKDIR /app

# Unbuffered stdout/stderr. On a service whose only window into what it is doing is `docker logs`,
# a full 8 KB block buffer means the log appears in bursts minutes apart — and, worse, that whatever
# was written before a crash is lost with the process. It is also what lets ci/smoke.py wait for the
# uvicorn startup markers instead of guessing at a sleep.
ENV PYTHONUNBUFFERED=1

# gosu, and nothing else. This image serves as an unprivileged user, but /entrypoint.sh has to start
# as root to repair the ownership of a /data volume that an older root-based image wrote — so
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

# /data is created and handed to `app` HERE so that a FRESH volume inherits that ownership when
# docker seeds it from the image. That covers only half the fleet: a volume that already holds files
# — production's, written when this image still ran as root — is never re-seeded and keeps its own
# ownership, which is why /entrypoint.sh chowns again at run time.
RUN mkdir -p /data && chown app:app /data

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

# Where streams.json lives. Everything this service is configured with — the MQTT and InfluxDB
# hosts, their credentials, the topic filters — is that one file; there is no database and no
# environment fallback. src/config.py reads this variable once at import.
ENV DATA_DIR=/data

# Defaults for the write batching in src/mqtt_processor.py, which reads both at import time: flush
# after BATCH_SIZE points or after BATCH_INTERVAL seconds, whichever comes first. Production
# overrides them in docker-compose.yml (220/3).
ENV BATCH_SIZE=100
ENV BATCH_INTERVAL=1.0

# Declared so a `docker run` with no `-v` still gets a volume rather than writing the configuration
# into the container's writable layer, where the next image update would take it with it.
VOLUME ["/data"]

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
