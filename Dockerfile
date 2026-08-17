FROM python:3.11-slim

WORKDIR /app

# Unbuffered stdout/stderr. On a service whose only window into what it is doing is `docker logs`,
# a full 8 KB block buffer means the log appears in bursts minutes apart — and, worse, that whatever
# was written before a crash is lost with the process. It is also what lets ci/smoke.py wait for the
# uvicorn startup markers instead of guessing at a sleep.
ENV PYTHONUNBUFFERED=1

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

# No ENTRYPOINT on purpose: `docker exec` does not go through one, and ci/smoke.py drives every
# in-container check that way.
CMD ["python", "main.py"]
