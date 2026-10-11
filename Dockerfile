FROM python:3.11-slim

WORKDIR /app

# Unbuffered stdout/stderr: `docker logs` shows output as it happens, nothing written before a crash
# is lost, and ci/smoke.py can wait for the uvicorn startup markers.
ENV PYTHONUNBUFFERED=1

# /entrypoint.sh starts as root to chown the state volume, then drops to uid 1000 with gosu. gosu
# execs instead of forking like su/sudo, so python is PID 1 and gets SIGTERM, which is what flushes
# the pending batch on a clean stop. No curl: the compose healthcheck uses `python -c`.
RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*

# uid 1000 is pinned: the chown below and ci/smoke.py's check that PID 1 runs as 1000 depend on it.
RUN useradd -m -u 1000 app

# Owned by `app` here so a fresh volume inherits that ownership when docker seeds it from the image;
# a volume that already holds root-owned files is chowned by /entrypoint.sh at start.
RUN mkdir -p /app/data && chown app:app /app/data

# Copied before the source so the pip layer is rebuilt only when the dependencies change.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Explicit paths instead of `COPY . .`: nothing else in the repository belongs in the image.
# ci/smoke.py asserts that .venv, .git, tests/ and ci/ are absent and that /app/data is empty.
COPY main.py .
COPY src/ ./src/
COPY static/ ./static/

# Defaults for the write batching, declared in src/settings.py and read by src/mqtt_processor.py at
# import time: flush after BATCH_SIZE points or after BATCH_INTERVAL seconds, whichever comes first.
# Production overrides them in docker-compose.yml (220/3).
ENV BATCH_SIZE=100
ENV BATCH_INTERVAL=1.0

# The executable bit does not survive every build context, and without it every container fails at
# start with `permission denied`.
COPY --chmod=0755 entrypoint.sh /entrypoint.sh

# No USER line: /entrypoint.sh needs root to chown the volume and drops to uid 1000 itself.
# `docker exec` bypasses ENTRYPOINT and arrives as root, so ci/smoke.py's probes check PID 1's uid.
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "main.py"]
