# mqtt2influx

A bridge from MQTT to InfluxDB, with a web UI.

It subscribes to one or more MQTT brokers, recursively flattens whatever arrives — JSON objects
become one point per leaf, booleans and numeric strings become numbers, anything else is dropped —
filters out the topics that are noise (`zigbee2mqtt/bridge`, `homeassistant/`, firmware version
topics and so on, see `EXCLUDED_SUBSTRINGS` in `src/influx_writer.py`) and writes the rest to
InfluxDB in batches over the HTTP line-protocol endpoint.

Each **stream** is one MQTT source paired with one InfluxDB destination, and there can be several
running at once. Streams are created and edited in the UI, which also shows live counters, a rolling
log of the last topics seen and the state of the current batch.

Production scale, for a sense of what the thing is doing: 612 million messages processed, 604
million points written. MQTT does not buffer for a subscriber that is not connected, so a minute of
downtime is a minute of data that no longer exists anywhere.

## Running it locally

```bash
make install                       # creates .venv, installs pinned deps + pytest
DATA_DIR=./data make run           # http://127.0.0.1:8000
```

`DATA_DIR` defaults to `/data`, which is right inside the container and wrong on a workstation —
set it to something local, as above.

Other options:

```bash
.venv/bin/python main.py --host 127.0.0.1 --port 8000 --reload
```

`PORT` works as an environment variable too. `BATCH_SIZE` (default 100) and `BATCH_INTERVAL`
(default 1.0 s) bound the write batch: a flush happens when either is reached.

## Configuration

There are **no required environment variables**. Everything the service is configured with lives in
a single JSON file:

```text
{DATA_DIR}/streams.json
```

One record per stream: `id`, `name`, the MQTT side (`mqtt_host`, `mqtt_port`, `mqtt_user`,
`mqtt_password`, `mqtt_topic`, `topic_prefix`), the InfluxDB side (`influx_host`, `influx_port`,
`influx_user`, `influx_password`, `influx_database`) and `enabled`. It holds credentials in
plaintext, so it belongs on the volume and nowhere else — never in the repository, never in an
image.

In production `/data` is a docker volume named `mqtt2influx_data`. **That volume is the entire
configuration of the service**: lose it and every stream is gone. See the comment above the
`volumes:` block in `docker-compose.yml` before changing anything about how it is declared.

## HTTP API

| Method | Path | What it does |
| --- | --- | --- |
| `GET` | `/api/streams` | list every stream, each with a `running` flag |
| `POST` | `/api/streams` | create one; the server mints the `id`, and `enabled: true` starts it immediately |
| `PUT` | `/api/streams/{id}` | partial update; fields not sent are carried over, then the stream is restarted |
| `DELETE` | `/api/streams/{id}` | stop and remove; 404 if it is not there |
| `POST` | `/api/streams/{id}/start` | start a stopped stream |
| `POST` | `/api/streams/{id}/stop` | stop a running one |
| `GET` | `/api/stats` | per-stream counters; `[]` when nothing is running. Also the container's healthcheck target |
| `WS` | `/ws` | a `{"type": "snapshot"}` frame on connect, then `{"type": "stats"}` every 2 s, plus per-message and per-flush events |

`/` serves `static/index.html`. Any other unknown path is a 404 — the static mount is not an SPA
catch-all.

## Layout

```text
main.py              argparse + uvicorn.run("src.api:app")
src/config.py        StreamConfig + CRUD over {DATA_DIR}/streams.json
src/api.py           REST, /ws, the static mount
src/stream_manager.py  the `manager` singleton owning the running processors
src/mqtt_processor.py  one subscription + batching per stream
src/influx_writer.py   line protocol, topic filtering, the HTTP write
static/index.html    the whole UI, one file
tests/               pytest
ci/smoke.py          the CI gate, run against the built image
```

## Tests

```bash
make test          # or: .venv/bin/python -m pytest
```

They are not optional. Both CI workflows run the suite inside `python:3.11-slim` — the same base the
image is built from — before anything is built, and a red suite stops the pipeline there.

The suite runs against a temporary `DATA_DIR` and a stubbed stream manager, so it never touches
`/data` and never opens a socket.

## Deployment

Built and published by Gitea Actions to `gitea.vvzvlad.xyz/projects/mqtt2influx`:

- a push to `main` builds `:<sha>` and `:latest`, in that order;
- **between the build and the push sits `ci/smoke.py`** — it starts the real image, drives the whole
  CRUD lifecycle through its HTTP API, reads the resulting `streams.json` off the container's disk,
  checks the static UI and opens a `/ws` connection. Nothing is pushed until all of that is green;
- the registry login happens only after the gate passes;
- pull requests run the identical suite, build the identical image and run the identical gate, and
  never log in or push.

`docker-compose.yml` in this repo mirrors the production stack (a Portainer compose stack named
`mqtt2influx`): the UI is published on port 8111 straight onto the LAN, with no Traefik in front.
The container carries `io.portainer.update.enable`, so a published `:latest` rolls out on its own —
which is what makes the gate the last place a broken image can be stopped.
