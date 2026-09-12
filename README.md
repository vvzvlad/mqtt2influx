# mqtt2influx

A bridge from MQTT to InfluxDB, with a web UI.

It subscribes to one or more MQTT brokers, recursively flattens whatever arrives — JSON objects
become one point per leaf, booleans and numeric strings become numbers, anything else is dropped
unless the stream turns on `write_strings` (see below) —
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

`DATA_DIR` defaults to `/app/data`, which is right inside the container and wrong on a workstation
— set it to something local, as above, or put it in `.env` (`make env`), which is read at startup.

Other options:

```bash
.venv/bin/python main.py --host 127.0.0.1 --port 8000 --reload
```

`PORT` works as an environment variable too, though an explicit `--port` still wins over it.
`BATCH_SIZE` (default 100) and `BATCH_INTERVAL` (default 1.0 s) bound the write batch: a flush
happens when either is reached.

All four are declared in `src/settings.py` (pydantic-settings) and read from the environment or from
a `.env` beside the project — see `.env.example`, which is the one place the whole environment
surface is written down.

## Configuration

There are **no required environment variables**. Everything the service is configured with lives in
a single JSON file:

```text
{DATA_DIR}/streams.json
```

One record per stream: `id`, `name`, the MQTT side (`mqtt_host`, `mqtt_port`, `mqtt_user`,
`mqtt_password`, `mqtt_topic`, `topic_prefix`), the InfluxDB side (`influx_host`, `influx_port`,
`influx_user`, `influx_password`, `influx_database`), `enabled` and the optional
`value_precision` and `write_strings`. It holds credentials in plaintext, so it belongs on the
volume and nowhere else — never in the repository, never in an image.

### `value_precision`

How many decimals a stream's numbers are rounded to on the way in. Three states:

| In `streams.json` | What happens |
| --- | --- |
| key absent, or `null` | **two decimals** — what this service has always done, and what every existing stream keeps doing |
| a non-negative integer `N` | rounded to `N` decimals |
| a negative integer (write `-1`) | **not rounded at all** — stored exactly as it arrived |

It is per stream, and it is set in the UI ("Value Precision") or with a `PUT /api/streams/{id}`.
The API takes an integer or `null` there and nothing else: `"8"`, `2.0`, `true` and the rest are
answered with **422** instead of being stored, because a precision that is written to the file and
then not applied to the data is the exact bug this setting exists to remove.

Two decimals is fine for on/off states and room temperatures and destroys anything smaller: a
calibration coefficient of `0.00003618` is stored as a flat `0.0` — stored, not dropped, so the
series looks like a working sensor reading zero. Raising the number is not always the fix, because
the right number depends on the magnitude of the value: `0.00003618` needs eight decimals, and the
next device's coefficient may need more. For values like that use `-1` and store them as they came.

An empty box in the UI and a missing key mean the same thing — the default. Turning rounding off
is the state you have to type on purpose, so that clearing a field can never silently change what
a production stream writes.

### `write_strings`

Off by default: a payload that is not a number and is not `true`/`false` is dropped, which is what
this bridge has always done. Turn it on for one stream and a leftover like `Printing` on
`/data/gcode/status` is written as an InfluxDB **string field** instead of disappearing.

It changes nothing about the values that were already being written: numbers, numeric strings and
the `true`/`false` → `10`/`0` mapping all take the same path as before, and only a value the number
parser has already refused can reach the new branch. An empty payload stays dropped — clearing a
retained topic publishes zero bytes, and that is the absence of a reading, not a reading of `""`.
So does a payload that is a number the bridge refuses to store — `nan`, `inf`, a literal that
overflows: those come from topics that carry floats the rest of the time, and writing them as
strings would poison exactly the numeric series they belong to. The value is otherwise stored byte
for byte, with one exception: a real newline or carriage return is written as the two characters
`\n` / `\r`, because line protocol ends a record at a newline.

It is per stream, set in the UI ("Write string values") or with a `PUT /api/streams/{id}`, and the
API takes `true` or `false` and nothing else — `"false"` is a **422**, because it is a non-empty
string and would otherwise be read as a yes.

**The one thing to know before switching it on:** InfluxDB fixes a field's type on the first write.
If a topic under this stream has ever written numbers into `value`, the first string for that
measurement is refused with a 400 — and the refusal costs the entire batch it was sent in, not just
that point. Use it on streams whose non-numeric topics are genuinely non-numeric.

**One caveat, and it is about rolling the image back.** `load_streams()` builds each record with
`StreamConfig(**record)` under a blanket `except Exception: return []`, so a key an older build
does not declare does not skip that record — it empties the entire config, and the next save writes
that emptiness to disk. A stream that never sets `value_precision`, and one that leaves
`write_strings` off, never gets that key written (see `_serialize` in `src/config.py`), so it stays
readable by an older image exactly as before. A stream that does set either one makes the whole file
unreadable to a build that predates that field. **If you ever roll back past these features, delete
the `value_precision` and `write_strings` keys from `streams.json` first.**

In production the state directory is a docker volume named `mqtt2influx_data`. **That volume is the
entire configuration of the service**: lose it and every stream is gone. See the comment above the
`volumes:` block in `docker-compose.yml` before changing anything about how it is declared — in
particular, `DATA_DIR` and the mount point are two halves of one fact and have to move together, in
a single stack update.

Where it is MOUNTED depends on how old the deployment is, and the image copes with both without
being told:

- mounted at `/app/data` (with `DATA_DIR` set to match, or left to the default) — the current shape;
- mounted at `/data` with no environment variables at all — the older stacks. The image declares no
  `ENV DATA_DIR`, precisely so that "nothing was chosen" stays visible inside the container:
  `entrypoint.sh` then falls back to `/data` when there is no `streams.json` at `/app/data` **and
  `/data` exists as a directory** (`[ -d /data ]`), and logs a warning saying the deployment should
  set `DATA_DIR` explicitly.

  **The signal is the MOUNT, not a file on it, and that distinction is the whole fix.** This image
  never creates `/data`, so the directory being there at all is what says a volume was bound there.
  A test for the legacy *file* (`[ -e /data/streams.json ]`) looks equivalent and is not: a
  legacy-shaped stack whose volume holds no `streams.json` **yet** — one just created, one restored
  empty, one whose operator has not saved a stream since the migration — fails it, comes up on
  `/app/data`, and writes the operator's first stream into the container's writable layer, where the
  next automatic image update deletes it. Everything answers 200 and the healthcheck stays green
  throughout. That regression has shipped twice, `ci/smoke.py` check (q) exists to catch the third,
  and `[ -d /data ]` is therefore not a redundant widening to be "simplified" away. Setting
  `DATA_DIR` is still the right thing to do — the fallback is a safety net, not a configuration.

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
src/settings.py      every environment variable this service reads, typed, with defaults
src/config_errors.py turns a settings ValidationError into a readable startup message
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
`/app/data` and never opens a socket.

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
