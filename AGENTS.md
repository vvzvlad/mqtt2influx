# Agent Instructions — mqtt2influx

## What this service is

A bridge: it subscribes to MQTT, flattens whatever arrives into InfluxDB line protocol and
writes it in batches, with a small web UI to configure and watch the streams.

It is in production and under real load — 612 million messages processed, 604 million points
written. **MQTT does not buffer for a subscriber that is not connected**, so downtime does not
delay data points, it loses them permanently. That is the reason the CI gate in this repo is
as thorough as it is, and it is the reason not to skip it.

## Project structure
- `src/` — application code
  - `config.py` — the stream store: `StreamConfig` + CRUD over `{DATA_DIR}/streams.json`
  - `api.py` — FastAPI app: REST under `/api`, the `/ws` websocket, `static/` mounted on `/`
  - `stream_manager.py` — the `manager` singleton that owns the running processors
  - `mqtt_processor.py` — one MQTT subscription + batching per stream
  - `influx_writer.py` — line-protocol formatting, topic filtering, the HTTP write
- `static/index.html` — the entire web UI, one file
- `tests/` — pytest
- `ci/` — `smoke.py`, the gate both workflows run against the built image
- `.gitea/workflows/` — CI (Gitea Actions); there is no `.github/workflows`
- `data/` — runtime state on a workstation; in production `/data` is a docker volume
- `main.py` — thin entry point: argparse + `uvicorn.run("src.api:app")`
- `entrypoint.sh` — the image's ENTRYPOINT: starts as root, chowns `/data`, then `exec`s gosu to
  drop to uid 1000. `exec` in both branches, so the python process is PID 1 and gets SIGTERM
- `CLAUDE.md` — one line, `@AGENTS.md`. Claude Code reads only `CLAUDE.md` and does not read this
  file at all, so that import is the only thing putting these instructions in an agent's context.
  Keep it a native import — a copy would drift and a symlink is not what the loader follows

## Setup
All routine actions go through the `Makefile` — run `make help` to list targets.
```bash
make install   # create .venv and install dev/test deps
```

## Running tests
```bash
make test
```

## Running the app
```bash
make run       # http://127.0.0.1:8000, DATA_DIR defaults to /data — override it locally
```

## Conventions
- Code comments are in English.
- Repeated actions go through `make` targets (`install` / `test` / `run`).
- Python always runs inside a local `.venv`, created automatically by `make`.
- Commit files by name (`git add <path>`), never `git add -A`.
- `.gitignore` is editable (the owner's earlier standing instruction not to touch it has been
  lifted), but the `data/` entry is not free-form: it is the pair `data/*` + `!data/.gitkeep`.
  A bare `data/` would ignore the `.gitkeep` too, and since git does not carry empty directories
  the directory would simply not exist in a fresh clone. What must never change is that
  `data/streams.json` stays ignored — it holds real broker credentials and this repository is
  public.
- Dependencies in `requirements.txt` are PINNED with `==`. Adding one means pinning it;
  upgrading one means editing this file deliberately and letting the gate judge the result.
- Tests are required for new code. An invariant with no test that goes red when it is broken
  is an intention, not an invariant — so when fixing a bug, check the new test by breaking
  the mechanism again and confirming it fails.
- No `EXPOSE` in the Dockerfile; ports are published in docker-compose.
- No static `USER` in the Dockerfile either, and that is not an oversight to be tidied up:
  `entrypoint.sh` needs root to chown `/data` and drops to uid 1000 itself with gosu. A `USER app`
  line would take that root away and break every deployment whose volume was created by an older
  root-based image.

## Things that are easy to get wrong here
- **There are no required environment variables.** Everything the service is configured with
  lives in `{DATA_DIR}/streams.json`. Do not add a startup guard for missing variables — there
  is nothing to guard.
- **`enabled: true` starts a network connection immediately.** `create_stream()` in `src/api.py`
  calls `manager.start_stream()` the instant a stream is created enabled, and the app's lifespan
  does the same for every enabled stream on disk at startup. Anything automated — tests, the
  smoke gate — must create streams with `enabled: false` or it will dial a real broker.
- **`load_streams()` swallows every error and returns `[]`.** A corrupt or schema-mismatched
  `streams.json` is therefore indistinguishable from an empty one, and the next save overwrites
  it. Worth remembering when a stream "disappears".
  This is also why adding a field to `StreamConfig` is a compatibility event and not a free act:
  an older image reading a key it does not declare loses EVERY stream, not the one record. The
  optional `value_precision` field is written only when a stream actually sets it (`_serialize` in
  `src/config.py`) so that untouched records stay readable by older builds — copy that pattern for
  any field added later, and see the caveat in README before rolling an image back.
- **`StaticFiles(html=True)` is NOT an SPA catch-all.** It looks for a `404.html`, does not find
  one in `static/`, and returns 404. Verified against the starlette source and asserted in both
  the test suite and the gate.
- **The compose volume key is `data`, not `mqtt2influx_data`.** Compose prefixes it with the
  project name, and Portainer uses the stack name as the project name: `mqtt2influx` + `data`
  = `mqtt2influx_data`, the volume that exists in production. Writing the full name as the key
  would ask for `mqtt2influx_mqtt2influx_data` — an empty volume, and every stream gone.

## CI (Gitea Actions)
Two workflows in `.gitea/workflows`, and they must stay a matched pair:
- `image-check-publish.yml` — `push` to `main` + `workflow_dispatch`. Tests, build, gate, then
  login and push to `gitea.vvzvlad.xyz/projects/mqtt2influx`.
- `tests.yml` — `pull_request`. The same tests, the same image, the same gate, no credential
  and no push.

Rules worth knowing before editing either:
- The `run:` bodies the two files share are BYTE-IDENTICAL. Change one, change both.
- The registry login comes AFTER the gate, never before: until the gate is green the registry
  PAT has no business being on a runner shared with every other repository.
- `:<sha>` is pushed BEFORE `:latest`, so a half-failed push leaves production on the previous
  image with its rollback point intact.
- No `${{ }}` inside a `run:` body — values arrive through `env:`.
- `ci/smoke.py` runs against the built image between build and push. Read its module docstring
  before touching a check; in particular it publishes no ports and bind-mounts nothing (the job
  and the docker daemon are in different namespaces — everything internal goes through
  `docker exec`), and it drives a full CRUD cycle whose result it then reads off the
  container's disk rather than believing the API's echo.
- A push of `:latest` deploys itself: the production container carries
  `io.portainer.update.enable`, so an updater picks it up with nobody in the loop.
