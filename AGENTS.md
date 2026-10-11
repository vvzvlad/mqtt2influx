# Agent Instructions — mqtt2influx

## What this service is

A bridge: it subscribes to MQTT, flattens whatever arrives into InfluxDB line protocol and
writes it in batches, with a small web UI to configure and watch the streams.

It runs in production under real load. **MQTT does not buffer for a subscriber that is not
connected**, so downtime loses data points permanently; that is why the CI gate is thorough and
why it is never skipped.

## Project structure
- `src/` — application code
  - `settings.py` — every environment variable this service reads, in one pydantic-settings class
  - `config_errors.py` — `load_settings_or_exit()`: a readable startup message instead of a
    pydantic traceback. Shared with the other services in the fleet; keep it a copy, not a fork
  - `config.py` — the stream store: `StreamConfig` + CRUD over `{DATA_DIR}/streams.json`
  - `api.py` — FastAPI app: REST under `/api`, the `/ws` websocket, `static/` mounted on `/`
  - `stream_manager.py` — the `manager` singleton that owns the running processors
  - `mqtt_processor.py` — one MQTT subscription + batching per stream
  - `influx_writer.py` — line-protocol formatting, topic filtering, the HTTP write
- `static/index.html` — the entire web UI, one file
- `tests/` — pytest
- `ci/` — `smoke.py`, the gate both workflows run against the built image
- `.gitea/workflows/` — CI (Gitea Actions); there is no `.github/workflows`
- `data/` — runtime state on a workstation; in production `/app/data` is a docker volume
- `main.py` — thin entry point: argparse + `uvicorn.run("src.api:app")`
- `entrypoint.sh` — the image's ENTRYPOINT: starts as root, creates and chowns `"$DATA_DIR"`
  (default `/app/data`), then `exec`s gosu to drop to uid 1000. `exec` in both branches, so the
  python process is PID 1 and gets SIGTERM
- `CLAUDE.md` — one line, `@AGENTS.md`; keep it an import, not a copy or a symlink

## Setup
All routine actions go through the `Makefile` — run `make help` to list targets.
```bash
make install   # create .venv and install dev/test deps
make env       # copy .env.example to .env if it is not there yet
```
`.env.example` lists every environment variable this service reads. `src/settings.py` reads `.env`
at startup and a real environment variable wins over it; `.env` never reaches the image
(`.dockerignore`) or the CI test container (excluded from the tar).

## Tests and local run
Tests run in CI (see below); `make test` runs the same pytest suite.
```bash
make run       # http://127.0.0.1:8000, DATA_DIR defaults to /app/data — override it locally
```

## Conventions
- Code comments are in English.
- Repeated actions go through `make` targets (`install` / `test` / `run`).
- Python always runs inside a local `.venv`, created automatically by `make`.
- Commit files by name (`git add <path>`), never `git add -A`.
- In `.gitignore`, `data/` is the pair `data/*` + `!data/.gitkeep`: a bare `data/` would ignore the
  `.gitkeep` too, and a fresh clone would have no `data/`. `data/streams.json` must stay ignored —
  it holds real broker credentials and this repository is public.
- Dependencies in `requirements.txt` are pinned with `==`; an upgrade is a deliberate edit that the
  gate judges.
- Tests are required for new code. When fixing a bug, confirm the new test fails with the fix
  reverted.
- No `EXPOSE` in the Dockerfile; ports are published in docker-compose.
- No `USER` in the Dockerfile: `entrypoint.sh` needs root to chown `"$DATA_DIR"` and drops to
  uid 1000 itself with gosu.

## Things that are easy to get wrong here
- **There are no required environment variables**; everything is configured in
  `{DATA_DIR}/streams.json`. Every field in `src/settings.py` must keep a default:
  `load_settings_or_exit()` exits 1 on a validation error, so a required field makes the container
  exit at import, and `restart: always` turns that into a restart loop on both production hosts,
  which update themselves.
- **`DATA_DIR` (default `/app/data`) and the volume mount are one fact in two places.** A stack that
  moves the mount without the variable, or the other way round, comes up healthy with an empty
  stream list and saves into the writable layer, which the next image update deletes. Change both
  in one stack update.
- **`enabled: true` starts a network connection immediately.** `create_stream()` in `src/api.py`
  calls `manager.start_stream()` as soon as a stream is created enabled, and the lifespan does the
  same for every enabled stream on disk at startup. Tests and the smoke gate create streams with
  `enabled: false`, or they dial a real broker.
- **`load_streams()` swallows every error and returns `[]`**, so a corrupt or schema-mismatched
  `streams.json` looks empty and the next save overwrites it. Adding a field to `StreamConfig` is
  therefore a compatibility event: an older image reading a key it does not declare loses every
  stream. Optional fields are written only when set (`_serialize` in `src/config.py`); copy that
  pattern, and see the caveat in README before rolling an image back.
- **`StaticFiles(html=True)` is not an SPA catch-all**: with no `404.html` in `static/` an unknown
  path is a 404. Asserted in the test suite and the gate.
- **The compose volume key is `data`, not `mqtt2influx_data`.** Portainer uses the stack name as
  the compose project name, so `data` becomes `mqtt2influx_data`; the full name as the key would
  ask for `mqtt2influx_mqtt2influx_data`, an empty volume.

## CI and deployment (Gitea Actions)
Deploying is merging to `main`; images are built and pushed only by CI, never from a workstation.
Two workflows in `.gitea/workflows`, and they must stay a matched pair:
- `image-check-publish.yml` — `push` to `main` + `workflow_dispatch`. Tests, build, gate, then
  login and push to `gitea.vvzvlad.xyz/projects/mqtt2influx`.
- `tests.yml` — `pull_request` + `workflow_dispatch`. The same tests, the same image, the same
  gate, no credential and no push.

Rules worth knowing before editing either:
- Each file is two jobs — `test`, then `build` with `needs: test` — so a red suite never reaches
  a `docker build`. The jobs get separate workspaces: each checks out for itself and removes only
  the container it started (`-tests` in `test`, `-serve` in `build`).
- The `run:` bodies the two files share are byte-identical. Change one, change both.
- The registry login comes after the gate, never before: the runner is shared with every other
  repository.
- `:<sha>` is pushed before `:latest`, so a half-failed push leaves production on the previous
  image with its rollback point intact.
- No `${{ }}` inside a `run:` body — values arrive through `env:`.
- `ci/smoke.py` runs against the built image between build and push; read its module docstring
  before touching a check. It publishes no ports and bind-mounts nothing (the job and the docker
  daemon are in different namespaces, so everything internal goes through `docker exec`), and it
  reads the CRUD cycle's result off the container's disk rather than trusting the API's echo.
- A push of `:latest` deploys itself: the production container carries
  `io.portainer.update.enable`, so an updater picks it up with nobody in the loop.

## Limits for agents
- Tests: at most three times the code (`wc -l` of `tests/` against the code). Over the limit,
  still write the test you need, and in the same change go through the old tests: drop or merge
  duplicates and tests of implementation rather than behaviour.
- Comments explain the code as it is and why it is built this way. History ("used to", "after
  the review", "since the incident") goes into the commit message.
- This file stays under ~150 lines: only what an agent would get wrong without it. No decision
  history, incident logs or retelling of the code.
