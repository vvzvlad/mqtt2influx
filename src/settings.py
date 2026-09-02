"""Every environment variable this service reads, in one typed place.

Before this file the four settings were read with a bare `os.environ.get()` in three different
modules — src/config.py, main.py and src/mqtt_processor.py — each with its own literal default and
its own `int()` / `float()` cast. Nothing listed them, nothing typed them, and a `BATCH_SIZE=many`
produced a ValueError traceback out of an import rather than a sentence naming the variable.

WHY EVERY FIELD HERE HAS A DEFAULT, AND WHY THAT IS A CONTRACT RATHER THAN A CONVENIENCE.
This service starts on an EMPTY environment today: everything it is actually configured with — the
brokers, the databases, their credentials — lives in {DATA_DIR}/streams.json, not here. So a field
without a default would turn `load_settings_or_exit()` from a better error message into a new
failure mode: the container exits 1 at import, and `restart: always` in docker-compose.yml turns
that into an unbounded restart loop on two production hosts, both of which carry
`io.portainer.update.enable` and would pick the change up on their own. Adding a required field to
this class is therefore a deployment event, not a refactor — see AGENTS.md, "There are no required
environment variables."
"""

from pydantic_settings import BaseSettings, SettingsConfigDict

from .config_errors import load_settings_or_exit


class Settings(BaseSettings):
    # Directory holding streams.json — i.e. EVERY stream this bridge knows about, with its MQTT and
    # InfluxDB credentials. Read once, at import, by src/config.py.
    #
    # The default sits under WORKDIR (/app) rather than at the filesystem root: /app/data is the
    # directory the image creates and docker-compose.yml mounts the named volume on.
    #
    # INSIDE THE IMAGE THIS DEFAULT NEVER ACTUALLY DECIDES ANYTHING, and that is deliberate.
    # /entrypoint.sh resolves DATA_DIR before python starts — including the fallback to the legacy
    # /data layout, for a deployment that binds its volume there and passes no environment at all —
    # and `export`s the result, so this process always inherits a value that is already set. The
    # literal here is kept identical so the two agree if the entrypoint is ever bypassed; the
    # DETECTION itself deliberately exists in one place only, and that place is the DATA_DIR block
    # at the top of /entrypoint.sh. Do not reimplement it here: two copies would be free to disagree
    # about which directory holds the configuration, which is the exact failure both are written to
    # prevent.
    #
    # What this default really serves is a process started OUTSIDE a container — `make run` on a
    # workstation — which is why .env.example overrides it to ./data.
    data_dir: str = "/app/data"

    # TCP port uvicorn binds to. Consumed by main.py as the DEFAULT of --port, so an explicit
    # `--port` on the command line still wins over this.
    port: int = 8000

    # Write batching for InfluxDB: flush after batch_size points or after batch_interval seconds,
    # whichever comes first. Both are read once at import by src/mqtt_processor.py; production runs
    # 220 / 3 from docker-compose.yml against these 100 / 1.0 defaults.
    batch_size: int = 100
    batch_interval: float = 1.0

    # `extra="ignore"` because .env is also read by the shell and by docker-compose and carries
    # variables that are not settings of this class (TZ, and whatever a developer adds); pydantic's
    # default would refuse to build at all over one of them.
    #
    # `env_file` is a RELATIVE path, so it resolves against the process's working directory rather
    # than against this file — the repository root when the app is run through `make run`, and
    # nothing at all in the image, where .dockerignore keeps .env out. It also means a developer's
    # own .env would otherwise reach the test suite, which builds fresh Settings() objects
    # (src/mqtt_processor.py does so per call): tests/conftest.py therefore switches this off for
    # the duration of the suite. See the `no_dotenv` fixture there.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Built with a clear startup error (naming the offending variable) instead of a raw pydantic
# traceback. No field above is required, so this cannot fail on a missing variable — only on one
# whose value does not parse, e.g. BATCH_SIZE=many.
settings = load_settings_or_exit(Settings)
