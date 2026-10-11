"""Every environment variable this service reads, in one typed place.

Every field has a default, and that is a contract: the service starts on an empty environment (the
brokers, databases and credentials live in {DATA_DIR}/streams.json), so a required field would make
the container exit at import and restart-loop under `restart: always`. Adding one is a deployment
event, not a refactor — see AGENTS.md.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict

from .config_errors import load_settings_or_exit


class Settings(BaseSettings):
    # Directory holding streams.json — every stream with its MQTT and InfluxDB credentials. Read once,
    # at import, by src/config.py. /app/data is the directory the image creates and the volume is
    # mounted on; /entrypoint.sh exports the same default before python starts.
    data_dir: str = "/app/data"

    # TCP port uvicorn binds to. Consumed by main.py as the DEFAULT of --port, so an explicit
    # `--port` on the command line still wins over this.
    port: int = 8000

    # Write batching for InfluxDB: flush after batch_size points or after batch_interval seconds,
    # whichever comes first. Both are read once at import by src/mqtt_processor.py; production runs
    # 220 / 3 from docker-compose.yml against these 100 / 1.0 defaults.
    batch_size: int = 100
    batch_interval: float = 1.0

    # `extra="ignore"`: .env also carries variables that are not settings of this class (TZ, anything
    # a developer adds), and pydantic's default would refuse to build over one of them.
    # `env_file` resolves against the working directory (the repository root under `make run`; the
    # image has no .env). tests/conftest.py's `no_dotenv` fixture keeps a developer's .env out of the
    # suite.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Built with a clear startup error (naming the offending variable) instead of a raw pydantic
# traceback. No field above is required, so this cannot fail on a missing variable — only on one
# whose value does not parse, e.g. BATCH_SIZE=many.
settings = load_settings_or_exit(Settings)
