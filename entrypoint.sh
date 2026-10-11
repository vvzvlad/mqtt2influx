#!/bin/sh
# Postgres-style hybrid entrypoint: start as root, fix state-dir ownership,
# then drop privileges to the unprivileged `app` user (uid 1000).
set -e

# Exported so the python process inherits the same directory this script repairs.
DATA_DIR="${DATA_DIR:-/app/data}"
export DATA_DIR

if [ "$(id -u)" = "0" ]; then
    # Created as root before the chown, so a DATA_DIR that does not exist yet is not a chown failure
    # and a restart loop. On an existing directory, a read-only mount included, it is a no-op.
    if ! mkdir -p "$DATA_DIR"; then
        echo "FATAL: $DATA_DIR does not exist and could not be created." >&2
        echo "Check the DATA_DIR value and that the volume is really mounted at that path." >&2
        exit 1
    fi

    # Hands a root-owned volume to uid 1000. A failed chown (read-only mount, a filesystem without
    # ownership, userns-remap) is fatal only if uid 1000 cannot write there anyway; chown's stderr
    # stays visible because it names the reason.
    if ! chown -R app:app "$DATA_DIR"; then
        echo "WARN: could not chown $DATA_DIR; checking whether uid 1000 can write to it anyway" >&2
        gosu app test -w "$DATA_DIR" || {
            echo "FATAL: $DATA_DIR is not writable by uid 1000 and could not be chowned." >&2
            echo "Fix ownership on the host: chown -R 1000:1000 <volume>/_data" >&2
            exit 1
        }
    fi
    exec gosu app "$@"
else
    # A compose `user:` override is in effect — respect it, but fail fast if the
    # volume is not writable by that uid instead of failing later at runtime.
    # An unprivileged mkdir may fail; the -w test below reports that case.
    mkdir -p "$DATA_DIR" 2>/dev/null || true
    if [ ! -w "$DATA_DIR" ]; then
        echo "FATAL: $DATA_DIR is not writable by uid $(id -u)." >&2
        echo "Fix ownership on the host: chown -R $(id -u) <volume>/_data" >&2
        exit 1
    fi
    exec "$@"
fi
