#!/bin/sh
# Postgres-style hybrid entrypoint: start as root, fix state-dir ownership,
# then drop privileges to the unprivileged `app` user (uid 1000).
set -e

if [ "$(id -u)" = "0" ]; then
    # Heals volumes left by older root-based images (self-healing migration).
    #
    # Guarded rather than left to `set -e`, and the guard is the whole point: a /data that cannot be
    # chowned is not the same thing as a /data that cannot be used. A read-only mount, a filesystem
    # that carries no ownership at all, a daemon running with userns-remap — all three fail this
    # chown and all three worked before this image started dropping privileges. Bare, `set -e` would
    # exit before `exec` on every one of them, and `restart: always` in docker-compose.yml turns
    # that into an unbounded restart loop whose only symptom is a container that never stays up.
    # So a failed chown asks the question instead of answering it, and only `no` is fatal.
    #
    # chown's own stderr is deliberately NOT discarded: its message names the reason ("Read-only
    # file system", "Operation not permitted") and that line is the whole diagnosis for whoever
    # reads the log. /data holds one file, so there is no flood to suppress.
    if ! chown -R app:app /data; then
        echo "WARN: could not chown /data; checking whether uid 1000 can write to it anyway" >&2
        gosu app test -w /data || {
            echo "FATAL: /data is not writable by uid 1000 and could not be chowned." >&2
            echo "Fix ownership on the host: chown -R 1000:1000 <volume>/_data" >&2
            exit 1
        }
    fi
    exec gosu app "$@"
else
    # A compose `user:` override is in effect — respect it, but fail fast if the
    # volume is not writable by that uid instead of failing later at runtime.
    if [ ! -w /data ]; then
        echo "FATAL: /data is not writable by uid $(id -u)." >&2
        echo "Fix ownership on the host: chown -R $(id -u) <volume>/_data" >&2
        exit 1
    fi
    exec "$@"
fi
