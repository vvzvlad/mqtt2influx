#!/bin/sh
# Postgres-style hybrid entrypoint: start as root, fix state-dir ownership,
# then drop privileges to the unprivileged `app` user (uid 1000).
set -e

# WHERE THE STATE LIVES IS DECIDED HERE, ONCE, AND EXPORTED — so this script and the python process
# cannot possibly disagree about it.
#
# THE DIRECTORY THIS SCRIPT REPAIRS IS THE ONE THE APP WILL ACTUALLY USE, not a path assumed here.
# It used to be the literal /data in five places, which was true only for as long as the app's own
# default was /data too. The moment the two could differ, a hard-coded path would have this script
# chown one directory and src/config.py write to another: the container comes up healthy, serves an
# empty stream list, and the first save lands in the writable layer where the next image update
# takes it away.
#
# WHY THERE IS A DETECTION HERE AND NOT JUST A DEFAULT. The image deliberately declares no
# `ENV DATA_DIR` (see the Dockerfile). That is what makes "this deployment did not choose a path"
# observable at all — and it is the only signal that the OLDER layout may be in play. Two layouts
# exist in this fleet:
#
#   * the current one — the volume is mounted at /app/data, or DATA_DIR is set explicitly to
#     wherever it really is. Nothing is detected in that case: an explicit value always wins.
#   * the older one — the stack binds its volume at /data and passes NO environment at all. Those
#     stacks carry `io.portainer.update.enable`, so they pick up a new :latest on their own with
#     nobody editing anything. Defaulting to /app/data there would start the container on an empty
#     directory: load_streams() returns [], /api/stats answers 200 with a valid empty body, the
#     healthcheck goes green, the updater records a success and rolls nothing back — while the
#     volume holding every stream with its MQTT and InfluxDB credentials sits unread at /data and
#     the bridge writes nothing at all. A stream re-created by hand afterwards would then land in a
#     directory the next update discards.
#
# WHAT THE TEST LOOKS AT IS A MOUNT, NOT A FILE. The thing that distinguishes the two layouts is
# where the volume is BOUND, so that is what is asked. `/data` is not in this image at all: the
# Dockerfile creates only `/app/data`, under WORKDIR, and the python:3.11-slim base carries no
# `/data` either — check with `docker run --rm --entrypoint sh <image> -c 'ls -1 /'` before changing
# this. This script's own `mkdir -p` runs further down and only on the path already resolved. So a
# `/data` DIRECTORY existing at THIS point can only mean docker created one for a mount.
#
# An earlier version asked for `$LEGACY_DATA_DIR/streams.json` instead, and was blind to exactly the
# case that matters most: a legacy-shaped stack whose volume holds no streams.json YET — a
# deployment just created, a volume restored empty, a file removed by hand. The detection did not
# fire, DATA_DIR became /app/data, and since there is no VOLUME line any more that path is the
# container's WRITABLE LAYER. The first stream saved through the UI would survive a `restart` and be
# destroyed by the next automatic image update; /data/streams.json would never appear, so the same
# hole would open again on every update cycle — with the container healthy and /api/stats answering
# 200 the whole time. The same silent failure the fallback exists to prevent.
#
# The other half of the test — nothing at $DEFAULT_DATA_DIR/streams.json — is what stops a
# deployment that really uses /app/data from being dragged back to /data. It also settles the ONE
# case the mount-based test widens into: a deployment that mounts BOTH paths, with /app/data still
# empty, now resolves to /data. That is accepted deliberately and loses nothing — both are real
# mounts, so either way the state lands on a volume somebody created — and the WARN below names the
# path that was taken.
#
# WITH A streams.json AT BOTH PATHS /app/data wins, and that is ANNOUNCED rather than done quietly.
# There is no way to tell which one somebody means, so picking one is a guess; a guess that discards
# the other in silence is how this whole class of bug arrives. The `else` branch says which file it
# read and which it is ignoring.
#
# The detection exists in ONE place. src/settings.py keeps the same plain /app/data default for a
# process started outside this image, but inside it this script has already exported a decided value
# by the time python reads anything, so the shell and the app cannot drift apart. Do not implement
# the detection a second time there.
LEGACY_DATA_DIR=/data
DEFAULT_DATA_DIR=/app/data
if [ -n "${DATA_DIR:-}" ]; then
    :
elif [ ! -e "$DEFAULT_DATA_DIR/streams.json" ] && [ -d "$LEGACY_DATA_DIR" ]; then
    DATA_DIR="$LEGACY_DATA_DIR"
    echo "WARN: DATA_DIR is not set, $DEFAULT_DATA_DIR/streams.json does not exist, and" >&2
    echo "WARN: $LEGACY_DATA_DIR exists — which this image never creates, so something is MOUNTED" >&2
    echo "WARN: there. This container is therefore starting on the LEGACY layout with" >&2
    echo "WARN: DATA_DIR=$LEGACY_DATA_DIR." >&2
    echo "WARN: That is a compatibility fallback, not a configuration. Set DATA_DIR explicitly in" >&2
    echo "WARN: the deployment (and keep the volume mount and that value in step, in one update) so" >&2
    echo "WARN: this container stops depending on a guess about where its configuration is." >&2
else
    DATA_DIR="$DEFAULT_DATA_DIR"
    # The ambiguous case, resolved in favour of $DEFAULT_DATA_DIR and said out loud. Silence here
    # would mean a deployment reading one volume while a second one — holding a full set of streams
    # with their MQTT and InfluxDB credentials — sits mounted and untouched, with nothing anywhere
    # to say so.
    if [ -e "$DEFAULT_DATA_DIR/streams.json" ] && [ -e "$LEGACY_DATA_DIR/streams.json" ]; then
        echo "WARN: DATA_DIR is not set and BOTH $DEFAULT_DATA_DIR/streams.json and" >&2
        echo "WARN: $LEGACY_DATA_DIR/streams.json exist. There is no way to tell which one this" >&2
        echo "WARN: deployment means, so DATA_DIR=$DEFAULT_DATA_DIR was TAKEN and" >&2
        echo "WARN: $LEGACY_DATA_DIR is IGNORED — nothing under it will be read or written by this" >&2
        echo "WARN: container, and every stream it holds is invisible to this service." >&2
        echo "WARN: If that is the wrong way round, set DATA_DIR=$LEGACY_DATA_DIR explicitly." >&2
        echo "WARN: Either way, set DATA_DIR so this stops being a guess." >&2
    fi
fi
# Exported, not merely assigned: the python process below inherits this environment, and the whole
# point of resolving the path here is that it does not have to work it out again.
export DATA_DIR

if [ "$(id -u)" = "0" ]; then
    # Created before it is chowned. Without this a DATA_DIR naming a path that is not there — a typo
    # in the stack, a volume that silently failed to mount — is a chown that fails, a `set -e` exit
    # before `exec`, and under `restart: always` an unbounded restart loop whose only symptom is a
    # container that never stays up. The old src/config.py did the same thing with
    # os.makedirs(..., exist_ok=True); doing it here, as root, also means the directory arrives with
    # the right ownership instead of root's. `mkdir -p` on a directory that already exists is a
    # no-op and succeeds, so this costs a read-only mount nothing.
    if ! mkdir -p "$DATA_DIR"; then
        echo "FATAL: $DATA_DIR does not exist and could not be created." >&2
        echo "Check the DATA_DIR value and that the volume is really mounted at that path." >&2
        exit 1
    fi

    # Heals volumes left by older root-based images (self-healing migration).
    #
    # Guarded rather than left to `set -e`, and the guard is the whole point: a state directory that
    # cannot be chowned is not the same thing as one that cannot be used. A read-only mount, a
    # filesystem that carries no ownership at all, a daemon running with userns-remap — all three
    # fail this chown and all three worked before this image started dropping privileges. Bare,
    # `set -e` would exit before `exec` on every one of them, and `restart: always` in
    # docker-compose.yml turns that into an unbounded restart loop whose only symptom is a container
    # that never stays up. So a failed chown asks the question instead of answering it, and only
    # `no` is fatal.
    #
    # chown's own stderr is deliberately NOT discarded: its message names the reason ("Read-only
    # file system", "Operation not permitted") and that line is the whole diagnosis for whoever
    # reads the log. The directory holds one file, so there is no flood to suppress.
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
    #
    # The mkdir is attempted and its failure ignored on purpose: an unprivileged process may well be
    # unable to create the directory, and when it cannot the `-w` test below is the check that
    # reports it — with a message about ownership, which is what this case actually is.
    mkdir -p "$DATA_DIR" 2>/dev/null || true
    if [ ! -w "$DATA_DIR" ]; then
        echo "FATAL: $DATA_DIR is not writable by uid $(id -u)." >&2
        echo "Fix ownership on the host: chown -R $(id -u) <volume>/_data" >&2
        exit 1
    fi
    exec "$@"
fi
