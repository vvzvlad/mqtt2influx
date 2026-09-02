#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# flake8: noqa
# pylint: disable=broad-exception-raised, raise-missing-from, too-many-arguments, redefined-outer-name
# pylint: disable=multiple-statements, logging-fstring-interpolation, trailing-whitespace, line-too-long
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring
# pylint: disable=f-string-without-interpolation, global-statement

import argparse

import uvicorn

from src.settings import settings

def main():
    # The description said "Label Maker Server" until now — a leftover from the project this file
    # was copied from, and the string `--help` printed to anyone asking what this service is.
    parser = argparse.ArgumentParser(
        description="mqtt2influx — MQTT to InfluxDB bridge with a web UI")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind the server to")

    # The DEFAULT of --port, not an override of it: argparse only reaches for `default` when the
    # flag is absent, so `python main.py --port 9000` still wins over PORT in the environment. That
    # order is the documented one (.env.example, README) — do not invert it by reading the setting
    # after args have been parsed.
    default_port = settings.port
    parser.add_argument("--port", type=int, default=default_port,
                        help=f"Port to bind the server to (default: {default_port}, can be set via PORT env var)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")

    args = parser.parse_args()
    uvicorn.run("src.api:app", host=args.host, port=args.port, reload=args.reload)

if __name__ == "__main__":
    main()
