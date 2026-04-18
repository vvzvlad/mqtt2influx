#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# flake8: noqa
# pylint: disable=broad-exception-raised, raise-missing-from, too-many-arguments, redefined-outer-name
# pylint: disable=multiple-statements, logging-fstring-interpolation, trailing-whitespace, line-too-long
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring
# pylint: disable=f-string-without-interpolation, global-statement

import argparse
import os

import uvicorn

def main():
    parser = argparse.ArgumentParser(description="Label Maker Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind the server to")

    default_port = int(os.getenv("PORT", "8000"))
    parser.add_argument("--port", type=int, default=default_port,
                        help=f"Port to bind the server to (default: {default_port}, can be set via PORT env var)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")

    args = parser.parse_args()
    uvicorn.run("src.api:app", host=args.host, port=args.port, reload=args.reload)

if __name__ == "__main__":
    main()
