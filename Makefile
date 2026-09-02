# Makefile — single entry point for every repeated action in this project.
# Run `make` (or `make help`) to list the available targets.

# --- Configuration -----------------------------------------------------------
VENV   ?= .venv
PY     := $(VENV)/bin/python
PIP    := $(VENV)/bin/python -m pip
PYTEST := $(VENV)/bin/python -m pytest

.DEFAULT_GOAL := help

# --- Help --------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# --- Environment -------------------------------------------------------------
# The project ALWAYS runs inside a local .venv, created automatically on first
# use and reused after — the system Python is never used directly.
# Note that pip is invoked as `python -m pip`, not as `.venv/bin/pip`: a venv
# that was copied from another directory keeps the ORIGINAL absolute path in the
# shebang of its console scripts, so `.venv/bin/pip` can quietly install into a
# different project's environment. `python -m pip` always follows the
# interpreter it is run with.
.PHONY: venv
venv: $(VENV)/bin/python ## Create the local virtualenv (.venv) if missing

$(VENV)/bin/python:
	python3 -m venv $(VENV)

# Sentinel: dependencies are (re)installed only when a requirements file changes.
$(VENV)/.deps-installed: requirements-dev.txt requirements.txt | $(VENV)/bin/python
	$(PIP) install -r requirements-dev.txt
	touch $@

.PHONY: install
install: $(VENV)/.deps-installed ## Create .venv (if missing) and install dev/test deps

# Guarded by `test -f` rather than written unconditionally: .env is where a developer's own
# DATA_DIR and batching values live, and an unguarded `cp` would silently overwrite them every time
# this target ran as a dependency of something else.
#
# The file IS read automatically by the application: src/settings.py names `.env` in its
# SettingsConfigDict and pydantic-settings brings python-dotenv in to read it, so `make run` picks
# it up with nothing else to do. A real environment variable still beats it. TZ is the exception —
# nothing in src/ reads it, so it only takes effect if the shell exports it. The test suite
# deliberately ignores the file entirely; see the `no_dotenv` fixture in tests/conftest.py.
.PHONY: env
env: ## Create .env from .env.example if it does not exist yet
	@test -f .env || cp .env.example .env

# --- Develop -----------------------------------------------------------------
.PHONY: test
test: install ## Run the test suite (auto-creates .venv if missing)
	$(PYTEST)

.PHONY: run
run: install ## Run the application (auto-creates .venv if missing)
	$(PY) main.py

# --- Housekeeping ------------------------------------------------------------
.PHONY: clean
clean: ## Remove the venv and Python caches
	rm -rf $(VENV) .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
