.PHONY: help install style format format-check lint lint-fix typecheck \
	tracker-service local-tracker venv_check tool-install build test unit-test sdk-models sdk-models-check docs-reference docs-reference-check

PYTHON_VERSION := 3.12

TRACKER_PORT ?= 8000
help:
	@echo "Makefile for Valkyrie"
	@echo ""
	@echo "Setup:"
	@echo "  make install             Install cli dependencies"
	@echo "  make tool-install        Install valkyrie as a global executable"
	@echo ""
	@echo "Development:"
	@echo "  make test                Run CLI, TUI, SDK, contract, docs, run-monitor and local integration tests with coverage"
	@echo "  make style               Format, apply ruff fixes, then lint"
	@echo "  make lint                Lint without fixing (CI)"
	@echo "  make typecheck           Typecheck"
	@echo "  make docs-reference      Generate CLI and SDK reference pages"
	@echo "  make docs-reference-check  Check generated reference pages"
	@echo ""
	@echo "Build:"
	@echo "  make build               Build valkyrie CLI binary to dist/"
	@echo ""
	@echo "Services:"
	@echo "  make tracker-service     Start tracker service docker container"
	@echo "  make local-tracker [CONFIG=<path>]  Run Tracker on this machine without AWS"

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi

install:
	uv venv --python $(PYTHON_VERSION)
	@echo "Installing cli dependencies..."
	uv cache clean model-library valsai
	uv sync --dev
	@echo "🎉 Done! Run 'source .venv/bin/activate' to activate the environment locally."

tool-install:
	uv tool install --force --exclude-newer "7 days" --exclude-newer-package certifi=false -e .

build: venv_check
	@echo "Building valkyrie CLI binary..."
	@uv run pyinstaller \
		--onefile \
		--name valkyrie \
		--distpath dist \
		--workpath build \
		--specpath build \
		--clean \
		--collect-submodules valkyrie.cli \
		--collect-data valkyrie.tui \
		scripts/binary/valkyrie_cli.py
	@echo "✓ Binary built at dist/valkyrie"

format: venv_check
	@uv run ruff format --exclude services/run-monitor/vendor .

format-check: venv_check
	@uv run ruff format --check --exclude services/run-monitor/vendor .

lint: venv_check
	@uv run ruff check --exclude services/run-monitor/vendor .
	@uv run python scripts/check_no_comments.py
	@uv run python scripts/check_descriptions.py
	@uv run python scripts/check_doc_paragraphs.py
	@uv run python scripts/check_tracker_no_utils.py

test: venv_check
	@uv run pytest tests/unit/cli tests/unit/tui tests/unit/sdk tests/unit/docs tests/contract \
		$(wildcard tests/unit/egress_proxy) tests/integration/local \
		--cov=src/valkyrie --cov-report=xml --cov-report=term-missing --cov-fail-under=80
	@cd services/run-monitor && uv run --frozen pytest -q
	@cd services/projects && uv run --frozen pytest -q

unit-test: test

sdk-models:
	uv run python -m scripts.sdk.generate_models

sdk-models-check:
	uv run python -m scripts.sdk.generate_models --check

docs-reference:
	uv run python scripts/generate_reference.py

docs-reference-check:
	uv run python scripts/generate_reference.py --check

lint-fix: venv_check
	@uv run ruff check --fix --exclude services/run-monitor/vendor .

style: format lint-fix lint

typecheck: venv_check
	@uv run basedpyright
	@cd services/run-monitor && uv run --frozen basedpyright
	@cd services/projects && uv run --frozen basedpyright

tracker-service:
	cd services/tracker && make tracker-service

local-tracker:
	cd services/tracker && make local-tracker $(if $(CONFIG),CONFIG=$(abspath $(CONFIG)))
