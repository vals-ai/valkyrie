.PHONY: help build run stop clean logs tracker-service local-tracker \
        test test-integration-local test-integration-live test-unit test-alembic \
        migrate-gen venv-check

PYTHON_VERSION := 3.12
PYTEST := uv run pytest
COMPOSE := docker compose
EXECUTOR_IMAGE ?= valkyrie-executor:local
CONFIG ?= $(abspath ../../local-server.yaml)
LOCAL_DATABASE_URL ?= postgresql://tracker:tracker@localhost:5432/tracker

help:
	@echo "Available commands:"
	@echo "  make install         - Install local development environment and dependencies"
	@echo "  make build           - Build Docker images"
	@echo "  make run             - Start PostgreSQL, migrate, control, and Tracker with docker-compose"
	@echo "  make stop            - Stop all services"
	@echo "  make clean           - Stop services and remove images"
	@echo "  make logs            - View container logs"
	@echo "  make tracker-service - Build, run, and view logs for the tracker service"
	@echo "  make local-tracker [CONFIG=<path>] - Migrate and run host control/Tracker (requires EXECUTOR_BOOTSTRAP_KEY)"
	@echo "  make migrate-gen     - Generate a new migration from model changes"

# --- Setup ---
venv-check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "❌ Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi
install:
	uv venv --python $(PYTHON_VERSION)
	@echo "Installing tracker service dependencies..."
	uv cache clean agentic-harness
	uv sync --dev

# --- Docker ---
build:
	@echo "Building Docker images..."
	@$(COMPOSE) build
.PHONY: executor-image
executor-image:
	docker build --target executor --platform linux/arm64 -t "$(EXECUTOR_IMAGE)" .
run:
	@echo "Starting services with docker-compose..."
	@$(COMPOSE) up -d
	@echo "Tracker service is running on http://localhost:8000"
	@echo "Health check: http://localhost:8000/health"
stop:
	@echo "Stopping services..."
	@$(COMPOSE) down
clean:
	@echo "Stopping services and removing images..."
	@$(COMPOSE) down --rmi local
logs:
	@$(COMPOSE) logs -f tracker
tracker-service: build run logs
local-tracker: venv-check
	@: "$${EXECUTOR_BOOTSTRAP_KEY:?Set EXECUTOR_BOOTSTRAP_KEY for local control}"
	@$(COMPOSE) up -d --wait postgres
	@DATABASE_URL="$(LOCAL_DATABASE_URL)" uv run alembic upgrade head
	@set -e; \
	  if [ -z "$${EXECUTOR_PAYLOAD_LOCAL_KEY+x}" ]; then \
	    EXECUTOR_PAYLOAD_LOCAL_KEY="$$(uv run python -c 'import base64, os; print(base64.b64encode(os.urandom(32)).decode())')"; \
	  fi; \
	  export EXECUTOR_PAYLOAD_LOCAL_KEY EXECUTOR_LAUNCHER=local; \
	  unset EXECUTOR_PAYLOAD_KMS_KEY_ID; \
	  DATABASE_URL="$(LOCAL_DATABASE_URL)" uv run uvicorn tracker.executor.control_app:app --host 127.0.0.1 --port 8002 & \
	  control_pid=$$!; \
	  trap 'kill "$$control_pid"; wait "$$control_pid"' EXIT; \
	  DATABASE_URL="$(LOCAL_DATABASE_URL)" EXECUTOR_CONTROL_URL=http://127.0.0.1:8002 uv run python -m tracker.serve --config "$(CONFIG)"

# --- Tests ---
test: venv-check
	$(PYTEST) tests/unit tests/integration/local --test-alembic -n 4 --cov=src/tracker --cov=main --cov-report=term-missing --cov-report=xml --cov-fail-under=85
test-unit: venv-check
	$(PYTEST) tests/unit --test-alembic
test-alembic: venv-check
	$(PYTEST) --test-alembic -m alembic
test-integration-local: venv-check
	$(PYTEST) tests/integration/local/api -n auto
	$(PYTEST) tests/integration/local/database
test-integration-live: venv-check
	$(PYTEST) tests/integration/live -n 2

# --- Migrations ---
migrate-gen: venv-check
	@read -p "Migration message: " msg; \
	uv run alembic revision --autogenerate -m "$$msg"
