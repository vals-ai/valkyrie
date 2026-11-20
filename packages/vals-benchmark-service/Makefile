.PHONY: install lint format typecheck test

PYTHON_VERSION := 3.12

install:
	uv venv --python $(PYTHON_VERSION)
	uv sync --dev

lint:
	uv run ruff check .

format:
	uv run ruff check --fix .

typecheck:
	uv run basedpyright .

test:
	uv run pytest
