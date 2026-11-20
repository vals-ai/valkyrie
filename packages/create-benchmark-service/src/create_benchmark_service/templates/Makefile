.PHONY: install dev run format lint test clean help

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --dev

dev: ## Start the development server
	uv run fastapi dev main.py --port 0

lint: ## Check style with ruff
	uv run ruff check .

format: ## Format code with ruff
	uv run ruff check --fix .

typecheck: ## Type check code with basedpyright
	uv run basedpyright .

test: ## Run tests
	uv run pytest

docker-build: ## Build Docker image
	docker build -t benchmark-service:latest .

docker-run: ## Run Docker container
	docker run -p 8001:8001 benchmark-service:latest
