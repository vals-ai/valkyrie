FROM python:3.12-slim AS build-base

RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13.5@sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff /uv /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Install from the repository layout so uv.lock's ../../packages path resolves.
WORKDIR /build/services/tracker
COPY packages/create-benchmark-service /build/packages/create-benchmark-service
COPY services/tracker/pyproject.toml services/tracker/uv.lock ./

FROM build-base AS executor-deps
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --no-install-project --reinstall-package create-benchmark-service --no-group database

FROM build-base AS server-deps
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --no-install-project --reinstall-package create-benchmark-service

FROM python:3.12-slim AS source
WORKDIR /app
COPY services/tracker/alembic.ini services/tracker/main.py ./
COPY services/tracker/src ./src
RUN python -m compileall -q /app/src

FROM python:3.12-slim AS runtime
# Pick up distro ca-certificates updates (e.g. the eMudhra intermediate CA distrust) before base images refresh.
RUN apt-get update && apt-get install -y --no-install-recommends --only-upgrade ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The Tracker health check uses curl.
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY services/tracker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app/src
ENTRYPOINT ["/entrypoint.sh"]

FROM runtime AS executor
COPY --from=executor-deps /app/.venv /app/.venv
COPY --from=source /app /app

FROM runtime AS server
COPY --from=server-deps /app/.venv /app/.venv
COPY --from=source /app /app
