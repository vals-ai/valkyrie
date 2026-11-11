FROM python:3.12-slim AS build-base

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13.5@sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff /uv /bin/

COPY pyproject.toml uv.lock alembic.ini ./

COPY src ./src
COPY main.py ./

FROM build-base AS executor-build
RUN uv sync --locked --no-dev --no-group database --compile-bytecode \
    && .venv/bin/python -m compileall -q src

FROM build-base AS server-build
RUN uv sync --locked --no-dev --compile-bytecode \
    && .venv/bin/python -m compileall -q src

FROM python:3.12-slim AS runtime
# Pick up distro ca-certificates updates (e.g. the eMudhra intermediate CA distrust) before base images refresh.
RUN apt-get update && apt-get install -y --no-install-recommends --only-upgrade ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The Tracker health check uses curl.
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["/entrypoint.sh"]

FROM runtime AS executor
COPY --from=executor-build /app /app

FROM runtime AS server
COPY --from=server-build /app /app
EXPOSE 8000
