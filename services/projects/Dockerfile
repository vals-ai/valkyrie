FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.13.5@sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff /uv /bin/

COPY pyproject.toml uv.lock ./
COPY alembic.ini ./
COPY projects ./projects
RUN uv sync --locked --no-dev --compile-bytecode

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["uvicorn", "projects.main:app", "--host", "0.0.0.0", "--port", "8000"]
