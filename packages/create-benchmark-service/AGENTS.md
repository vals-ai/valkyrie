# Agent notes for create-benchmark-service

This is the public benchmark service framework (`benchmark_service`) and its scaffold CLI. The [README](README.md) has the overview and the docs table, [DEVELOPMENT.md](docs/DEVELOPMENT.md) has the directory map, checks, and versioning, and the root [AGENTS.md](../../AGENTS.md) has the rules every file here follows.

## Before you open a PR

- Bump `version` in `pyproject.toml` when a change touches the [public surface](docs/DEVELOPMENT.md#versioning).
- Use `uv` (not pip) on Python 3.12 or newer, and run the [checks](docs/DEVELOPMENT.md#checks) before asking for review.
- The public sync publishes this directory. Keep Vals AI-only code and notes out of it (the root `AGENTS.md` says where they go).
- When a fact in these docs changes, edit the page that owns it, and link to that page from anywhere else that needs it.
