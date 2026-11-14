# Project service

The project service will own each project, its limit, its key secret and its total cost. Today it is the only store of Tracker's project rows.

- `GET /health` returns `{"status": "ok"}` when the service can query its Postgres database at `DATABASE_URL`, and 503 otherwise.
- `GET /orgs/{org_id}/projects` lists an org's projects in Tracker's `GET /projects` body, and the per-project routes create, read, save, archive, unarchive and delete one project and set its Gateway IDs. Every project route needs `Authorization: Bearer $PROJECT_SERVICE_TOKEN`.
- Alembic owns the schema: `alembic upgrade head` creates or updates it before the service starts.
- Run it with its own database: `docker compose up --build`, then open http://localhost:8010/health.
- Test it: `uv run pytest`.
