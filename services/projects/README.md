# Project service

The project service will own each project, its limit, its key secret and its total cost. Today it is the only store of Tracker's project rows.

- `GET /health` returns `{"status": "ok"}` when the service can query its Postgres database at `DATABASE_URL`, and 503 otherwise.
- `GET /orgs/{org_id}/projects` lists an org's projects in Tracker's `GET /projects` body, and the per-project routes create, read, save, archive, unarchive and delete one project and set its Gateway IDs. Every project route needs `Authorization: Bearer $PROJECT_SERVICE_TOKEN`.
- `PUT /orgs/{org_id}/projects/{project_id}/limit` with `{"limit_dollars": "80", "reason": "full rerun"}` saves a project's limit and its optional reason in `project_limit`, and each save raises the row's `version` by one.
- `GET` on the same path returns the limit, null when none is set, with `applied` true once the Gateway has confirmed it.
- `POST /orgs/{org_id}/projects/{project_id}/limit-requests` with `{"limit_dollars": "120", "reason": "full rerun"}` files a pending raise in `project_limit_request`, and `GET` on the same path lists a project's raises, newest first.
- `GET /limit-requests?status=pending` lists every org's raises on live projects, newest first, each with its `org_id`; Tracker serves it to platform admins.
- `POST .../limit-requests/{request_id}/approve` saves the raise's limit as `PUT .../limit` does, and `.../deny` closes it without one. Tracker checks who may decide; this service trusts its token.
- The service then pushes each limit the Gateway has not confirmed (`gateway_confirmed_limit`) to the Gateway's `PUT /admin/managed-auth/machine/projects/{id}/budget` with its `version` and reason, at once and then every 30 seconds until the Gateway accepts it. It uses `MODEL_GATEWAY_URL`, `MODEL_GATEWAY_MACHINE_API_KEY` and `VALKYRIE_STAGE`.
- Archive and unarchive also call the Gateway's `POST /admin/managed-auth/machine/projects/{id}/archive` or `/unarchive` when the project has a `gateway_key_id`, and return 503 when the Gateway fails.
- A retry sends the same version, and the Gateway ignores a version older than the one it applied, so a late push cannot overwrite a newer limit.
- `GET /orgs/{org_id}/projects/{project_id}/cost?environment=` totals a project's all-time dollars: Tracker's counted dollars on its runs (`TRACKER_URL`, `PROJECT_SERVICE_TOKEN`) plus the Gateway's model dollars (`MODEL_GATEWAY_URL`, `MODEL_GATEWAY_MACHINE_API_KEY`).
- Key spend counts when its run is not linked to any project, and the body carries `tracker_by_type_usd`, `run_model_usd` and `key_model_usd` beside `total_usd`. Raw provider-key spend is not counted yet.
- Alembic owns the schema: `alembic upgrade head` creates or updates it before the service starts.
- Run it with its own database: `docker compose up --build`, then open http://localhost:8010/health.
- Test it: `uv run pytest`.
