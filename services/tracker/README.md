# Tracker service

The public FastAPI service called by the CLI and SDK keeps run and task state in PostgreSQL, schedules tasks onto sandboxes, and stores benchmark artifacts.

- A separately deployed Control API owns database access for executors on the same PostgreSQL database.
- Its task-read response includes the persisted starter email for grading attribution.
- Each dispatch starts one business executor under a minimal independent deadline watchdog.
- The executor entrypoint owns SIGTERM coordination and joins dispatch-owned work before closing its event loop; the watchdog remains the hard-kill fallback.
- The executor uses authenticated typed business HTTP operations and its invocation key; neither process connects to PostgreSQL.

## Schema changes

Executors never connect to PostgreSQL, so migrations deploy on the ordinary push path without stopping runs.

- A migration that declares `requires_maintenance = True` makes the push maintenance-required. CI waits for approval, stops runs, migrates, deploys, then reopens admission.

- First adoption of DB-independent executors stopped all runs and the old SQL-dependent executor cohort once.
- Drop a table or column only after the code that reads it has shipped.

Ordinary deployments retain the existing public HTTPS entrypoint with a private backend.
The release-test driver retains its existing private HTTP connection.

## Run it locally

From `services/tracker`:

```bash
make install

# Build and start PostgreSQL, the explicit migration, control, and Tracker API with Docker Compose,
# then tail the logs
make tracker-service
```

- The public API serves at `http://localhost:8000`.
  - Point the CLI at it with `export VALKYRIE_URL=http://localhost:8000`.
  - The separate control service listens on `127.0.0.1:8002` for executors, not CLI traffic.
- Docker Compose reads AWS credentials and `EXECUTOR_BOOTSTRAP_KEY` from your shell.
  - Its local-only payload key is shared by the API and control containers; do not use this fixed key in a deployed stage.
  - Runs still need a benchmark service and AWS access.
  - To run benchmarks without AWS, use `make local-tracker`; see [Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker).
- Rebuild the image to run changed code. Every dispatch, including a retry or resume, runs
  the code in the current image.

```bash
# Build the image
make build

# Start PostgreSQL, the explicit migration task, control, and Tracker API
make run

# Tail the Tracker logs
make logs

# Stop the stack
make stop

# Stop the stack and remove its images
make clean
```

`MODEL_GATEWAY_PROJECT_KEYS_MODE` controls project Gateway keys for `vals.ai`
projects using managed secrets.

- Hosted DEV, BENCH, and PROD deploy with `automatic` mode. Their protected GitHub
  Environments each supply `MODEL_GATEWAY_URL` and an account-local
  `MODEL_GATEWAY_MACHINE_API_KEY_SECRET_NAME`.
- The three stages share the connected DEV Gateway URL, but each stage loads its
  registered machine bearer from a secret in its own AWS account.
- Release-test and file-backed local Tracker use supplied keys.
- For a standalone Tracker with no mode set, the process defaults to `manual`.

| Mode | New `vals.ai` projects | New runs selecting a `vals.ai` project |
| --- | --- | --- |
| `manual` | No Gateway request | Use supplied keys |
| `provision` | Request a Gateway key and save its request ID | Use supplied keys |
| `automatic` | Request a Gateway key and save its request ID | Collect an approved key and select its secret references; a missing or unapproved binding blocks the run, with no supplied-key fallback |

- For a standalone AWS-backed Tracker explicitly using `provision` or `automatic`, set
  `MODEL_GATEWAY_URL` and `MODEL_GATEWAY_MACHINE_API_KEY` in the Tracker process.
- Tracker reads them when requesting or collecting a key, not at startup.
- Hosted deployments instead inject the account-local machine bearer from the Secrets
  Manager secret named by their GitHub Environment.
- Runner tasks do not receive that bearer or the Gateway endpoint.
- The machine API key is not the executor's project key or the release-test gateway
  control token.

Backfill a Gateway key request for every existing eligible project in the
relevant hosted stage and obtain its approval before deploying that stage's
Tracker.

- Other organizations and projectless runs continue to use supplied keys.
- A file-only local Tracker also uses supplied keys, even for `vals.ai`.
- An AWS-backed local Tracker can explicitly select a non-manual mode only with a Gateway
  configured for its own project database and organization. A DEV Gateway URL alone does
  not establish that trust.
- On retry or resume, a run keeps its original saved `MODEL_GATEWAY_URL` and
  `MODEL_GATEWAY_API_KEY` references, or their absence.
- It doesn't accept overrides for those keys or select a new project's key after a retag.
  Other secret overrides still work.

## Tests

```bash
# Unit and local integration tests
make test

# Unit tests and Alembic migrations
make test-unit

# Alembic migration tests only
make test-alembic

# Local API and PostgreSQL tests
make test-integration-local

# AWS, benchmark service and sandbox tests
make test-integration-live
```

Tracker PR CI runs two required static partitions in parallel on 8-vCPU runners: unit tests and local integration (including all eight watchdog cases), each with six workers.

- The broad Tracker-related trigger remains in place.
- The `tracker-tests` aggregate combines both PR coverage artifacts and requires 85% coverage.
- Local `make test` still runs the entire unit and local integration suite.

The Docker-backed continuity tests build their own MinIO image from a checksum-pinned
official release package in `tests/integration/local/minio/Dockerfile`. They need Docker
and network access to fetch the Alpine base image and native x86-64 or ARM64 package;
no Go compilation or prebuilt MinIO registry image is required.

The Docker runtime check needs a Docker daemon. Use your Docker context's socket when it
differs:

```bash
DOCKER_HOST=unix:///var/run/docker.sock uv run pytest tests/integration/docker
```

`tests/integration/live` uses the AWS SDK default credential chain, including an AWS SSO
profile, and reads these settings from the shell or `services/tracker/.env`:

```env
# Region for Secrets Manager, S3 and CloudWatch
AWS_DEFAULT_REGION=us-east-1

# S3 bucket for agent artifacts
TEST_AWS_S3_BUCKET=

# CloudWatch log group
TEST_LOG_GROUP=

# Secrets Manager secret holding the Daytona provider settings
TEST_DAYTONA_SECRET_NAME=

# Domain of the benchmark service
BENCHMARK_SERVICE_BASE_URL=

# Cloud Map namespace used when no base URL is set
BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE=local

# Access key for the benchmark service
BENCHMARK_SERVICE_AUTH_KEY=
```

## Migrations

See [Database and migrations](src/tracker/database/README.md).
