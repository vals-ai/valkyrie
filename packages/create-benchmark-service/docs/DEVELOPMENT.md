# Development

This page is for people changing the framework itself: the checks to run, how it's versioned, and how a release reaches the services that pin it. Repository-wide setup and pull request rules are in the root [CONTRIBUTING.md](../../../CONTRIBUTING.md).

## Directory map

```
.
├── src/benchmark_service/           # The framework
│   ├── base.py                      # BenchmarkService base class
│   ├── app.py                       # BenchmarkServiceApp, the FastAPI app
│   ├── client.py                    # BenchmarkServiceClient
│   ├── schemas.py, v1_schemas.py    # Wire types
│   ├── sandbox/                     # Daytona, Daytona v2, Modal, Compose, and local Docker
│   ├── eval_resume.py               # Retryable grading
│   ├── valkyrie_stage.py            # Stage reporter for credited generation
│   └── draining.py                  # Request drain gate
├── src/create_benchmark_service/    # The scaffold CLI
│   ├── cli.py
│   ├── generator.py
│   └── templates/                   # Files a new service starts from
├── docs/
└── tests/
```

## Checks

Run these from `packages/create-benchmark-service`, and get them passing before asking for review:

```bash
make install
make lint
make typecheck
make test
```

- `make test` skips `tests/integration`. `make test-provider-integration` runs those provider tests, and [Sandboxes](SANDBOXES.md#local-docker-sandboxes) shows the local Docker run.
- Add tests for new behavior.

## Versioning

The framework uses semantic versioning. The version is the static `version` in `pyproject.toml`, exposed as `benchmark_service.__version__` and on every service's `GET /version`.

Bump it in the same pull request as the change, by what the change does to the public surface below:

| Bump | When |
| --- | --- |
| Patch | A bug fix that keeps the public surface |
| Minor | An additive change, such as a new route, a new schema field with a default, or a new optional CLI flag |
| Major | Removing or renaming anything, or another breaking signature or wire-format change |

While the major version is `0`, breaking changes take a minor bump instead, so treat any minor bump as possibly breaking. From `1.0.0` on, normal semver applies.

### Public surface

- `benchmark_service.BenchmarkService`: its abstract methods, signatures, and behavior.
- `benchmark_service.BenchmarkServiceApp`: its constructor and the routes it exposes.
- `benchmark_service.BenchmarkServiceClient` and `BenchmarkServiceError`.
- HTTP route shapes: paths, request and response schemas, and status codes.
- The WebSocket protocol (`StreamMessageChunk`, `StreamErrorChunk`, `StreamResultChunk`).
- The CLI invocation (`create-benchmark-service <name>`), and the public interface of generated projects (their CLI flags and on-disk layout).
- The Pydantic schemas in `schemas.py`.

Template contents aren't part of it, since changing them only affects newly generated services.

## Releases and pinning

- When the public sync publishes `prod`, it tags `cbs-v<version>` on the public repository if that tag doesn't exist yet. Bumping `version` is the whole release step.
- A generated service pins the version of the CLI that generated it, as `create-benchmark-service @ git+https://github.com/vals-ai/Valkyrie.git@cbs-v<version>#subdirectory=packages/create-benchmark-service`.
- To upgrade a service, change the tag in its `pyproject.toml` and run `uv lock`.
- The Tracker installs this directory as a path source instead of a tag, so a framework change and the Tracker change that uses it land in the same pull request.
- [vals-grading](../../vals-grading/README.md) is versioned and released on its own, so changes there need no bump here.
