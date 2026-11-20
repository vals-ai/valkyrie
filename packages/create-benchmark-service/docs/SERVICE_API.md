# Service API

This page lists what your `BenchmarkService` subclass implements, what `BenchmarkServiceApp` serves for it, and the wire types between them. Grading, sandboxes, credited generation, and dataset versions each have their own page, and this one links to them.

## `BenchmarkService`

`BenchmarkService` (in `base.py`) is the class your benchmark subclasses. Its `create()` factory calls `load_datasets()` and keeps the result as `self.datasets`.

### Methods you implement

| Method | What it does |
| --- | --- |
| `load_datasets()` | Load every task from your source. Return `dict[dataset_name, dict[task_id, task_object]]`. |
| `retrieve_task(task_id, skip_validation, dataset)` | Return the task's metadata: sandbox source, problem path, resources, and so on. |
| `setup_task(task_id, sandbox, dataset)` | Async generator that sets the task up in a sandbox, yielding `StreamChunk`s. |
| `evaluate_response(request, dataset)` | Grade a text response directly, with no sandbox. |
| `calculate_final_score(evaluation_results, dataset)` | Aggregate per-task results into one `FinalScoreResult`. |

`evaluate_instance(task_id, sandbox, dataset)` grades the agent's work in its live sandbox. Override it, or implement the grading hooks in [Grading](GRADING.md#make-grading-retryable-from-saved-input) and keep the default, which collects, saves, and grades the input for you.

### Methods with defaults

- `get_dataset(dataset)` returns the task dictionary for a dataset name, `"default"` when none is given.
- `list_datasets()` returns the available dataset names, the keys of `self.datasets` by default. Override it for datasets you discover at runtime, and note that the HTTP route still applies `check_dataset_access` to each name.
- `filter_tasks(task_filter, dataset)` returns the task IDs matching a list or Python slice notation (for example `"0:10:2"`).
- `validate_task_ids(task_ids, dataset)` raises `ValueError` if any ID isn't in the dataset.
- `list_tasks(dataset)` returns `list[V1Task]` (ID, question, and timeout). Override it before exposing task listing, because the base implementation fails closed rather than leak evaluator-only data.
- `check_auth(headers)`, `resolve_tenant(headers)`, and `check_dataset_access(tenant, dataset)` are the auth hooks. The [README](../README.md#authentication) shows when to use each.
- `resolve_tenant` returns a tenant ID, `"unauthenticated"` for boolean auth or local development, or `None` to reject.
- `get_service_version()` lets the benchmark report its own service version. When it returns `None`, `/version` falls back to the installed benchmark package version.
- `get_dataset_version(dataset)` returns the dataset's version label, and `supports_dataset_version_selection(dataset)` and `open_dataset_version(dataset, version)` pin one version per request. Both are covered in [Dataset versions](DATASET_VERSIONS.md).
- `grading_requirements(task_id, dataset)` lists the Gateway model keys a task's grading needs. See [Grading](GRADING.md#grading-requirements).
- `monitoring_requirements(task_ids, dataset)` returns the policy text, Watcher policy pack, and allowed CIDRs and hosts that an external run monitor applies to a run of those tasks. It returns an empty `MonitoringRequirementsResponse` by default.

### Dispatch order

- Task order within a dataset is the order the Tracker dispatches tasks in. Task rows are created in `filter_tasks` order, and `?slice=` slices that same order.
- Set the `priority_dataset` class attribute (for example `priority_dataset = "vals_index"`) to move the tasks a dataset shares with that subset to the front, keeping their order. A run of the full split then finishes the index subset before the long tail.
- `create()` fails if the named priority dataset isn't loaded.

### Task folder

To hand the agent a task folder in Harbor's layout (`instruction.md` and `task.toml` at the root, other files below), build a `TaskFolder` and write it from your own `setup_task`. The framework never writes one for you.

```python
from benchmark_service import TaskFolder, write_task_folder

async def setup_task(self, task_id, sandbox, dataset=None):
    folder = TaskFolder(
        instruction=query,
        metadata={"benchmark": "a2j", "task_id": task_id},
        agent_timeout_sec=10800,
        extra_files={"agent/conductor/conductor.json": conductor_json},
    )
    await write_task_folder(sandbox, folder)
    yield StreamResultChunk(type="result", data={"status": "ok"})
```

- `instruction` goes into `instruction.md` as is, and `metadata` becomes the `[metadata]` table. `agent_timeout_sec` sets `[agent].timeout_sec`, and leaving it out writes no `[agent]` table.
- The folder is `/task` unless you pass an absolute `task_dir`, and `extra_files` paths are relative to it.
- `TaskFolder` refuses an extra file path that is absolute, contains `..`, is `instruction.md` or `task.toml`, or sits below another file.
- `write_task_folder` raises `RuntimeError` when the sandbox can't create the directories.
- Set [`task_dir`](#tasks) on the task to the same path, so the caller can hand the folder to the agent.

## `BenchmarkServiceApp`

`BenchmarkServiceApp(service_cls)` (in `app.py`) wraps your subclass in a configured FastAPI app. Run the result with any ASGI server.

### HTTP routes

| Method | Path | What it does |
| --- | --- | --- |
| `GET` | `/health` | Returns `{"status": "ok"}` |
| `GET` | `/version` | Returns the framework, service, and dataset versions, plus the benchmark's `eval_mode` |
| `GET` | `/v1/datasets` | Returns the caller's accessible dataset names, such as `{"datasets": ["default", "validation"]}` |
| `POST` | `/resolve-dataset` | Resolves a dataset default or fixed release to an immutable version ID |
| `POST` | `/grading-requirements` | Takes `{"task_id": "…", "dataset": null}` and returns `{"models": ["provider/model"]}` for Gateway-backed grading |
| `POST` | `/monitoring-requirements` | Takes `{"task_ids": [...], "dataset": null}` and returns the run monitor's rules |
| `GET` | `/verify-task-ids` | Returns task IDs filtered by `?task_ids=…` or `?slice=start:stop:step`, with an optional `?dataset=…` |
| `GET` | `/retrieve-task/?task_id=…` | Returns one task's metadata, with an optional `?dataset=…` |
| `POST` | `/evaluate-response/` | Grades without a sandbox and returns one final response |
| `POST` | `/final-score/` | Aggregates results from `{"evaluation_results": {task_id: result, …}, "dataset": "…"}` |

### WebSocket routes

Each of these streams `StreamChunk` JSON objects.

| Path | What it does |
| --- | --- |
| `/ws/setup-task` | Sets a task up in a sandbox, streaming progress, errors, and a final result |
| `/ws/evaluate-response` | Grades without a sandbox, or resumes grading, and accepts an optional `grading_token` in the body |
| `/ws/evaluate-instance` | Grades a live sandbox, and accepts an optional `grading_token` in the body |

### Dataset discovery

- `GET /v1/datasets` returns sorted, unique names, with no task IDs, task content, or evaluation calls.
- It uses the service's normal auth and dataset access hooks. A tenant with no matching datasets gets an empty list.

```python
async with BenchmarkServiceClient(service_url, headers=auth_headers) as client:
    names = (await client.list_datasets()).datasets
```

## Streaming protocol

The WebSocket routes and your generator methods talk in these chunk types:

```python
StreamMessageChunk(type="message", data="log line")              # progress / log output
StreamErrorChunk(type="error", data="error text")                # non-fatal errors
StreamEvalResumeStateChunk(type="eval_resume_state", data={...}) # evaluation progress state
StreamResultChunk(type="result", data=<any>)                     # final result payload
```

- Yield these from your generators. The framework serializes them and forwards them to the WebSocket client.
- When a request sets `announce_sandboxes`, the evaluate streams also send `{"type": "sandbox_created", "data": {"provider": "daytona", "sandbox_id": "…"}}` each time the Daytona provider creates a sandbox for it.
- The client sets `announce_sandboxes` when you pass `on_sandbox_created` to `evaluate_instance` or `resume_evaluation`. Without a callback the stream is unchanged, so older clients never see the chunk.

## Schemas

These Pydantic models (in `schemas.py`) make up the requests and responses.

### Tasks

- **`RetrieveTaskResponse`**: `source`, `problem_path`, `cwd`, `agent_timeout`, `resources`, `agent_install_order`, and stage-specific `egress`.
  - Optional fields: `credited_generation` (a `CreditedGeneration(allowance_seconds, stage_protocol)`), `episode` (an `Episode(command, parallel_agents)`), persistent `volumes`, bounded `sandbox_recovery`, and the non-secret `eval_sandbox` and `resume_sandbox`.
  - [Credited generation](CREDITED_GENERATION.md) covers `credited_generation` and `episode`.
- **`agent_install_order`**: `"before_setup"` (the default) installs agent dependencies before benchmark setup, and `"after_setup"` lets setup prepare the environment first.
- **`mcp_servers`**, **`skills_dir`**, **`task_dir`**: what the task gives its agent, left out of the response when unset. The caller passes them on, so the service writes no agent-specific config.
  - `mcp_servers` is a list of `MCPServer(name, transport, url, command, args, headers)`, with the fields of `[[environment.mcp_servers]]` in a Harbor `task.toml`.
  - `skills_dir` is the sandbox path of the task's skills folder, and `task_dir` is the sandbox path of the [task folder](#task-folder) that setup writes.
- **`Resources`**: `vcpu`, `memory`, `disk`, and an optional `gpu` count (default 0) with `gpu_type`. `gpu_type` is one type or a list of acceptable types in order of preference, and requires `gpu >= 1`.
- **`VolumeMount`**: a named persistent volume with an absolute `mount_path`, and optional `read_only`, `create_if_missing`, and relative `subpath`. `{run_id}` in a subpath comes from the sandbox's `run-id` or `run_id` label, and fails when that label is missing.
- **`TaskFilter`**: a `task_ids` list or a `slice_str`. `parse_slice()` turns `"start:stop:step"` into a Python `slice`.

### Sandboxes

- **`SandboxSource`**: `ImageSource`, `SnapshotSource`, the Daytona-only `TargetedSnapshotSource` (top level only), `DockerfileSource`, or `ComposeSource` with an outer image or snapshot.
- **`EvalSandboxSpec`**: grading overrides, whose optional `source` is an image or snapshot (never `ComposeSource`).
- **`SandboxRecoveryPolicy`**: an explicit opt-in to recreate a lost generation sandbox with the same run identity and volumes. `max_sandbox_attempts` (2 to 20, inclusive) counts the first sandbox.
- **`SandboxProviderConfig`**: request-scoped provider config, picked by `type` (`daytona`, `daytona_v2`, `modal`, or `docker`). [Sandboxes](SANDBOXES.md#choose-a-provider-per-request) lists each type's fields.

### Egress

- **`BenchmarkEgressPlan`**: egress policies for `setup_task`, the agent `run`, and `evaluation`. Applying and composing the plan is the caller's job.
  - `"*"` is unrestricted, `[]` denies everything, and a non-empty list is an allowlist.
  - Setup and evaluation default to `"*"`, and `run=None` leaves the benchmark with no opinion on the run stage.
  - `run_through_proxy=True` asks the caller to send the run stage through its egress proxy, which enforces `run` per request. A caller without a proxy should refuse the task.
  - `sandbox_local_hosts` names hosts inside the sandbox (Compose service names, say) that the run stage reaches directly rather than through the proxy.
  - `run_inspect` lists the hosts the proxy decrypts and holds to GET and HEAD. Other hosts are spliced through untouched.
  - `children` names `ChildPolicy` profiles (`egress`, `inspect`, `models`) for sandboxes the service creates itself.
- **`ChildEgress`**: one child profile's proxy URL, per-attempt credential, proxy CIDRs, and CA certificate, plus a gateway URL and token when the profile names `models`.
  - Read it with `current_child_egress(profile)` during setup or evaluation. It raises when the caller sent none.
  - `tunnel_command` starts the packaged `egress_tunnel.py` in the sandbox, falling back across `proxy_hosts` when the caller sends more than one address. That way `environment(tunnel_port=...)` also works for clients that can't speak TLS to a proxy.

### Requests and results

- **`SetupTaskRequest`** and **`EvaluateInstanceRequest`**: `task_id`, `instance_id`, `dataset`, an optional `sandbox_provider` (falling back to [Daytona headers](SANDBOXES.md#daytona-header-fallback)), and optional `child_egress` by profile name. Only `EvaluateInstanceRequest` takes an optional WebSocket `grading_token`.
- **`EvaluateResponseRequest`**: `task_id`, `response` or `eval_resume_state`, `dataset`, an optional `sandbox_provider`, and an optional caller-owned `instance_id` for resume. A `grading_token` is accepted on WebSocket requests only.
- **`GradingSubmission`**: a typed `TextGradingSubmission` or `ArtifactGradingSubmission`, passed only to the sandbox-grading hook.
- **`GradingRequirementsRequest`** and **`GradingRequirementsResponse`**: a task ID and nullable dataset in, the complete list of Gateway model keys for that task out.
- **`MonitoringRequirementsResponse`**: `policy`, `watcher_policy_pack`, `allowed_cidrs`, and `allowed_hosts` for an external run monitor.
- **`FinalScoreResult`** and **`FinalScoreResponse`**: `score` (a float), `metadata`, and `tasks_evaluated`.

## Utilities

- `stream_command(sandbox, command, cwd, ignore_error=False)` (in `utils.py`) runs a shell command in a sandbox, yields its output as it arrives, and checks the exit code at the end. Use it in `setup_task` and `evaluate_instance` to forward output as `StreamMessageChunk`s.
- For process-scoped credentials, call `sandbox.command(..., env_vars={...})`. Providers pass the values through their native process environment, and Compose puts only the variable names on its command line, so values are never shell-interpolated.
