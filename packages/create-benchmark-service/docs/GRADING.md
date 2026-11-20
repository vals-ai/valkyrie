# Grading

This page picks up after the agent finishes: which judge models a task's grading needs, which Gateway credentials those judge calls run under, and how a failed grade is retried without rerunning the agent. The hooks it names are on `BenchmarkService` ([Service API](SERVICE_API.md)).

## Grading requirements

- Before minting a project-keyed grader token, a caller asks `BenchmarkServiceClient.grading_requirements(task_id, dataset=...)` for the complete list of Gateway model keys that task's grading and finalizers need.
- The service hook `grading_requirements(task_id, dataset=None)` defaults to the comma-separated `GRADING_MODELS`. Override it when the judges depend on the task.
- Leave out direct-provider (bring-your-own-key) models, since they aren't Gateway keys.

## Grading credentials

Judge calls run under a Gateway credential bound for the whole request (finalizers included), which graders read with `current_grading_credentials()`. Which credential, if any, depends on whether the caller sent a token and how the service is configured.

### A token from the caller

- `BenchmarkServiceClient` sends `X-Vals-Run-ID`, `X-Vals-Task-ID`, and `X-Vals-Attempt-ID` from `correlation_scope(...)`.
- A caller can pass a `grading_token` on `/ws/evaluate-instance` or a resumed `/ws/evaluate-response`. The framework binds it to model-library's `gateway_token_scope` too, and never mints or revokes a service-key token.
- `POST /evaluate-response/` rejects an explicit token with 400.

### A token the service mints

- If the request has a run and attempt ID, and `MODEL_GATEWAY_URL`, `MODEL_GATEWAY_API_KEY`, and `GRADING_MODELS` are all set, the service mints a token with its own key and revokes it afterwards.
- That token's claims name the run, task, attempt, and the service's `IDENTITY` with `agent_name="grader"` (plus the tenant, when the caller authenticated).
- model-library sends it on judge queries, and grading sandboxes receive it as `MODEL_GATEWAY_API_KEY`. Remote graders can forward `current_grading_credentials().as_environment()`.
- Without `GRADING_MODELS` or the Gateway settings, nothing is bound. With both set but no run ID, the service's own credentials are bound.
- A run without an attempt ID fails, rather than charging the service key.
- `current_evaluation_caller()` exposes the run, task, attempt, and `X-Vals-Email` even without `GRADING_MODELS`, for graders that mint tokens somewhere else.

The binding lives in a `ContextVar`. A grader that hands work to threads should use `asyncio.to_thread` or `contextvars.copy_context().run`, not a bare `run_in_executor`.

## Resume grading from a checkpoint

A service can save a checkpoint as it grades, so an eval-only retry picks up from there instead of recreating the agent's sandbox. The service decides what the checkpoint means, and the Tracker only stores it.

1. The service yields a `StreamEvalResumeStateChunk` before failure-prone grading work.
2. The Tracker stores the latest `eval_resume_state` on the task row.
3. If grading fails after that, the retry calls `/ws/evaluate-response` with the saved state (and, when needed, the request's sandbox provider config).
4. The service streams a new result, plus any newer checkpoint.

The retry request looks like this:

```json
{"task_id": "…", "eval_resume_state": {...}, "sandbox_provider": {...}, "dataset": "…", "grading_token": "…"}
```

- `sandbox_provider` is optional for benchmarks that resume without a sandbox.
- Keep the provider config and the grading token out of the saved `eval_resume_state`.

### Resume in the caller's sandbox

- Set `RetrieveTaskResponse.resume_sandbox` to an `EvalSandboxSpec` when resumed grading needs a sandbox the caller owns. Its unset source and resource fields fall back to the task's values.
- The caller provisions that sandbox under its own capacity controls, passes `instance_id` to `BenchmarkServiceClient.resume_evaluation`, and cleans it up afterwards.
- The service borrows the sandbox, and must not create a replacement or delete it. A service that needs this mode must reject a resume with no instance ID.
- `instance_id` is accepted only alongside `eval_resume_state`, on both the HTTP and WebSocket evaluate-response routes. Keep it out of the saved checkpoint, since a later retry can reuse that checkpoint in a different sandbox.

## Make grading retryable from saved input

If your grader reads the agent's output (a result file, a patch, an archive), implement these three hooks instead of `evaluate_instance`:

- `collect_grading_input(task_id, sandbox, dataset) -> bytes`: what to save from the finished sandbox.
- `grader_identity(task_id, dataset)`: what decides the grade, such as the judge model and rubric revision, or the test revision and grader image. A retry refuses to grade if this changed.
- `grade_input(task_id, content, dataset)`: how to grade those bytes.

What the framework does with them:

- It saves the bytes to S3 before grading, and a retry grades the saved copy instead of rerunning the agent.
- Storage comes from `EVAL_RESUME_BUCKET` and `EVAL_RESUME_PREFIX`, which the deployment sets when a service opts in.
- Implement `collect_grading_input`, `grader_identity`, and one grader, or none of them. The details are in `benchmark_service/eval_resume.py`.

### Variations

- **It grades inside a sandbox:** implement `grade_in_sandbox` instead of `grade_input`. A retry grades in a fresh sandbox, or in the caller's sandbox when the request names one.
  - The fresh sandbox starts from the task's image, so the bytes `collect_grading_input` saves must hold the whole graded workspace.
  - It grades under the task's `egress.evaluation`, the same policy the first attempt used. A task with no `egress.evaluation` grades its retry with the network blocked.
- **It needs its own image or network:** set the task's `eval_sandbox`. Each attempt then grades in a fresh sandbox built from it, under its `network_block_all`.
  - The exception is a retry that names the caller's sandbox (`instance_id`), which borrows that sandbox instead.
  - Its `timeout_s` sizes the sandbox's auto-stop. It doesn't bound grading.
- **It saves progress part-way** (after each judge call, say): yield `GradingInput`.
- **Some output can't be graded:** raise `NotGradeable(message)` from `collect_grading_input`. The task ends with that error, and nothing is saved.
- **There's no bounded input to save:** keep your own `evaluate_instance`.
