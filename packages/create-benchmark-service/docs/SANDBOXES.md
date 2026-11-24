# Sandboxes

The caller (usually the Tracker) creates each task's sandbox and sends its `instance_id` on setup and evaluate-instance requests. The service connects with the provider config on that same request, so this page covers how that config is chosen and what each provider can and can't do.

## Choose a provider per request

`sandbox_provider` rides on each setup and evaluate-instance request and carries the provider credentials. One hosted service can therefore use a different provider for every request.

- The Tracker resolves it from AWS Secrets Manager (`sandbox_provider_secret_name`), and the secret's keys are the config fields.
- `{"type": "docker"}` selects [local Docker](#local-docker-sandboxes), which takes no credentials.

```json
{"type": "daytona", "DAYTONA_API_KEY": "...", "DAYTONA_API_URL": "...", "DAYTONA_TARGET": "...", "DAYTONA_ORGANIZATION_ID": "..."}
{"type": "modal", "MODAL_TOKEN_ID": "...", "MODAL_TOKEN_SECRET": "..."}
{"type": "daytona_v2", "DAYTONA_V2_API_KEY": "...", "DAYTONA_V2_API_URL": "https://api.daytona.com", "DAYTONA_V2_REGION": "...", "DAYTONA_V2_SPOT": false}
```

### Daytona header fallback

- When a request omits `sandbox_provider`, the framework reads Daytona settings from the `x-api-key`, `x-api-url`, and `x-target` headers, plus an optional `x-organization-id`.
- It also accepts the `daytona_api_key`, `daytona_api_url`, and `daytona_target` aliases.
- An organization ID turns on organization-scoped capacity admission. Without one, sandboxes are created directly.

### Daytona v2

`daytona_v2` is opt-in, and uses Daytona's v2 API (still alpha). It differs from `daytona` in a few ways that change what a task gets:

- It keeps fractional `vcpu`, where `daytona` rounds up because the v0 API takes whole CPUs.
- It converts units: `cpu` cores are `vcpu / 2` (1 core is 2 vCPUs), and `memory_mib` and `disk_mib` are GiB times 1024.
- It reserves `cpu` and caps it at `cores_max`, which is `cpu` rounded up to 0.5 cores (v2's smallest step). So `vcpu=0.25` reserves 0.125 cores and can burst to 1 vCPU.
- Memory is capped at the request (`memory_max_mib`). Without caps, v2 boots at the plan cap of 4 cores and 2 GiB.
- It supports `image`, `snapshot` (an existing v2 image name or ID), and self-contained `dockerfile` sources with no local `COPY` or `ADD` context.
- It rejects GPUs, volumes, and sandbox secrets.
- Egress can only be set at creation (`network_block_all`), and the egress rule methods raise. So an eval resume with a non-empty `egress.evaluation` list fails right after it creates the sandbox.
- It sends no `sandbox_created` announcements, because `SandboxCreatedData.provider` only accepts `daytona`.

## Capacity

These are for a caller, such as the Tracker, deciding where a sandbox fits. They report usage, and nothing is reserved.

- `SandboxProvider.get_capacity_domains()` reports Daytona CPU, memory, disk, and GPU usage, plus allowed GPU types, for every canonical target and sandbox class. `get_capacity()` reports the configured target only.
- Daytona's `reservable_demand()` returns a `SandboxReservationDemand` with the canonical `target_id`, the `sandbox_class`, and the CPU, memory, disk, and GPU quota charges. Image demand comes from the create request, and snapshot demand from the snapshot's own metadata.
- GPU builds charge only GPU quota. Shared GPU targets use `earth`, and dedicated and custom targets keep their region ID.
- Unreadable metadata raises instead of falling back to the requested resources, and providers without capacity support raise `NotImplementedError`.
- The API key needs organization-usage and region-list access, plus snapshot-read access for targeted snapshot admission and snapshot demand.

## How providers differ

### Egress

- `Sandbox.modify_egress_rules(addresses)` applies a non-empty allowlist, `Sandbox.block_all_egress()` denies all outbound traffic, and `Sandbox.clear_egress_rules()` restores unrestricted access.
- Deny-all is its own provider-native operation, so don't express it as `modify_egress_rules([])`. `ComposeSandbox` forwards all three methods to its outer sandbox.
- Hostnames, including `*.` wildcards, are enforced as domain rules on every provider, so other sites on the same CDN address stay blocked.
- Modal filters domains only on sandboxes created with a domain list, so every Modal sandbox starts with one.

### Inventory and pricing

- `Sandbox.labels` and `Sandbox.created_at` expose provider inventory metadata when the provider has it, and are `None` otherwise. Creation times are timezone-aware UTC.
- `SandboxQuery.created_at_lte` is an inclusive creation-time bound. Daytona supports it (and always limits listing to the configured target, a region name or ID), while Modal rejects it.
- `Sandbox.cost_usd(ended_at)` prices a sandbox from `created_at` to `ended_at` at the provider's published rates, and `Sandbox.has_ended` says when that lifetime is final.
- Both exist for providers with no usage API, which `priced_provider_types()` lists (only `daytona_v2`). Other providers return `None` and `False`.

### Image and snapshot sources

- Modal supports `ImageSource` (a registry pull) and `SnapshotSource` (a filesystem snapshot from `Sandbox.snapshot_filesystem()`, restored by image ID).
- `TargetedSnapshotSource(snapshot=..., target=...)` is Daytona-only, and picks the target for both admission and creation. `docker_image` is invalid on it, because that field can't preserve the target.
- CPU snapshot admission checks the snapshot's CPU, memory, disk, and sandbox class against the exact target's organization-usage row.
- GPU snapshot prechecks validate that the target is available, and `reservable_demand()` reports their GPU quota charge.

### Dockerfile sources

- `DockerfileSource(dockerfile=...)` builds the image from an absolute Dockerfile path on the machine creating the sandbox. The folder holding the Dockerfile is the build context.
- Daytona builds it with `Image.from_dockerfile`, Modal with `Image.from_dockerfile(context_dir=...)`, and local Docker with `docker build`.
- `create_timeout` bounds the build, and the source's optional `build_timeout` adds that many seconds on top. Local Docker is different: `build_timeout` alone bounds the build, and `create_timeout` bounds the container start.
- A failed build raises `SandboxBuildError`, a `SandboxError` whose `build_log` holds the build output. For Daytona and local Docker that's the last 64 KB of the build log, and for Modal the error text it reports.
- Daytona's builder ignores `.dockerignore`, so it uploads everything the Dockerfile copies even when `.dockerignore` lists it. Local Docker and Modal honor it.
- A Dockerfile source can't be a Compose outer source or an `eval_sandbox.source`. Sandbox grading falls back to the task's own source when `eval_sandbox.source` is unset, though, so a Dockerfile task gets built again for grading.

### Resources and GPUs

- Modal has no disk-size parameter, so `Resources.disk` is accepted there but not enforced.
- Request GPUs with `Resources.gpu` (a count) and `Resources.gpu_type` (one type, or a list of acceptable types in order of preference).
- Common spellings such as `nvidia-h100-80gb` or `RTX 4090` are understood, and an unknown name is refused on every provider.
- Modal requires a type. It asks for the first listed type Modal offers, in Modal's spelling, and sends `"<type>:<count>"` to the sandbox.
- Daytona takes a count with optional types from its `GpuType` enum (`H100`, `H200`, `RTX-PRO-6000`, `RTX-4090`, `RTX-5090`). It passes the listed types it offers in order, so Daytona pins the sandbox to the first with capacity, and refuses the request when none match.
- Daytona rejects GPU requests for snapshot sandboxes, since a snapshot's resources are fixed when it's made.
- Modal GPU requests always use `gvisor`, because Modal doesn't support GPUs on `vm`. CPU requests use the configured runtime.

### Nested Docker

- Daytona supports nested Docker natively. On Modal, use the `vm` runtime.
- Set `requires_nested_docker` on the create request when the sandbox runs Docker. Local Docker, and Modal off `vm`, then raise `UnsupportedSandboxConfigError` before provisioning.
- Without that flag, Modal `gvisor` still turns on the undocumented `enable_docker` option. Nested containers there need Docker bridge networking disabled and host networking.
- The service owns the Docker-capable image, the dockerd startup flags, the Compose workflow, and cleanup.

### Modal creation and retries

- Transient connection errors during lookup, execution, and deletion are retried, up to three attempts. Other command failures raise `SandboxCommandError` with the exit code.
- Creation is one SDK call bounded by `create_timeout`. Resource exhaustion (quota, capacity, or placement) raises `SandboxError` without another try.
- A sandbox that arrives after a timeout or cancellation is terminated in the background. Keep the process running after `close()` so that cleanup can finish.
- Resume with `get_sandbox(id)`. Create never recovers a sandbox by name, since each call uses a unique allocation name.

## Local Docker sandboxes

Local Docker is for running the whole stack on one machine. The local Tracker, executor, and benchmark service must share one Docker daemon.

### Set it up

- Select the provider with `{"type": "docker"}`. The service accepts it only with `CBS_DOCKER_ENABLED=true`, so leave that unset on a hosted service.
- `DOCKER_HOST` overrides the Docker context or the detected local socket. With a custom `DOCKER_CONFIG`, set `DOCKER_HOST` explicitly so image pulls and sandbox operations use the same daemon.
- The `docker` CLI must be on `PATH`. A missing image is pulled with it before `create_timeout` starts, so private registries work with the host's `docker login` and credential helpers.
- Images must provide `/bin/sh` and `setsid --wait`. Creation fails with `SandboxError` otherwise (for example with BusyBox `setsid` in Alpine images).

### What you get

- Task containers get no Docker socket and no host-directory mounts.
- Containers run with `no-new-privileges`, so setuid programs such as `sudo` can't gain root. An image whose commands call `sudo` must run as root.
- CPU and memory limits are enforced. `Resources.disk` is accepted but sets no filesystem quota.
- `auto_stop_interval` is accepted but sets no idle timer. Callers must delete containers after use, and reconcile abandoned ones after a restart.
- Listing and deletion only see containers marked as managed by this provider.
- Timeouts and cancelled streams terminate the command's process group.

### What's not supported

- Network access can only be turned off at creation, with `network_block_all`. The egress rule methods raise `SandboxError`, so egress allowlists and staged egress policies fail on Docker.
- Snapshots, Compose, GPUs, persistent volumes, and provider-managed secrets are rejected.
- Nested Docker doesn't work. Containers run unprivileged, so a benchmark that starts `dockerd` inside the sandbox fails here (unlike on Daytona and Modal).

To run the local provider checks:

```bash
DOCKER_HOST=unix:///var/run/docker.sock \
  uv run pytest tests/integration/test_docker_sandbox.py
```

## Recover a lost sandbox

A task can opt in to getting a fresh sandbox when its provider loses the first one. It sets `sandbox_recovery` to a `SandboxRecoveryPolicy`, and the caller runs it through `BenchmarkServiceClient.run_with_sandbox_recovery(...)`, which owns the bounded retry loop.

- The loop loads the task once and retries only losses the provider confirms (`SandboxNotFoundError`). Every fresh-sandbox attempt counts against one overall cap.
- Callers can add setup errors that also need a fresh sandbox with `retryable_attempt_errors`.
- `default_max_attempts` is the cap for benchmarks without a policy. With a policy, it bounds consecutive setup attempts.
- Each attempt gets a `SandboxRecoveryAttempt`. Merge its `environment` into the sandbox environment, and call `mark_replacement_ready()` only once benchmark setup has durably recorded the outage.
- That ordering is what lets a failed replacement setup carry the same outage ID into the next sandbox. A later provider loss gets a new, globally unique ID that can't collide after a runner restart.
- `sandbox_loss_retry_available` lets a runner record its normal terminal task error without redoing the policy math.

```python
async def run_attempt(attempt):
    task = await attempt.retrieve_task()
    async with create_sandbox(
        source=task.source,
        env_vars={**base_environment, **attempt.environment},
    ) as sandbox:
        await client.setup_task(task_id, sandbox.id)
        attempt.mark_replacement_ready()
        return await run_agent(sandbox)


result = await client.run_with_sandbox_recovery(
    task_id,
    run_id,
    run_attempt,
    retryable_attempt_errors=(TransientSandboxSetupError,),
    default_max_attempts=2,
    attempt_id=attempt_id,  # the caller's identity for this execution of the task
)
```
