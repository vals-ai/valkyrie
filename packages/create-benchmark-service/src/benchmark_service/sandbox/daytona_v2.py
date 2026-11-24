"""Opt-in Daytona v2 provider (``api.daytona.com``) with fractional CPU, on the ``daytona`` SDK's v2 client.

The SDK's sandbox handle drops ``created_at``, so pricing reads it with one raw ``GET /sandboxes/{id}``.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import signal
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import aclosing, asynccontextmanager, contextmanager, suppress
from functools import partial
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal, cast
from urllib.parse import quote

import httpx
from daytona import AsyncDaytona as DaytonaClient
from daytona import DaytonaConfig
from daytona.v2 import AsyncProcessHandle, AsyncSandbox, ClientTimeoutError, ConflictError, DaytonaError
from daytona.v2 import ConnectionError as DaytonaConnectionError
from daytona.v2 import (
    Image,
    ImageBuildFailedError,
    LifecyclePolicyOptions,
    NetworkPolicyInput,
    NotFoundError,
    RequestTimeoutError,
    SandboxTerminalStateError,
    Snapshot,
    WaitResult,
)
from daytona.v2 import Resources as DaytonaResources
from daytona.v2 import SandboxLifecycle
from daytona.v2.daytona import AsyncDaytona as DaytonaV2Client
from pydantic import BaseModel, Field, field_validator
from tenacity import RetryCallState, retry, retry_if_exception, stop_before_delay, wait_random_exponential

from benchmark_service.sandbox.created import notify_sandbox_created
from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import (
    CHECKPOINT_TIMEOUT_SECONDS,
    Checkpoint,
    CheckpointKind,
    DockerfileSource,
    ExecResult,
    ImageSource,
    MAX_SANDBOX_LIFETIME_SECONDS,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
    SandboxAccountRejectedError,
    SandboxBuildError,
    SandboxCapabilities,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxRejectedError,
    SandboxSource,
    SnapshotSource,
    provider_details,
    validate_command_env,
)

DEFAULT_DAYTONA_V2_API_URL = "https://api.daytona.com"
MIB_PER_GIB = 1024
VCPUS_PER_CORE = 2
_POLL_SECONDS = 1.0
_LOG_WAIT_SECONDS = 5.0
_VM_PENDING_SECONDS = 120
_KILL_AFTER_SECONDS = 5.0
_DELETED_STATES = frozenset({"destroying", "destroyed"})
_ENDED_STATES = frozenset({"stopped", "destroying", "destroyed", "error"})
_TIMEOUT_EXIT_CODE = 124
_ACCOUNT_REJECTED_CODES = frozenset({"ORG_SUSPENDED", "USER_SUSPENDED", "PROJECT_BUDGET_EXCEEDED", "BILLING_DISABLED"})
_REJECTED_CODES = frozenset(
    {"IMAGE_NOT_FOUND", "SNAPSHOT_NOT_FOUND", "IMAGE_TOO_LARGE", "ENTITLEMENT_REQUIRED", "FEATURE_NOT_ENABLED"}
)
_POLICY_TIMEOUT_SECONDS = 60.0
_RESERVED_ENV_PREFIX = "DAYTONA_"
_COMMAND_ENV_LABEL = "cbs-command-env"
_COMMAND_ENV_PATH = "/etc/cbs-command-env.json"
_ALLOWED_RESERVED_ENV = frozenset({"DAYTONA_API_URL", "DAYTONA_OTEL_ENDPOINT", "DAYTONA_DOCKER_START"})
_OUTPUT_CHANNELS = frozenset({"stdout", "stderr", "pty"})
_MAX_TIMEOUT_SECONDS = min(MAX_SANDBOX_LIFETIME_SECONDS, 24 * 60 * 60)
_RESERVED_NAME_PREFIXES = (
    ".",
    "sbx_",
    "vol_",
    "fn_",
    "img_",
    "snap_",
    "snap-",
    "build-",
    "refs",
    "by-digest",
    "prj-",
    "glb-",
)
_RATE_LIMIT_SECONDS = 300.0
_backoff = wait_random_exponential(multiplier=1, max=30)


class DaytonaV2ProviderConfig(BaseModel):
    type: Literal["daytona_v2"] = "daytona_v2"
    prices_sandboxes: ClassVar[bool] = True
    DAYTONA_V2_API_KEY: str
    DAYTONA_V2_API_URL: str = DEFAULT_DAYTONA_V2_API_URL
    DAYTONA_V2_REGION: str | None = Field(default=None, exclude_if=lambda value: value is None)
    DAYTONA_V2_SPOT: bool = False

    @field_validator("DAYTONA_V2_API_URL")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        if value.rstrip("/") != DEFAULT_DAYTONA_V2_API_URL:
            raise ValueError(
                f"DAYTONA_V2_API_URL must be {DEFAULT_DAYTONA_V2_API_URL}, the only API the Daytona SDK's v2 client calls"
            )
        return DEFAULT_DAYTONA_V2_API_URL

    @classmethod
    def from_env(cls) -> "DaytonaV2ProviderConfig":
        api_key = os.environ.get("DAYTONA_V2_API_KEY")
        if not api_key:
            raise MissingSandboxConfigError("Missing required environment variables: DAYTONA_V2_API_KEY")
        return cls(
            DAYTONA_V2_API_KEY=api_key,
            DAYTONA_V2_API_URL=os.environ.get("DAYTONA_V2_API_URL") or DEFAULT_DAYTONA_V2_API_URL,
            DAYTONA_V2_REGION=os.environ.get("DAYTONA_V2_REGION") or None,
            DAYTONA_V2_SPOT=(os.environ.get("DAYTONA_V2_SPOT") or "").lower() in ("1", "true", "yes"),
        )

    def create_provider(self) -> SandboxProvider:
        return DaytonaV2SandboxProvider(self)


def v2_resources(resources: Resources) -> DaytonaResources:
    """Map CBS resources (logical vCPUs, GiB) to v2 cores and GiB; fractions pass through.

    v2 boots a sandbox at the plan cap unless told otherwise, so memory is capped at the
    request and CPU at whole vCPUs, the `cpu_max` step (0.5 cores), rounded up.
    """
    return DaytonaResources(
        cpu=resources.vcpu / VCPUS_PER_CORE,
        cpu_max=math.ceil(resources.vcpu) / VCPUS_PER_CORE,
        memory=resources.memory,
        memory_max=resources.memory,
        disk=resources.disk,
    )


def _error_code(response: httpx.Response) -> str | None:
    with suppress(ValueError):
        match response.json():
            case {"code": str(code)}:
                return code
            case _:
                return None
    return None


def _error(exc: DaytonaError | httpx.HTTPError, what: str) -> SandboxError:
    if isinstance(exc, DaytonaError):
        if isinstance(exc, DaytonaConnectionError | ClientTimeoutError):
            return SandboxConnectionError(f"{what}: {exc}")
        status, code, detail = exc.status_code, exc.code, str(exc)
    elif isinstance(exc, httpx.HTTPStatusError):
        status, code, detail = exc.response.status_code, _error_code(exc.response), exc.response.text
    else:
        return SandboxConnectionError(f"{what}: {exc}")
    if status == 402 or code in _ACCOUNT_REJECTED_CODES:
        return SandboxAccountRejectedError(f"{what}: Daytona v2 rejected the account: {code or status}")
    if code in _REJECTED_CODES:
        return SandboxRejectedError(f"{what}: Daytona v2 rejected the request: {code}")
    if status == 404:
        return SandboxNotFoundError(f"{what}: not found")
    if status is not None and (status in (408, 429) or status >= 500):
        return SandboxConnectionError(f"{what}: Daytona v2 returned HTTP {status}" + (f" {code}" if code else ""))
    return SandboxError(f"{what}: Daytona v2 returned {status or 'an error'}: {detail[:500]}")


def _rate_limited(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429
    return isinstance(exc, DaytonaError) and exc.status_code == 429


def _rate_limited_or_dropped(exc: BaseException) -> bool:
    return _rate_limited(exc) or isinstance(exc, DaytonaConnectionError)


def _retry_after(exc: BaseException | None) -> object:
    if isinstance(exc, httpx.HTTPStatusError):
        with suppress(ValueError):
            match exc.response.json():
                case {"retry_after_seconds": seconds}:
                    return seconds
                case _:
                    return None
        return None
    match getattr(exc, "retry_after_seconds", None), getattr(exc, "retry_after_ms", None):
        case int(seconds), _:
            return seconds
        case None, int(ms):
            return ms / 1000
        case _:
            return None


def _rate_limit_wait(state: RetryCallState) -> float:
    retry_after = _retry_after(state.outcome and state.outcome.exception())
    return max(retry_after if isinstance(retry_after, int | float) else 0, _backoff(state))


async def _patient[T](
    call: Callable[[], Awaitable[T]],
    deadline: float | None = None,
    *,
    retry_on: Callable[[BaseException], bool] = _rate_limited,
) -> T:
    budget = _RATE_LIMIT_SECONDS
    if deadline is not None:
        budget = min(budget, deadline - asyncio.get_running_loop().time())

    @retry(
        retry=retry_if_exception(retry_on),
        wait=_rate_limit_wait,
        stop=stop_before_delay(max(budget, 0)),
        reraise=True,
    )
    async def attempt() -> T:
        return await call()

    return await attempt()


def _v2_name(name: str) -> str:
    return f"vals-{name}" if name.startswith(_RESERVED_NAME_PREFIXES) else name


@contextmanager
def _errors(what: str) -> Iterator[None]:
    try:
        yield
    except (DaytonaError, httpx.HTTPError) as exc:
        raise _error(exc, what) from exc


async def _await_started(sandbox: AsyncSandbox, timeout: float) -> None:
    """Wait until v2 itself reads the sandbox as started, since the SDK's event wait can return while it is still pending."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(timeout, 1)
    try:
        await _patient(lambda: sandbox.wait(until="started", timeout=max(deadline - loop.time(), 1)), deadline)
        while sandbox.state != "started":
            if sandbox.state in _ENDED_STATES or loop.time() > deadline:
                raise SandboxError(f"Daytona v2 sandbox {sandbox.id} did not start: {sandbox.state}")
            await asyncio.sleep(_POLL_SECONDS)
            await _patient(sandbox.refresh_data, deadline)
    except (SandboxTerminalStateError, RequestTimeoutError) as exc:
        detail = f"{sandbox.state} {sandbox.error or ''}".strip()
        raise SandboxError(f"Daytona v2 sandbox {sandbox.id} did not start: {detail}") from exc
    except DaytonaError as exc:
        raise _error(exc, f"wait for sandbox {sandbox.id}") from exc


def _lifecycle_policy(auto_stop_minutes: int) -> LifecyclePolicyOptions:
    policy: LifecyclePolicyOptions = {"timeout_seconds": _MAX_TIMEOUT_SECONDS, "on_timeout": "delete"}
    if auto_stop_minutes > 0:
        policy |= {"idle_expiry_seconds": auto_stop_minutes * 60, "on_idle_timeout": "delete"}
    return policy


def _exit_code(result: WaitResult) -> int:
    """The process's exit code; a timeout exits 124 like the other providers, a signal 128 + its number."""
    if result.exit_code is not None:
        return result.exit_code
    if result.reason == "timed_out":
        return _TIMEOUT_EXIT_CODE
    name = result.signal or ""
    signum = getattr(signal.Signals, name if name.startswith("SIG") else f"SIG{name}", None)
    return 128 + signum if signum is not None else -1


def _created_at(sandbox: Mapping[str, Any]) -> datetime | None:
    value = sandbox.get("created_at")
    if not isinstance(value, str):
        return None
    created_at = datetime.fromisoformat(value)
    return created_at.astimezone(UTC) if created_at.utcoffset() is not None else None


def _deleted(sandbox: AsyncSandbox) -> SandboxNotFoundError:
    policy = sandbox.lifecycle_policy or SandboxLifecycle()
    return SandboxNotFoundError(
        f"sandbox {sandbox.id} was deleted: "
        + provider_details(
            {
                "state": sandbox.raw_state,
                "error": sandbox.error,
                "spot_evicted_at": sandbox.spot_evicted_at,
                "last_activity_at": policy.last_activity_at,
                "last_activity_source": policy.last_activity_source,
            }
        )
    )


async def _wrap(
    client: DaytonaV2Client,
    http: httpx.AsyncClient,
    sandbox: AsyncSandbox,
    command_env: Mapping[str, str] | None = None,
) -> DaytonaV2Sandbox:
    async def fetch() -> httpx.Response:
        response = await http.get(f"/sandboxes/{quote(sandbox.id, safe='')}")
        return response.raise_for_status()

    with _errors(f"get sandbox {sandbox.id}"):
        data = (await _patient(fetch)).json()
    return DaytonaV2Sandbox(sandbox, client, http, data.get("labels"), _created_at(data), command_env)


# On-demand AMD Zen 5 rates from the https://www.daytona.com/pricing calculator, billed per second while
# running. Ephemeral disk and the Linux licence are $0. Region multipliers scale every base rate; they come
# from GET /orgs/{org_id}/billing/region-multipliers for the Vals org, and match
# https://www.daytona.com/docs/en/regions (us-east-ash-1 at 2.0x). A sandbox's region_id is the region it
# was placed in, never a broader one it was requested in.
ZEN5_CORE_USD_PER_HOUR = 0.235
MEMORY_GIB_USD_PER_HOUR = 0.0162
PREEMPTIBLE_MULTIPLIER = 0.7
REGION_MULTIPLIERS: dict[str, float] = {
    "earth": 1.0,
    "us": 1.15,
    "us-east": 1.75,
    "us-east-ash-1": 2.0,
    "us-east-bos-1": 2.0,
}


class DaytonaV2Sandbox(Sandbox):
    def __init__(
        self,
        sandbox: AsyncSandbox,
        client: DaytonaV2Client,
        http: httpx.AsyncClient,
        labels: dict[str, str] | None,
        created_at: datetime | None,
        command_env: Mapping[str, str] | None = None,
    ) -> None:
        self._sandbox = sandbox
        self._command_env: dict[str, str] | None = None if command_env is None else dict(command_env)
        self._client = client
        self._http = http
        self.labels = labels
        self.created_at = created_at

    @property
    def id(self) -> str:
        return self._sandbox.id

    async def _saved_env(self) -> dict[str, str]:
        if self._command_env is None:
            saved = (self.labels or {}).get(_COMMAND_ENV_LABEL)
            self._command_env = cast(
                "dict[str, str]", json.loads(await self.download_file(_COMMAND_ENV_PATH)) if saved else {}
            )
        return self._command_env

    @property
    def name(self) -> str:
        return self._sandbox.name or self.id

    @property
    def state(self) -> str:
        return self._sandbox.state

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        memory = self._sandbox.memory
        fields = {
            "region_id": self._sandbox.region,
            "cpu": self._sandbox.cpu,
            "memory_mib": None if memory is None else round(memory * MIB_PER_GIB),
            "spot": self._sandbox.spot,
        }
        return {key: str(value) for key, value in fields.items() if value is not None}

    @property
    def has_ended(self) -> bool:
        return self.state in _ENDED_STATES

    def cost_usd(self, ended_at: datetime) -> float | None:
        cpu, memory, spot = self._sandbox.cpu, self._sandbox.memory, self._sandbox.spot
        region = REGION_MULTIPLIERS.get(self._sandbox.region or "")
        if self.created_at is None or cpu is None or memory is None or spot is None or region is None:
            return None
        per_hour = (cpu * ZEN5_CORE_USD_PER_HOUR + memory * MEMORY_GIB_USD_PER_HOUR) * region
        if spot:
            per_hour *= PREEMPTIBLE_MULTIPLIER
        return per_hour * max((ended_at - self.created_at).total_seconds(), 0) / 3600

    @asynccontextmanager
    async def _sandbox_errors(self, what: str, missing_path: str | None = None) -> AsyncIterator[None]:
        try:
            with _errors(what):
                yield
        except SandboxError as exc:
            with _errors(f"get sandbox {self.id}"):
                await _patient(self._sandbox.refresh_data)
            if self._sandbox.state in _DELETED_STATES:
                raise _deleted(self._sandbox) from exc
            if missing_path is not None and isinstance(exc, SandboxNotFoundError):
                raise SandboxError(f"{missing_path} not found in sandbox {self.id}") from exc
            raise

    async def _stream(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str] | None
    ) -> AsyncGenerator[str | int, None]:
        """Yield output text, then the exit code as the final int.

        Polls the log rather than following its stream: back-to-back followed streams wedge small v2 sandboxes.
        """
        what = f"exec in {self.id}"
        async with self._sandbox_errors(what):
            handle = await _patient(
                lambda: self._sandbox.process.start(command, cwd=cwd, env=env, execution_timeout=timeout)
            )
        result: WaitResult | None = None
        try:
            cursor: str | None = None
            while True:
                async with self._sandbox_errors(what):
                    page = await _patient(lambda: handle.logs(cursor=cursor), retry_on=_rate_limited_or_dropped)
                for frame in page.items:
                    if frame.channel in _OUTPUT_CHANNELS:
                        yield frame.data
                cursor = page.next_cursor
                if page.eof:
                    break
                if not page.items:
                    async with self._sandbox_errors(what):
                        await _patient(
                            lambda: handle.wait(timeout=_LOG_WAIT_SECONDS), retry_on=_rate_limited_or_dropped
                        )
            async with self._sandbox_errors(what):
                result = await _patient(handle.wait, retry_on=_rate_limited_or_dropped)
            yield _exit_code(result)
        finally:
            if result is None:
                await _stop_process(handle)

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        exit_code = -1
        async with aclosing(self._stream(command, cwd, timeout, await self._saved_env() or None)) as stream:
            async for item in stream:
                if isinstance(item, int):
                    exit_code = item
                else:
                    output.append(item)
        return ExecResult(exit_code=exit_code, output="".join(output))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = {**await self._saved_env(), **validate_command_env(env_vars)}
        async with aclosing(self._stream(command, cwd, timeout, env)) as stream:
            async for item in stream:
                if isinstance(item, int):
                    if item != 0:
                        raise SandboxCommandError(item)
                else:
                    yield item

    async def _set_network_policy(self, rules: NetworkPolicyInput) -> None:
        """Replace the sandbox's egress rules and wait until Daytona reports them applied."""
        what = f"network policy of {self.id}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _POLICY_TIMEOUT_SECONDS
        with _errors(what):
            while True:
                try:
                    current = await self._sandbox.get_network_policy()
                    policy = await self._sandbox.update_network_policy(network_policy=rules, if_match=current.etag)
                    break
                except ConflictError as exc:
                    if "network_policy_transition_in_progress" not in f"{exc.code} {exc}" or loop.time() >= deadline:
                        raise
                await asyncio.sleep(_POLL_SECONDS)
        revision = policy.desired_revision
        deadline = loop.time() + _POLICY_TIMEOUT_SECONDS
        while policy.status != "applied" or (policy.applied_revision or 0) < revision:
            if policy.error:
                raise SandboxError(f"Daytona v2 could not apply the {what}: {policy.error}")
            if loop.time() > deadline:
                raise SandboxConnectionError(f"Daytona v2 did not apply the {what} in time")
            await asyncio.sleep(_POLL_SECONDS)
            with _errors(what):
                policy = await self._sandbox.get_network_policy()

    async def checkpoint(self, kind: CheckpointKind, name: str) -> Checkpoint:
        what = f"checkpoint of {self.id}"
        with _errors(what):
            created = await self._capture(kind, name)
        try:
            with _errors(what):
                if kind == CheckpointKind.FILES:
                    ready: Image | Snapshot = await self._client.images.get(
                        created.id, wait="ready", timeout=CHECKPOINT_TIMEOUT_SECONDS
                    )
                else:
                    ready = await self._client.snapshots.get(
                        created.id, wait="ready", timeout=CHECKPOINT_TIMEOUT_SECONDS
                    )
            if ready.status != "ready":
                detail = f"{ready.status} {ready.error or ''}".strip()
                raise SandboxError(f"Daytona v2 {what} did not become ready: {detail}")
        except BaseException:
            with suppress(DaytonaError):
                await _delete_artifact(self._client, kind, created.id)
            raise
        return Checkpoint(provider="daytona_v2", kind=kind, ref=ready.id)

    async def _capture(self, kind: CheckpointKind, name: str) -> Image | Snapshot:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _VM_PENDING_SECONDS
        while True:
            try:
                if kind == CheckpointKind.FILES:
                    return await self._sandbox.capture_image()
                return await self._sandbox.create_snapshot(name)
            except ConflictError as exc:
                if "vm status is pending" not in str(exc) or loop.time() >= deadline:
                    raise
            await asyncio.sleep(_POLL_SECONDS)

    async def fork(self, name: str) -> DaytonaV2Sandbox:
        with _errors(f"fork {self.id}"):
            forked = await self._sandbox.fork(name=name, memory=True, wait_seconds=0)
        try:
            await notify_sandbox_created("daytona_v2", forked.id)
            await _await_started(forked, CHECKPOINT_TIMEOUT_SECONDS)
            return await _wrap(self._client, self._http, forked)
        except BaseException:
            with suppress(DaytonaError):
                await self._client.delete(forked.id)
            raise

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        cidrs, domains = resolve_allowed_addresses(allowed_addresses)
        await self._set_network_policy({"block_all": True, "allow_cidrs": cidrs, "allow_domains": domains or None})

    async def block_all_egress(self) -> None:
        await self._set_network_policy({"block_all": True, "allow_cidrs": [], "allow_domains": None})

    async def clear_egress_rules(self) -> None:
        await self._set_network_policy({"block_all": False, "allow_cidrs": [], "allow_domains": None})

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        async with self._sandbox_errors(f"upload {remote_path} to {self.id}", remote_path):
            await _patient(lambda: self._sandbox.fs.write_bytes(remote_path, content))

    async def download_file(self, remote_path: str) -> bytes:
        async with self._sandbox_errors(f"download {remote_path} from {self.id}", remote_path):
            return await _patient(lambda: self._sandbox.fs.read_bytes(remote_path))


async def _stop_process(handle: AsyncProcessHandle) -> None:
    """SIGTERM a process its caller cancelled or closed, escalating to SIGKILL."""
    with suppress(DaytonaError):
        await handle.kill(signal="TERM", escalate_after=_KILL_AFTER_SECONDS)


async def _delete_artifact(client: DaytonaV2Client, kind: CheckpointKind, ref: str) -> None:
    if kind == CheckpointKind.FILES:
        await client.images.delete(ref)
    else:
        await client.snapshots.delete(ref)


def _sandbox_env(env_vars: Mapping[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in env_vars.items()
        if not name.startswith(_RESERVED_ENV_PREFIX) or name in _ALLOWED_RESERVED_ENV
    }


class DaytonaV2SandboxProvider(SandboxProvider):
    def __init__(self, config: DaytonaV2ProviderConfig, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._config = config
        self._client = DaytonaClient(config=DaytonaConfig(api_key=config.DAYTONA_V2_API_KEY)).v2
        self._http = httpx.AsyncClient(
            base_url=(os.environ.get("DAYTONA_V2_API_URL") or "").strip().rstrip("/") or config.DAYTONA_V2_API_URL,
            transport=transport,
            headers={"Authorization": f"Bearer {config.DAYTONA_V2_API_KEY}"},
            timeout=60.0,
        )

    async def close(self) -> None:
        try:
            await self._client.close()
        finally:
            await self._http.aclose()

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(checkpoints=frozenset(CheckpointKind), fork=True)

    async def delete_checkpoint(self, checkpoint: Checkpoint) -> None:
        if checkpoint.provider != "daytona_v2":
            raise SandboxError(f"Daytona v2 cannot delete a {checkpoint.provider} checkpoint")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CHECKPOINT_TIMEOUT_SECONDS
        while True:
            try:
                await _delete_artifact(self._client, checkpoint.kind, checkpoint.ref)
                return
            except NotFoundError:
                return
            except ConflictError as exc:
                if loop.time() >= deadline:
                    raise _error(exc, f"delete {checkpoint.ref}") from exc
                await asyncio.sleep(_POLL_SECONDS)
            except DaytonaError as exc:
                raise _error(exc, f"delete {checkpoint.ref}") from exc

    async def _image(self, source: SandboxSource, deadline: float) -> str:
        region = self._config.DAYTONA_V2_REGION
        match source:
            case SnapshotSource(snapshot=snapshot):
                return snapshot
            case Checkpoint(provider="daytona_v2", kind=CheckpointKind.FILES, ref=ref):
                return ref
            case ImageSource(image=image):
                submitted: Callable[[], Awaitable[Image]] = partial(
                    self._client.images.pull, image, region=region, wait=False
                )
            case DockerfileSource(dockerfile=dockerfile):
                path = Path(dockerfile).resolve()
                submitted = partial(
                    self._client.images.build_from_context, path.parent, dockerfile=path.name, region=region
                )
            case _:
                raise SandboxError(f"Daytona v2 provider does not support {type(source).__name__}")
        loop = asyncio.get_running_loop()
        try:
            built = await _patient(submitted, deadline)
            built = await _patient(
                lambda: self._client.images.get(built.id, wait="ready", timeout=max(deadline - loop.time(), 1)),
                deadline,
            )
        except ImageBuildFailedError as exc:
            raise SandboxBuildError(f"Daytona v2 image failed: {exc}", exc.build_error or str(exc)) from exc
        except RequestTimeoutError as exc:
            raise SandboxBuildError("Daytona v2 image was not ready in time") from exc
        except DaytonaError as exc:
            raise _error(exc, "register image") from exc
        if built.status != "ready":
            raise SandboxBuildError(f"Daytona v2 image {built.id} failed: {built.error}", built.error or "")
        return built.id

    def _reject_unsupported(self, request: SandboxCreateRequest) -> None:
        unsupported = [
            name
            for name, used in (
                ("gpu", request.resources.gpu),
                ("volumes", request.volumes),
                ("sandbox_secrets", request.sandbox_secrets),
            )
            if used
        ]
        if unsupported:
            raise SandboxError(f"Daytona v2 provider does not support: {', '.join(unsupported)}")

    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaV2Sandbox:
        """A memory restore takes no image, resources, env or network policy.

        Its fresh env is saved in the sandbox and goes to every command it runs, from any handle to it. Processes it
        resumes keep their saved env, and the saved network policy is cleared once the copy runs.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + request.provider_timeout
        self._reject_unsupported(request)
        region = self._config.DAYTONA_V2_REGION
        lifecycle = _lifecycle_policy(request.auto_stop_interval)
        labels = {key: value for key, value in request.labels.items() if key != "name"}
        command_env: dict[str, str] = {}
        match request.source:
            case Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref=ref):
                if request.network_block_all:
                    raise SandboxError(
                        "A Daytona v2 memory checkpoint resumes its processes under the saved network policy, "
                        "so it cannot be restored with network_block_all. Restore from a files checkpoint instead."
                    )
                command_env = _sandbox_env(request.env_vars)
                labels[_COMMAND_ENV_LABEL] = "true"
                restored = True
                submitted = partial(
                    self._client.create_from_snapshot,
                    ref,
                    name=_v2_name(request.name),
                    labels=labels,
                    lifecycle_policy=lifecycle,
                    region=region,
                    idempotency_key=request.name,
                    wait_seconds=0,
                    create_timeout_seconds=request.create_timeout,
                )
            case source:
                restored = False
                image = await self._image(source, deadline)
                submitted = partial(
                    self._client.create,
                    image,
                    name=_v2_name(request.name),
                    env=_sandbox_env(request.env_vars),
                    labels=labels,
                    resources=v2_resources(request.resources),
                    spot=self._config.DAYTONA_V2_SPOT,
                    lifecycle_policy=lifecycle,
                    network_policy={"block_all": True} if request.network_block_all else {},
                    region=region,
                    idempotency_key=request.name,
                    wait_seconds=0,
                    create_timeout_seconds=request.create_timeout,
                )
        with _errors(f"create {request.name}"):
            created = await _patient(submitted, deadline)
        try:
            await notify_sandbox_created("daytona_v2", created.id)
            await _await_started(created, deadline - loop.time())
            sandbox = await _wrap(self._client, self._http, created, command_env)
            if restored:
                await sandbox.upload_file(_COMMAND_ENV_PATH, json.dumps(command_env).encode())
                await sandbox.clear_egress_rules()
        except BaseException:
            with suppress(SandboxError):
                await self.delete_sandbox(created.id)
            raise
        return sandbox

    async def get_sandbox(self, instance_id: str) -> DaytonaV2Sandbox:
        with _errors(f"get sandbox {instance_id}"):
            sandbox = await _patient(lambda: self._client.get(instance_id))
        if sandbox.state in _DELETED_STATES:
            raise _deleted(sandbox)
        return await _wrap(self._client, self._http, sandbox)

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            with _errors(f"delete sandbox {instance_id}"):
                await _patient(lambda: self._client.delete(instance_id))
        except SandboxNotFoundError:
            return

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        created_end_at = None if query.created_at_lte is None else query.created_at_lte + timedelta(microseconds=1)
        cursor: str | None = None
        while True:
            with _errors("search sandboxes"):
                page = await self._client.search(
                    labels=query.labels or None, created_end_at=created_end_at, limit=query.page_size, cursor=cursor
                )
            for item in page.items:
                try:
                    yield await self.get_sandbox(item.id)
                except SandboxNotFoundError:
                    continue
            if not page.next_cursor:
                return
            cursor = page.next_cursor
