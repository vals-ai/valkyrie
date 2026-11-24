from __future__ import annotations

import math
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from tenacity import wait_fixed, wait_none
from daytona.v2 import (
    ConflictError,
    DaytonaError,
    ImageBuildFailedError,
    LogFrame,
    NotFoundError,
    SandboxLifecycle,
    SandboxTerminalStateError,
    WaitResult,
)

from benchmark_service.sandbox import (
    Checkpoint,
    CheckpointKind,
    ComposeSandbox,
    ComposeSource,
    DaytonaProviderConfig,
    DaytonaV2ProviderConfig,
    ImageSource,
    Resources,
    SandboxAccountRejectedError,
    SandboxBuildError,
    SandboxCapabilities,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxRejectedError,
    priced_provider_types,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.sandbox import daytona_v2
from benchmark_service.sandbox.created import sandbox_created_scope
from benchmark_service.sandbox.daytona_v2 import DaytonaV2Sandbox, DaytonaV2SandboxProvider, v2_resources
from benchmark_service.sandbox.types import SandboxQuery

CREATED_AT = "2026-11-11T00:00:00Z"


def _wait(exit_code: int | None = 0, reason: str = "exited", signal: str | None = None) -> WaitResult:
    return WaitResult(reason=reason, exit_code=exit_code, signal=signal, raw_reason=reason)  # pyright: ignore[reportArgumentType]


def _frame(data: str, channel: str = "stdout") -> LogFrame:
    return LogFrame(cursor="c", channel=channel, data=data, timestamp_ms=0)


class FakeProcess:
    def __init__(self, frames: list[LogFrame], wait: WaitResult | None = None, hang: bool = False) -> None:
        self.frames, self.result, self.hang = frames, wait, hang
        self.started: list[dict[str, Any]] = []
        self.killed = False

    async def start(self, command: str, **kwargs: Any) -> FakeProcess:
        self.started.append({"command": command, **kwargs})
        return self

    async def logs(self, *, cursor: str | None = None) -> SimpleNamespace:
        items = self.frames if cursor is None else []
        return SimpleNamespace(items=items, next_cursor="end", eof=not self.hang)

    async def wait(self, *, timeout: float | None = None) -> WaitResult:
        if self.hang:
            return _wait(None, "timed_out")
        assert self.result is not None
        return self.result

    async def kill(self, **kwargs: Any) -> None:
        self.killed = True


class FakeFs:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    async def write_bytes(self, path: str, content: bytes) -> None:
        self.files[path] = content

    async def read_bytes(self, path: str) -> bytes:
        if path not in self.files:
            raise NotFoundError("missing", status_code=404, code="FILE_NOT_FOUND")
        return self.files[path]


class FakeSandbox:
    def __init__(self, sandbox_id: str = "sbx-1", state: str = "started", **info: Any) -> None:
        self.id, self.state, self.error = sandbox_id, state, info.get("error")
        self.spot_evicted_at = info.get("spot_evicted_at")
        self.lifecycle_policy = info.get("lifecycle_policy")
        self.name = info.get("name", sandbox_id)
        self.labels = info.get("labels", {"run": "r1"})
        self.cpu, self.memory = info.get("cpu", 0.25), info.get("memory", 2.0)
        self.region, self.spot = info.get("region", "us-east-ash-1"), info.get("spot", False)
        self.process = FakeProcess([], _wait())
        self.fs = FakeFs()
        self.after_refresh = state
        self.policies: list[dict[str, Any]] = []
        self.policy_states: list[SimpleNamespace] = []
        self.forks: list[dict[str, Any]] = []

    @property
    def raw_state(self) -> str:
        return self.state

    async def wait(self, *, until: str, timeout: float) -> FakeSandbox:
        if self.state != until:
            raise SandboxTerminalStateError("ended", state=self.state)  # pyright: ignore[reportArgumentType]
        return self

    async def refresh_data(self) -> None:
        self.state = self.after_refresh

    async def get_network_policy(self) -> SimpleNamespace:
        return self.policy_states.pop(0) if self.policy_states else SimpleNamespace(etag="e1")

    async def update_network_policy(self, *, network_policy: dict[str, Any], if_match: str) -> SimpleNamespace:
        self.policies.append({**network_policy, "if_match": if_match})
        return SimpleNamespace(desired_revision=2, applied_revision=1, status="pending", error=None)

    async def capture_image(self) -> SimpleNamespace:
        return SimpleNamespace(id="img-cp")

    async def create_snapshot(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(id=f"snap-{name}")

    async def fork(self, **kwargs: Any) -> FakeSandbox:
        self.forks.append(kwargs)
        return FakeSandbox("sbx-fork")


class FakeArtifacts:
    def __init__(self, status: str = "ready") -> None:
        self.status = status
        self.deleted: list[str] = []
        self.delete_errors: list[DaytonaError] = []
        self.pulled: list[tuple[str, dict[str, Any]]] = []

    async def pull(self, image: str, **kwargs: Any) -> SimpleNamespace:
        self.pulled.append((image, kwargs))
        return SimpleNamespace(id="img-1", status="pending", error=None)

    async def get(self, artifact_id: str, **kwargs: Any) -> SimpleNamespace:
        if self.status == "raise":
            raise ImageBuildFailedError("build failed", status_code=409, build_error="no space left")
        return SimpleNamespace(id=artifact_id, status=self.status, error="broken" if self.status != "ready" else None)

    async def delete(self, artifact_id: str) -> None:
        if self.delete_errors:
            raise self.delete_errors.pop(0)
        self.deleted.append(artifact_id)


class FakeClient:
    def __init__(self, sandbox: FakeSandbox | None = None) -> None:
        self.sandbox = sandbox or FakeSandbox()
        self.images, self.snapshots = FakeArtifacts(), FakeArtifacts()
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.deleted: list[str] = []
        self.create_error: DaytonaError | None = None
        self.pages: list[SimpleNamespace] = []
        self.sandboxes: dict[str, FakeSandbox] = {}

    async def create(self, image: str, **kwargs: Any) -> FakeSandbox:
        self.calls.append(("create", image, kwargs))
        if self.create_error is not None:
            raise self.create_error
        return self.sandbox

    async def create_from_snapshot(self, snapshot: str, **kwargs: Any) -> FakeSandbox:
        self.calls.append(("restore", snapshot, kwargs))
        return self.sandbox

    async def get(self, sandbox_id: str) -> FakeSandbox:
        return self.sandboxes.get(sandbox_id, self.sandbox)

    async def delete(self, sandbox_id: str) -> None:
        self.deleted.append(sandbox_id)

    async def search(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(("search", None, kwargs))
        return self.pages.pop(0)

    async def close(self) -> None:
        pass


def _provider(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeClient,
    *,
    spot: bool = False,
    created_at: str | None = CREATED_AT,
    labels: dict[str, str] | None = None,
) -> DaytonaV2SandboxProvider:
    monkeypatch.setattr(daytona_v2, "DaytonaClient", lambda *, config: SimpleNamespace(v2=client))  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]
    return DaytonaV2SandboxProvider(
        DaytonaV2ProviderConfig(DAYTONA_V2_API_KEY="key", DAYTONA_V2_REGION="us", DAYTONA_V2_SPOT=spot),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"labels": labels or {"run": "r1"}, "created_at": created_at})
        ),
    )


def _sandbox(sandbox: FakeSandbox, client: FakeClient | None = None) -> DaytonaV2Sandbox:
    return DaytonaV2Sandbox(
        sandbox,  # pyright: ignore[reportArgumentType]
        client or FakeClient(sandbox),  # pyright: ignore[reportArgumentType]
        httpx.AsyncClient(
            base_url="https://api.daytona.com",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"created_at": CREATED_AT})),
        ),
        sandbox.labels,
        datetime(2026, 11, 11, tzinfo=UTC),
    )


def _request(vcpu: float = 1, **overrides: Any) -> SandboxCreateRequest:
    fields: dict[str, Any] = {
        "source": ImageSource(image="python:3.13-slim"),
        "resources": Resources(vcpu=vcpu, memory=1, disk=3),
        "name": "tts-task-1",
        "labels": {"run": "r1"},
        "env_vars": {"A": "1", "DAYTONA_SECRET": "x", "DAYTONA_API_URL": "u"},
        "auto_stop_interval": 15,
        "create_timeout": 120,
    }
    fields.update(overrides)
    return SandboxCreateRequest(**fields)


@pytest.fixture(autouse=True)
def no_poll_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    monkeypatch.setattr(daytona_v2, "_RATE_LIMIT_SECONDS", 0.05)
    monkeypatch.setattr(daytona_v2, "_backoff", wait_none())


@pytest.mark.parametrize(
    ("vcpu", "cores", "cores_max"),
    [(0.25, 0.125, 0.5), (1, 0.5, 0.5), (1.0000000001, 0.50000000005, 1.0), (1.5, 0.75, 1.0), (4, 2, 2.0)],
)
def test_v2_resources_reserves_fractional_cores_and_caps_them(vcpu: float, cores: float, cores_max: float) -> None:
    resources = v2_resources(Resources(vcpu=vcpu, memory=2, disk=3))
    assert (resources.cpu, resources.cpu_max) == (cores, cores_max)
    assert (resources.memory, resources.memory_max, resources.disk) == (2, 2, 3)


def test_config_parses_from_mapping_and_rejects_other_apis() -> None:
    config = sandbox_provider_config_from_mapping({"type": "daytona_v2", "DAYTONA_V2_API_KEY": "k"})
    assert isinstance(config, DaytonaV2ProviderConfig) and config.DAYTONA_V2_API_URL == "https://api.daytona.com"
    v0 = {"type": "daytona", "DAYTONA_API_KEY": "k", "DAYTONA_API_URL": "https://app.daytona.io/api"}
    assert isinstance(sandbox_provider_config_from_mapping({**v0, "DAYTONA_TARGET": "us"}), DaytonaProviderConfig)
    with pytest.raises(ValueError, match="DAYTONA_V2_API_URL"):
        DaytonaV2ProviderConfig(DAYTONA_V2_API_KEY="k", DAYTONA_V2_API_URL="https://api.daytona.test")


async def test_create_pulls_the_image_and_waits_for_the_started_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    provider = _provider(monkeypatch, client, spot=True)
    announced: list[str] = []
    with sandbox_created_scope(lambda provider, sandbox_id: announced.append(sandbox_id) or _none()):
        sandbox = await provider.create_sandbox(
            _request(network_block_all=True, labels={"run": "r1", "name": "agent-sandbox"})
        )

    assert client.images.pulled == [("python:3.13-slim", {"region": "us", "wait": False})]
    _, image, kwargs = client.calls[0]
    assert image == "img-1"
    assert kwargs["env"] == {"A": "1", "DAYTONA_API_URL": "u"}
    assert kwargs["resources"] == v2_resources(Resources(vcpu=1, memory=1, disk=3))
    assert (kwargs["spot"], kwargs["region"], kwargs["idempotency_key"]) == (True, "us", "tts-task-1")
    assert kwargs["labels"] == {"run": "r1"}
    assert kwargs["network_policy"] == {"block_all": True}
    assert kwargs["lifecycle_policy"] == {
        "timeout_seconds": 24 * 60 * 60,
        "on_timeout": "delete",
        "idle_expiry_seconds": 900,
        "on_idle_timeout": "delete",
    }
    assert (kwargs["wait_seconds"], kwargs["create_timeout_seconds"]) == (0, 120)
    assert announced == ["sbx-1"]
    assert sandbox.created_at == datetime(2026, 11, 11, tzinfo=UTC)
    assert dict(sandbox.provider_metadata) == {
        "region_id": "us-east-ash-1",
        "cpu": "0.25",
        "memory_mib": "2048",
        "spot": "False",
    }


async def _none() -> None:
    return None


class EarlyWaitSandbox(FakeSandbox):
    async def wait(self, *, until: str, timeout: float) -> FakeSandbox:
        return self


@pytest.mark.description(
    "A create whose SDK wait returns while v2 still reads the sandbox pending waits until it starts."
)
async def test_create_waits_until_v2_reads_the_sandbox_started(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(EarlyWaitSandbox(state="creating"))
    client.sandbox.after_refresh = "started"

    sandbox = await _provider(monkeypatch, client).create_sandbox(_request())

    assert sandbox.state == "started"


@pytest.mark.description("A create whose SDK wait returns early for a sandbox that then errors is deleted.")
async def test_create_after_an_early_wait_deletes_a_sandbox_that_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(EarlyWaitSandbox(state="creating"))
    client.sandbox.after_refresh = "error"

    with pytest.raises(SandboxError, match="did not start: error"):
        await _provider(monkeypatch, client).create_sandbox(_request())
    assert client.deleted == ["sbx-1"]


async def test_sandbox_that_does_not_start_is_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient(FakeSandbox(state="error"))
    with pytest.raises(SandboxError, match="did not start"):
        await _provider(monkeypatch, client).create_sandbox(_request())
    assert client.deleted == ["sbx-1"]


@pytest.mark.description(
    "A create keeps a sandbox whose start wait was rate limited, and deletes a failed one past a rate-limited delete."
)
async def test_create_rides_out_rate_limits_on_the_start_wait_and_the_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_backoff", wait_fixed(0))
    limited = DaytonaError("x", status_code=429, code="RATE_LIMITED")

    class LimitedOnceSandbox(FakeSandbox):
        limited = True

        async def wait(self, *, until: str, timeout: float) -> FakeSandbox:
            if self.limited:
                self.limited = False
                raise limited
            return await super().wait(until=until, timeout=timeout)

    class LimitedDeleteClient(FakeClient):
        async def delete(self, sandbox_id: str) -> None:
            if not self.calls or self.calls[-1][0] != "delete":
                self.calls.append(("delete", sandbox_id, {}))
                raise limited
            await super().delete(sandbox_id)

    assert (
        await _provider(monkeypatch, FakeClient(LimitedOnceSandbox())).create_sandbox(_request())
    ).state == "started"

    client = LimitedDeleteClient(FakeSandbox(state="error"))
    with pytest.raises(SandboxError, match="did not start"):
        await _provider(monkeypatch, client).create_sandbox(_request())
    assert client.deleted == ["sbx-1"]


@pytest.mark.parametrize(
    ("status", "build_error", "expected"),
    [("failed", "", "img-1 failed: broken"), ("raise", "no space left", "image failed")],
)
async def test_failed_image_is_a_build_error(
    monkeypatch: pytest.MonkeyPatch, status: str, build_error: str, expected: str
) -> None:
    client = FakeClient()
    client.images.status = status
    with pytest.raises(SandboxBuildError, match=expected):
        await _provider(monkeypatch, client).create_sandbox(_request())
    assert client.calls == []


async def test_unsupported_features_fail_before_any_api_call(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    with pytest.raises(SandboxError, match="gpu, volumes"):
        await _provider(monkeypatch, client).create_sandbox(
            _request(resources=Resources(vcpu=1, memory=1, disk=3, gpu=1), volumes=[{"name": "v", "mount_path": "/v"}])
        )
    assert client.calls == [] and client.images.pulled == []


@pytest.mark.description(
    "A rate-limited Daytona v2 call waits the delay the server gives, and one that stays limited names the code."
)
async def test_rate_limited_calls_are_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    limited = DaytonaError("x", status_code=429, code="RATE_LIMITED", retry_after_seconds=0)
    calls = 0

    async def get(sandbox_id: str) -> FakeSandbox:
        nonlocal calls
        calls += 1
        if calls <= 8:
            raise limited if calls % 2 else DaytonaError("x", status_code=429, retry_after_ms=0)
        return client.sandbox

    monkeypatch.setattr(client, "get", get)
    monkeypatch.setattr(daytona_v2, "_RATE_LIMIT_SECONDS", 60)
    provider = _provider(monkeypatch, client)
    assert (await provider.get_sandbox("sbx-1")).id == "sbx-1"
    assert calls == 9
    monkeypatch.setattr(daytona_v2, "_RATE_LIMIT_SECONDS", 0.05)

    responses = iter([httpx.Response(429, json={"retry_after_seconds": 0}), httpx.Response(200, json={})])
    provider._http = httpx.AsyncClient(  # pyright: ignore[reportPrivateUsage]
        base_url="https://api.daytona.com", transport=httpx.MockTransport(lambda request: next(responses))
    )
    assert (await provider.get_sandbox("sbx-1")).id == "sbx-1"

    client.create_error = limited
    with pytest.raises(SandboxConnectionError, match="HTTP 429 RATE_LIMITED"):
        await provider.create_sandbox(_request())
    assert len(client.calls) > 1


@pytest.mark.description(
    "A create that stays rate limited gives up at the request's start deadline, not the longer retry budget."
)
async def test_rate_limited_create_stops_at_its_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_RATE_LIMIT_SECONDS", 60)
    monkeypatch.setattr(daytona_v2, "_backoff", wait_fixed(0.05))
    client = FakeClient()
    client.create_error = DaytonaError("x", status_code=429, code="RATE_LIMITED")
    started = time.monotonic()
    with pytest.raises(SandboxConnectionError, match="HTTP 429 RATE_LIMITED"):
        await _provider(monkeypatch, client).create_sandbox(_request(create_timeout=1))
    assert time.monotonic() - started < 5 and len(client.calls) > 1


@pytest.mark.description(
    "A task name that starts with a Daytona v2 reserved prefix gets a `vals-` prefix, and the idempotency key keeps it."
)
async def test_reserved_names_are_prefixed(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    await _provider(monkeypatch, client).create_sandbox(_request(name="build-pmars_ae1f28"))
    _, _, kwargs = client.calls[0]
    assert (kwargs["name"], kwargs["idempotency_key"]) == ("vals-build-pmars_ae1f28", "build-pmars_ae1f28")


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (DaytonaError("x", status_code=403, code="ORG_SUSPENDED"), SandboxAccountRejectedError),
        (DaytonaError("x", status_code=402, code="PROJECT_BUDGET_EXCEEDED"), SandboxAccountRejectedError),
        (DaytonaError("x", status_code=402), SandboxAccountRejectedError),
        (DaytonaError("x", status_code=404, code="IMAGE_NOT_FOUND"), SandboxRejectedError),
        (DaytonaError("x", status_code=404, code="SANDBOX_NOT_FOUND"), SandboxNotFoundError),
        (DaytonaError("x", status_code=429, code="RATE_LIMITED", retry_after_seconds=0), SandboxConnectionError),
        (DaytonaError("x", status_code=503, code="OVERLOADED"), SandboxConnectionError),
        (daytona_v2.DaytonaConnectionError("reset"), SandboxConnectionError),
        (DaytonaError("x", status_code=400, code="BAD_RANGE"), SandboxError),
    ],
)
async def test_create_classifies_refusals_by_code(
    monkeypatch: pytest.MonkeyPatch, error: DaytonaError, expected: type[SandboxError]
) -> None:
    client = FakeClient()
    client.create_error = error
    with pytest.raises(SandboxError) as exc_info:
        await _provider(monkeypatch, client).create_sandbox(_request())
    assert type(exc_info.value) is expected


@pytest.mark.parametrize(
    ("frames", "wait", "exit_code"),
    [
        ([_frame("hi "), _frame("err", "stderr"), _frame("x", "system")], _wait(3), 3),
        ([], _wait(None, "timed_out"), 124),
        ([], _wait(None, "signaled", "KILL"), 137),
    ],
)
async def test_exec_collects_output_and_maps_exit_codes(
    frames: list[LogFrame], wait: WaitResult, exit_code: int
) -> None:
    inner = FakeSandbox()
    inner.process = FakeProcess(frames, wait)
    result = await _sandbox(inner).exec("echo hi", cwd="/w", timeout=5)
    assert result.exit_code == exit_code
    assert result.output == ("hi err" if frames else "")
    assert inner.process.started == [{"command": "echo hi", "cwd": "/w", "env": None, "execution_timeout": 5}]


@pytest.mark.description("An exec whose log read loses its connection reads the log again instead of failing.")
async def test_exec_rereads_the_log_after_a_dropped_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_backoff", wait_fixed(0))

    class DroppingProcess(FakeProcess):
        dropped = False

        async def logs(self, *, cursor: str | None = None) -> SimpleNamespace:
            if not self.dropped:
                self.dropped = True
                raise daytona_v2.DaytonaConnectionError("ConnectionTerminated")
            return await super().logs(cursor=cursor)

    inner = FakeSandbox()
    inner.process = DroppingProcess([_frame("hi")], _wait(0))
    result = await _sandbox(inner).exec("echo hi")
    assert (result.exit_code, result.output) == (0, "hi")


async def test_command_streams_output_raises_on_failure_and_stops_when_closed() -> None:
    inner = FakeSandbox()
    inner.process = FakeProcess([_frame("a")], _wait(2))
    sandbox = _sandbox(inner)
    with pytest.raises(SandboxCommandError):
        assert [line async for line in sandbox.command("x", env_vars={"K": "v"})] == ["a"]
    assert inner.process.killed is False

    inner.process = FakeProcess([_frame("a")], hang=True)
    stream = sandbox.command("x")
    assert await anext(stream) == "a"
    await stream.aclose()
    assert inner.process.killed is True


async def test_missing_file_is_not_a_lost_sandbox_until_the_sandbox_is_gone() -> None:
    inner = FakeSandbox()
    sandbox = _sandbox(inner)
    await sandbox.upload_file("/w/a.txt", b"hi")
    assert await sandbox.download_file("/w/a.txt") == b"hi"
    with pytest.raises(SandboxError, match="/w/b.txt not found") as exc_info:
        await sandbox.download_file("/w/b.txt")
    assert not isinstance(exc_info.value, SandboxNotFoundError)

    inner.after_refresh = "destroyed"
    with pytest.raises(SandboxNotFoundError):
        await sandbox.download_file("/w/b.txt")


@pytest.mark.description("A sandbox deleted mid-task reports the state, error, eviction and last activity v2 recorded.")
async def test_lost_sandbox_reports_why_v2_deleted_it() -> None:
    evicted = datetime(2026, 11, 21, 23, 30, tzinfo=UTC)
    inner = FakeSandbox(
        error="host lost",
        spot_evicted_at=evicted,
        lifecycle_policy=SandboxLifecycle(last_activity_at=evicted, last_activity_source="exec"),
    )
    inner.after_refresh = "destroyed"
    with pytest.raises(SandboxNotFoundError) as exc_info:
        await _sandbox(inner).download_file("/w/missing.txt")
    assert str(exc_info.value) == (
        f"sandbox sbx-1 was deleted: state=destroyed, error=host lost, spot_evicted_at={evicted}, "
        f"last_activity_at={evicted}, last_activity_source=exec"
    )


@pytest.mark.parametrize("state", ["destroying", "destroyed"])
async def test_get_sandbox_treats_a_destroyed_sandbox_as_gone(monkeypatch: pytest.MonkeyPatch, state: str) -> None:
    with pytest.raises(SandboxNotFoundError):
        await _provider(monkeypatch, FakeClient(FakeSandbox(state=state))).get_sandbox("sbx-1")


async def test_list_sandboxes_pages_by_labels_bounds_creation_and_skips_destroyed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    client.sandboxes = {"a": FakeSandbox("a"), "b": FakeSandbox("b", state="destroyed"), "c": FakeSandbox("c")}
    client.pages = [
        SimpleNamespace(items=[SimpleNamespace(id="a"), SimpleNamespace(id="b")], next_cursor="n1"),
        SimpleNamespace(items=[SimpleNamespace(id="c")], next_cursor=None),
    ]
    cutoff = datetime(2026, 11, 1, tzinfo=UTC)
    query = SandboxQuery(labels={"run": "r1"}, page_size=2, created_at_lte=cutoff)
    found = [sandbox async for sandbox in _provider(monkeypatch, client).list_sandboxes(query)]

    assert [sandbox.id for sandbox in found] == ["a", "c"]
    assert [call[2] for call in client.calls] == [
        {"labels": {"run": "r1"}, "created_end_at": cutoff + timedelta(microseconds=1), "limit": 2, "cursor": None},
        {"labels": {"run": "r1"}, "created_end_at": cutoff + timedelta(microseconds=1), "limit": 2, "cursor": "n1"},
    ]


async def test_egress_changes_replace_the_policy_and_wait_until_applied() -> None:
    inner = FakeSandbox()
    inner.policy_states = [
        SimpleNamespace(etag="e1"),
        SimpleNamespace(status="applied", applied_revision=2, error=None),
    ]
    await _sandbox(inner).block_all_egress()
    assert inner.policies == [{"block_all": True, "allow_cidrs": [], "allow_domains": None, "if_match": "e1"}]

    inner.policy_states = [
        SimpleNamespace(etag="e2"),
        SimpleNamespace(status="failed", applied_revision=1, error="bad"),
    ]
    with pytest.raises(SandboxError, match="could not apply"):
        await _sandbox(inner).clear_egress_rules()


class PolicyInTransitionSandbox(FakeSandbox):
    def __init__(self, conflict: str, busy: int) -> None:
        super().__init__()
        self.conflict, self.busy = conflict, busy

    async def update_network_policy(self, *, network_policy: dict[str, Any], if_match: str) -> SimpleNamespace:
        if self.busy:
            self.busy -= 1
            raise ConflictError("conflict", status_code=409, code=self.conflict)
        return await super().update_network_policy(network_policy=network_policy, if_match=if_match)


@pytest.mark.description("An egress change waits out a policy transition that is still in progress.")
async def test_egress_changes_wait_out_a_policy_transition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    inner = PolicyInTransitionSandbox("network_policy_transition_in_progress", busy=2)
    inner.policy_states = [
        SimpleNamespace(etag="e1"),
        SimpleNamespace(etag="e1"),
        SimpleNamespace(etag="e2"),
        SimpleNamespace(status="applied", applied_revision=2, error=None),
    ]
    await _sandbox(inner).clear_egress_rules()
    assert inner.policies == [{"block_all": False, "allow_cidrs": [], "allow_domains": None, "if_match": "e2"}]


@pytest.mark.description("A policy transition that outlasts the policy timeout fails the egress change.")
async def test_egress_changes_stop_waiting_for_a_policy_transition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    monkeypatch.setattr(daytona_v2, "_POLICY_TIMEOUT_SECONDS", 0)
    inner = PolicyInTransitionSandbox("network_policy_transition_in_progress", busy=5)
    with pytest.raises(SandboxError, match="409"):
        await _sandbox(inner).clear_egress_rules()
    assert inner.busy == 4


@pytest.mark.description("An egress change fails at once on a policy conflict other than a transition.")
async def test_egress_changes_fail_on_other_policy_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    inner = PolicyInTransitionSandbox("etag_mismatch", busy=5)
    with pytest.raises(SandboxError, match="409"):
        await _sandbox(inner).clear_egress_rules()
    assert inner.busy == 4


def test_cost_is_cores_and_memory_over_lifetime() -> None:
    ended = datetime(2026, 11, 11, 0, 30, tzinfo=UTC)
    on_demand = _sandbox(FakeSandbox()).cost_usd(ended)
    spot = _sandbox(FakeSandbox(spot=True)).cost_usd(ended)
    earth = _sandbox(FakeSandbox(region="earth")).cost_usd(ended)
    assert on_demand is not None and math.isclose(on_demand, 2.0 * (0.25 * 0.235 + 2 * 0.0162) / 2)
    assert spot is not None and math.isclose(spot, 0.7 * on_demand)
    assert earth is not None and math.isclose(earth, on_demand / 2)


@pytest.mark.parametrize("info", [{"cpu": None}, {"spot": None}, {"region": None}, {"region": "eu-central-fra-1"}])
def test_cost_needs_resources_and_a_known_region(info: dict[str, Any]) -> None:
    assert _sandbox(FakeSandbox(**info)).cost_usd(datetime(2026, 11, 11, 1, tzinfo=UTC)) is None


async def test_cost_needs_created_at(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox = await _provider(monkeypatch, FakeClient(), created_at=None).get_sandbox("sbx-1")
    assert sandbox.cost_usd(datetime(2026, 11, 11, 1, tzinfo=UTC)) is None


def test_compose_sandbox_is_priced_as_its_outer_sandbox() -> None:
    outer = _sandbox(FakeSandbox(state="destroyed"))
    compose = ComposeSandbox(outer, ComposeSource(outer=ImageSource(image="docker:28.3.3-dind")))
    ended = datetime(2026, 11, 11, 0, 30, tzinfo=UTC)
    assert compose.has_ended is True
    assert compose.cost_usd(ended) == outer.cost_usd(ended)


@pytest.mark.parametrize(("state", "ended"), [("started", False), ("destroying", True), ("stopped", True)])
def test_has_ended_follows_state(state: str, ended: bool) -> None:
    assert _sandbox(FakeSandbox(state=state)).has_ended is ended


def test_only_daytona_v2_is_priced_from_rates_and_it_checkpoints_and_forks(monkeypatch: pytest.MonkeyPatch) -> None:
    assert priced_provider_types() == ["daytona_v2"]
    assert _provider(monkeypatch, FakeClient()).capabilities == SandboxCapabilities(
        checkpoints=frozenset(CheckpointKind), fork=True
    )


@pytest.mark.parametrize(("kind", "ref"), [(CheckpointKind.FILES, "img-cp"), (CheckpointKind.MEMORY, "snap-cp-1")])
async def test_checkpoint_waits_until_ready_and_deletes_a_failed_capture(kind: CheckpointKind, ref: str) -> None:
    client = FakeClient()
    sandbox = _sandbox(client.sandbox, client)
    assert await sandbox.checkpoint(kind, "cp-1") == Checkpoint(provider="daytona_v2", kind=kind, ref=ref)

    artifacts = client.images if kind == CheckpointKind.FILES else client.snapshots
    artifacts.status = "failed"
    with pytest.raises(SandboxError, match="did not become ready"):
        await sandbox.checkpoint(kind, "cp-1")
    assert artifacts.deleted == [ref]


class PendingVmSandbox(FakeSandbox):
    def __init__(self, conflict: str, pending: int) -> None:
        super().__init__()
        self.conflict, self.pending, self.captures = conflict, pending, 0

    async def _capture(self) -> None:
        self.captures += 1
        if self.captures <= self.pending:
            raise ConflictError(self.conflict, status_code=409)

    async def capture_image(self) -> SimpleNamespace:
        await self._capture()
        return await super().capture_image()

    async def create_snapshot(self, name: str) -> SimpleNamespace:
        await self._capture()
        return await super().create_snapshot(name)


@pytest.mark.description("A checkpoint taken while v2 still reports the VM pending is retried until the VM runs.")
@pytest.mark.parametrize(("kind", "ref"), [(CheckpointKind.FILES, "img-cp"), (CheckpointKind.MEMORY, "snap-cp-1")])
async def test_checkpoint_retries_while_the_vm_is_pending(
    monkeypatch: pytest.MonkeyPatch, kind: CheckpointKind, ref: str
) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    client = FakeClient()
    client.sandbox = PendingVmSandbox("conflict: vm status is pending, snapshots require a running vm", pending=2)
    assert await _sandbox(client.sandbox, client).checkpoint(kind, "cp-1") == Checkpoint(
        provider="daytona_v2", kind=kind, ref=ref
    )
    assert client.sandbox.captures == 3


@pytest.mark.description("A pending-VM conflict that outlasts the retry window fails the checkpoint.")
async def test_checkpoint_stops_retrying_a_pending_vm_at_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    monkeypatch.setattr(daytona_v2, "_VM_PENDING_SECONDS", 0)
    client = FakeClient()
    client.sandbox = PendingVmSandbox("conflict: vm status is pending, snapshots require a running vm", pending=5)
    with pytest.raises(SandboxError, match="vm status is pending"):
        await _sandbox(client.sandbox, client).checkpoint(CheckpointKind.MEMORY, "cp-1")
    assert client.sandbox.captures == 1


@pytest.mark.description("A checkpoint conflict other than a pending VM fails at once.")
async def test_checkpoint_fails_on_other_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daytona_v2, "_POLL_SECONDS", 0)
    client = FakeClient()
    client.sandbox = PendingVmSandbox("conflict: sandbox is stopped", pending=5)
    with pytest.raises(SandboxError, match="409"):
        await _sandbox(client.sandbox, client).checkpoint(CheckpointKind.MEMORY, "cp-1")
    assert client.sandbox.captures == 1


async def test_fork_keeps_memory_and_waits_for_the_copy() -> None:
    client = FakeClient()
    fork = await _sandbox(client.sandbox, client).fork("fork-1")
    assert client.sandbox.forks == [{"name": "fork-1", "memory": True, "wait_seconds": 0}]
    assert fork.id == "sbx-fork"


async def test_restoring_a_memory_checkpoint_creates_from_it_and_opens_egress(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    client.sandbox.policy_states = [
        SimpleNamespace(etag="e1"),
        SimpleNamespace(status="applied", applied_revision=2, error=None),
    ]
    saved = Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref="snap-1")
    await _provider(monkeypatch, client).create_sandbox(_request(source=saved, env_vars={}))

    kind, snapshot, kwargs = client.calls[0]
    assert (kind, snapshot, kwargs["idempotency_key"], kwargs["region"]) == ("restore", "snap-1", "tts-task-1", "us")
    assert client.sandbox.policies[0]["block_all"] is False


@pytest.mark.description(
    "A memory restore keeps fresh env off the snapshot create and gives it to every command it runs."
)
async def test_restoring_a_memory_checkpoint_hands_its_env_to_each_command(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    client.sandbox.policy_states = [
        SimpleNamespace(etag="e1"),
        SimpleNamespace(status="applied", applied_revision=2, error=None),
    ]
    saved = Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref="snap-1")
    sandbox = await _provider(monkeypatch, client).create_sandbox(_request(source=saved))

    assert "env" not in client.calls[0][2]
    assert client.calls[0][2]["labels"] == {"run": "r1", "cbs-command-env": "true"}
    await sandbox.exec("echo hi")
    assert [chunk async for chunk in sandbox.command("echo hi", env_vars={"A": "2", "B": "3"})] == []
    assert [start["env"] for start in client.sandbox.process.started] == [
        {"A": "1", "DAYTONA_API_URL": "u"},
        {"A": "2", "DAYTONA_API_URL": "u", "B": "3"},
    ]


@pytest.mark.description(
    "A memory-restored sandbox reopened by ID reads its saved env at its first command, not when it is opened."
)
async def test_reopening_a_memory_restored_sandbox_reads_its_saved_env(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    client.sandbox.policy_states = [
        SimpleNamespace(etag="e1"),
        SimpleNamespace(status="applied", applied_revision=2, error=None),
    ]
    saved = Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref="snap-1")
    await _provider(monkeypatch, client).create_sandbox(_request(source=saved))

    labels = {"run": "r1", "cbs-command-env": "true"}
    saved_env = client.sandbox.fs.files.pop("/etc/cbs-command-env.json")
    reopened = await _provider(monkeypatch, client, labels=labels).get_sandbox("sbx-1")
    client.sandbox.fs.files["/etc/cbs-command-env.json"] = saved_env
    await reopened.exec("echo hi")
    assert client.sandbox.process.started[-1]["env"] == {"A": "1", "DAYTONA_API_URL": "u"}


@pytest.mark.description("A memory restore that asks to block all egress is rejected before any sandbox is created.")
async def test_restoring_a_memory_checkpoint_blocked_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    saved = Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref="snap-1")
    with pytest.raises(SandboxError, match="network_block_all"):
        await _provider(monkeypatch, client).create_sandbox(_request(source=saved, network_block_all=True))
    assert client.calls == []


async def test_restoring_a_files_checkpoint_boots_its_image(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    saved = Checkpoint(provider="daytona_v2", kind=CheckpointKind.FILES, ref="img-cp")
    await _provider(monkeypatch, client).create_sandbox(_request(source=saved))
    assert client.calls[0][:2] == ("create", "img-cp") and client.images.pulled == []


async def test_delete_checkpoint_waits_out_conflicts_and_ignores_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    provider = _provider(monkeypatch, client)
    client.snapshots.delete_errors = [ConflictError("in use", status_code=409)]
    await provider.delete_checkpoint(Checkpoint(provider="daytona_v2", kind=CheckpointKind.MEMORY, ref="snap-1"))
    assert client.snapshots.deleted == ["snap-1"]

    client.images.delete_errors = [NotFoundError("gone", status_code=404)]
    await provider.delete_checkpoint(Checkpoint(provider="daytona_v2", kind=CheckpointKind.FILES, ref="img-1"))
    assert client.images.deleted == []
