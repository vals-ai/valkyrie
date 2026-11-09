import json
from typing import Any, cast

import httpx
import asyncio
import time

import pytest
from benchmark_service import Sandbox
from benchmark_service.sandbox.types import ExecResult

import tracker.runtime.model_gateway as model_gateway
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.runtime.model_gateway import GatewayTokenRotation, scoped_gateway_url, task_scoped_gateway_key


IDENTITY = {"benchmark_name": "swebench", "agent_name": "opencode"}
STATIC_KEY = "sk-static"
TOKEN = "mgwt_scoped"
MODEL = "openai/gpt-4o"


def _env(**overrides: str) -> dict[str, str]:
    env = {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": STATIC_KEY}
    env.update(overrides)
    return env


def _scoped(env: dict[str, str], **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "run_id": "run-1",
        "task_id": "task_0",
        "attempt_id": "attempt-1",
        "attested_model": MODEL,
        "companion_models": None,
        "identity": IDENTITY,
        "org_name": "vals.ai",
        "egress": "*",
    }
    kwargs.update(overrides)
    return task_scoped_gateway_key(env, **kwargs)


class RecordingGateway:
    def __init__(
        self,
        *,
        mint_status: int = 200,
        revoke_status: int = 200,
        expires_at: float = 1_800_000_000.0,
        lease_seconds: float | None = None,
    ) -> None:
        self.expires_at = expires_at
        self.lease_seconds = lease_seconds
        self.mint_status = mint_status
        self.revoke_status = revoke_status
        self.requests: list[tuple[str, dict[str, Any], str | None]] = []
        self.session_headers: list[str | None] = []
        self.control_headers: list[str | None] = []
        self.urls: list[str] = []
        self.mints = 0

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self))
        monkeypatch.setattr(model_gateway, "_client", client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.url.path, json.loads(request.content), request.headers.get("Authorization")))
        self.session_headers.append(request.headers.get("X-SSP-Session-ID"))
        self.control_headers.append(request.headers.get("X-SSP-Control-Token"))
        self.urls.append(str(request.url))
        if request.url.path == "/service-auth":
            self.mints += 1
            token = TOKEN if self.mints == 1 else f"{TOKEN}-{self.mints}"
            return httpx.Response(
                self.mint_status, json={"token": token, "lease_id": f"lease-{self.mints}", "expires_at": self._expiry()}
            )
        return httpx.Response(self.revoke_status)

    def _expiry(self) -> float:
        return self.expires_at if self.lease_seconds is None else time.time() + self.lease_seconds

    @property
    def paths(self) -> list[str]:
        return [path for path, _, _ in self.requests]

    def payload_for(self, path: str) -> dict[str, Any]:
        return next(payload for request_path, payload, _ in self.requests if request_path == path)

    @property
    def minted(self) -> list[str]:
        return [f"lease-{index}" for index in range(1, self.mints + 1)]

    @property
    def revoked(self) -> list[str]:
        return [payload["lease_id"] for path, payload, _ in self.requests if path == "/service-auth/revoke"]


@pytest.mark.description("The gateway mint request includes the sandbox's egress allowlist.")
async def test_scoped_credential_carries_the_sandbox_egress_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env(), egress=["https://gateway.test"]):
        pass

    assert gateway.payload_for("/service-auth")["egress"] == ["https://gateway.test"]


@pytest.mark.description(
    "Only the mint and revoke calls use the static gateway key, and the scoped lease is revoked at the end."
)
async def test_scoped_credential_replaces_the_static_key_and_is_revoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    env = _env(
        VALKYRIE_AGENT_MODEL="anthropic/claude-4-opus",
        VALKYRIE_AGENT_VARIANT="max",
        RUN_ID="someone-elses-run",
        TASK_ID="someone-elses-task",
        SOME_OTHER_SECRET="untouched",
    )
    async with _scoped(env) as scoped:
        assert scoped == {**env, "MODEL_GATEWAY_API_KEY": TOKEN}
        assert gateway.paths == ["/service-auth"]
    assert env["MODEL_GATEWAY_API_KEY"] == STATIC_KEY

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert gateway.payload_for("/service-auth") == {
        "run_id": "run-1",
        "task_id": "task_0",
        "attempt_id": "attempt-1",
        "allowed_models": [MODEL],
        "identity": IDENTITY,
        "egress": "*",
        "ttl_seconds": 7 * 24 * 60 * 60,
    }
    assert gateway.payload_for("/service-auth/revoke") == {"lease_id": "lease-1"}
    assert {authorization for _, _, authorization in gateway.requests} == {f"Bearer {STATIC_KEY}"}
    assert gateway.session_headers == [None, None]
    assert gateway.urls == ["https://gateway.test/service-auth", "https://gateway.test/service-auth/revoke"]


@pytest.mark.description("The mint request allows the contract's companion models along with the run's model.")
async def test_companion_models_are_allowed_alongside_the_run_model(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env(), companion_models="openai/gpt-5-2025-08-07,anthropic/claude-sonnet-5"):
        pass

    assert gateway.payload_for("/service-auth")["allowed_models"] == [
        MODEL,
        "openai/gpt-5-2025-08-07",
        "anthropic/claude-sonnet-5",
    ]


@pytest.mark.description("The mint request sets `allow_provider_tools` only when the contract allows provider tools.")
async def test_provider_tools_are_requested_only_when_the_contract_allows_them(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env()):
        pass
    assert "allow_provider_tools" not in gateway.payload_for("/service-auth")

    gateway.requests.clear()
    async with _scoped(_env(), allow_provider_tools=True):
        pass
    assert gateway.payload_for("/service-auth")["allow_provider_tools"] is True


@pytest.mark.description("An unattested or missing gateway configuration is left alone.")
@pytest.mark.parametrize(
    "attested_model,missing_env_key",
    [(None, None), (MODEL, "MODEL_GATEWAY_URL"), (MODEL, "MODEL_GATEWAY_API_KEY")],
)
async def test_unattested_or_missing_gateway_configuration_is_untouched(
    monkeypatch: pytest.MonkeyPatch, attested_model: str | None, missing_env_key: str | None
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(**{missing_env_key: ""}) if missing_env_key else _env()

    async with _scoped(env, attested_model=attested_model) as scoped:
        assert scoped == env

    assert gateway.requests == []


@pytest.mark.description(
    "A passkey-only org task is refused before any sandbox exists unless it can hold a scoped token."
)
@pytest.mark.parametrize(
    ("env", "attested_model"),
    [
        (_env(), None),
        ({"MODEL_GATEWAY_API_KEY": STATIC_KEY}, MODEL),
        ({"MODEL_GATEWAY_URL": "https://gateway.test"}, MODEL),
        ({}, MODEL),
    ],
)
async def test_an_approved_identity_org_never_gets_the_raw_key(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], attested_model: str | None
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "PASSKEY_ORGS", frozenset({"passkey-org"}))
    with pytest.raises(InvalidSandboxConfigurationError):
        async with _scoped(env, org_name="passkey-org", attested_model=attested_model):
            pass
    assert gateway.requests == []


@pytest.mark.description("A task from an approved identity org runs on a scoped token.")
async def test_an_approved_identity_org_task_runs_on_a_scoped_token(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "PASSKEY_ORGS", frozenset({"passkey-org"}))
    async with _scoped(_env(), org_name="passkey-org") as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN
    assert [path for path, _, _ in gateway.requests] == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.description("A failed gateway mint raises before the sandbox starts.")
async def test_a_failed_mint_stops_the_task(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway(mint_status=503)
    gateway.install(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError):
        async with _scoped(_env()):
            pytest.fail("the sandbox must not start without a scoped credential")


@pytest.mark.description("A failed lease revoke logs one warning and leaves the task result alone.")
async def test_revoke_failure_is_logged_and_does_not_fail_a_finished_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway(revoke_status=503)
    gateway.install(monkeypatch)
    warnings: list[str] = []
    monkeypatch.setattr(model_gateway.logger, "warning", lambda message, *args: warnings.append(message % args))

    async with _scoped(_env()) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert len(warnings) == 1
    assert "Could not revoke gateway lease lease-1" in warnings[0]


@pytest.mark.description("The gateway lease is revoked even when the task raises.")
async def test_the_credential_is_revoked_when_the_task_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(RuntimeError):
        async with _scoped(_env()):
            raise RuntimeError("agent blew up")

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.description("The gateway key is never sent to a private destination.")
async def test_the_key_is_not_sent_to_a_private_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "AUTH_REQUIRED", True)
    env = _env(MODEL_GATEWAY_URL="http://169.254.169.254")

    with pytest.raises(ValueError):
        async with _scoped(env, org_name="tenant.example"):
            pytest.fail("the executor's key must not leave for a private host")

    assert gateway.requests == []


@pytest.mark.description("Tenant orgs also get a scoped `MODEL_GATEWAY_API_KEY` that is minted and revoked.")
async def test_the_gateway_stays_reachable_for_every_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "AUTH_REQUIRED", True)
    env = _env(MODEL_GATEWAY_URL="https://model-gateway.vals.ai")

    async with _scoped(env, org_name="tenant.example") as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.description(
    "Mint and revoke send only the static key as a bearer token, with no session or control headers."
)
async def test_native_mint_and_revoke_never_send_ssp_control_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env()) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.urls == ["https://gateway.test/service-auth", "https://gateway.test/service-auth/revoke"]
    assert gateway.session_headers == [None, None]
    assert gateway.control_headers == [None, None]
    assert [authorization for _, _, authorization in gateway.requests] == [f"Bearer {STATIC_KEY}"] * 2


@pytest.mark.description("A given `attempt_id` is sent in the mint request.")
async def test_mint_names_the_task_attempt_when_given(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    async with _scoped(_env(), attempt_id="attempt-7"):
        pass
    assert gateway.payload_for("/service-auth")["attempt_id"] == "attempt-7"


@pytest.mark.description("The scoped gateway URL is built for the task.")
@pytest.mark.parametrize(
    ("env", "attested_model", "expected"),
    [
        (_env(MODEL_GATEWAY_URL="https://gateway.test/"), MODEL, "https://gateway.test"),
        ({"MODEL_GATEWAY_URL": "https://gateway.test"}, MODEL, None),
        (_env(), None, None),
    ],
)
def test_scoped_gateway_url(env: dict[str, str], attested_model: str | None, expected: str | None) -> None:
    assert scoped_gateway_url(env, attested_model=attested_model, org_name="vals.ai") == expected


def _child(env: dict[str, str], models: list[str]) -> Any:
    return model_gateway.child_gateway_token(
        env,
        profile="gpu",
        models=models,
        egress=["pypi.org"],
        run_id="run-1",
        task_id="task_0",
        attempt_id="attempt-1",
        identity=IDENTITY,
        org_name="vals.ai",
    )


@pytest.mark.description("A child profile with no models makes no gateway request and gets no credential.")
async def test_child_profile_without_models_gets_no_token(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _child(_env(), []) as scoped:
        assert scoped is None

    assert gateway.requests == []


@pytest.mark.description(
    "A child profile token allows only the child's models and egress hosts and is revoked at the end."
)
async def test_child_profile_token_is_scoped_to_its_models_and_revoked(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _child(_env(), ["a/b", "c/*"]) as scoped:
        assert scoped is not None
        assert (scoped.url, scoped.token) == ("https://gateway.test", TOKEN)

    (mint_path, mint_body, _), (revoke_path, _, _) = gateway.requests
    assert (mint_path, mint_body["allowed_models"], mint_body["attempt_id"], mint_body["egress"]) == (
        "/service-auth",
        ["a/b", "c/*"],
        "attempt-1",
        ["pypi.org"],
    )
    assert revoke_path == "/service-auth/revoke"


@pytest.mark.description(
    "A child profile that names models on a Tracker with no gateway raises `InvalidSandboxConfigurationError`."
)
async def test_child_profile_models_need_a_gateway() -> None:
    with pytest.raises(model_gateway.InvalidSandboxConfigurationError, match="gpu"):
        async with _child({}, ["a/b"]):
            pass


class RecordingSandbox:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []
        self.commands: list[str] = []
        self._uploaded = asyncio.Event()

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.uploads.append((remote_path, content))
        self._uploaded.set()

    async def wait_for_uploads(self, count: int) -> None:
        while len(self.uploads) < count:
            self._uploaded.clear()
            await asyncio.wait_for(self._uploaded.wait(), timeout=2.0)

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.commands.append(command)
        return ExecResult(exit_code=0)


@pytest.mark.description("A lease that spans the deadline is not rotated.")
async def test_a_lease_spanning_the_wall_is_not_rotated(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway(expires_at=time.time() + model_gateway.GATEWAY_TOKEN_TTL_SECONDS)
    gateway.install(monkeypatch)
    rotation = GatewayTokenRotation()
    sandbox = RecordingSandbox()

    async with _scoped(_env(), rotation=rotation) as env, rotation.into(cast(Sandbox, sandbox)):
        assert "MODEL_GATEWAY_API_KEY_FILE" not in env

    assert sandbox.uploads == []
    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.description(
    "A lease that expires early is reminted with the same models, and every minted lease is revoked."
)
async def test_a_shortened_lease_is_rotated_through_the_token_file(monkeypatch: pytest.MonkeyPatch) -> None:
    lifetime = 12 * 60 * 60
    gateway = RecordingGateway(lease_seconds=lifetime)
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS", lifetime - 0.05)
    rotation = GatewayTokenRotation()
    sandbox = RecordingSandbox()

    async with _scoped(_env(), rotation=rotation) as env, rotation.into(cast(Sandbox, sandbox)):
        assert env["MODEL_GATEWAY_API_KEY_FILE"] == model_gateway.GATEWAY_TOKEN_FILE
        assert env["MODEL_GATEWAY_API_KEY"] == TOKEN
        assert sandbox.uploads == [(f"{model_gateway.GATEWAY_TOKEN_FILE}.next", TOKEN.encode())]
        await sandbox.wait_for_uploads(3)

    assert [content for _, content in sandbox.uploads[:3]] == [
        TOKEN.encode(),
        f"{TOKEN}-2".encode(),
        f"{TOKEN}-3".encode(),
    ]
    assert "chmod 0600" in sandbox.commands[1]
    assert f"chown --reference={model_gateway.GATEWAY_TOKEN_FILE}" in sandbox.commands[2]
    mints = [payload for path, payload, _ in gateway.requests if path == "/service-auth"]
    assert all(mint["allowed_models"] == [MODEL] for mint in mints)
    assert gateway.revoked[:2] == ["lease-1", "lease-2"]
    assert sorted(gateway.revoked) == sorted(gateway.minted)


@pytest.mark.description("Teardown during the file write must not leave the just-minted lease alive for its full term.")
async def test_a_rotation_cancelled_mid_write_revokes_the_fresh_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    lifetime = 12 * 60 * 60
    gateway = RecordingGateway(lease_seconds=lifetime)
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS", lifetime)
    writing = asyncio.Event()
    real_write = model_gateway._write_token  # pyright: ignore[reportPrivateUsage]

    async def stalled_write(sandbox: Sandbox, token: str, *, first: bool) -> None:
        if first:
            await real_write(sandbox, token, first=True)
            return
        writing.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(model_gateway, "_write_token", stalled_write)
    rotation = GatewayTokenRotation()
    sandbox = RecordingSandbox()

    async with _scoped(_env(), rotation=rotation), rotation.into(cast(Sandbox, sandbox)):
        await asyncio.wait_for(writing.wait(), timeout=2.0)

    assert gateway.minted == ["lease-1", "lease-2"]
    assert sorted(gateway.revoked) == ["lease-1", "lease-2"]
