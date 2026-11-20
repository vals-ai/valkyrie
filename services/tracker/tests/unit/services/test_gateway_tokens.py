import json
from typing import Any, cast

import httpx
import asyncio
import time

import pytest
from benchmark_service import Sandbox
from benchmark_service.sandbox.types import ExecResult
from cachetools import TTLCache

import tracker.services.gateway.tokens as tokens
from tracker.clients.model_gateway import tenant
from tracker.exceptions import InvalidSandboxConfigurationError, ModelGatewayConfigurationError
from tracker.services.gateway.tokens import GatewayTokenRotation, scoped_gateway_url, task_scoped_gateway_key


IDENTITY = {"benchmark_name": "swebench", "agent_name": "opencode"}
STATIC_KEY = "sk-static"
TOKEN = "mgwt_scoped"
MODEL = "openai/gpt-4o"


def _registry_entry(provider: str, key: str, *, chat: bool = True, deprecated: bool = False) -> dict[str, Any]:
    return {
        "provider_name": provider,
        "full_key": key,
        "properties": {} if chat else None,
        "metadata": {"deprecated": deprecated},
    }


@pytest.fixture(autouse=True)
def isolated_registry_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tenant, "_registry_cache", TTLCache(maxsize=128, ttl=60))


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
        registry_status: int = 200,
        registry_models: dict[str, Any] | None = None,
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
        self.registry_status = registry_status
        self.registry_models = (
            registry_models if registry_models is not None else {MODEL: _registry_entry("openai", MODEL)}
        )
        self.registry_requests: list[httpx.Request] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self))
        monkeypatch.setattr(tenant, "_client", client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/registry":
            self.registry_requests.append(request)
            return httpx.Response(self.registry_status, json={"models": self.registry_models})
        self.requests.append((request.url.path, json.loads(request.content), request.headers.get("Authorization")))
        self.session_headers.append(request.headers.get("X-SSP-Session-ID"))
        self.control_headers.append(request.headers.get("X-SSP-Control-Token"))
        self.urls.append(str(request.url))
        if request.url.path == "/service-auth":
            self.mints += 1
            token = TOKEN if self.mints == 1 else f"{TOKEN}-{self.mints}"
            if self.mint_status >= 400:
                return httpx.Response(
                    self.mint_status,
                    json={"message": f"Unknown model(s): invalid-model. Credential: {STATIC_KEY}", "token": TOKEN},
                )
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
    monkeypatch.setattr(tokens, "PASSKEY_ORGS", frozenset({"passkey-org"}))
    with pytest.raises(InvalidSandboxConfigurationError):
        async with _scoped(env, org_name="passkey-org", attested_model=attested_model):
            pass
    assert gateway.requests == []


@pytest.mark.description("A task from an approved identity org runs on a scoped token.")
async def test_an_approved_identity_org_task_runs_on_a_scoped_token(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(tokens, "PASSKEY_ORGS", frozenset({"passkey-org"}))
    async with _scoped(_env(), org_name="passkey-org") as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN
    assert [path for path, _, _ in gateway.requests] == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.description("Transient Gateway mint responses remain HTTP errors and prevent sandbox creation.")
@pytest.mark.parametrize("status", [408, 429, 503])
async def test_a_failed_mint_stops_the_task(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    gateway = RecordingGateway(mint_status=status)
    gateway.install(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError) as error:
        async with _scoped(_env()):
            pytest.fail("the sandbox must not start without a scoped credential")

    assert error.value.response.status_code == status


class TestGatewayRegistry:
    @pytest.mark.description(
        "Registry membership includes aliases and transcription but limits wildcards to active chat providers."
    )
    @pytest.mark.parametrize(
        ("model", "expected"),
        [
            (MODEL, True),
            ("alias", True),
            ("voice/scribe", True),
            ("old/model", True),
            ("openai/future/*", True),
            ("OPENAI/*", True),
            ("/*", True),
            ("voice/*", False),
            ("old/*", False),
            ("unknown/*", False),
            ("openai/future", False),
            ("", False),
        ],
    )
    async def test_membership(self, monkeypatch: pytest.MonkeyPatch, model: str, expected: bool) -> None:
        gateway = RecordingGateway(
            registry_models={
                MODEL: _registry_entry("openai", MODEL),
                "alias": _registry_entry("openai", MODEL),
                "voice/scribe": _registry_entry("voice", "voice/scribe", chat=False),
                "old/model": _registry_entry("old", "old/model", deprecated=True),
            }
        )
        gateway.install(monkeypatch)

        registry = await tenant.gateway_registry("https://gateway.test", STATIC_KEY)

        assert registry is not None
        assert registry.serves(model) is expected
        assert gateway.registry_requests[0].headers["Authorization"] == f"Bearer {STATIC_KEY}"
        assert dict(gateway.registry_requests[0].url.params) == {
            "include_excluded_fields": "true",
            "include_transcription": "true",
            "include_alt_keys": "true",
        }

    @pytest.mark.description("Registry snapshots expire and stay isolated by Gateway URL and credential.")
    async def test_cache_scope_and_expiry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        clock = [0.0]
        monkeypatch.setattr(
            tenant,
            "_registry_cache",
            TTLCache[tuple[str, str], tenant.GatewayRegistry](maxsize=128, ttl=60, timer=lambda: clock[0]),
        )
        gateway = RecordingGateway()
        gateway.install(monkeypatch)

        first = await tenant.gateway_registry("https://gateway.test", STATIC_KEY)
        gateway.registry_models = {}
        cached = await tenant.gateway_registry("https://gateway.test", STATIC_KEY)
        other_key = await tenant.gateway_registry("https://gateway.test", "another-managed-key")
        other_url = await tenant.gateway_registry("https://another.test", STATIC_KEY)
        clock[0] = 61.0
        expired = await tenant.gateway_registry("https://gateway.test", STATIC_KEY)

        assert first is not None and first.serves(MODEL)
        assert cached is not None and cached.serves(MODEL)
        assert other_key is not None and not other_key.serves(MODEL)
        assert other_url is not None and not other_url.serves(MODEL)
        assert expired is not None and not expired.serves(MODEL)


class TestDirectProviderGatewayCredentials:
    @pytest.mark.description("Direct-provider tasks mint no Gateway token and receive no root Gateway configuration.")
    @pytest.mark.parametrize("models", [{MODEL: _registry_entry("openai", MODEL)}, {}])
    async def test_direct_provider_strips_root_configuration(
        self, monkeypatch: pytest.MonkeyPatch, models: dict[str, Any]
    ) -> None:
        gateway = RecordingGateway(registry_models=models)
        gateway.install(monkeypatch)
        env = _env(ELEVENLABS_API_KEY="direct-key", MODEL_GATEWAY_API_KEY_FILE="/root/key")

        async with _scoped(env, attested_model="eleven_v4") as scoped:
            assert scoped == {"ELEVENLABS_API_KEY": "direct-key"}
            assert scoped_gateway_url(scoped, attested_model="eleven_v4", org_name="vals.ai") is None

        assert gateway.mints == 0
        assert env["MODEL_GATEWAY_API_KEY"] == STATIC_KEY

    @pytest.mark.description("Unavailable or malformed registries defer task validation to token minting.")
    @pytest.mark.parametrize("failure", ["status", "network", "malformed"])
    async def test_registry_failure_still_mints(self, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
        gateway = RecordingGateway()

        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/registry":
                if failure == "network":
                    raise httpx.ConnectError("unavailable", request=request)
                return httpx.Response(503 if failure == "status" else 200, json={"invalid": True})

            return gateway(request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            monkeypatch.setattr(tenant, "_client", client)

            async with _scoped(_env(), attested_model="eleven_v4") as scoped:
                assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

        assert gateway.payload_for("/service-auth")["allowed_models"] == ["eleven_v4"]

    @pytest.mark.description(
        "Gateway mint client errors become non-transient configuration errors with sanitized Gateway details."
    )
    @pytest.mark.parametrize("status", [400, 401, 403, 422])
    async def test_mint_client_error(self, monkeypatch: pytest.MonkeyPatch, status: int) -> None:
        gateway = RecordingGateway(mint_status=status)
        gateway.install(monkeypatch)

        with pytest.raises(ModelGatewayConfigurationError, match="Unknown model\\(s\\): invalid-model") as error:
            async with _scoped(_env()):
                pytest.fail("a rejected mint must not start the sandbox")

        assert error.value.transient is False
        assert STATIC_KEY not in str(error.value)
        assert TOKEN not in str(error.value)

    @pytest.mark.description("A network error while minting remains a network error.")
    async def test_mint_network_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        gateway = RecordingGateway()

        def respond(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/service-auth":
                raise httpx.ConnectError("unavailable", request=request)

            return gateway(request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            monkeypatch.setattr(tenant, "_client", client)

            with pytest.raises(httpx.ConnectError):
                async with _scoped(_env()):
                    pytest.fail("a failed mint must not start the sandbox")


@pytest.mark.description("A failed lease revoke logs one warning and leaves the task result alone.")
async def test_revoke_failure_is_logged_and_does_not_fail_a_finished_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway(revoke_status=503)
    gateway.install(monkeypatch)
    warnings: list[str] = []
    monkeypatch.setattr(tokens.logger, "warning", lambda message, *args: warnings.append(message % args))

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
    monkeypatch.setattr(tokens, "AUTH_REQUIRED", True)
    env = _env(MODEL_GATEWAY_URL="http://169.254.169.254")

    with pytest.raises(ValueError):
        async with _scoped(env, org_name="tenant.example"):
            pytest.fail("the executor's key must not leave for a private host")

    assert gateway.requests == []


@pytest.mark.description("Tenant orgs also get a scoped `MODEL_GATEWAY_API_KEY` that is minted and revoked.")
async def test_the_gateway_stays_reachable_for_every_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(tokens, "AUTH_REQUIRED", True)
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
    return tokens.child_gateway_token(
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
    with pytest.raises(tokens.InvalidSandboxConfigurationError, match="gpu"):
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
    gateway = RecordingGateway(expires_at=time.time() + tokens.GATEWAY_TOKEN_TTL_SECONDS)
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
    monkeypatch.setattr(tokens, "GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS", lifetime - 0.05)
    rotation = GatewayTokenRotation()
    sandbox = RecordingSandbox()

    async with _scoped(_env(), rotation=rotation) as env, rotation.into(cast(Sandbox, sandbox)):
        assert env["MODEL_GATEWAY_API_KEY_FILE"] == tokens.GATEWAY_TOKEN_FILE
        assert env["MODEL_GATEWAY_API_KEY"] == TOKEN
        assert sandbox.uploads == [(f"{tokens.GATEWAY_TOKEN_FILE}.next", TOKEN.encode())]
        await sandbox.wait_for_uploads(3)

    assert [content for _, content in sandbox.uploads[:3]] == [
        TOKEN.encode(),
        f"{TOKEN}-2".encode(),
        f"{TOKEN}-3".encode(),
    ]
    assert "chmod 0600" in sandbox.commands[1]
    assert f"chown --reference={tokens.GATEWAY_TOKEN_FILE}" in sandbox.commands[2]
    mints = [payload for path, payload, _ in gateway.requests if path == "/service-auth"]
    assert all(mint["allowed_models"] == [MODEL] for mint in mints)
    assert gateway.revoked[:2] == ["lease-1", "lease-2"]
    assert sorted(gateway.revoked) == sorted(gateway.minted)


@pytest.mark.description("Teardown during the file write must not leave the just-minted lease alive for its full term.")
async def test_a_rotation_cancelled_mid_write_revokes_the_fresh_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    lifetime = 12 * 60 * 60
    gateway = RecordingGateway(lease_seconds=lifetime)
    gateway.install(monkeypatch)
    monkeypatch.setattr(tokens, "GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS", lifetime)
    writing = asyncio.Event()
    real_write = tokens._write_token  # pyright: ignore[reportPrivateUsage]

    async def stalled_write(sandbox: Sandbox, token: str, *, first: bool) -> None:
        if first:
            await real_write(sandbox, token, first=True)
            return
        writing.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(tokens, "_write_token", stalled_write)
    rotation = GatewayTokenRotation()
    sandbox = RecordingSandbox()

    async with _scoped(_env(), rotation=rotation), rotation.into(cast(Sandbox, sandbox)):
        await asyncio.wait_for(writing.wait(), timeout=2.0)

    assert gateway.minted == ["lease-1", "lease-2"]
    assert sorted(gateway.revoked) == ["lease-1", "lease-2"]
