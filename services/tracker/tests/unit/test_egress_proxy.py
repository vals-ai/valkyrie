import hashlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from benchmark_service import ChildPolicy, ImageSource, Resources
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.egress_proxy.run_stage as proxy_run_stage
import tracker.runtime.run_egress as run_egress
import tracker.sandbox as sandbox_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import Benchmark, BenchmarkStatus, EgressProxyPolicy, Org, Task, TaskStatus
from tracker.executor.control_tasks import TaskControl
from tracker.egress import EgressPolicy, child_key_id
from tracker.egress_proxy.client import (
    install_proxy_ca,
    proxy_environment,
    route_providers_through_gateway,
    start_proxy_tunnel,
)
from tracker.egress_proxy.policy import AttemptStoppedError, PolicyStore, resolve_run_egress_proxy
from tracker.egress_proxy.run_stage import EgressProxy
from tracker.egress_proxy.settings import EgressProxyEndpoint, load_egress_proxy_endpoint
from tracker.exceptions import InvalidSandboxConfigurationError, SandboxSetupError
from tracker.runtime.run_egress import RunAttempt, RunStageAccess, SandboxAllowlist, SandboxEnvironment, ScopedGateway
from tracker.services.runs.stop import apply_shed_benchmark, apply_stop_benchmark
from tracker.services.tasks.execute import task_attempt_id

_ENDPOINT = EgressProxyEndpoint(("proxy.example", "proxy-2.example"), 3128, ("192.0.2.1/32",), "certificate")
_create_sandbox = getattr(sandbox_module, "_create_sandbox")


@pytest.mark.description("A run's egress policy resolves from its contract and settings.")
@pytest.mark.parametrize(
    ("policy", "expected_domains", "expected_methods"),
    [
        ("*", None, frozenset({"GET", "HEAD", "POST"})),
        (
            [
                "https://API.Example.com/path",
                "https://dev.model-gateway.vals.ai",
                "example.com:443",
                "example.com",
                "10.1.2.0/24",
            ],
            frozenset(
                {
                    "api.example.com",
                    "dev.model-gateway.vals.ai",
                    "example.com",
                    "10.1.2.0/24",
                }
            ),
            frozenset({"GET", "HEAD"}),
        ),
    ],
)
def test_resolve_run_policy(
    policy: EgressPolicy,
    expected_domains: frozenset[str] | None,
    expected_methods: frozenset[str],
) -> None:
    proxy = resolve_run_egress_proxy(policy, "attempt-1")

    assert proxy is not None
    assert proxy.domains == expected_domains
    assert proxy.methods == expected_methods
    assert proxy.token_sha256 == hashlib.sha256(proxy.token.encode()).hexdigest()


@pytest.mark.description("A blocked egress policy allows no domains but still routes to the model gateway.")
def test_blocked_policy_gets_a_credential_only_for_its_gateway() -> None:
    assert resolve_run_egress_proxy([], "attempt-1") is None
    assert resolve_run_egress_proxy([], "attempt-1", "http://gw.test") is None

    proxy = resolve_run_egress_proxy([], "attempt-1", "https://gw.test")
    assert proxy is not None
    assert proxy.domains == frozenset()
    assert proxy.gateway_url == "https://gw.test"


@pytest.mark.description("The egress proxy endpoint comes from the environment.")
@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ({}, None),
        ({"EGRESS_PROXY_HOSTS": "192.0.2.1"}, None),
        (
            {
                "EGRESS_PROXY_HOSTS": "192.0.2.1, 192.0.2.2",
                "EGRESS_PROXY_CIDRS": " 192.0.2.1/32 ,192.0.2.2/32,",
                "EGRESS_PROXY_CA_CERT": "c",
            },
            EgressProxyEndpoint(("192.0.2.1", "192.0.2.2"), 3128, ("192.0.2.1/32", "192.0.2.2/32"), "c"),
        ),
    ],
)
def test_endpoint_from_environment(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str], expected: EgressProxyEndpoint | None
) -> None:
    for name in ("EGRESS_PROXY_HOSTS", "EGRESS_PROXY_CIDRS", "EGRESS_PROXY_CA_CERT", "EGRESS_PROXY_PORT"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    assert load_egress_proxy_endpoint() == expected


@pytest.mark.description("Loading the egress proxy endpoint without `EGRESS_PROXY_CA_CERT` raises `ValueError`.")
def test_endpoint_without_a_ca_fails_at_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EGRESS_PROXY_HOSTS", "192.0.2.1")
    monkeypatch.setenv("EGRESS_PROXY_CIDRS", "192.0.2.1/32")
    monkeypatch.delenv("EGRESS_PROXY_CA_CERT", raising=False)

    with pytest.raises(ValueError, match="EGRESS_PROXY_CA_CERT"):
        load_egress_proxy_endpoint()


@pytest.mark.description("Provider requests are rewritten to the gateway only when its URL uses HTTPS.")
@pytest.mark.parametrize(
    ("gateway_url", "expected"),
    [("https://gw.test", "https://gw.test"), ("http://gw.test", None), (None, None)],
)
def test_only_an_https_gateway_receives_rewrites(gateway_url: str | None, expected: str | None) -> None:
    proxy = resolve_run_egress_proxy("*", "attempt-1", gateway_url)

    assert proxy is not None
    assert proxy.gateway_url == expected


@pytest.mark.description("An egress policy is stored until it expires and is deleted on revoke.")
def test_policy_is_stored_until_expiry_and_deleted_on_revoke(database_session: Session) -> None:
    benchmark = make_benchmark(session=database_session)
    task = _running_task(database_session, benchmark)
    proxy = resolve_run_egress_proxy("*", "attempt-1", "https://gw.test")
    assert proxy is not None
    expires_at = datetime.now(UTC) + timedelta(minutes=1)
    engine = cast(Engine, database_session.get_bind())

    PolicyStore(engine).grant(proxy, benchmark.id, task.id, expires_at)

    stored = database_session.get(EgressProxyPolicy, proxy.key_id)
    assert stored is not None
    assert stored.expires_at.replace(tzinfo=UTC) == expires_at
    assert stored.gateway_url == "https://gw.test"

    PolicyStore(engine).revoke(proxy.key_id)

    database_session.expire_all()
    assert database_session.get(EgressProxyPolicy, proxy.key_id) is None


@pytest.mark.description("No egress policy row is saved for an attempt stopped before its grant.")
def test_an_attempt_stopped_before_its_grant_gets_no_policy(database_session: Session) -> None:
    benchmark = make_benchmark(session=database_session)
    task = _running_task(database_session, benchmark)
    org = database_session.get(Org, benchmark.org_id)
    assert org is not None
    apply_stop_benchmark(benchmark, database_session, True, org)
    database_session.commit()
    proxy = resolve_run_egress_proxy("*", "attempt-1")
    assert proxy is not None

    with pytest.raises(AttemptStoppedError):
        PolicyStore(cast(Engine, database_session.get_bind())).grant(
            proxy, benchmark.id, task.id, datetime.now(UTC) + timedelta(minutes=1)
        )

    assert database_session.get(EgressProxyPolicy, proxy.key_id) is None


@pytest.mark.description("A forced stop revokes the stopped attempt's egress.")
@pytest.mark.parametrize(
    ("operation", "kept"),
    [
        ("force-stop", {"newer"}),
        ("force-stop-run", set[str]()),
        ("stop", {"older", "newer"}),
        ("shed", {"older"}),
    ],
)
def test_forced_stops_revoke_the_stopped_attempts_egress(
    database_session: Session, operation: str, kept: set[str]
) -> None:
    benchmark = make_benchmark(session=database_session)
    now = datetime.now(UTC)
    tasks = [
        make_task(benchmark, "older", status=TaskStatus.IN_PROGRESS, started_at=now - timedelta(minutes=2)),
        make_task(benchmark, "newer", status=TaskStatus.IN_PROGRESS, started_at=now - timedelta(minutes=1)),
    ]
    database_session.add_all(tasks)
    database_session.commit()
    attempts = {task_attempt_id(task.id, task.started_at): task.task_id for task in tasks}
    attempts |= {child_key_id(key_id, "gpu"): task_id for key_id, task_id in attempts.items()}
    database_session.add_all(
        EgressProxyPolicy(
            key_id=key_id,
            benchmark_id=benchmark.id,
            token_sha256="a" * 64,
            methods=["GET"],
            expires_at=now + timedelta(minutes=10),
        )
        for key_id in attempts
    )
    database_session.commit()
    org = database_session.get(Org, benchmark.org_id)
    assert org is not None

    if operation == "shed":
        apply_shed_benchmark(benchmark, 1, database_session, org)
    else:
        task_ids = ["older"] if operation == "force-stop" else None
        apply_stop_benchmark(benchmark, database_session, operation != "stop", org, task_ids=task_ids)
    database_session.commit()

    database_session.expire_all()
    remaining = database_session.exec(select(EgressProxyPolicy.key_id)).all()
    assert {attempts[key_id] for key_id in remaining} == kept


@pytest.mark.description("`revoke_finished` deletes policies of finished or expired attempts and keeps active ones.")
def test_finished_sweep_deletes_finished_and_expired_policies(database_session: Session) -> None:
    finished = make_benchmark(status=BenchmarkStatus.FINISHED, session=database_session)
    active = make_benchmark(name="active", session=database_session)
    now = datetime.now(UTC)
    database_session.add_all(
        [
            EgressProxyPolicy(
                key_id="finished",
                benchmark_id=finished.id,
                token_sha256="a" * 64,
                methods=["GET"],
                expires_at=now + timedelta(minutes=1),
            ),
            EgressProxyPolicy(
                key_id="expired",
                benchmark_id=active.id,
                token_sha256="b" * 64,
                methods=["GET"],
                expires_at=now - timedelta(seconds=1),
            ),
            EgressProxyPolicy(
                key_id="active",
                benchmark_id=active.id,
                token_sha256="c" * 64,
                methods=["GET"],
                expires_at=now + timedelta(minutes=1),
            ),
        ]
    )
    database_session.commit()

    engine = cast(Engine, database_session.get_bind())
    assert PolicyStore(engine).revoke_finished() == 2
    database_session.expire_all()
    assert list(database_session.exec(select(EgressProxyPolicy.key_id)).all()) == ["active"]


@pytest.mark.description("A proxied sandbox gets the proxy environment.")
def test_proxy_environment() -> None:
    env = proxy_environment("attempt-1", "token")

    assert env["HTTP_PROXY"] == env["https_proxy"] == "http://attempt-1:token@127.0.0.1:3128"
    assert env["NO_PROXY"] == env["no_proxy"] == "localhost,127.0.0.1,::1"
    with_compose = proxy_environment("attempt-1", "token", local_hosts=["api", "db"])
    assert with_compose["NO_PROXY"] == with_compose["no_proxy"] == "localhost,127.0.0.1,::1,api,db"
    assert env["SSL_CERT_FILE"] == env["REQUESTS_CA_BUNDLE"] == env["PIP_CERT"] == "/tmp/egress-proxy-ca-bundle.pem"
    assert env["NODE_EXTRA_CA_CERTS"] == "/tmp/egress-proxy-ca.crt"


@pytest.mark.description("Provider traffic routes through the gateway.")
@pytest.mark.parametrize(
    ("env_vars", "expected"),
    [
        (
            {
                "OPENAI_API_KEY": "sk-real",
                "ANTHROPIC_AUTH_TOKEN": "oauth",
                "GOOGLE_API_KEY": "g-real",
                "OPENROUTER_API_KEY": "or-real",
                "OTHER": "kept",
            },
            {
                "OPENAI_BASE_URL": "https://gw.test/passthrough/openai/v1",
                "OPENAI_API_KEY": "token",
                "ANTHROPIC_BASE_URL": "https://gw.test/passthrough/anthropic",
                "ANTHROPIC_API_KEY": "token",
                "GOOGLE_GEMINI_BASE_URL": "https://gw.test/passthrough/google",
                "GEMINI_API_KEY": "token",
                "OPENROUTER_BASE_URL": "https://gw.test/passthrough/openrouter/api/v1",
                "OPENROUTER_API_KEY": "token",
                "OTHER": "kept",
            },
        ),
        (
            {"OPENAI_BASE_URL": "https://router.test/v1", "OPENAI_API_KEY": "router-key"},
            {"OPENAI_BASE_URL": "https://router.test/v1", "OPENAI_API_KEY": "router-key"},
        ),
    ],
)
def test_route_providers_through_gateway(env_vars: dict[str, str], expected: dict[str, str]) -> None:
    routed = route_providers_through_gateway(SandboxEnvironment(env_vars), "https://gw.test/", "token")

    assert {name: routed.env_vars[name] for name in expected} == expected
    assert not {"sk-real", "oauth", "g-real", "or-real"} & set(routed.env_vars.values())
    assert routed.secrets == {}


@pytest.mark.description("Without an HTTPS gateway, the proxy keeps provider keys.")
@pytest.mark.parametrize("gateway", [None, ScopedGateway("http://gw.test", "token")])
def test_proxy_keeps_provider_keys_without_an_https_gateway(gateway: ScopedGateway | None) -> None:
    egress = EgressProxy(_ENDPOINT, cast(Engine, None))

    environment = SandboxEnvironment({"OPENAI_API_KEY": "sk-real"})

    assert egress.sandbox_environment(environment, gateway) == environment


@pytest.mark.description("A proxied sandbox keeps only the managed secrets that are not routed.")
@pytest.mark.parametrize(
    ("gateway", "env_vars", "expected_secrets"),
    [
        (ScopedGateway("https://gw.test", "token"), {}, {"TAVILY_API_KEY": "managed-tavily"}),
        (
            ScopedGateway("https://gw.test", "token"),
            {"OPENAI_BASE_URL": "https://router.test/v1"},
            {"OPENAI_API_KEY": "managed-openai", "TAVILY_API_KEY": "managed-tavily"},
        ),
        (None, {}, {"OPENAI_API_KEY": "managed-openai", "TAVILY_API_KEY": "managed-tavily"}),
        (
            ScopedGateway("http://gw.test", "token"),
            {},
            {"OPENAI_API_KEY": "managed-openai", "TAVILY_API_KEY": "managed-tavily"},
        ),
    ],
)
async def test_proxy_sandbox_keeps_only_unrouted_managed_secrets(
    gateway: ScopedGateway | None, env_vars: dict[str, str], expected_secrets: dict[str, str]
) -> None:
    egress = EgressProxy(_ENDPOINT, cast(Engine, None))
    managed = {"OPENAI_API_KEY": "managed-openai", "TAVILY_API_KEY": "managed-tavily"}
    environment = egress.sandbox_environment(SandboxEnvironment(env_vars, managed), gateway)
    provider = AsyncMock()

    await _create_sandbox(
        provider,
        "task",
        ImageSource(image="image"),
        Resources(vcpu=1, memory=2, disk=3),
        env_vars=environment.env_vars,
        sandbox_secrets=environment.secrets,
    )

    request = provider.create_sandbox.await_args.args[0]
    assert request.sandbox_secrets == expected_secrets
    assert request.env_vars == environment.env_vars
    assert environment.env_vars.get("OPENAI_API_KEY") == (
        "token" if gateway and gateway.url.startswith("https://") and "OPENAI_BASE_URL" not in env_vars else None
    )


@pytest.mark.description("The proxy keeps a provider that has a managed custom base URL.")
async def test_proxy_preserves_provider_with_managed_custom_base_url() -> None:
    egress = EgressProxy(_ENDPOINT, cast(Engine, None))
    environment = egress.sandbox_environment(
        SandboxEnvironment({}, {"OPENAI_BASE_URL": "managed-router-url", "OPENAI_API_KEY": "managed-router-key"}),
        ScopedGateway("https://gw.test", "token"),
    )
    provider = AsyncMock()

    await _create_sandbox(
        provider,
        "task",
        ImageSource(image="image"),
        Resources(vcpu=1, memory=2, disk=3),
        env_vars=environment.env_vars,
        sandbox_secrets=environment.secrets,
    )

    request = provider.create_sandbox.await_args.args[0]
    assert "OPENAI_BASE_URL" not in request.env_vars
    assert "OPENAI_API_KEY" not in request.env_vars
    assert request.sandbox_secrets == {
        "OPENAI_BASE_URL": "managed-router-url",
        "OPENAI_API_KEY": "managed-router-key",
    }


@pytest.mark.description("The proxy tunnel blocks traffic when the sandbox has no Python.")
@pytest.mark.parametrize(
    ("exit_code", "error_type", "error"),
    [
        (0, None, None),
        (3, InvalidSandboxConfigurationError, "needs python3"),
        (1, SandboxSetupError, "Failed to start"),
    ],
)
async def test_proxy_tunnel_fails_closed_without_python(
    exit_code: int, error_type: type[Exception] | None, error: str | None
) -> None:
    sandbox = SimpleNamespace(
        upload_file=AsyncMock(), exec=AsyncMock(return_value=SimpleNamespace(exit_code=exit_code))
    )

    if error_type is None:
        await start_proxy_tunnel(cast(Any, sandbox), _ENDPOINT)
    else:
        with pytest.raises(error_type, match=error):
            await start_proxy_tunnel(cast(Any, sandbox), _ENDPOINT)
    command = sandbox.exec.await_args.args[0]
    assert "/usr/local/bin" in command
    assert "proxy.example 3128" in command


def _running_task(session: Session, benchmark: Benchmark) -> Task:
    task = make_task(benchmark, "task", status=TaskStatus.IN_PROGRESS)
    session.add(task)
    session.commit()
    return task


def _attempt(benchmark_id: UUID, task_row_id: UUID | None = None) -> RunAttempt:
    return RunAttempt(benchmark_id, task_row_id or uuid4(), "attempt-1", datetime.now(UTC) + timedelta(minutes=1))


@pytest.mark.description("The allowlist run stage applies the run's allowed domains to the sandbox.")
async def test_allowlist_run_stage_applies_the_run_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    apply = AsyncMock()
    monkeypatch.setattr(run_egress, "apply_egress_policy", apply)

    async with SandboxAllowlist().run_stage(cast(Any, object()), ["example.com"], _attempt(uuid4())) as access:
        assert access == RunStageAccess()

    assert apply.await_args is not None and apply.await_args.args[1] == ["example.com"]


@pytest.mark.description("A failed proxy run stage revokes its credential.")
async def test_proxy_run_stage_revokes_the_credential_when_the_stage_fails(
    control_postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply = AsyncMock()
    monkeypatch.setattr(proxy_run_stage, "apply_egress_policy", apply)
    monkeypatch.setattr(proxy_run_stage, "install_proxy_ca", AsyncMock())
    monkeypatch.setattr(proxy_run_stage, "start_proxy_tunnel", AsyncMock(return_value=True))
    benchmark = make_benchmark(session=control_postgres_session)
    task = _running_task(control_postgres_session, benchmark)
    control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
    egress = EgressProxy(_ENDPOINT, control)
    attempt = _attempt(benchmark.id, task.id)

    with pytest.raises(RuntimeError):
        async with egress.run_stage(cast(Any, object()), ["example.com"], attempt) as access:
            assert control_postgres_session.get(EgressProxyPolicy, "attempt-1") is not None
            assert access.secrets[0] in access.env_vars["HTTPS_PROXY"]
            raise RuntimeError

    assert apply.await_args is not None and apply.await_args.args[1] == ["192.0.2.1/32"]
    control_postgres_session.expire_all()
    assert control_postgres_session.get(EgressProxyPolicy, "attempt-1") is None


@pytest.mark.description("Inspected URLs are saved as lowercase hostnames.")
def test_inspected_hosts_are_stored_as_hostnames(database_session: Session) -> None:
    proxy = resolve_run_egress_proxy(["pypi.org", "github.com"], "attempt-1", inspect=["https://PyPI.org/simple"])

    assert proxy is not None
    assert proxy.inspect_domains == frozenset({"pypi.org"})


@pytest.mark.description("Wildcard and leading-dot egress entries fail the attempt instead of never matching.")
@pytest.mark.parametrize(
    ("egress", "inspect"),
    [
        (["*.example.com"], []),
        (["https://*.example.com/x"], []),
        ([".example.com"], []),
        ([" .example.com"], []),
        (["pypi.org"], ["*.pypi.org"]),
    ],
)
def test_wildcard_egress_entries_are_rejected(egress: list[str], inspect: list[str]) -> None:
    with pytest.raises(InvalidSandboxConfigurationError, match="list the domain itself"):
        resolve_run_egress_proxy(egress, "attempt-1", inspect=inspect)


@pytest.mark.description("Child egress through the proxy has its own credential and policy.")
async def test_proxy_child_egress_has_its_own_credential_and_policy(
    control_postgres_session: Session, bound_controls: Callable[..., dict[str, object]]
) -> None:
    benchmark = make_benchmark(session=control_postgres_session)
    task = _running_task(control_postgres_session, benchmark)
    control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
    egress = EgressProxy(_ENDPOINT, control)
    profile = ChildPolicy(egress=["example.com", "pypi.org"], inspect=["pypi.org"], models=["a/b"])
    key_id = child_key_id("attempt-1", "gpu")

    async with egress.child_egress(
        "gpu", profile, _attempt(benchmark.id, task.id), ScopedGateway("https://gw.test", "child-token")
    ) as child_egress:
        stored = control_postgres_session.get(EgressProxyPolicy, key_id)
        assert stored is not None
        assert (stored.domains, stored.inspect_domains) == (["example.com", "pypi.org"], ["pypi.org"])
        assert control_postgres_session.get(EgressProxyPolicy, "attempt-1") is None
        token = child_egress.proxy_url.split(":")[2].split("@")[0]
        assert stored.token_sha256 == hashlib.sha256(token.encode()).hexdigest()
        assert child_egress.proxy_url == f"https://{key_id}:{token}@proxy.example:3128"
        assert child_egress.proxy_cidrs == ["192.0.2.1/32"]
        assert child_egress.ca_certificate == "certificate"
        assert (child_egress.gateway_url, child_egress.gateway_token) == ("https://gw.test", "child-token")

    control_postgres_session.expire_all()
    assert control_postgres_session.get(EgressProxyPolicy, key_id) is None


@pytest.mark.description("A child profile with no egress gets a policy with no domains and no gateway token.")
async def test_child_profile_without_egress_reaches_only_the_gateway(
    control_postgres_session: Session, bound_controls: Callable[..., dict[str, object]]
) -> None:
    benchmark = make_benchmark(session=control_postgres_session)
    task = _running_task(control_postgres_session, benchmark)
    control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
    egress = EgressProxy(_ENDPOINT, control)

    async with egress.child_egress("judge", ChildPolicy(), _attempt(benchmark.id, task.id), None) as child_egress:
        stored = control_postgres_session.get(EgressProxyPolicy, child_key_id("attempt-1", "judge"))
        assert stored is not None
        assert stored.domains == []
        assert child_egress.gateway_token is None


@pytest.mark.description(
    "A tunnel start that the provider fails raises `SandboxSetupError`, while an untranslated error propagates unchanged."
)
@pytest.mark.parametrize(
    ("raised", "expected"),
    [(ProviderSandboxError("provider down"), SandboxSetupError), (RuntimeError("tracker bug"), RuntimeError)],
    ids=["provider", "untranslated"],
)
async def test_proxy_tunnel_wraps_only_sandbox_errors(raised: Exception, expected: type[Exception]) -> None:
    sandbox = SimpleNamespace(upload_file=AsyncMock(), exec=AsyncMock(side_effect=raised))

    with pytest.raises(expected) as error:
        await start_proxy_tunnel(cast(Any, sandbox), _ENDPOINT)

    assert type(error.value) is expected


@pytest.mark.description(
    "A proxy CA or tunnel script upload that the provider fails raises `SandboxSetupError` before any setup command runs."
)
@pytest.mark.parametrize("step", ["ca", "tunnel"])
async def test_proxy_upload_failure_is_setup_error(step: str) -> None:
    sandbox = SimpleNamespace(
        upload_file=AsyncMock(side_effect=ProviderSandboxError("connection dropped")),
        exec=AsyncMock(return_value=SimpleNamespace(exit_code=0)),
    )

    with pytest.raises(SandboxSetupError) as error:
        if step == "ca":
            await install_proxy_ca(cast(Any, sandbox), "certificate")
        else:
            await start_proxy_tunnel(cast(Any, sandbox), _ENDPOINT)

    assert isinstance(error.value.__cause__, ProviderSandboxError)
    sandbox.exec.assert_not_awaited()
