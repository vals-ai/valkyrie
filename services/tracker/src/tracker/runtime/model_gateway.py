import asyncio
import shlex
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from functools import partial
from datetime import UTC, datetime

import httpx
from benchmark_service import Sandbox

from tracker.config import AUTH_REQUIRED, PASSKEY_ORGS
from tracker.egress import EgressPolicy
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax
from tracker.runtime.run_egress import ScopedGateway


logger = get_logger(__name__)
_client = httpx.AsyncClient()


GATEWAY_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60
GATEWAY_TOKEN_FILE = "/run/valkyrie/gateway_token"
GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS = 2 * 60 * 60
_ROTATION_RETRY_SECONDS = 60


def scoped_gateway_url(env_vars: dict[str, str], *, attested_model: str | None, org_name: str) -> str | None:
    if not env_vars.get("MODEL_GATEWAY_URL") or not env_vars.get("MODEL_GATEWAY_API_KEY") or not attested_model:
        return None
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    return url


@asynccontextmanager
async def task_scoped_gateway_key(
    env_vars: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    attempt_id: str,
    attested_model: str | None,
    companion_models: str | None,
    identity: dict[str, str],
    org_name: str,
    egress: EgressPolicy,
    allow_provider_tools: bool = False,
    rotation: "GatewayTokenRotation | None" = None,
) -> AsyncGenerator[dict[str, str]]:
    url = scoped_gateway_url(env_vars, attested_model=attested_model, org_name=org_name)
    if url is None or attested_model is None:
        if org_name in PASSKEY_ORGS:
            raise InvalidSandboxConfigurationError(
                f"Runs in {org_name} need a Gateway (MODEL_GATEWAY_URL and MODEL_GATEWAY_API_KEY) "
                "and a tracker-resolved model; the sandbox gets only a task-scoped token"
            )
        yield env_vars
        return
    allowed_models = [attested_model, *(companion_models.split(",") if companion_models else [])]

    api_key = env_vars["MODEL_GATEWAY_API_KEY"]
    mint = partial(
        mint_gateway_lease,
        url,
        api_key,
        run_id=run_id,
        task_id=task_id,
        attempt_id=attempt_id,
        allowed_models=allowed_models,
        identity=identity,
        egress=egress,
        operations=None,
        ttl_seconds=GATEWAY_TOKEN_TTL_SECONDS,
        allow_provider_tools=allow_provider_tools,
    )
    lease = await mint()
    revoke = partial(_revoke_lease, url, api_key)
    try:
        scoped = {**env_vars, "MODEL_GATEWAY_API_KEY": lease.token}
        if (
            rotation is not None
            and lease.expires_at < time.time() + GATEWAY_TOKEN_TTL_SECONDS - GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS
        ):
            rotation.arm(lease, mint, revoke)
            scoped["MODEL_GATEWAY_API_KEY_FILE"] = GATEWAY_TOKEN_FILE
        yield scoped
    finally:
        await revoke(rotation.lease if rotation is not None and rotation.lease is not None else lease)


@asynccontextmanager
async def grading_gateway_token(
    env_vars: dict[str, str],
    *,
    models: list[str],
    run_id: str,
    task_id: str,
    attempt_id: str,
    identity: dict[str, str],
    org_name: str,
) -> AsyncGenerator[str]:
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    async with _gateway_lease(
        url,
        env_vars["MODEL_GATEWAY_API_KEY"],
        run_id=run_id,
        task_id=task_id,
        attempt_id=attempt_id,
        allowed_models=models,
        identity=identity,
        egress="*",
        operations=["query", "tokens_count"],
    ) as token:
        yield token


@asynccontextmanager
async def child_gateway_token(
    env_vars: dict[str, str],
    *,
    profile: str,
    models: list[str],
    egress: EgressPolicy,
    run_id: str,
    task_id: str,
    attempt_id: str,
    identity: dict[str, str],
    org_name: str,
) -> AsyncGenerator[ScopedGateway | None]:
    if not models:
        yield None
        return
    if not env_vars.get("MODEL_GATEWAY_URL") or not env_vars.get("MODEL_GATEWAY_API_KEY"):
        raise InvalidSandboxConfigurationError(
            f"Child profile {profile!r} names models but the run has no Model Gateway"
        )
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    async with _gateway_lease(
        url,
        env_vars["MODEL_GATEWAY_API_KEY"],
        run_id=run_id,
        task_id=task_id,
        attempt_id=attempt_id,
        allowed_models=models,
        identity=identity,
        egress=egress,
        operations=None,
    ) as token:
        yield ScopedGateway(url, token)


@dataclass(frozen=True)
class GatewayLease:
    token: str = field(repr=False)
    lease_id: str
    expires_at: float


@dataclass
class GatewayTokenRotation:
    lease: GatewayLease | None = None
    _mint: Callable[[], Awaitable[GatewayLease]] | None = None
    _revoke: Callable[[GatewayLease], Awaitable[None]] | None = None

    def arm(
        self,
        lease: GatewayLease,
        mint: Callable[[], Awaitable[GatewayLease]],
        revoke: Callable[[GatewayLease], Awaitable[None]],
    ) -> None:
        self.lease, self._mint, self._revoke = lease, mint, revoke

    @asynccontextmanager
    async def into(self, sandbox: Sandbox) -> AsyncGenerator[None]:
        if self.lease is None:
            yield
            return
        await _write_token(sandbox, self.lease.token, first=True)
        rotating = asyncio.create_task(self._rotate(sandbox))
        try:
            yield
        finally:
            rotating.cancel()
            with suppress(asyncio.CancelledError):
                await rotating

    async def _rotate(self, sandbox: Sandbox) -> None:
        assert self.lease is not None and self._mint is not None and self._revoke is not None
        while True:
            await asyncio.sleep(max(0.0, self.lease.expires_at - time.time() - GATEWAY_TOKEN_REFRESH_MARGIN_SECONDS))
            try:
                fresh = await self._mint()
            except Exception as exc:
                logger.warning("Could not mint a replacement gateway token: %s: %s", type(exc).__name__, exc)
                await asyncio.sleep(_ROTATION_RETRY_SECONDS)
                continue
            try:
                await _write_token(sandbox, fresh.token, first=False)
            except BaseException as exc:
                await self._revoke(fresh)
                if not isinstance(exc, Exception):
                    raise
                logger.warning("Could not rotate the gateway token: %s: %s", type(exc).__name__, exc)
                await asyncio.sleep(_ROTATION_RETRY_SECONDS)
                continue
            previous, self.lease = self.lease, fresh
            try:
                await self._revoke(previous)
            except asyncio.CancelledError:
                await self._revoke(previous)
                raise


async def _write_token(sandbox: Sandbox, token: str, *, first: bool) -> None:
    live = shlex.quote(GATEWAY_TOKEN_FILE)
    staged = shlex.quote(f"{GATEWAY_TOKEN_FILE}.next")
    if first:
        await _sandbox_command(sandbox, f"install -d -m 0700 {shlex.quote(GATEWAY_TOKEN_FILE.rsplit('/', 1)[0])}")
    await sandbox.upload_file(f"{GATEWAY_TOKEN_FILE}.next", token.encode())
    permissions = (
        f"chmod 0600 {staged}" if first else f"chown --reference={live} {staged} && chmod --reference={live} {staged}"
    )
    await _sandbox_command(sandbox, f"{permissions} && mv -f {staged} {live}")


async def _sandbox_command(sandbox: Sandbox, command: str) -> None:
    result = await sandbox.exec(command, timeout=30)
    if result.exit_code != 0:
        raise RuntimeError(f"Writing the gateway token failed ({result.exit_code}): {result.output[-500:]}")


async def mint_gateway_lease(
    url: str,
    api_key: str,
    *,
    run_id: str,
    task_id: str,
    attempt_id: str,
    allowed_models: list[str],
    identity: dict[str, str],
    egress: EgressPolicy,
    operations: list[str] | None,
    ttl_seconds: int,
    allow_provider_tools: bool = False,
) -> GatewayLease:
    response = await _client.post(
        f"{url}/service-auth",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "run_id": run_id,
            "task_id": task_id,
            "attempt_id": attempt_id,
            "allowed_models": allowed_models,
            "identity": identity,
            "egress": egress,
            "ttl_seconds": ttl_seconds,
            **({"allowed_operations": operations} if operations is not None else {}),
            **({"allow_provider_tools": True} if allow_provider_tools else {}),
        },
        timeout=30.0,
    )
    response.raise_for_status()
    body = response.json()
    lease = GatewayLease(body["token"], body["lease_id"], body["expires_at"])
    expires = datetime.fromtimestamp(lease.expires_at, UTC).isoformat()
    logger.info(
        "Scoped gateway credential to %s for task %s (lease %s, expires %s)",
        ", ".join(allowed_models),
        task_id,
        lease.lease_id,
        expires,
    )
    return lease


async def revoke_gateway_lease(url: str, api_key: str, lease_id: str) -> None:
    response = await _client.post(
        f"{url}/service-auth/revoke",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"lease_id": lease_id},
        timeout=5.0,
    )
    response.raise_for_status()


async def _revoke_lease(url: str, api_key: str, lease: GatewayLease) -> None:
    try:
        await revoke_gateway_lease(url, api_key, lease.lease_id)
    except httpx.HTTPError as exc:
        logger.warning("Could not revoke gateway lease %s: %s: %s", lease.lease_id, type(exc).__name__, exc)


@asynccontextmanager
async def _gateway_lease(
    url: str,
    api_key: str,
    *,
    run_id: str,
    task_id: str,
    attempt_id: str,
    allowed_models: list[str],
    identity: dict[str, str],
    egress: EgressPolicy,
    operations: list[str] | None,
    allow_provider_tools: bool = False,
) -> AsyncGenerator[str]:
    lease = await mint_gateway_lease(
        url,
        api_key,
        run_id=run_id,
        task_id=task_id,
        attempt_id=attempt_id,
        allowed_models=allowed_models,
        identity=identity,
        egress=egress,
        operations=operations,
        allow_provider_tools=allow_provider_tools,
        ttl_seconds=GATEWAY_TOKEN_TTL_SECONDS,
    )
    try:
        yield lease.token
    finally:
        await _revoke_lease(url, api_key, lease)
