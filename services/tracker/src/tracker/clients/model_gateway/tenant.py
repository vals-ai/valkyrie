import hashlib
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

import httpx
from cachetools import TTLCache
from pydantic import BaseModel

from tracker.egress import EgressPolicy
from tracker.exceptions import ModelGatewayConfigurationError
from tracker.logging import get_logger


logger = get_logger(__name__)
_client = httpx.AsyncClient()


class _RegistryMetadata(BaseModel):
    deprecated: bool


class _RegistryEntry(BaseModel):
    provider_name: str
    full_key: str
    properties: dict[str, object] | None
    metadata: _RegistryMetadata


class GatewayRegistry(BaseModel):
    models: dict[str, _RegistryEntry]

    def serves(self, model: str) -> bool:
        if model in self.models:
            return True
        if not model.endswith("/*"):
            return False
        provider = model.partition("/")[0].lower()

        return any(
            entry.properties is not None
            and not entry.metadata.deprecated
            and entry.full_key in self.models
            and self.models[entry.full_key].properties is not None
            and (not provider or entry.provider_name.lower() == provider)
            for entry in self.models.values()
        )


_registry_cache = TTLCache[tuple[str, str], GatewayRegistry](maxsize=128, ttl=60)


async def gateway_registry(url: str, api_key: str) -> GatewayRegistry | None:
    cache_key = (url, hashlib.sha256(api_key.encode()).hexdigest())
    if cache_key in _registry_cache:
        return _registry_cache[cache_key]
    try:
        response = await _client.get(
            f"{url}/registry",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"include_excluded_fields": "true", "include_transcription": "true", "include_alt_keys": "true"},
            timeout=10.0,
        )
        response.raise_for_status()
        registry = GatewayRegistry.model_validate(response.json())
    except (httpx.HTTPError, ValueError):
        logger.warning("Model Gateway registry lookup failed, deferring model validation to token minting")
        return None
    _registry_cache[cache_key] = registry

    return registry


@dataclass(frozen=True)
class GatewayLease:
    token: str = field(repr=False)
    lease_id: str
    expires_at: float


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
    if 400 <= response.status_code < 500 and response.status_code not in {408, 429}:
        message = "The Model Gateway rejected this Valkyrie run's token configuration."
        with suppress(ValueError):
            error_body: object = response.json()
            if isinstance(error_body, dict):
                fields = cast(dict[str, object], error_body)
                detail = fields.get("message")
                if isinstance(detail, str):
                    message = detail
                for credential in (api_key, fields.get("token")):
                    if isinstance(credential, str) and credential:
                        message = message.replace(credential, "[REDACTED]")
        raise ModelGatewayConfigurationError(f"Model Gateway configuration error: {message}") from None
    response.raise_for_status()
    body = response.json()
    lease = GatewayLease(body["token"], body["lease_id"], body["expires_at"])
    logger.info(
        "Scoped gateway credential to %s for task %s (lease %s, expires %s)",
        ", ".join(allowed_models),
        task_id,
        lease.lease_id,
        datetime.fromtimestamp(lease.expires_at, UTC).isoformat(),
    )
    return lease


async def revoke_gateway_lease(url: str, api_key: str, lease_id: str) -> None:
    (
        await _client.post(
            f"{url}/service-auth/revoke",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"lease_id": lease_id},
            timeout=5.0,
        )
    ).raise_for_status()
