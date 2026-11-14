from typing import Protocol, TypeAlias

from benchmark_service import SandboxProviderConfig, sandbox_provider_config_from_mapping
from pydantic import ValidationError

from tracker.exceptions import InvalidSandboxConfigurationError, SecretsError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SecretValue: TypeAlias = JsonValue
TENANT_SECRET_ROOT = "valkyrie/tenants/"


def secret_display_name(name: str) -> str:
    if not name.startswith(TENANT_SECRET_ROOT):
        return name
    return name.removeprefix(TENANT_SECRET_ROOT).partition("/")[2] or name


def sandbox_provider_config_from_secret(secret: SecretValue, provider_type: str) -> SandboxProviderConfig:
    if not isinstance(secret, dict):
        raise InvalidSandboxConfigurationError("Expected sandbox provider secret to be a JSON object")
    try:
        return sandbox_provider_config_from_mapping({**secret, "type": provider_type})
    except ValidationError as error:
        details = "; ".join(
            f"{'.'.join(map(str, detail['loc']))}: {detail['msg']}" for detail in error.errors(include_input=False)
        )
        raise InvalidSandboxConfigurationError(f"Invalid {provider_type} settings: {details}") from None


class SecretStore(Protocol):
    async def get(self, name: str) -> SecretValue:
        raise NotImplementedError


async def resolve_secrets(secrets: dict[str, str], secret_store: SecretStore) -> dict[str, str]:
    resolved: dict[str, str] = {}
    fetched: dict[str, SecretValue] = {}
    for env_name, secret_name in secrets.items():
        if secret_name not in fetched:
            fetched[secret_name] = await secret_store.get(secret_name)
        secret_value = fetched[secret_name]
        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_display_name(secret_name)}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = str(secret_value)
    return resolved
