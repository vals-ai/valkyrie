import re
from collections.abc import Awaitable, Callable, Mapping

from tracker.exceptions import SecretNotFoundError, SecretsError
from tracker.runtime.secrets import SecretStore

TEMPLATE = re.compile(r"\$\{([^}:]+)(?::-(.*))?\}")

SecretReader = Callable[[str], Awaitable[str | None]]


def secret_reader(store: SecretStore, stored_name: Callable[[str], str]) -> SecretReader:
    async def read(name: str) -> str | None:
        try:
            value = await store.get(stored_name(name))
        except (SecretNotFoundError, ValueError):
            return None
        if isinstance(value, dict):
            return str(value[name]) if name in value else None
        return str(value)

    return read


async def resolve_env(
    env: Mapping[str, str], read_secret: SecretReader | None, setting: str
) -> tuple[dict[str, str], list[str]]:
    resolved: dict[str, str] = {}
    from_secrets: list[str] = []
    for key, value in env.items():
        template = TEMPLATE.fullmatch(value)
        if template is None:
            resolved[key] = value
            continue
        name, default = template.groups()
        secret = await read_secret(name) if read_secret is not None else None
        if secret is not None:
            resolved[key] = secret
            from_secrets.append(key)
        elif default is not None:
            resolved[key] = default
        else:
            raise SecretsError(f"{setting} sets {key} from the secret '{name}', which this run can't read")
    return resolved, from_secrets
