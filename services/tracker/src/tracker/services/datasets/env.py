import re
from collections.abc import Awaitable, Callable, Mapping

from tracker.exceptions import (
    SecretAccessDeniedError,
    SecretConfigurationError,
    SecretDecryptionError,
    SecretNotFoundError,
)
from tracker.runtime.secrets import SecretStore, secret_display_name, secret_set_instruction

TEMPLATE = re.compile(r"\$\{([^}:]+)(?::-(.*))?\}")

SecretReader = Callable[[str], Awaitable[str | None]]


def secret_reader(store: SecretStore, stored_names: Callable[[str], list[str]]) -> SecretReader:
    async def read(name: str) -> str | None:
        reference = name
        name, separator, field = name.partition("#")
        display = secret_display_name(name)
        selected_field = field if separator else name
        try:
            candidates = stored_names(name)
        except ValueError:
            raise SecretConfigurationError(
                f"This run may not reference secret '{display}'. "
                "Set it with `valkyrie secret set` and use the printed reference."
            ) from None
        failures: list[SecretConfigurationError | SecretAccessDeniedError] = []
        for stored in candidates:
            try:
                value = await store.get(stored)
            except (SecretConfigurationError, SecretAccessDeniedError) as error:
                failures.append(error)
                continue
            if isinstance(value, dict):
                if selected_field in value:
                    return str(value[selected_field])
                failures.append(
                    SecretConfigurationError(
                        f"Secret '{secret_display_name(stored)}' has no JSON field '{selected_field}'. "
                        f"Set a JSON object containing '{selected_field}'. {secret_set_instruction(name)}"
                    )
                )
                continue
            if separator:
                failures.append(
                    SecretConfigurationError(
                        f"Secret '{secret_display_name(stored)}' is not a JSON object, so field '{selected_field}' cannot be read. "
                        "Remove #field from the reference."
                    )
                )
                continue
            return str(value)
        detail = " ".join(dict.fromkeys(str(error.args[0]) for error in failures))
        checked = ", ".join(dict.fromkeys(secret_display_name(candidate) for candidate in candidates))
        primary = next((error for error in failures if isinstance(error, SecretAccessDeniedError)), None)
        if primary is None:
            primary = next(
                (error for error in failures if not isinstance(error, SecretNotFoundError)),
                failures[0] if failures else None,
            )
        error_type = type(primary) if primary is not None else SecretNotFoundError
        raise error_type(
            f"Secret reference '{secret_display_name(reference)}' could not be resolved. Checked names: {checked}. {detail}"
        ) from primary

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
        try:
            secret = await read_secret(name) if read_secret is not None else None
        except (SecretConfigurationError, SecretAccessDeniedError, SecretDecryptionError) as error:
            if isinstance(error, (SecretConfigurationError, SecretAccessDeniedError)) and default is not None:
                resolved[key] = default
                continue
            raise type(error)(f"{setting} sets {key}: {error.args[0]}") from error
        if secret is not None:
            resolved[key] = secret
            from_secrets.append(key)
        elif default is not None:
            resolved[key] = default
        else:
            raise SecretConfigurationError(
                f"{setting} sets {key} from the secret '{secret_display_name(name)}', which this run can't read. "
                f"{secret_set_instruction(name.partition('#')[0])}"
            )
    return resolved, from_secrets
