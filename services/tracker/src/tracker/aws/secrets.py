import json
import re
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import (
    SecretAccessDeniedError,
    SecretConfigurationError,
    SecretDecryptionError,
    SecretNotFoundError,
    SecretRegionError,
    SecretsError,
)
from tracker.runtime.secrets import TENANT_SECRET_ROOT, SecretValue, secret_display_name, secret_set_instruction


def _error_code(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Code", "")


class SecretBeingDeletedError(SecretConfigurationError):
    status_code = 409
    cause_code = "secret_being_deleted"


class SecretsManagerStore:
    def __init__(self, clients: AWSClientProvider) -> None:
        self._clients = clients

    async def get(self, name: str) -> SecretValue:
        region = self._clients.region
        arn = name.split(":", 6)
        stored_name = arn[6] if len(arn) == 7 and arn[0] == "arn" and arn[2] == "secretsmanager" else name
        display_name = re.sub(r"-[A-Za-z0-9]{6}$", "", stored_name) if stored_name != name else name
        display = secret_display_name(display_name)
        set_instruction = secret_set_instruction(name)
        if len(arn) == 7 and arn[0] == "arn" and arn[2] == "secretsmanager" and arn[3] != region:
            display = secret_display_name(re.sub(r"-[A-Za-z0-9]{6}$", "", arn[6]))
            raise SecretRegionError(
                f"Secret '{display}' is in region '{arn[3]}', but Valkyrie reads secrets from region '{region}'. "
                f"{set_instruction}"
            )
        async with self._clients.secretsmanager_async_client() as client:
            try:
                response: dict[str, Any] = await client.get_secret_value(  # pyright: ignore[reportUnknownMemberType]
                    SecretId=name
                )
            except ClientError as error:
                error_code = _error_code(error)
                if error_code == "ResourceNotFoundException":
                    raise SecretNotFoundError(
                        f"Secret '{display}' is not set for your organization. {set_instruction}"
                    ) from error
                if error_code in {"DecryptionFailure", "DecryptionFailureException", "KMSAccessDeniedException"} or (
                    error_code in {"AccessDenied", "AccessDeniedException"}
                    and any(
                        marker in error.response.get("Error", {}).get("Message", "").lower()
                        for marker in ("kms:decrypt", "kms key", "access to kms", "kms access")
                    )
                ):
                    raise SecretDecryptionError(
                        f"Valkyrie can't decrypt secret '{display}': decryption failed. "
                        "This is a Valkyrie problem, not your configuration, and has been reported."
                    ) from error
                if error_code in {"AccessDenied", "AccessDeniedException"}:
                    if stored_name.startswith(TENANT_SECRET_ROOT):
                        try:
                            description = await client.describe_secret(SecretId=name)
                        except (ClientError, BotoCoreError) as describe_error:
                            if (
                                isinstance(describe_error, ClientError)
                                and _error_code(describe_error) == "ResourceNotFoundException"
                            ):
                                raise SecretNotFoundError(
                                    f"Secret '{display}' is not set for your organization. {set_instruction}"
                                ) from describe_error
                        else:
                            if description.get("DeletedDate") is not None:
                                raise SecretBeingDeletedError(
                                    f"Secret '{display}' is being deleted. Wait for the deletion to finish, then "
                                    f"{set_instruction[0].lower()}{set_instruction[1:]}"
                                ) from error
                            raise SecretAccessDeniedError(
                                f"Secret '{display}' exists, but this run can't read it. "
                                "This is a Valkyrie problem, not your configuration, and has been reported."
                            ) from error
                    raise SecretAccessDeniedError(
                        f"Valkyrie can't read secret '{display}': it isn't set for your organization or access to it was denied. "
                        f"If it isn't set, {set_instruction[0].lower()}{set_instruction[1:]} "
                        "Otherwise this is a Valkyrie problem and has been reported."
                    ) from error
                if error_code == "InvalidRequestException" and any(
                    state in error.response.get("Error", {}).get("Message", "").lower()
                    for state in ("scheduled for deletion", "being deleted")
                ):
                    raise SecretBeingDeletedError(
                        f"Secret '{display}' is being deleted. Wait for the deletion to finish, then "
                        f"{set_instruction[0].lower()}{set_instruction[1:]}"
                    ) from error
                raise SecretsError(f"Failed to retrieve secret '{secret_display_name(name)}' ({error_code})") from error

        secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]
        try:
            return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
        except json.JSONDecodeError:
            return secret_string

    async def put(self, name: str, value: str, *, tags: dict[str, str]) -> None:
        async with self._clients.secretsmanager_async_client() as client:
            try:
                try:
                    await client.create_secret(  # pyright: ignore[reportUnknownMemberType]
                        Name=name, SecretString=value, Tags=[{"Key": key, "Value": tag} for key, tag in tags.items()]
                    )
                except ClientError as error:
                    if _error_code(error) != "ResourceExistsException":
                        raise
                    await client.put_secret_value(SecretId=name, SecretString=value)  # pyright: ignore[reportUnknownMemberType]
            except ClientError as error:
                if _error_code(error) == "InvalidRequestException" and "scheduled for deletion" in str(error):
                    raise SecretBeingDeletedError(
                        f"Secret '{secret_display_name(name)}' is still being deleted"
                    ) from error
                raise SecretsError(
                    f"Failed to save secret '{secret_display_name(name)}' ({_error_code(error)})"
                ) from error

    async def list_names(self, prefix: str) -> list[str]:
        async with self._clients.secretsmanager_async_client() as client:
            pages = client.get_paginator("list_secrets").paginate(Filters=[{"Key": "name", "Values": [prefix]}])  # pyright: ignore[reportUnknownMemberType]
            try:
                names = [str(secret["Name"]) async for page in pages for secret in page.get("SecretList", [])]  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            except ClientError as error:
                raise SecretsError(f"Failed to list secrets ({_error_code(error)})") from error
        return sorted(name for name in names if name.startswith(prefix))

    async def delete(self, name: str) -> bool:
        async with self._clients.secretsmanager_async_client() as client:
            try:
                await client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)  # pyright: ignore[reportUnknownMemberType]
            except ClientError as error:
                if _error_code(error) == "AccessDeniedException":
                    return False
                raise SecretsError(
                    f"Failed to delete secret '{secret_display_name(name)}' ({_error_code(error)})"
                ) from error
        return True
