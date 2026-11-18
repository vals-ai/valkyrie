import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from botocore.session import get_session
from botocore.validate import ParamValidator

from tracker.aws.clients import AWSClientProvider
from tracker.aws.secrets import SecretBeingDeletedError, SecretsManagerStore
from tracker.exceptions import (
    SecretAccessDeniedError,
    SecretConfigurationError,
    SecretDecryptionError,
    SecretNotFoundError,
    SecretRegionError,
    SecretsError,
)


_ORIGINAL_GET = SecretsManagerStore.get


@pytest.fixture(autouse=True)
def use_real_secret_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SecretsManagerStore, "get", _ORIGINAL_GET)


_SECRETS_MANAGER = get_session().get_service_model("secretsmanager")


class FakeAsyncSecretsManagerClient:
    def __init__(
        self,
        response: dict[str, Any] | None = None,
        error: ClientError | None = None,
        *,
        errors: dict[str, ClientError] | None = None,
        names: list[str] | None = None,
    ) -> None:
        self.response = response or {}
        self.error = error
        self.secret_ids: list[str] = []
        self.errors = errors or {}
        self.names = names or []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
        self.secret_ids.append(SecretId)
        if self.error is not None:
            raise self.error
        return self.response

    def _call(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        report = ParamValidator().validate(params, _SECRETS_MANAGER.operation_model(operation).input_shape)
        assert not report.has_errors(), report.generate_report()
        self.calls.append((operation, params))
        if operation in self.errors:
            raise self.errors[operation]
        return {}

    async def create_secret(self, **params: Any) -> dict[str, Any]:
        return self._call("CreateSecret", params)

    async def put_secret_value(self, **params: Any) -> dict[str, Any]:
        return self._call("PutSecretValue", params)

    async def delete_secret(self, **params: Any) -> dict[str, Any]:
        return self._call("DeleteSecret", params)

    def get_paginator(self, operation: str) -> "FakeAsyncSecretsManagerClient":
        assert operation == "list_secrets"
        return self

    def paginate(self, **params: Any) -> AsyncIterator[dict[str, Any]]:
        self._call("ListSecrets", params)
        return self._pages()

    async def _pages(self) -> AsyncIterator[dict[str, Any]]:
        for name in self.names:
            yield {"SecretList": [{"Name": name}]}


class FakeAsyncClientContext:
    def __init__(self, client: FakeAsyncSecretsManagerClient) -> None:
        self.client = client
        self.exited = False

    async def __aenter__(self) -> FakeAsyncSecretsManagerClient:
        return self.client

    async def __aexit__(self, *_args: object) -> None:
        self.exited = True


class FakeAsyncClientProvider:
    region = "us-east-1"

    def __init__(self, context: FakeAsyncClientContext) -> None:
        self.context = context
        self.calls = 0

    def secretsmanager_async_client(self) -> FakeAsyncClientContext:
        self.calls += 1
        return self.context


@pytest.mark.description("Reading a secret keeps JSON values and raw strings apart.")
@pytest.mark.parametrize(
    ("secret_string", "expected"),
    [
        ('{"key": "value"}', {"key": "value"}),
        ('["value", 1]', ["value", 1]),
        ('"json-string"', "json-string"),
        ("42", 42),
        ("true", True),
        ("null", None),
        ("plain-text", "plain-text"),
    ],
)
async def test_get_preserves_json_and_raw_string_domains(secret_string: str, expected: object) -> None:
    client = FakeAsyncSecretsManagerClient({"SecretString": secret_string})
    context = FakeAsyncClientContext(client)
    provider = FakeAsyncClientProvider(context)

    assert await SecretsManagerStore(cast(AWSClientProvider, provider)).get("named-secret") == expected
    assert client.secret_ids == ["named-secret"]
    assert provider.calls == 1
    assert context.exited


@pytest.mark.description("A failed secret read is translated and keeps the client error as its cause.")
@pytest.mark.parametrize(
    ("code", "error_type", "message"),
    [
        (
            "ResourceNotFoundException",
            SecretNotFoundError,
            "Secret 'named-secret' is not set for your organization. Run `valkyrie secret set named-secret`.",
        ),
        ("AccessDeniedException", SecretAccessDeniedError, "access to it was denied"),
        ("AccessDenied", SecretAccessDeniedError, "access to it was denied"),
        ("DecryptionFailure", SecretDecryptionError, "decryption failed"),
        ("DecryptionFailureException", SecretDecryptionError, "decryption failed"),
        ("KMSAccessDeniedException", SecretDecryptionError, "decryption failed"),
        ("ThrottlingException", SecretsError, "Failed to retrieve secret 'named-secret' (ThrottlingException)"),
        ("InternalServiceError", SecretsError, "Failed to retrieve secret 'named-secret' (InternalServiceError)"),
        (
            "InvalidParameterException",
            SecretsError,
            "Failed to retrieve secret 'named-secret' (InvalidParameterException)",
        ),
        ("InvalidRequestException", SecretsError, "Failed to retrieve secret 'named-secret' (InvalidRequestException)"),
    ],
)
async def test_get_preserves_client_error_translation_and_cause(
    code: str, error_type: type[SecretsError], message: str
) -> None:
    provider_error = ClientError({"Error": {"Code": code, "Message": "provider failure"}}, "GetSecretValue")
    client = FakeAsyncSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        await SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(FakeAsyncClientContext(client)))).get(
            "named-secret"
        )

    assert type(captured.value) is error_type
    assert message in str(captured.value)
    assert "provider failure" not in str(captured.value)
    assert captured.value.__cause__ is provider_error


@pytest.mark.description(
    "An access denied tenant secret names the Valkyrie fault without the tenant prefix or provider details."
)
async def test_denied_tenant_secret_is_named_without_its_prefix_or_the_provider_error() -> None:
    provider_error = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "role/Tenant is not authorized on secret:valkyrie/tenants/x",
            }
        },
        "GetSecretValue",
    )
    client = FakeAsyncSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        await SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(FakeAsyncClientContext(client)))).get(
            "valkyrie/tenants/00000000-0000-0000-0000-000000000001/daytona"
        )

    assert isinstance(captured.value, SecretAccessDeniedError)
    assert captured.value.args[0] == (
        "Valkyrie can't read secret 'daytona': it isn't set for your organization or access to it was denied. "
        "If it isn't set, run `valkyrie secret set daytona`. "
        "Otherwise this is a Valkyrie problem and has been reported."
    )
    assert not isinstance(captured.value, SecretConfigurationError)
    assert "role/Tenant" not in str(captured.value)
    assert "valkyrie/tenants" not in str(captured.value)


@pytest.mark.description("Secret reads distinguish Valkyrie permission faults from deletion configuration errors.")
@pytest.mark.parametrize(
    ("code", "detail", "error_type", "fix"),
    [
        ("AccessDeniedException", "Access to KMS key is denied", SecretDecryptionError, "decryption failed"),
        ("AccessDenied", "KMS access denied", SecretDecryptionError, "decryption failed"),
        ("AccessDeniedException", "Access to KMS is not allowed", SecretDecryptionError, "decryption failed"),
        (
            "AccessDeniedException",
            "Cannot read secret kms-token",
            SecretAccessDeniedError,
            "access to it was denied",
        ),
        (
            "InvalidRequestException",
            "scheduled for deletion",
            SecretBeingDeletedError,
            "Wait for the deletion to finish",
        ),
        (
            "InvalidRequestException",
            "secret is being deleted",
            SecretBeingDeletedError,
            "Wait for the deletion to finish",
        ),
    ],
)
async def test_get_reports_configuration_fix(code: str, detail: str, error_type: type[SecretsError], fix: str) -> None:
    error = ClientError({"Error": {"Code": code, "Message": detail}}, "GetSecretValue")
    with pytest.raises(error_type, match=fix):
        await _store(FakeAsyncSecretsManagerClient(error=error)).get("named-secret")


@pytest.mark.description("A deleting secret gives the exact wait and recreate instruction.")
async def test_get_deleting_secret_message() -> None:
    error = ClientError(
        {"Error": {"Code": "InvalidRequestException", "Message": "scheduled for deletion"}}, "GetSecretValue"
    )
    with pytest.raises(SecretBeingDeletedError) as captured:
        await _store(FakeAsyncSecretsManagerClient(error=error)).get("named-secret")
    assert captured.value.args[0] == (
        "Secret 'named-secret' is being deleted. Wait for the deletion to finish, then "
        "run `valkyrie secret set named-secret`."
    )


@pytest.mark.description("A cross-region ARN is rejected before AWS can return an ambiguous access or lookup error.")
@pytest.mark.parametrize(
    "stored_name",
    ["named-secret-aB12Cd", "valkyrie/tenants/org-id/named-secret-aB12Cd", "valkyrie/tenants/org-id/API_KEY"],
)
async def test_get_rejects_wrong_region_arn(stored_name: str) -> None:
    client = FakeAsyncSecretsManagerClient()
    with pytest.raises(SecretRegionError) as captured:
        await _store(client).get(f"arn:aws:secretsmanager:us-west-2:123456789012:secret:{stored_name}")
    expected_name = "API_KEY" if stored_name.endswith("API_KEY") else "named-secret"
    assert str(captured.value).startswith(f"Secret error: Secret '{expected_name}' is in region")
    assert "123456789012" not in str(captured.value)
    assert "valkyrie/tenants/" not in str(captured.value)
    assert "org-id" not in str(captured.value)
    assert "aB12Cd" not in str(captured.value)
    assert "Tracker reads region 'us-east-1'" in str(captured.value)
    assert "is in region 'us-west-2'" in str(captured.value)
    assert "Use a Tracker in region 'us-west-2'." in str(captured.value)
    assert not client.secret_ids


@pytest.mark.description("A missing ARN suggests setting the secret and using its printed reference.")
async def test_missing_arn_suggests_a_valid_cli_command() -> None:
    client = FakeAsyncSecretsManagerClient(error=_client_error("ResourceNotFoundException", "GetSecretValue"))
    with pytest.raises(SecretNotFoundError) as captured:
        await _store(client).get("arn:aws:secretsmanager:us-east-1:123456789012:secret:api-key-abcdef")
    assert "Set it with `valkyrie secret set` and use the printed reference." in str(captured.value)
    assert "run-secret" not in str(captured.value)


@pytest.mark.description("Cancelling a secret read cancels the request and closes the client.")
async def test_get_cancels_underlying_request_and_exits_client() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingClient(FakeAsyncSecretsManagerClient):
        async def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return {"SecretString": SecretId}

    context = FakeAsyncClientContext(BlockingClient())
    task = asyncio.create_task(
        SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(context))).get("named-secret")
    )
    await started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    assert context.exited


@pytest.mark.description("Building the secret store does not touch the client.")
def test_store_constructs_without_accessing_the_client() -> None:
    provider = FakeAsyncClientProvider(FakeAsyncClientContext(FakeAsyncSecretsManagerClient({"SecretString": "value"})))

    SecretsManagerStore(cast(AWSClientProvider, provider))

    assert provider.calls == 0


def _store(client: FakeAsyncSecretsManagerClient) -> SecretsManagerStore:
    context = FakeAsyncClientContext(client)
    return SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(context)))


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "provider failure"}}, operation)


_NAME = "valkyrie/tenants/org/daytona"
_CREATE = (
    "CreateSecret",
    {"Name": _NAME, "SecretString": "value", "Tags": [{"Key": "valkyrie:org-id", "Value": "org"}]},
)


@pytest.mark.description("Putting a secret creates it or replaces its value.")
@pytest.mark.parametrize(
    ("create_error", "calls"),
    [
        (None, [_CREATE]),
        ("ResourceExistsException", [_CREATE, ("PutSecretValue", {"SecretId": _NAME, "SecretString": "value"})]),
    ],
    ids=["new", "existing"],
)
async def test_put_creates_the_secret_or_replaces_its_value(create_error: str | None, calls: list[object]) -> None:
    client = FakeAsyncSecretsManagerClient(
        errors={"CreateSecret": _client_error(create_error, "CreateSecret")} if create_error else {}
    )

    await _store(client).put(_NAME, "value", tags={"valkyrie:org-id": "org"})

    assert client.calls == calls


@pytest.mark.description("Putting a secret reports other failures.")
@pytest.mark.parametrize("failing", ["CreateSecret", "PutSecretValue"])
async def test_put_reports_other_failures(failing: str) -> None:
    error = _client_error("AccessDeniedException", failing)
    errors = {failing: error}
    if failing == "PutSecretValue":
        errors["CreateSecret"] = _client_error("ResourceExistsException", "CreateSecret")

    with pytest.raises(SecretsError) as captured:
        await _store(FakeAsyncSecretsManagerClient(errors=errors)).put(_NAME, "value", tags={})

    assert str(captured.value) == "Secret error: Failed to save secret 'daytona' (AccessDeniedException)"
    assert captured.value.__cause__ is error


@pytest.mark.description("Listing secret names filters by prefix and sorts them.")
async def test_list_names_filters_by_prefix_and_sorts() -> None:
    client = FakeAsyncSecretsManagerClient(
        names=["valkyrie/tenants/org/modal", "valkyrie/tenants/org-other/x", "valkyrie/tenants/org/daytona"]
    )

    names = await _store(client).list_names("valkyrie/tenants/org/")

    assert names == ["valkyrie/tenants/org/daytona", "valkyrie/tenants/org/modal"]
    assert client.calls == [("ListSecrets", {"Filters": [{"Key": "name", "Values": ["valkyrie/tenants/org/"]}]})]


@pytest.mark.description("Deleting a secret removes it without a recovery window.")
@pytest.mark.parametrize(
    ("error_code", "deleted"),
    [(None, True), ("AccessDeniedException", False)],
    ids=["deleted", "missing-under-tenant-role"],
)
async def test_delete_removes_the_secret_without_recovery(error_code: str | None, deleted: bool) -> None:
    errors = {"DeleteSecret": _client_error(error_code, "DeleteSecret")} if error_code else {}
    client = FakeAsyncSecretsManagerClient(errors=errors)

    assert await _store(client).delete(_NAME) is deleted
    assert client.calls == [("DeleteSecret", {"SecretId": _NAME, "ForceDeleteWithoutRecovery": True})]


@pytest.mark.description("Putting a secret whose name is still being deleted reports that.")
async def test_put_reports_a_name_still_being_deleted() -> None:
    pending = ClientError(
        {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": "a secret with this name is already scheduled for deletion",
            }
        },
        "CreateSecret",
    )

    with pytest.raises(SecretBeingDeletedError):
        await _store(FakeAsyncSecretsManagerClient(errors={"CreateSecret": pending})).put(_NAME, "value", tags={})
