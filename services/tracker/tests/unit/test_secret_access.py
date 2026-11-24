from types import SimpleNamespace
from typing import cast
from uuid import UUID
from unittest.mock import AsyncMock

import pytest

from tracker import config
from tracker.exceptions import (
    SecretAccessDeniedError,
    SecretConfigurationError,
    SecretDecryptionError,
    SecretNotFoundError,
    SecretsError,
)
from tracker.runtime.secrets import SecretValue, secret_display_name
from tracker.runtime.services import RuntimeServices
from tracker.services.runs.start import task_secret_reader
from tracker.database.models import Org
from tracker.secret_access import (
    OrgSecretStore,
    resolve_secret_name,
    secret_name_candidates,
    selected_project_gateway_reference,
    tenant_secret_prefix,
    validate_secret_names,
)
from tracker.types import AgentContractRequest, OrgContext, RunExecutionRequest

_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")


@pytest.mark.description("A confirmed missing tenant secret stays missing when shared fallback is missing or denied.")
@pytest.mark.parametrize("shared_error", [SecretNotFoundError, SecretAccessDeniedError])
async def test_missing_tenant_precedes_shared_failure(shared_error: type[SecretsError]) -> None:
    source = AsyncMock()
    missing = SecretNotFoundError("Secret 'TOKEN' is not set for your organization. Run `valkyrie secret set TOKEN`.")
    source.get.side_effect = [missing, shared_error("Shared lookup failed.")]
    store = OrgSecretStore(source, ["TOKEN"], org_id=_ORG_ID, org_name="internal.example", auth_required=True)

    with pytest.raises(SecretNotFoundError) as captured:
        await store.get("TOKEN")

    assert captured.value is missing
    assert "Shared lookup failed" not in str(captured.value)


@pytest.mark.description(
    "Organization secret reads fall back on missing or denied tenant secrets but not decryption faults."
)
@pytest.mark.parametrize("error_type", [SecretNotFoundError, SecretAccessDeniedError, SecretDecryptionError])
async def test_org_secret_store_candidate_fallback(error_type: type[SecretsError]) -> None:
    source = AsyncMock()
    error = error_type("Tenant lookup failed.")
    source.get.side_effect = [error, "shared-value"]
    store = OrgSecretStore(source, ["TOKEN"], org_id=_ORG_ID, org_name="internal.example", auth_required=True)

    if error_type is SecretDecryptionError:
        with pytest.raises(SecretDecryptionError) as captured:
            await store.get("TOKEN")
        assert captured.value is error
        assert [call.args[0] for call in source.get.await_args_list] == [f"{tenant_secret_prefix(_ORG_ID)}TOKEN"]
    else:
        assert await store.get("TOKEN") == "shared-value"
        assert [call.args[0] for call in source.get.await_args_list] == [
            f"{tenant_secret_prefix(_ORG_ID)}TOKEN",
            "TOKEN",
        ]


@pytest.mark.description(
    "Dataset fallback reads an overlapping shared secret exactly while contract names stay tenant-first."
)
@pytest.mark.parametrize("shared_value", [{"FIELD": "shared-field"}, {"OTHER": "shared-other"}, None])
async def test_dataset_fallback_preserves_overlapping_contract_secret(
    shared_value: SecretValue,
    runtime_services: RuntimeServices,
    contract: AgentContractRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = f"valkyrie/tenants/{_ORG_ID}/"
    values = {f"{prefix}dataset-config": {"OTHER": "tenant-value"}}

    async def read(name: str) -> SecretValue:
        if name == "dataset-config":
            if shared_value is None:
                raise SecretNotFoundError("Shared secret is missing")
            return shared_value
        return values[name]

    source = AsyncMock()
    source.get.side_effect = read
    store = OrgSecretStore(source, ["dataset-config"], org_id=_ORG_ID, org_name="internal.example", auth_required=True)
    assert await store.get("dataset-config") == {"OTHER": "tenant-value"}
    source.get.reset_mock()
    runtime_services.secrets = store
    monkeypatch.setattr("tracker.services.runs.start.AUTH_REQUIRED", True)
    reader = task_secret_reader(
        RunExecutionRequest(benchmark_name="swebench", contract=contract),
        runtime_services,
        OrgContext(id=_ORG_ID, name="internal.example", storage_key="test-bucket"),
    )

    if shared_value == {"FIELD": "shared-field"}:
        assert await reader("dataset-config#FIELD") == "shared-field"
    else:
        with pytest.raises(SecretConfigurationError, match="has no JSON field 'FIELD'"):
            await reader("dataset-config#FIELD")
    assert [call.args[0] for call in source.get.await_args_list] == [f"{prefix}dataset-config", "dataset-config"]


@pytest.fixture(autouse=True)
def shared_secret_tenants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example, partner-*")


@pytest.mark.description(
    "Orgs on the shared tenant list or a matching prefix, and self-hosted Trackers without auth, may name shared secrets such as `SharedDaytonaSecrets`."
)
@pytest.mark.parametrize(
    ("org_name", "auth_required"),
    [("internal.example", True), ("partner-acme", True), ("acme.example", False)],
    ids=["listed-tenant", "listed-prefix", "self-hosted"],
)
def test_trusted_callers_may_use_shared_secrets(org_name: str, auth_required: bool) -> None:
    validate_secret_names(
        ["SharedDaytonaSecrets", "benchmark-services/shared-api-key"],
        org_id=_ORG_ID,
        org_name=org_name,
        auth_required=auth_required,
    )


@pytest.mark.description("An org off the shared list may name secrets under `valkyrie/tenants/<org ID>/`.")
def test_other_orgs_may_use_secrets_under_their_own_prefix() -> None:
    validate_secret_names(
        [f"valkyrie/tenants/{_ORG_ID}/daytona", f"valkyrie/tenants/{_ORG_ID}/model-keys"],
        org_id=_ORG_ID,
        org_name="acme.example",
        auth_required=True,
    )


@pytest.mark.description(
    "An org off the shared list that names a shared secret, another org's secret, or a full ARN gets a not found error."
)
@pytest.mark.parametrize(
    ("org_name", "secret_name"),
    [
        ("acme.example", "SharedDaytonaSecrets"),
        ("acme.example", f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona"),
        ("acme.example", f"valkyrie/tenants/{_ORG_ID}-other/daytona"),
        ("acme.example", f"arn:aws:secretsmanager:us-east-1:123456789012:secret:valkyrie/tenants/{_ORG_ID}/daytona"),
        ("partneracme", "SharedDaytonaSecrets"),
        ("acme-partner-", "SharedDaytonaSecrets"),
        ("internal.example.evil", "SharedDaytonaSecrets"),
    ],
    ids=[
        "shared",
        "other-org",
        "lookalike-prefix",
        "arn",
        "prefix-without-dash",
        "prefix-not-at-start",
        "listed-name-as-prefix",
    ],
)
def test_other_orgs_cannot_name_secrets_outside_their_prefix(org_name: str, secret_name: str) -> None:
    with pytest.raises(ValueError) as error:
        validate_secret_names([secret_name], org_id=_ORG_ID, org_name=org_name, auth_required=True)

    shown = "daytona" if secret_name.startswith("valkyrie/tenants/") else secret_name
    assert str(error.value) == f"Secret '{shown}' does not exist"


@pytest.mark.description("With no trusted list set, every org is limited to its own prefix.")
def test_unset_list_limits_every_org_to_its_own_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "")

    with pytest.raises(ValueError):
        validate_secret_names(["SharedDaytonaSecrets"], org_id=_ORG_ID, org_name="internal.example", auth_required=True)


@pytest.mark.description(
    "`secret_display_name` drops the `valkyrie/tenants/<org ID>/` prefix and leaves other names alone."
)
@pytest.mark.parametrize(
    ("secret_name", "shown"),
    [
        (f"valkyrie/tenants/{_ORG_ID}/daytona", "daytona"),
        (f"valkyrie/tenants/{_ORG_ID}/team/model-keys", "team/model-keys"),
        ("valkyrie/tenants/", "valkyrie/tenants/"),
        ("SharedDaytonaSecrets", "SharedDaytonaSecrets"),
        ("valkyrie/sentry-dsn", "valkyrie/sentry-dsn"),
    ],
    ids=["tenant", "nested", "bare-root", "shared", "platform"],
)
def test_tenant_secrets_are_shown_without_the_tenant_prefix(secret_name: str, shown: str) -> None:
    assert secret_display_name(secret_name) == shown


@pytest.mark.description("A customer's short secret name resolves to the full name under its tenant prefix.")
@pytest.mark.parametrize(
    ("org_name", "auth_required", "name", "resolved"),
    [
        ("acme.example", True, "daytona", f"valkyrie/tenants/{_ORG_ID}/daytona"),
        ("acme.example", True, f"valkyrie/tenants/{_ORG_ID}/daytona", f"valkyrie/tenants/{_ORG_ID}/daytona"),
        (
            "acme.example",
            True,
            f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona",
            f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona",
        ),
        ("internal.example", True, "SharedDaytonaSecrets", "SharedDaytonaSecrets"),
        ("acme.example", False, "SharedDaytonaSecrets", "SharedDaytonaSecrets"),
    ],
    ids=["short-name", "full-reference", "other-org-reference", "shared-tenant", "self-hosted"],
)
def test_customers_name_their_own_secrets_without_the_prefix(
    org_name: str, auth_required: bool, name: str, resolved: str
) -> None:
    assert resolve_secret_name(name, org_id=_ORG_ID, org_name=org_name, auth_required=auth_required) == resolved


@pytest.mark.description(
    "A dedicated ValSmith org's secret names resolve under its own tenant prefix, even when a wildcard shared tenant pattern matches it."
)
@pytest.mark.parametrize("org_name", ["vals-smith-dev-gh-52346350", "vals-smith-prod-gh-52346350"])
@pytest.mark.parametrize(
    "shared_tenants", ["vals.ai,vals-smith-*", "vals-smith-dev-gh-52346350,vals-smith-prod-gh-52346350"]
)
def test_dedicated_customer_secret_names_stay_in_their_tenant(
    monkeypatch: pytest.MonkeyPatch, org_name: str, shared_tenants: str
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", shared_tenants)

    assert (
        resolve_secret_name("devModelGatewayClientConfig", org_id=_ORG_ID, org_name=org_name, auth_required=True)
        == f"valkyrie/tenants/{_ORG_ID}/devModelGatewayClientConfig"
    )

    with pytest.raises(ValueError):
        validate_secret_names(
            [f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona"],
            org_id=_ORG_ID,
            org_name=org_name,
            auth_required=True,
        )


@pytest.mark.description("`vals.ai` and the research ValSmith orgs still resolve `SharedDaytonaSecrets` to itself.")
@pytest.mark.parametrize("org_name", ["vals.ai", "vals-smith-research", "vals-smith-dev-gh-research"])
def test_legacy_research_tenants_keep_explicit_shared_secret_access(
    monkeypatch: pytest.MonkeyPatch, org_name: str
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "vals.ai,vals-smith-*")

    assert (
        resolve_secret_name("SharedDaytonaSecrets", org_id=_ORG_ID, org_name=org_name, auth_required=True)
        == "SharedDaytonaSecrets"
    )


@pytest.mark.description("Shared tenants cannot name another org's gateway key.")
@pytest.mark.parametrize(
    "key",
    [
        "model-gateway-00000000-0000-0000-0000-0000000000aa",
        "projects/gateway-keys/00000000-0000-0000-0000-0000000000aa",
    ],
    ids=["tracker-copy", "project-service"],
)
def test_shared_tenants_cannot_name_another_orgs_gateway_key(key: str) -> None:
    validate_secret_names(
        [f"valkyrie/tenants/{_ORG_ID}/{key}"], org_id=_ORG_ID, org_name="internal.example", auth_required=True
    )

    with pytest.raises(ValueError) as error:
        validate_secret_names(
            [f"valkyrie/tenants/{_OTHER_ORG_ID}/{key}"],
            org_id=_ORG_ID,
            org_name="internal.example",
            auth_required=True,
        )

    assert str(error.value) == f"Secret '{key}' does not exist"


@pytest.mark.description(
    "A shared tenant looks up its own tenant secret before the shared one, and other orgs only read their own."
)
@pytest.mark.parametrize(
    ("org_name", "auth_required", "name", "candidates"),
    [
        ("internal.example", True, "TOKEN", [f"valkyrie/tenants/{_ORG_ID}/TOKEN", "TOKEN"]),
        ("internal.example", True, f"valkyrie/tenants/{_ORG_ID}/TOKEN", [f"valkyrie/tenants/{_ORG_ID}/TOKEN"]),
        ("acme.example", True, "TOKEN", [f"valkyrie/tenants/{_ORG_ID}/TOKEN"]),
        ("acme.example", False, "TOKEN", ["TOKEN"]),
    ],
    ids=["shared-tenant", "shared-tenant-full-reference", "customer", "self-hosted"],
)
def test_secret_name_candidates_put_the_run_org_first(
    monkeypatch: pytest.MonkeyPatch, org_name: str, auth_required: bool, name: str, candidates: list[str]
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example")

    assert secret_name_candidates(name, org_id=_ORG_ID, org_name=org_name, auth_required=auth_required) == candidates


@pytest.mark.description("A hosted run keeps a project-service Gateway key under its tenant path as its selected key.")
def test_project_service_gateway_key_is_selected_for_a_hosted_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GATEWAY_KEY_ORGS", frozenset({"internal.example"}))

    def hosted(_benchmark_name: str, _custom_service: str | None) -> config.BenchmarkServiceDestination:
        return config.BenchmarkServiceDestination.HOSTED

    monkeypatch.setattr(config, "classify_benchmark_service_destination", hosted)
    reference = f"valkyrie/tenants/{_ORG_ID}/projects/gateway-keys/00000000-0000-0000-0000-0000000000aa"
    org = cast(Org, SimpleNamespace(id=_ORG_ID, name="internal.example"))
    names = {"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}

    assert selected_project_gateway_reference(names, org, reference, "swebench", None) == reference
    assert selected_project_gateway_reference(names, org, reference.replace("aa", "zz"), "swebench", None) is None


@pytest.mark.description("A dataset project key does not depend on its benchmark name being a service hostname.")
def test_dataset_gateway_key_accepts_resource_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GATEWAY_KEY_ORGS", frozenset({"internal.example"}))
    reference = f"valkyrie/tenants/{_ORG_ID}/projects/gateway-keys/00000000-0000-0000-0000-0000000000aa"
    org = Org(id=_ORG_ID, name="internal.example")
    names = {"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}

    selected = selected_project_gateway_reference(names, org, reference, "my_benchmark", None, dataset_task=True)

    assert selected == reference
