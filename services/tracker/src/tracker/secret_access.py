import re
from collections.abc import Iterable
from typing import TYPE_CHECKING
from uuid import UUID

from tracker import config
from tracker.exceptions import SecretNotFoundError
from tracker.runtime.secrets import TENANT_SECRET_ROOT, SecretStore, SecretValue, secret_display_name

if TYPE_CHECKING:
    from tracker.database.models import Org

GATEWAY_SECRET_NAME_PREFIX = "model-gateway-"


def is_generated_project_gateway_secret_name(name: str) -> bool:
    if not name.startswith(GATEWAY_SECRET_NAME_PREFIX):
        return False
    key_id = name.removeprefix(GATEWAY_SECRET_NAME_PREFIX)
    try:
        return str(UUID(key_id)) == key_id
    except ValueError:
        return False


def tenant_secret_prefix(org_id: UUID) -> str:
    return f"{TENANT_SECRET_ROOT}{org_id}/"


def may_use_shared_secrets(org_name: str) -> bool:
    if re.fullmatch(r"vals-smith-(?:dev|prod)-gh-[1-9][0-9]*", org_name):
        return False

    for entry in config.AWS_SHARED_SECRET_TENANTS.split(","):
        entry = entry.strip()
        if entry.endswith("*") and len(entry) > 1 and org_name.startswith(entry[:-1]):
            return True
        if entry and org_name == entry:
            return True
    return False


def resolve_secret_name(name: str, *, org_id: UUID, org_name: str, auth_required: bool) -> str:
    if not auth_required or may_use_shared_secrets(org_name) or name.startswith(TENANT_SECRET_ROOT):
        return name
    return f"{tenant_secret_prefix(org_id)}{name}"


def secret_name_candidates(name: str, *, org_id: UUID, org_name: str, auth_required: bool) -> list[str]:
    if auth_required and may_use_shared_secrets(org_name) and not name.startswith(TENANT_SECRET_ROOT):
        return [f"{tenant_secret_prefix(org_id)}{name}", name]
    return [resolve_secret_name(name, org_id=org_id, org_name=org_name, auth_required=auth_required)]


class OrgSecretStore:
    def __init__(
        self, store: SecretStore, names: Iterable[str], *, org_id: UUID, org_name: str, auth_required: bool
    ) -> None:
        self._store = store
        self._names = frozenset(names)
        self._org_id = org_id
        self._org_name = org_name
        self._auth_required = auth_required

    @property
    def store(self) -> SecretStore:
        return self._store

    async def get(self, name: str) -> SecretValue:
        if name not in self._names:
            return await self._store.get(name)
        candidates = secret_name_candidates(
            name, org_id=self._org_id, org_name=self._org_name, auth_required=self._auth_required
        )
        for candidate in candidates[:-1]:
            try:
                return await self._store.get(candidate)
            except SecretNotFoundError:
                pass
        return await self._store.get(candidates[-1])


def validate_secret_names(names: Iterable[str], *, org_id: UUID, org_name: str, auth_required: bool) -> None:
    if not auth_required:
        return

    prefix = tenant_secret_prefix(org_id)
    shared = may_use_shared_secrets(org_name)
    for name in names:
        if not name.startswith(prefix) and (
            not shared or is_generated_project_gateway_secret_name(name.split("/")[-1])
        ):
            raise ValueError(f"Secret '{secret_display_name(name)}' does not exist")


def selected_project_gateway_reference(
    secret_names: dict[str, str],
    org: "Org",
    selected_project_gateway_secret_ref: str | None,
    benchmark_name: str,
    custom_benchmark_service: str | None,
) -> str | None:
    reference = secret_names.get("MODEL_GATEWAY_API_KEY")
    prefix = tenant_secret_prefix(org.id)
    if (
        org.name in config.GATEWAY_KEY_ORGS
        and config.classify_benchmark_service_destination(benchmark_name, custom_benchmark_service)
        == config.BenchmarkServiceDestination.HOSTED
        and selected_project_gateway_secret_ref is not None
        and reference == selected_project_gateway_secret_ref
        and reference == secret_names.get("MODEL_GATEWAY_URL")
        and reference.startswith(prefix)
        and is_generated_project_gateway_secret_name(reference.removeprefix(prefix))
    ):
        return reference
    return None
