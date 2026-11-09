import re
from collections.abc import Iterable
from uuid import UUID

from tracker import config
from tracker.runtime.secrets import TENANT_SECRET_ROOT, secret_display_name


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


def validate_secret_names(names: Iterable[str], *, org_id: UUID, org_name: str, auth_required: bool) -> None:
    if not auth_required:
        return
    from tracker.project_gateway import is_generated_project_gateway_secret_name

    prefix = tenant_secret_prefix(org_id)
    shared = may_use_shared_secrets(org_name)
    for name in names:
        if not name.startswith(prefix) and (
            not shared or is_generated_project_gateway_secret_name(name.split("/")[-1])
        ):
            raise ValueError(f"Secret '{secret_display_name(name)}' does not exist")
