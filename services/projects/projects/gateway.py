import hashlib
import json
import logging
import os
import secrets
from enum import StrEnum
from functools import cache
from typing import TYPE_CHECKING
from uuid import UUID

import httpx
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from projects.models import Project, ProjectLimit

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import SecretsManagerClient

MACHINE_PATH = "/admin/managed-auth/machine"
ORG_ID_TAG = "valkyrie:org-id"
TIMEOUT_SECONDS = 5.0
BUDGET_TIMEOUT_SECONDS = 10.0
ANOTHER_VERIFIER = "Key is active with another verifier"

logger = logging.getLogger(__name__)


class KeyRequestStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


class KeyStatus(StrEnum):
    PENDING_COLLECTION = "pending_collection"
    ACTIVE = "active"
    REVOKED = "revoked"


class VerifierOutcome(StrEnum):
    SET = "set"
    ANOTHER = "another"
    CHANGED = "changed"


def secret_name(org_id: UUID, key_id: UUID) -> str:
    return f"valkyrie/tenants/{org_id}/projects/gateway-keys/{key_id}"


def tracker_copy_name(org_id: UUID, key_id: UUID) -> str:
    return f"valkyrie/tenants/{org_id}/model-gateway-{key_id}"


def new_secret() -> str:
    return f"mgwk_{secrets.token_urlsafe(32)}"


def verifier(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


class GatewayClient:
    def __init__(self, url: str, machine_key: str) -> None:
        self._client = httpx.Client(
            base_url=url,
            headers={"Authorization": f"Bearer {machine_key}"},
            timeout=httpx.Timeout(TIMEOUT_SECONDS, connect=2.0),
        )

    def _get(self, path: str) -> httpx.Response:
        try:
            return self._client.get(path)
        except (httpx.ConnectError, httpx.TimeoutException):
            return self._client.get(path)

    def request_status(self, request_id: UUID) -> tuple[KeyRequestStatus, UUID | None]:
        response = self._get(f"{MACHINE_PATH}/requests/{request_id}")
        _ = response.raise_for_status()
        body = response.json()
        key_id = body.get("key_id")
        return KeyRequestStatus(body["status"]), UUID(key_id) if key_id else None

    def key_state(self, key_id: UUID) -> tuple[KeyStatus, int]:
        response = self._get(f"{MACHINE_PATH}/keys/{key_id}")
        _ = response.raise_for_status()
        body = response.json()
        return KeyStatus(body["status"]), int(body["revision"])

    def set_verifier(self, key_id: UUID, revision: int, verifier_hash: str) -> VerifierOutcome:
        response = self._client.post(
            f"{MACHINE_PATH}/keys/{key_id}/verifier",
            json={"expected_revision": revision, "verifier_hash": verifier_hash},
        )
        if response.status_code == 409:
            return (
                VerifierOutcome.ANOTHER
                if response.json().get("detail") == ANOTHER_VERIFIER
                else VerifierOutcome.CHANGED
            )
        _ = response.raise_for_status()
        return VerifierOutcome.SET

    def put_project_budget(self, project: Project, environment: str, limit: ProjectLimit) -> None:
        body = {
            "project_organization_id": str(project.org_id),
            "project_environment": environment,
            "limit_usd": str(limit.limit_dollars),
            "version": limit.version,
            "reason": limit.reason,
        }
        response = self._client.put(
            f"{MACHINE_PATH}/projects/{project.id}/budget", json=body, timeout=BUDGET_TIMEOUT_SECONDS
        )
        _ = response.raise_for_status()


class SecretStore:
    def __init__(self, client: "SecretsManagerClient", gateway_url: str) -> None:
        self._client = client
        self._gateway_url = gateway_url

    def keep_first(self, name: str, org_id: UUID, secret: str) -> str:
        try:
            _ = self._client.create_secret(
                Name=name, SecretString=self._value(secret), Tags=[{"Key": ORG_ID_TAG, "Value": str(org_id)}]
            )
        except self._client.exceptions.ResourceExistsException:
            return self._read(name)
        return secret

    def get(self, name: str) -> str | None:
        try:
            return self._read(name)
        except self._client.exceptions.ResourceNotFoundException:
            return None

    def replace(self, name: str, secret: str) -> None:
        _ = self._client.put_secret_value(SecretId=name, SecretString=self._value(secret))

    def _value(self, secret: str) -> str:
        return json.dumps({"MODEL_GATEWAY_URL": self._gateway_url, "MODEL_GATEWAY_API_KEY": secret})

    def _read(self, name: str) -> str:
        value = self._client.get_secret_value(SecretId=name)["SecretString"]
        return str(json.loads(value)["MODEL_GATEWAY_API_KEY"])


@cache
def gateway() -> GatewayClient:
    return GatewayClient(os.environ["MODEL_GATEWAY_URL"], os.environ["MODEL_GATEWAY_MACHINE_API_KEY"])


def put_project_budget(project: Project, limit: ProjectLimit) -> None:
    gateway().put_project_budget(project, os.environ["VALKYRIE_STAGE"], limit)


def push_budgets(bound: Engine, project_id: UUID | None = None) -> None:
    unconfirmed = ProjectLimit.gateway_confirmed_limit.is_(None)
    if project_id is not None:
        unconfirmed = unconfirmed & (ProjectLimit.project_id == project_id)
    with Session(bound) as session:
        pending_ids = select(ProjectLimit.project_id).where(unconfirmed).order_by(ProjectLimit.updated_at)
        ids = session.scalars(pending_ids).all()
        session.commit()
        for pending in ids:
            locked = select(ProjectLimit).where(unconfirmed, ProjectLimit.project_id == pending)
            item = session.scalars(locked.with_for_update(skip_locked=True)).first()
            if item is not None:
                _push_locked(session, item)
            session.commit()


def _push_locked(session: Session, item: ProjectLimit) -> None:
    sent = item.limit_dollars
    try:
        put_project_budget(session.get_one(Project, item.project_id), item)
        item.gateway_confirmed_limit = sent
    except Exception:
        logger.warning("Gateway budget push failed for project %s", item.project_id, exc_info=True)
