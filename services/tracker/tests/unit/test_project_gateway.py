from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import HTTPException
import pytest

from tracker import project_gateway
from tracker.aws.secrets import SecretsManagerStore
from tracker.clients.projects import GatewayKeyConflictError
from tracker.database.models import Org, Project, SpendType
from tracker.exceptions import SecretAccessDeniedError, SecretDecryptionError, SecretNotFoundError, SecretsError
from tracker.secret_access import (
    GATEWAY_SECRET_NAME_PREFIX,
    PROJECT_SERVICE_GATEWAY_SECRET_PREFIX,
    tenant_secret_prefix,
)


@pytest.mark.description(
    "Gateway handover treats missing and denied held keys as absent but propagates decryption faults."
)
@pytest.mark.parametrize(
    "error", [None, SecretNotFoundError("missing"), SecretAccessDeniedError("denied"), SecretDecryptionError("decrypt")]
)
async def test_gateway_secret_probe_errors(monkeypatch: pytest.MonkeyPatch, error: SecretsError | None) -> None:
    org = Org(id=uuid4(), name="vals.ai")
    key_id = uuid4()
    project = Project(
        org_id=org.id, handle="held-key", name="Held Key", spend_type=SpendType.DEVELOPMENT, gateway_key_id=key_id
    )
    projects = MagicMock()
    projects.mint_gateway_key.side_effect = GatewayKeyConflictError("Key already exists")
    monkeypatch.setattr(project_gateway, "project_service", lambda: projects)
    store = MagicMock(spec=SecretsManagerStore)
    store.get.side_effect = error
    store.get.return_value = {"MODEL_GATEWAY_API_KEY": "held-secret"}
    if isinstance(error, SecretDecryptionError):
        with pytest.raises(SecretDecryptionError) as captured:
            await project_gateway._project_service_gateway_key(project, org, store)
        assert captured.value is error
    elif error is not None:
        with pytest.raises(HTTPException) as missing:
            await project_gateway._project_service_gateway_key(project, org, store)
        assert missing.value.status_code == 409
        assert "held-key" in missing.value.detail
    else:
        assert await project_gateway._project_service_gateway_key(project, org, store) == key_id
        projects.hand_over_gateway_key.assert_called_once_with(org.id, project.id, key_id, "held-secret")
    if error is not None:
        projects.hand_over_gateway_key.assert_not_called()
    store.get.assert_awaited_once_with(f"{tenant_secret_prefix(org.id)}{GATEWAY_SECRET_NAME_PREFIX}{key_id}")


@pytest.mark.description(
    "An existing project Gateway key the project service confirms needs no database lock or Projects write."
)
@pytest.mark.parametrize("same_org", [True, False])
def test_existing_gateway_key(monkeypatch: pytest.MonkeyPatch, same_org: bool) -> None:
    org = Org(id=uuid4(), name="vals.ai")
    key_id = uuid4()
    project = Project(
        org_id=org.id if same_org else uuid4(),
        handle="existing-key",
        name="Existing Key",
        spend_type=SpendType.DEVELOPMENT,
        gateway_key_id=key_id,
    )
    session = MagicMock(side_effect=AssertionError("Unexpected database access"))
    repository = MagicMock(side_effect=AssertionError("Unexpected Projects access"))
    monkeypatch.setattr(project_gateway, "Session", session)
    monkeypatch.setattr(project_gateway, "ProjectRepository", repository)
    confirmed = MagicMock(return_value=key_id)
    monkeypatch.setattr(project_gateway, "_gateway_key", confirmed)
    if same_org:
        references = project_gateway._prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        reference = f"{tenant_secret_prefix(org.id)}{PROJECT_SERVICE_GATEWAY_SECRET_PREFIX}{key_id}"
        assert references == {
            "MODEL_GATEWAY_URL": reference,
            "MODEL_GATEWAY_API_KEY": reference,
        }
    else:
        with pytest.raises(HTTPException) as error:
            project_gateway._prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        assert error.value.status_code == 404
        confirmed.assert_not_called()
    session.assert_not_called()
    repository.assert_not_called()
