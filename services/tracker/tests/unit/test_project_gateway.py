from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import HTTPException
import pytest

from tracker import project_gateway
from tracker.database.models import Org, Project, SpendType
from tracker.secret_access import GATEWAY_SECRET_NAME_PREFIX, tenant_secret_prefix


@pytest.mark.description("An existing project Gateway key needs no database lock or Projects service call.")
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
    if same_org:
        references = project_gateway._prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        reference = f"{tenant_secret_prefix(org.id)}{GATEWAY_SECRET_NAME_PREFIX}{key_id}"
        assert references == {
            "MODEL_GATEWAY_URL": reference,
            "MODEL_GATEWAY_API_KEY": reference,
        }
    else:
        with pytest.raises(HTTPException) as error:
            project_gateway._prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        assert error.value.status_code == 404
    session.assert_not_called()
    repository.assert_not_called()
