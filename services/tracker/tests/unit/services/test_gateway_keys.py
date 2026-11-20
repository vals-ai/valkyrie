from typing import cast
from unittest.mock import MagicMock
from uuid import UUID, uuid4

from fastapi import HTTPException
import pytest

from tests.utils import TEST_ORG_ID
from tracker.aws.secrets import SecretsManagerStore
from tracker.clients.projects import GatewayKeyConflictError
from tracker.database.models import Org, Project, SpendType
from tracker.exceptions import SecretAccessDeniedError, SecretDecryptionError, SecretNotFoundError, SecretsError
from tracker.secret_access import (
    GATEWAY_SECRET_NAME_PREFIX,
    PROJECT_SERVICE_GATEWAY_SECRET_PREFIX,
    tenant_secret_prefix,
)
from tracker.services.gateway import keys

TENANT = f"valkyrie/tenants/{TEST_ORG_ID}/"


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
    monkeypatch.setattr(keys, "project_service", lambda: projects)
    store = MagicMock(spec=SecretsManagerStore)
    store.get.side_effect = error
    store.get.return_value = {"MODEL_GATEWAY_API_KEY": "held-secret"}
    if isinstance(error, SecretDecryptionError):
        with pytest.raises(SecretDecryptionError) as captured:
            await keys._project_service_gateway_key(project, org, store)
        assert captured.value is error
    elif error is not None:
        with pytest.raises(HTTPException) as missing:
            await keys._project_service_gateway_key(project, org, store)
        assert missing.value.status_code == 409
        assert "held-key" in missing.value.detail
    else:
        assert await keys._project_service_gateway_key(project, org, store) == key_id
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
    monkeypatch.setattr(keys, "Session", session)
    monkeypatch.setattr(keys, "ProjectRepository", repository)
    confirmed = MagicMock(return_value=key_id)
    monkeypatch.setattr(keys, "_gateway_key", confirmed)
    if same_org:
        references = keys.prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        reference = f"{tenant_secret_prefix(org.id)}{PROJECT_SERVICE_GATEWAY_SECRET_PREFIX}{key_id}"
        assert references == {
            "MODEL_GATEWAY_URL": reference,
            "MODEL_GATEWAY_API_KEY": reference,
        }
    else:
        with pytest.raises(HTTPException) as error:
            keys.prepare_project_gateway_key(MagicMock(), project, org, MagicMock())
        assert error.value.status_code == 404
        confirmed.assert_not_called()
    session.assert_not_called()
    repository.assert_not_called()


class FakeStore:
    def __init__(self) -> None:
        self.secrets: dict[str, str] = {}
        self.reads: list[str] = []

    async def list_names(self, prefix: str) -> list[str]:
        return sorted(name for name in self.secrets if name.startswith(prefix))

    async def get(self, name: str) -> dict[str, str]:
        self.reads.append(name)
        return {"MODEL_GATEWAY_URL": "https://gw", "MODEL_GATEWAY_API_KEY": self.secrets[name]}


class FakeProjectService:
    def __init__(self, store: FakeStore) -> None:
        self.store = store
        self.records: dict[UUID, Project] = {}
        self.hand_overs: list[UUID] = []

    def list_rows(self, org_id: UUID, include_archived: bool) -> list[Project]:
        return [record.model_copy(update={"gateway_key_id": None}) for record in self.records.values()]

    def get(self, org_id: UUID, project_id: UUID) -> Project | None:
        return self.records.get(project_id)

    def mint_gateway_key(self, org_id: UUID, project_id: UUID) -> UUID:
        raise GatewayKeyConflictError("Gateway key was collected elsewhere")

    def set_gateway_key(self, org_id: UUID, project_id: UUID, key_id: UUID) -> UUID:
        self.records[project_id] = self.records[project_id].model_copy(update={"gateway_key_id": key_id})
        return key_id

    def hand_over_gateway_key(self, org_id: UUID, project_id: UUID, key_id: UUID, secret: str) -> None:
        self.hand_overs.append(project_id)
        self.store.secrets[f"{TENANT}projects/gateway-keys/{key_id}"] = secret


def _project(handle: str, key_id: UUID | None) -> Project:
    return Project(
        org_id=TEST_ORG_ID, handle=handle, name=handle, spend_type=SpendType.DEVELOPMENT, gateway_key_id=key_id
    )


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> FakeProjectService:
    service = FakeProjectService(FakeStore())
    monkeypatch.setattr(keys, "project_service", lambda: service)
    return service


def _projects(service: FakeProjectService) -> list[Project]:
    held, moving, lost = uuid4(), uuid4(), uuid4()
    service.store.secrets[f"{TENANT}projects/gateway-keys/{held}"] = "mgwk_held"
    service.store.secrets[f"{TENANT}model-gateway-{held}"] = "mgwk_held"
    service.store.secrets[f"{TENANT}model-gateway-{moving}"] = "mgwk_moving"
    projects = [_project("held", held), _project("moving", moving), _project("lost", lost), _project("unkeyed", None)]
    service.records = {project.id: project for project in projects}
    return projects


async def _run(service: FakeProjectService, apply: bool) -> dict[str, object]:
    org = Org(id=TEST_ORG_ID, name="vals.ai")
    response = await keys.hand_over_gateway_keys(org, cast(SecretsManagerStore, cast(object, service.store)), apply)
    return response.model_dump()


@pytest.mark.description("A dry run counts keyed projects by secret name and reads no secret.")
async def test_dry_run_counts_by_name(service: FakeProjectService) -> None:
    _ = _projects(service)

    assert await _run(service, apply=False) == {
        "applied": False,
        "projects_with_key": 3,
        "held": 1,
        "to_hand_over": 1,
        "without_tracker_copy": ["lost"],
    }
    assert service.store.reads == []


@pytest.mark.description("Applying hands each missing secret over once, and a rerun finds every movable one held.")
async def test_apply_hands_over_once(service: FakeProjectService) -> None:
    projects = _projects(service)

    first = await _run(service, apply=True)
    second = await _run(service, apply=False)

    assert (first["held"], first["to_hand_over"]) == (1, 1)
    assert service.store.secrets[f"{TENANT}projects/gateway-keys/{projects[1].gateway_key_id}"] == "mgwk_moving"
    assert (second["held"], second["to_hand_over"], second["without_tracker_copy"]) == (2, 0, ["lost"])


@pytest.mark.description(
    "Applying also hands over a key the project service holds by name, since only a hand-over confirms that secret."
)
async def test_apply_confirms_a_secret_held_by_name(service: FakeProjectService) -> None:
    projects = _projects(service)

    _ = await _run(service, apply=True)

    assert service.hand_overs == [projects[0].id, projects[1].id]


@pytest.mark.description(
    "A project whose approved request holds the key, with no key ID saved, is counted and handed over like the run path."
)
async def test_a_requested_key_without_a_saved_id_is_handed_over(
    service: FakeProjectService, monkeypatch: pytest.MonkeyPatch
) -> None:
    key_id = uuid4()
    project = _project("requested", None).model_copy(update={"gateway_request_id": uuid4()})
    service.records = {project.id: project}
    service.store.secrets[f"{TENANT}model-gateway-{key_id}"] = "mgwk_requested"

    async def requested_key_id(requested: Project) -> UUID | None:
        return key_id if requested.gateway_request_id == project.gateway_request_id else None

    monkeypatch.setattr(keys, "requested_key_id", requested_key_id)

    dry = await _run(service, apply=False)
    _ = await _run(service, apply=True)

    assert (dry["projects_with_key"], dry["to_hand_over"]) == (1, 1)
    assert service.store.secrets[f"{TENANT}projects/gateway-keys/{key_id}"] == "mgwk_requested"
    assert service.records[project.id].gateway_key_id == key_id


@pytest.mark.description(
    "An approved request whose key Tracker never collected is left out, because its next run mints the key."
)
async def test_an_uncollected_requested_key_is_not_counted(
    service: FakeProjectService, monkeypatch: pytest.MonkeyPatch
) -> None:
    key_id = uuid4()
    project = _project("uncollected", None).model_copy(update={"gateway_request_id": uuid4()})
    service.records = {project.id: project}

    async def requested_key_id(requested: Project) -> UUID | None:
        return key_id

    monkeypatch.setattr(keys, "requested_key_id", requested_key_id)

    assert await _run(service, apply=False) == {
        "applied": False,
        "projects_with_key": 0,
        "held": 0,
        "to_hand_over": 0,
        "without_tracker_copy": [],
    }
