from typing import cast
from uuid import UUID, uuid4

import pytest

from tests.utils import TEST_ORG_ID
from tracker import project_gateway
from tracker.aws.secrets import SecretsManagerStore
from tracker.clients.projects import GatewayKeyConflictError
from tracker.database.models import Org, Project, SpendType

TENANT = f"valkyrie/tenants/{TEST_ORG_ID}/"


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
    monkeypatch.setattr(project_gateway, "project_service", lambda: service)
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
    response = await project_gateway.hand_over_gateway_keys(
        org, cast(SecretsManagerStore, cast(object, service.store)), apply
    )
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

    monkeypatch.setattr(project_gateway, "_requested_key_id", requested_key_id)

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

    monkeypatch.setattr(project_gateway, "_requested_key_id", requested_key_id)

    assert await _run(service, apply=False) == {
        "applied": False,
        "projects_with_key": 0,
        "held": 0,
        "to_hand_over": 0,
        "without_tracker_copy": [],
    }
