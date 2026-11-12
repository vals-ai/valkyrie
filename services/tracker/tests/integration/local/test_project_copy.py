import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Org, Project
from tracker.domain import SpendType
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.project import ProjectRepository
from tracker.services.projects import copy_projects


class MockProjectService:
    def __init__(self) -> None:
        self.kept: dict[str, dict[str, str]] = {}
        self.orgs: dict[str, list[dict[str, Any]]] = {}
        self.drop: set[str] = set()
        self.fail = False

    def handle(self, request: httpx.Request) -> httpx.Response:
        org_id = request.url.path.split("/")[2]
        if self.fail:
            return httpx.Response(500, request=request)
        if request.method == "PUT":
            rows = json.loads(request.content)["projects"]
            self.orgs[org_id] = [row | self.kept.get(row["handle"], {}) for row in rows]
            return httpx.Response(200, json={"projects": self.orgs[org_id]})
        rows = [row for row in self.orgs.get(org_id, []) if row["handle"] not in self.drop]
        if request.url.path.count("/") == 4:
            project_id = request.url.path.rsplit("/", 1)[1]
            return httpx.Response(200, json=next(row for row in rows if row["id"] == project_id))
        listed = [{k: v for k, v in row.items() if not k.startswith("gateway_")} for row in rows]
        return httpx.Response(200, json={"projects": sorted(listed, key=lambda row: row["handle"])})


@pytest.fixture
def service() -> MockProjectService:
    return MockProjectService()


def _copy(tracker_database: Session, service: MockProjectService) -> dict[Any, bool]:
    client = ProjectServiceClient(
        httpx.Client(base_url="http://projects", transport=httpx.MockTransport(service.handle))
    )
    return copy_projects(OrganizationRepository(tracker_database), ProjectRepository(tracker_database), client)


def _add(tracker_database: Session, handle: str, org_id: UUID = TEST_ORG_ID, **fields: Any) -> None:
    tracker_database.add(
        Project(org_id=org_id, handle=handle, name=handle.title(), spend_type=SpendType.DEVELOPMENT, **fields)
    )
    tracker_database.commit()


class TestCopyProjects:
    @pytest.mark.description(
        "Every Tracker project, archived ones included, is copied and the org's two lists compare equal."
    )
    def test_copies_every_project(self, tracker_database: Session, service: MockProjectService) -> None:
        _add(tracker_database, "router-bench", customer="acme", extra_tags={"engagement": "q4"})
        _add(tracker_database, "old-bench", archived_at=datetime(2026, 11, 1, 12, tzinfo=UTC))

        equal = _copy(tracker_database, service)

        assert equal[TEST_ORG_ID] is True
        assert [row["handle"] for row in service.orgs[str(TEST_ORG_ID)]] == ["old-bench", "router-bench"]

    @pytest.mark.description("Two orgs with the same handle each get only their own project and compare equal.")
    def test_keeps_orgs_apart(self, tracker_database: Session, service: MockProjectService) -> None:
        other = Org(id=uuid4(), name="other-org", storage_key="other-bucket")
        tracker_database.add(other)
        _add(tracker_database, "router-bench")
        _add(tracker_database, "router-bench", org_id=other.id)

        equal = _copy(tracker_database, service)

        assert equal == {TEST_ORG_ID: True, other.id: True}
        assert service.orgs[str(TEST_ORG_ID)][0]["id"] != service.orgs[str(other.id)][0]["id"]

    @pytest.mark.description("An org with no Tracker projects has its earlier copies cleared and compares equal.")
    def test_clears_org_without_projects(self, tracker_database: Session, service: MockProjectService) -> None:
        service.orgs[str(TEST_ORG_ID)] = [{"handle": "deleted-bench"}]

        assert _copy(tracker_database, service)[TEST_ORG_ID] is True
        assert service.orgs[str(TEST_ORG_ID)] == []

    @pytest.mark.description("An org whose project service list misses a Tracker project compares unequal.")
    def test_reports_missing_project(self, tracker_database: Session, service: MockProjectService) -> None:
        _add(tracker_database, "router-bench")
        service.drop.add("router-bench")

        assert _copy(tracker_database, service)[TEST_ORG_ID] is False

    @pytest.mark.description(
        "An org whose service row keeps a different Gateway key or request ID than Tracker's compares unequal."
    )
    @pytest.mark.parametrize(
        ("field", "tracker_value"),
        [("gateway_key_id", uuid4()), ("gateway_key_id", None), ("gateway_request_id", None)],
        ids=["different-key", "service-only-key", "service-only-request"],
    )
    def test_reports_different_gateway_id(
        self, tracker_database: Session, service: MockProjectService, field: str, tracker_value: UUID | None
    ) -> None:
        fields: dict[str, Any] = {field: tracker_value}
        _add(tracker_database, "router-bench", **fields)
        service.kept["router-bench"] = {field: str(uuid4())}

        assert _copy(tracker_database, service)[TEST_ORG_ID] is False
