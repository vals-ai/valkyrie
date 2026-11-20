import json
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session

import tracker.auth as tracker_auth
import tracker.repositories.project as project_repository
from main import app
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.auth import get_current_org, get_current_runner
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Org, Project, SpendType
from tracker.exceptions import ProjectLimitRequestDecidedError, ProjectLimitRequestNotFoundError

client = TestClient(app)
HUMAN = {"authorization": "Bearer human-session"}
RAISE = {"limit_dollars": "120", "reason": "full rerun"}
RECORD = RAISE | {
    "id": str(uuid4()),
    "status": "pending",
    "requested_by_email": "alice@vals.ai",
    "decided_by_email": None,
    "created_at": "2026-11-16T12:00:00Z",
    "decided_at": None,
}


class LimitRequestRoutes:
    def __init__(self, routes: MockProjectRoutes) -> None:
        self.routes = routes
        self.sent: list[httpx.Request] = []
        self.status = 200
        self.queue: list[dict[str, object]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        if "/limit-requests" not in request.url.path:
            return self.routes.handle(request)
        self.sent.append(request)
        if request.url.path == "/limit-requests":
            return httpx.Response(200, json=self.queue)
        if self.status in (404, 409):
            detail = "Limit request not found" if self.status == 404 else "Limit request is already approved"
            return httpx.Response(self.status, json={"detail": detail})
        record = RECORD | {"project_id": request.url.path.split("/")[4]}
        return httpx.Response(self.status, json=[record] if request.method == "GET" else record)


@pytest.fixture
def limit_requests(monkeypatch: MonkeyPatch, project_service: MockProjectRoutes) -> LimitRequestRoutes:
    routes = LimitRequestRoutes(project_service)
    service = ProjectServiceClient(
        httpx.Client(base_url="http://projects", transport=httpx.MockTransport(routes.handle))
    )
    monkeypatch.setattr(project_repository, "project_service", lambda: service)
    return routes


def _claims(_token: str) -> dict[str, object]:
    return {"sub": "U-alice", "tenants": {"default": {}}}


@pytest.fixture
def descope(monkeypatch: MonkeyPatch) -> MagicMock:
    monkeypatch.delitem(app.dependency_overrides, get_current_runner)
    monkeypatch.setattr(tracker_auth, "AUTH_REQUIRED", True)
    monkeypatch.setattr(tracker_auth, "DESCOPE_MANAGEMENT_KEY", "management-key")
    monkeypatch.setattr(tracker_auth, "_validate_bearer", _claims)
    descope = MagicMock()
    user = {"userId": "U-alice", "email": "alice@vals.ai", "verifiedEmail": True, "roleNames": []}
    descope.mgmt.role.load_all.return_value = {
        "roles": [{"name": "platform-operator", "permissionNames": ["platform-admin"]}]
    }
    descope.mgmt.user.load_by_user_id.return_value = {"user": user | {"userTenants": [{"tenantId": "default"}]}}
    monkeypatch.setattr(tracker_auth, "_descope_client", descope)
    monkeypatch.setattr(tracker_auth, "_descope_user_client", descope)
    return descope


@pytest.fixture
def project(project_service: MockProjectRoutes) -> Project:
    row = Project(org_id=TEST_ORG_ID, handle="router-bench", name="Router Bench", spend_type=SpendType.DEVELOPMENT)
    project_service.seed(row)
    return row


def _make_platform_admin(descope: MagicMock) -> None:
    descope.mgmt.user.load_by_user_id.return_value["user"]["roleNames"] = ["platform-operator"]


def _decide(project: Project, action: str) -> httpx.Response:
    return client.post(f"/projects/by-id/{project.id}/limit-requests/{uuid4()}/{action}", headers=HUMAN)


@pytest.mark.description("A member's raise reaches the project service with the amount, reason and their email.")
def test_a_member_files_a_raise(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    limit_requests.status = 201

    response = client.post(f"/projects/by-id/{project.id}/limit-requests", json=RAISE, headers=HUMAN)

    assert response.status_code == 201, response.text
    assert response.json()["status"] == "pending"
    (sent,) = limit_requests.sent
    assert (sent.method, sent.url.path) == ("POST", f"/orgs/{TEST_ORG_ID}/projects/{project.id}/limit-requests")
    assert json.loads(sent.content) == RAISE | {"requested_by_email": "alice@vals.ai"}


@pytest.mark.description("A member lists a project's raises as the project service returns them.")
def test_a_member_lists_raises(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    response = client.get(f"/projects/by-id/{project.id}/limit-requests", headers=HUMAN)

    assert response.status_code == 200, response.text
    assert [item["id"] for item in response.json()] == [RECORD["id"]]


@pytest.mark.parametrize("action", ["approve", "deny"])
@pytest.mark.description("A member who is not a platform admin cannot approve or deny, and nothing is sent.")
def test_only_a_platform_admin_decides(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes, action: str
) -> None:
    response = _decide(project, action)

    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "Platform administrator required"}
    assert limit_requests.sent == []


@pytest.mark.parametrize("action", ["approve", "deny"])
@pytest.mark.description("A platform admin's decision reaches the project service with their email.")
def test_a_platform_admin_decision_is_forwarded(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes, action: str
) -> None:
    _make_platform_admin(descope)

    response = _decide(project, action)

    assert response.status_code == 200, response.text
    (sent,) = limit_requests.sent
    assert sent.url.path.endswith(f"/{action}")
    assert json.loads(sent.content) == {"decided_by_email": "alice@vals.ai"}


@pytest.mark.description("A decided raise answers 409 with the project service's reason, not a 503.")
def test_a_decided_raise_answers_409(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    _make_platform_admin(descope)
    limit_requests.status = 409

    response = _decide(project, "approve")

    assert response.status_code == 409
    assert response.json()["detail"] == "Limit request is already approved"


@pytest.mark.parametrize(
    ("status", "error_type", "detail"),
    [
        (404, ProjectLimitRequestNotFoundError, "Limit request not found"),
        (409, ProjectLimitRequestDecidedError, "Limit request is already approved"),
    ],
)
@pytest.mark.description("A missing or decided limit request raises a domain error from the repository.")
def test_repository_raises_domain_errors(
    database_session: Session,
    project: Project,
    limit_requests: LimitRequestRoutes,
    status: int,
    error_type: type[Exception],
    detail: str,
) -> None:
    limit_requests.status = status
    repository = project_repository.ProjectRepository(database_session)

    with pytest.raises(error_type, match=detail):
        repository.limit_requests(project, "GET")


@pytest.mark.parametrize("status", [404, 409])
@pytest.mark.parametrize("method", ["GET", "POST", "approve", "deny"])
@pytest.mark.description(
    "Limit request routes preserve missing and already-decided responses from the project service."
)
def test_routes_map_domain_errors(
    database_session: Session,
    project: Project,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
    status: int,
    method: str,
) -> None:
    _make_platform_admin(descope)
    limit_requests.status = status
    path = f"/projects/by-id/{project.id}/limit-requests"

    if method in ("approve", "deny"):
        response = _decide(project, method)
    else:
        response = client.request(method, path, json=RAISE if method == "POST" else None, headers=HUMAN)

    assert response.status_code == status
    assert response.json()["detail"] == (
        "Limit request not found" if status == 404 else "Limit request is already approved"
    )


@pytest.mark.description(
    "A platform admin's queue lists other orgs' raises with each org's name, leaving out unknown orgs."
)
def test_a_platform_admin_reads_the_queue(
    acme_project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    acme_row = RECORD | {"project_id": str(acme_project.id), "org_id": str(acme_project.org_id)}
    limit_requests.queue = [acme_row, RECORD | {"id": str(uuid4()), "project_id": str(uuid4()), "org_id": str(uuid4())}]
    _make_platform_admin(descope)

    response = client.get("/limit-requests", params={"status": "pending"}, headers=HUMAN)

    assert response.status_code == 200, response.text
    (row,) = response.json()
    assert (row["id"], row["org_id"], row["organization"]) == (RECORD["id"], str(acme_project.org_id), "acme")
    (sent,) = limit_requests.sent
    assert (sent.url.path, sent.url.params["status"]) == ("/limit-requests", "pending")


@pytest.mark.description("A member who is not a platform admin cannot read the queue, and nothing is sent.")
def test_only_a_platform_admin_reads_the_queue(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 403, response.text
    assert limit_requests.sent == []


@pytest.mark.description("A project whose handle is limit-requests is still read by its handle, not by the queue.")
def test_a_limit_requests_handle_reads_its_project(
    database_session: Session,
    project_service: MockProjectRoutes,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
) -> None:
    row = Project(org_id=TEST_ORG_ID, handle="limit-requests", name="Limit Requests", spend_type=SpendType.DEVELOPMENT)
    project_service.seed(row)

    response = client.get("/projects/limit-requests", headers=HUMAN)

    assert response.status_code == 200, response.text
    assert response.json()["id"] == str(row.id)


@pytest.fixture
def acme_project(database_session: Session, project_service: MockProjectRoutes) -> Project:
    acme = Org(name="acme")
    database_session.add(acme)
    database_session.commit()
    row = Project(org_id=acme.id, handle="acme-evals", name="Acme Evals", spend_type=SpendType.CUSTOMER)
    project_service.seed(row)
    return row


ACME = HUMAN | {"x-valkyrie-tenant": "acme"}


@pytest.mark.description("A platform admin decides a raise in an org they are not a member of.")
def test_a_platform_admin_decides_in_another_org(
    acme_project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    _make_platform_admin(descope)

    response = client.post(
        f"/projects/by-id/{acme_project.id}/limit-requests/{uuid4()}/approve",
        headers=ACME,
    )

    assert response.status_code == 200, response.text
    (sent,) = limit_requests.sent
    assert sent.url.path.startswith(f"/orgs/{acme_project.org_id}/projects/{acme_project.id}/limit-requests/")


@pytest.mark.description(
    "A platform admin lists another org's project raises, and a non-member who is not one gets 403."
)
def test_a_platform_admin_lists_another_orgs_raises(
    acme_project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    url = f"/projects/by-id/{acme_project.id}/limit-requests"
    assert client.get(url, headers=ACME).status_code == 403
    _make_platform_admin(descope)
    response = client.get(url, headers=ACME)

    assert response.status_code == 200, response.text
    assert [item["id"] for item in response.json()] == [RECORD["id"]]


DASHBOARD_TENANT = HUMAN | {"x-valkyrie-tenant": str(TEST_ORG_ID)}


def _admin_claims(token: str) -> dict[str, object]:
    return _claims(token) | {"permissions": ["platform-admin"]}


@pytest.mark.parametrize("tenants", [[], [{"tenantId": "default"}, {"tenantId": "other-org"}]])
@pytest.mark.parametrize("method", ["GET", "POST", "approve"])
@pytest.mark.description("A platform admin with no selected organization gets 400 without forwarding a limit request.")
def test_platform_admin_needs_a_selected_org(
    database_session: Session,
    project: Project,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
    tenants: list[dict[str, str]],
    method: str,
) -> None:
    _make_platform_admin(descope)
    descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = tenants
    path = f"/projects/by-id/{project.id}/limit-requests"

    response = (
        _decide(project, method)
        if method == "approve"
        else client.request(method, path, json=RAISE if method == "POST" else None, headers=HUMAN)
    )

    assert response.status_code == 400, response.text
    assert limit_requests.sent == []


@pytest.mark.description("A member lists a project's raises when the Dashboard names the org by its id.")
def test_a_member_lists_raises_with_the_org_id_as_tenant(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    response = client.get(f"/projects/by-id/{project.id}/limit-requests", headers=DASHBOARD_TENANT)

    assert response.status_code == 200, response.text
    assert [item["id"] for item in response.json()] == [RECORD["id"]]


@pytest.mark.description("A member files a raise when the Dashboard names the org by its id.")
def test_a_member_files_a_raise_with_the_org_id_as_tenant(
    database_session: Session,
    project: Project,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delitem(app.dependency_overrides, get_current_org)
    limit_requests.status = 201

    response = client.post(f"/projects/by-id/{project.id}/limit-requests", json=RAISE, headers=DASHBOARD_TENANT)

    assert response.status_code == 201, response.text


@pytest.mark.description("A member of another org cannot list this org's raises by its id, and nothing is sent.")
def test_a_member_of_another_org_cannot_list_raises_by_org_id(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = [{"tenantId": "other-org"}]

    response = client.get(f"/projects/by-id/{project.id}/limit-requests", headers=DASHBOARD_TENANT)

    assert response.status_code == 403, response.text
    assert limit_requests.sent == []


@pytest.mark.description(
    "An old session that still claims platform-admin after its removal is refused, and nothing is sent."
)
def test_a_revoked_admins_old_session_is_refused(
    database_session: Session,
    project: Project,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(tracker_auth, "_validate_bearer", _admin_claims)
    _make_platform_admin(descope)
    assert client.get("/limit-requests", headers=HUMAN).status_code == 200
    limit_requests.sent.clear()
    descope.mgmt.user.load_by_user_id.return_value["user"]["roleNames"] = []
    descope.mgmt.user.load_by_user_id.return_value["user"]["permissions"] = ["platform-admin"]

    queue = client.get("/limit-requests", headers=HUMAN)
    decide = client.post(f"/projects/by-id/{project.id}/limit-requests/{uuid4()}/approve", headers=DASHBOARD_TENANT)

    assert queue.status_code == 403, queue.text
    assert decide.status_code == 403, decide.text
    assert limit_requests.sent == []


@pytest.mark.parametrize("tenant_role", [False, True])
@pytest.mark.description("Tenant roles and roles without the project permission cannot grant platform administration.")
def test_only_project_role_permissions_grant_platform_admin(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes, tenant_role: bool
) -> None:
    _make_platform_admin(descope)
    if tenant_role:
        descope.mgmt.role.load_all.return_value["roles"][0]["tenantId"] = "default"
        descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"][0]["roleNames"] = ["platform-operator"]
    else:
        descope.mgmt.role.load_all.return_value["roles"][0]["permissionNames"] = []

    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 403, response.text
    assert limit_requests.sent == []


@pytest.mark.parametrize("tenant_only", [False, True])
@pytest.mark.description("Only project roles assigned at the project level grant their permissions to a user.")
def test_unassigned_project_admin_role_does_not_grant_permissions(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes, tenant_only: bool
) -> None:
    user = descope.mgmt.user.load_by_user_id.return_value["user"]
    user["roleNames"] = ["project-reader"]
    descope.mgmt.role.load_all.return_value["roles"].append({"name": "project-reader", "permissionNames": []})
    if tenant_only:
        user["userTenants"][0]["roleNames"] = ["platform-operator"]

    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 403, response.text
    assert limit_requests.sent == []


@pytest.mark.description("An unavailable role service refuses authorization with 503 and sends nothing.")
def test_role_lookup_failure_refuses_authorization(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    _make_platform_admin(descope)
    descope.mgmt.role.load_all.side_effect = httpx.ConnectError("unavailable")

    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 503, response.text
    assert limit_requests.sent == []


@pytest.mark.description(
    "Project role definitions are reused for five seconds before permission revocation is observed."
)
def test_project_role_definitions_cache_expires(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes, monkeypatch: MonkeyPatch
) -> None:
    _make_platform_admin(descope)
    clock = MagicMock()
    clock.monotonic.return_value = 0.0
    monkeypatch.setattr(tracker_auth, "time", clock)
    lookup = descope.mgmt.role.load_all

    assert client.get("/limit-requests", headers=HUMAN).status_code == 200
    lookup.return_value = {"roles": [{"name": "platform-operator", "permissionNames": []}]}
    clock.monotonic.return_value = tracker_auth._USER_LOOKUP_CACHE_TTL_SECONDS - 0.001
    assert client.get("/limit-requests", headers=HUMAN).status_code == 200
    lookup.assert_called_once_with()
    limit_requests.sent.clear()
    clock.monotonic.return_value = tracker_auth._USER_LOOKUP_CACHE_TTL_SECONDS

    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 403, response.text
    assert lookup.call_count == 2
    assert limit_requests.sent == []


@pytest.mark.description(
    "Invalidating a user also reloads project roles so permission changes take effect immediately."
)
def test_user_invalidation_clears_project_roles(
    database_session: Session, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    _make_platform_admin(descope)
    assert client.get("/limit-requests", headers=HUMAN).status_code == 200
    descope.mgmt.role.load_all.return_value = {"roles": []}
    tracker_auth.invalidate_user_cache("U-alice")
    limit_requests.sent.clear()

    response = client.get("/limit-requests", headers=HUMAN)

    assert response.status_code == 403, response.text
    assert descope.mgmt.role.load_all.call_count == 2
    assert limit_requests.sent == []


@pytest.mark.description(
    "A platform admin who is not a member of the org cannot file a raise there, and nothing is sent."
)
def test_a_non_member_platform_admin_cannot_file_a_raise(
    database_session: Session,
    project: Project,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delitem(app.dependency_overrides, get_current_org)
    _make_platform_admin(descope)
    descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = [{"tenantId": "other-org"}]

    response = client.post(f"/projects/by-id/{project.id}/limit-requests", json=RAISE, headers=DASHBOARD_TENANT)

    assert response.status_code == 403, response.text
    assert limit_requests.sent == []


@pytest.mark.description("A sole membership named like another org's id is read as a name when no tenant is sent.")
def test_an_implicit_membership_named_like_an_id_is_read_as_a_name(
    database_session: Session,
    project_service: MockProjectRoutes,
    descope: MagicMock,
    limit_requests: LimitRequestRoutes,
) -> None:
    look_alike = Org(name=str(TEST_ORG_ID))
    database_session.add(look_alike)
    database_session.commit()
    own = Project(org_id=look_alike.id, handle="own-bench", name="Own Bench", spend_type=SpendType.DEVELOPMENT)
    project_service.seed(own)
    descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = [{"tenantId": str(TEST_ORG_ID)}]

    response = client.get(f"/projects/by-id/{own.id}/limit-requests", headers=HUMAN)

    assert response.status_code == 200, response.text


@pytest.mark.description("A platform admin decides a raise when the Dashboard names the org by its id.")
def test_a_platform_admin_decides_with_the_org_id_as_tenant(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    _make_platform_admin(descope)

    response = client.post(f"/projects/by-id/{project.id}/limit-requests/{uuid4()}/approve", headers=DASHBOARD_TENANT)

    assert response.status_code == 200, response.text


@pytest.mark.description("An org named with another org's id never captures that id, which names its own org.")
def test_an_org_named_like_another_orgs_id_does_not_capture_it(
    database_session: Session, project: Project, descope: MagicMock, limit_requests: LimitRequestRoutes
) -> None:
    database_session.add(Org(name=str(TEST_ORG_ID)))
    database_session.commit()
    member = client.get(f"/projects/by-id/{project.id}/limit-requests", headers=DASHBOARD_TENANT)
    descope.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = [{"tenantId": str(TEST_ORG_ID)}]

    impostor = client.get(f"/projects/by-id/{project.id}/limit-requests", headers=DASHBOARD_TENANT)

    assert member.status_code == 200, member.text
    assert impostor.status_code == 403, impostor.text
    assert len(limit_requests.sent) == 1
