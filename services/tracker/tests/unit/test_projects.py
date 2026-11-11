import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
import httpx
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, col, select

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
import main as main_module
from main import app
from tests.utils import TEST_ORG_ID
import tracker.auth as tracker_auth
from tracker.api.projects import reject_archived_project
from tracker.auth import RequestIdentity, get_current_org, get_current_runner, get_current_starter
from tracker.aws.tags import ORG_ID_TAG
from tracker.exceptions import ProjectNameTakenError
from tracker.repositories.project import ProjectRepository
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    Project,
    RunProject,
    SpendType,
    Task,
)
from tracker.types import AWSBenchmarkArguments, AgentContractRequest, StartBenchmarkRequest

pytestmark = pytest.mark.usefixtures("mapped_providers")

client = TestClient(app)

_CUSTOMER_PROJECT = {
    "handle": "google-report-xyz",
    "name": "Google Report XYZ",
    "spend_type": "customer",
    "subcategory": "report",
    "customer": "google",
    "extra_tags": {"engagement": "q4"},
}


class GatewayAPI:
    def __init__(self) -> None:
        self.request_id = uuid4()
        self.key_id = uuid4()
        self.status = "approved"
        self.requests: list[dict[str, Any]] = []
        self.collections: list[dict[str, Any]] = []
        self.paths: list[str] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        assert request.headers["authorization"] == "Bearer machine-test-key"
        if path == "/admin/managed-auth/machine/requests":
            assert request.method == "POST"
            self.requests.append(json.loads(request.content))
            return httpx.Response(200, json={"id": str(self.request_id), "status": "pending"})
        if path == f"/admin/managed-auth/machine/requests/{self.request_id}":
            assert request.method == "GET"
            return httpx.Response(
                200,
                json={"id": str(self.request_id), "status": self.status, "key_id": str(self.key_id)},
            )
        if path == f"/admin/managed-auth/machine/keys/{self.key_id}":
            assert request.method == "GET"
            return httpx.Response(200, json={"id": str(self.key_id), "revision": 3, "status": "pending_collection"})
        if path == f"/admin/managed-auth/machine/keys/{self.key_id}/collect":
            assert request.method == "POST"
            self.collections.append(json.loads(request.content))
            return httpx.Response(200, json={"secret": "project-gateway-key"})
        raise AssertionError(f"Unexpected Gateway request: {request.method} {path}")


@pytest.fixture(autouse=True)
def gateway_api(monkeypatch: MonkeyPatch) -> GatewayAPI:
    from tracker import project_gateway

    gateway = GatewayAPI()
    monkeypatch.setattr(project_gateway, "VALKYRIE_STAGE", "dev")
    monkeypatch.delenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", raising=False)
    monkeypatch.setenv("MODEL_GATEWAY_URL", "https://model-gateway.example")
    monkeypatch.setenv("MODEL_GATEWAY_MACHINE_API_KEY", "machine-test-key")
    monkeypatch.setattr(
        project_gateway,
        "_gateway_client",
        lambda: httpx.AsyncClient(
            base_url="https://model-gateway.example/",
            headers={"Authorization": "Bearer machine-test-key"},
            transport=httpx.MockTransport(gateway.handle),
        ),
    )
    return gateway


@pytest.fixture
def vals_org(monkeypatch: MonkeyPatch, database_session: Session) -> Org:
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.name = "vals.ai"
    database_session.add(org)
    database_session.commit()
    identity = RequestIdentity(org=org, email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: org)
    return org


@pytest.fixture
def active_executor_release(database_session: Session) -> None:
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="source:///test-release",
        artifact_digest="a" * 64,
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
    )
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    database_session.add(release)
    database_session.flush()
    admission.release_id = release.id
    database_session.add(admission)
    database_session.commit()


@pytest.fixture
def router_bench(database_session: Session) -> Project:
    project = Project(
        org_id=TEST_ORG_ID,
        handle="router-bench",
        name="Router Bench",
        spend_type=SpendType.DEVELOPMENT,
        subcategory="new_benchmark_generation",
    )
    database_session.add(project)
    database_session.commit()
    return project


@pytest.fixture
def google_report(database_session: Session) -> Project:
    project = Project.model_validate({**_CUSTOMER_PROJECT, "org_id": TEST_ORG_ID})
    database_session.add(project)
    database_session.commit()
    return project


async def _verify_single_task_id(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
    return VerifyTaskIdsResponse(task_ids=["task_0"])


def _start_request(contract: AgentContractRequest, project: str | None) -> dict[str, Any]:
    request = StartBenchmarkRequest(
        contract=contract, benchmark_name="swebench", sandbox_provider_secret_name="provider-secret", project=project
    )
    return request.model_dump(mode="json", exclude={"project"} if project is None else set())


def _saved_run(database_session: Session, contract: AgentContractRequest, org_id: UUID = TEST_ORG_ID) -> Benchmark:
    benchmark = Benchmark(
        org_id=org_id,
        name="swebench",
        status=BenchmarkStatus.FINISHED,
        finished_at=datetime.now(UTC),
        arguments=AWSBenchmarkArguments(contract=contract, concurrency=1),
    )
    database_session.add(benchmark)
    database_session.commit()
    return benchmark


def _other_org(database_session: Session) -> Org:
    org = Org(name="other-org")
    database_session.add(org)
    database_session.commit()
    return org


def _project_fields(project: Project) -> dict[str, Any]:
    return {
        "handle": project.handle,
        "name": project.name,
        "spend_type": project.spend_type.value,
        "subcategory": project.subcategory,
        "customer": project.customer,
        "extra_tags": project.extra_tags,
    }


def _link_rows(database_session: Session, run_id: UUID) -> list[RunProject]:
    database_session.expire_all()
    return list(
        database_session.exec(
            select(RunProject).where(RunProject.benchmark_id == run_id).order_by(col(RunProject.id))
        ).all()
    )


@pytest.mark.description("Creating a project saves it with its spend fields.")
@pytest.mark.parametrize(
    "project",
    [
        {
            "handle": "new-benchmark",
            "name": "New Benchmark",
            "spend_type": "development",
            "subcategory": "new_benchmark_generation",
        },
        {
            "handle": "launch-runs",
            "name": "Launch Runs",
            "spend_type": "development",
            "subcategory": "new_benchmark_launch_runs",
        },
        {
            "handle": "benchmark-update",
            "name": "Benchmark Update",
            "spend_type": "development",
            "subcategory": "post_launch_benchmark_update",
        },
        {
            "handle": "leaderboard",
            "name": "Leaderboard",
            "spend_type": "internal",
            "subcategory": "unpaid_leaderboard_run",
        },
        {
            "handle": "other-unpaid",
            "name": "Other Unpaid",
            "spend_type": "internal",
            "subcategory": "other_unpaid_run",
        },
        {
            "handle": "intern-program",
            "name": "Fellows/Interns",
            "spend_type": "internal",
            "subcategory": "fellows_interns",
        },
        {"handle": "blog", "name": "Blog", "spend_type": "internal", "subcategory": "blog"},
        {
            "handle": "internal-misc",
            "name": "Internal Miscellaneous",
            "spend_type": "internal",
            "subcategory": "miscellaneous",
        },
        _CUSTOMER_PROJECT,
        {
            "handle": "google-paid",
            "name": "Google Paid Run",
            "spend_type": "customer",
            "subcategory": "paid_run",
            "customer": "google",
        },
        {
            "handle": "google-eval",
            "name": "Google Eval Server",
            "spend_type": "customer",
            "subcategory": "eval_server",
            "customer": "google",
        },
        {
            "handle": "google-platform",
            "name": "Google Platform",
            "spend_type": "customer",
            "subcategory": "platform",
            "customer": "google",
        },
        {
            "handle": "a" * 64,
            "name": "Long",
            "spend_type": "development",
            "subcategory": "new_benchmark_generation",
        },
        {
            "handle": "blog-extra-tag",
            "name": "Blog",
            "spend_type": "internal",
            "subcategory": "blog",
            "extra_tags": {"extra_tags": "x"},
        },
    ],
    ids=[
        "new-benchmark-generation",
        "new-benchmark-launch-runs",
        "post-launch-benchmark-update",
        "unpaid-leaderboard-run",
        "other-unpaid-run",
        "intern-program",
        "blog",
        "internal-miscellaneous",
        "report",
        "paid-run",
        "eval-server",
        "platform",
        "handle-64-chars",
        "extra-tag-named-extra-tags",
    ],
)
def test_create_saves_the_project_with_its_spend_fields(
    monkeypatch: MonkeyPatch, database_session: Session, project: dict[str, Any], gateway_api: GatewayAPI
) -> None:
    creator = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: creator)

    response = client.post(
        "/projects", json={**project, "handle": f"  {project['handle']} ", "name": f" {project['name']}  "}
    )

    assert response.status_code == 200, response.text
    (row,) = database_session.exec(select(Project)).all()
    expected: dict[str, Any] = {"customer": None, "extra_tags": {}, **project}
    assert _project_fields(row) == expected
    assert (row.org_id, row.created_by_email) == (TEST_ORG_ID, "alice@vals.ai")
    assert {key: response.json()[key] for key in expected} == expected
    assert row.gateway_request_id is None
    assert row.gateway_key_id is None
    assert gateway_api.requests == []
    assert set(response.json()).isdisjoint({"gateway_request_id", "gateway_key_id"})


_BLOG = {"handle": "blog", "name": "Blog", "spend_type": "internal", "subcategory": "blog"}
_BUDGET = {"budget": "300", "budget_reason": "3 runs, about 100 each"}
_HANDLE_RULE = "must be lowercase letters, digits and hyphens, at most 64 characters, e.g. my-project"


@pytest.mark.description(
    "Creating a Vals project files one gateway key request and keeps gateway fields out of the response."
)
def test_vals_project_creation_requests_gateway_key_with_generated_description(
    database_session: Session, gateway_api: GatewayAPI, vals_org: Org
) -> None:
    response = client.post("/projects", json={**_BLOG, **_BUDGET})
    assert response.status_code == 200, response.text
    (project,) = database_session.exec(select(Project)).all()
    project_id = str(project.id)
    assert response.json()["id"] == project_id
    listed = client.get("/projects")
    assert listed.status_code == 200, listed.text
    assert [item["id"] for item in listed.json()["projects"]] == [project_id]
    read = client.get(f"/projects/by-id/{project.id}")
    assert read.status_code == 200, read.text
    assert read.json()["id"] == project_id
    assert (project.gateway_request_id, project.gateway_key_id) == (gateway_api.request_id, None)
    assert gateway_api.requests == [
        {
            "project_id": str(project.id),
            "project_organization_id": str(project.org_id),
            "project_environment": "dev",
            "display_name": "Blog",
            "description": "Valkyrie project blog",
            "requested_budget": "300",
            "budget_reason": "3 runs, about 100 each",
        }
    ]
    assert gateway_api.paths == ["/admin/managed-auth/machine/requests"]
    assert set(response.json()).isdisjoint({"gateway_request_id", "gateway_key_id", "budget", "budget_reason"})


@pytest.mark.description(
    "A Vals project with no budget or reason is refused, and no project or gateway request is created."
)
@pytest.mark.parametrize(
    "budget",
    [{}, {"budget": "300"}, {"budget_reason": "3 runs, about 100 each"}, {"budget": "300", "budget_reason": None}],
    ids=["neither", "budget-only", "reason-only", "null-reason"],
)
def test_vals_project_creation_without_budget_and_reason_names_both_flags_and_files_nothing(
    database_session: Session, gateway_api: GatewayAPI, vals_org: Org, budget: dict[str, Any]
) -> None:
    response = client.post("/projects", json={**_BLOG, **budget})

    assert response.status_code == 422, response.text
    assert response.json()["detail"] == (
        "Project 'blog' needs --budget and --budget-reason: "
        "creating it requests its main Gateway key, which carries that budget"
    )
    assert database_session.exec(select(Project)).all() == []
    assert gateway_api.requests == []


@pytest.mark.description("A project without gateway requests is created and its budget is dropped.")
def test_project_creation_without_gateway_requests_accepts_and_drops_budget(
    monkeypatch: MonkeyPatch, database_session: Session, gateway_api: GatewayAPI
) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")

    response = client.post("/projects", json={**_BLOG, **_BUDGET})

    assert response.status_code == 200, response.text
    (project,) = database_session.exec(select(Project)).all()
    assert (project.handle, project.gateway_request_id) == ("blog", None)
    assert gateway_api.requests == []
    assert set(response.json()).isdisjoint({"budget", "budget_reason"})


@pytest.mark.description("An invalid project is rejected without being saved.")
@pytest.mark.parametrize(
    ("project", "message"),
    [
        (
            {**_BLOG, "subcategory": "report"},
            "subcategory 'report' is not allowed for spend type 'internal'; allowed: unpaid_leaderboard_run, other_unpaid_run, fellows_interns, blog, miscellaneous",
        ),
        (
            {**_BLOG, "subcategory": "blogs", "spend_type": "development"},
            "subcategory 'blogs' is not allowed for spend type 'development'; allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
        ),
        (
            {**_BLOG, "subcategory": "miscellaneous", "spend_type": "development"},
            "subcategory 'miscellaneous' is not allowed for spend type 'development'; allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
        ),
        (
            {**_CUSTOMER_PROJECT, "subcategory": "miscellaneous"},
            "subcategory 'miscellaneous' is not allowed for spend type 'customer'; allowed: report, paid_run, eval_server, platform",
        ),
        ({key: value for key, value in _BLOG.items() if key != "subcategory"}, "Field required"),
        ({**_BLOG, "subcategory": None}, "Input should be a valid string"),
        ({**_CUSTOMER_PROJECT, "customer": None}, "spend type 'customer' requires customer"),
        ({**_CUSTOMER_PROJECT, "customer": "   "}, "spend type 'customer' requires customer"),
        ({**_BLOG, "spend_type": "research"}, "Input should be 'development', 'internal'"),
        ({key: value for key, value in _BLOG.items() if key != "handle"}, "Field required"),
        ({**_BLOG, "handle": ""}, _HANDLE_RULE),
        ({**_BLOG, "handle": "   "}, _HANDLE_RULE),
        ({**_BLOG, "handle": "My Project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "my_project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "my--project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "-my-project"}, _HANDLE_RULE),
        ({**_BLOG, "handle": "a" * 65}, _HANDLE_RULE),
        ({key: value for key, value in _BLOG.items() if key != "name"}, "Field required"),
        ({**_BLOG, "name": ""}, "at least 1 character"),
        ({**_BLOG, "name": "   "}, "at least 1 character"),
        ({**_BLOG, "extra_tags": {"name": "Blog"}}, "extra tag keys ['name'] name core fields"),
        ({**_BLOG, "extra_tags": {"handle": "blog"}}, "extra tag keys ['handle'] name core fields"),
        ({**_BLOG, "extra_tags": {"spend_type": "customer"}}, "extra tag keys ['spend_type'] name core fields"),
        ({**_BLOG, "extra_tags": {"week": 4}}, "Input should be a valid string"),
    ],
    ids=[
        "subcategory-of-another-spend-type",
        "retired-development-subcategory",
        "development-miscellaneous",
        "customer-miscellaneous",
        "missing-subcategory",
        "null-subcategory",
        "customer-without-customer",
        "blank-customer",
        "unknown-spend-type",
        "missing-handle",
        "empty-handle",
        "blank-handle",
        "handle-with-space-and-capitals",
        "handle-with-underscore",
        "handle-with-double-hyphen",
        "handle-with-leading-hyphen",
        "handle-65-chars",
        "missing-name",
        "empty-name",
        "blank-name",
        "extra-tag-shadows-name",
        "extra-tag-shadows-handle",
        "extra-tag-shadows-spend-type",
        "extra-tag-value-not-a-string",
    ],
)
def test_create_rejects_invalid_projects_without_saving(
    database_session: Session, project: dict[str, Any], message: str
) -> None:
    response = client.post("/projects", json=project)

    assert response.status_code == 422
    assert message in response.text
    assert database_session.exec(select(Project)).all() == []


@pytest.mark.description("A project cannot reuse a handle the org already has.")
def test_create_refuses_a_handle_the_org_already_uses(database_session: Session, router_bench: Project) -> None:
    response = client.post("/projects", json={**_BLOG, "handle": "router-bench", "name": "Another name"})

    assert response.status_code == 409
    assert response.json()["detail"] == "Project 'router-bench' already exists"
    assert len(database_session.exec(select(Project)).all()) == 1


@pytest.mark.description("A project cannot reuse a name the org already has, whatever its case or outer spaces.")
@pytest.mark.parametrize("name", ["Router Bench", "router bench ", "  ROUTER BENCH"])
def test_create_refuses_a_name_the_org_already_uses_and_names_its_handle(
    database_session: Session, router_bench: Project, name: str
) -> None:
    response = client.post("/projects", json={**_BLOG, "handle": "router-bench-2", "name": name})

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "A project named 'Router Bench' already exists (handle router-bench). Choose another name."
    )
    assert [row.handle for row in database_session.exec(select(Project)).all()] == ["router-bench"]


@pytest.mark.description("A non-ASCII name is matched the way the database's index folds it.")
def test_create_refuses_a_taken_non_ascii_name_and_names_its_handle(database_session: Session) -> None:
    assert client.post("/projects", json={**_BLOG, "handle": "equipe", "name": "Équipe"}).status_code == 200

    response = client.post("/projects", json={**_BLOG, "handle": "equipe-2", "name": "Équipe"})

    assert response.status_code == 409
    assert response.json()["detail"] == "A project named 'Équipe' already exists (handle equipe). Choose another name."


@pytest.mark.description("An archived project still holds its name.")
def test_create_refuses_the_name_of_an_archived_project(database_session: Session, router_bench: Project) -> None:
    assert client.post("/projects/router-bench/archive").status_code == 200

    response = client.post("/projects", json={**_BLOG, "handle": "router-bench-2", "name": "Router Bench"})

    assert response.status_code == 409
    assert "(handle router-bench)" in response.json()["detail"]


@pytest.mark.description("Another org's project names do not block this org.")
def test_create_accepts_a_name_another_org_uses(database_session: Session) -> None:
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Router Bench",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.post("/projects", json={**_BLOG, "handle": "router-bench", "name": "Router Bench"})

    assert response.status_code == 200, response.text


@pytest.mark.description("A same-named insert that slips past the lookup is refused by the index and named.")
def test_create_racing_a_same_named_insert_reports_the_name_from_the_index(
    monkeypatch: MonkeyPatch, database_session: Session, router_bench: Project
) -> None:
    repository = ProjectRepository(database_session)
    lookups = iter([None, router_bench])
    monkeypatch.setattr(repository, "get_by_name", lambda _org_id, _name: next(lookups))
    racer = Project(
        org_id=TEST_ORG_ID,
        handle="router-bench-2",
        name="ROUTER BENCH",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )

    with pytest.raises(ProjectNameTakenError, match=r"\(handle router-bench\)"):
        repository.create(racer)


@pytest.mark.description("A taken name is refused before any Gateway key request is filed.")
def test_vals_project_with_a_taken_name_files_no_gateway_request(
    vals_org: Org, router_bench: Project, gateway_api: GatewayAPI
) -> None:
    response = client.post("/projects", json={**_BLOG, **_BUDGET, "name": "Router Bench"})

    assert response.status_code == 409
    assert gateway_api.requests == []


@pytest.mark.description("Listing projects shows only the caller's org projects, by handle.")
def test_list_shows_only_the_callers_org_projects_by_handle(
    database_session: Session, router_bench: Project, google_report: Project
) -> None:
    database_session.add(
        Project(
            org_id=router_bench.org_id,
            handle="a-first-by-handle",
            name="Zulu last by name",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.get("/projects")

    assert response.status_code == 200, response.text
    projects = response.json()["projects"]
    assert [(project["handle"], project["name"]) for project in projects] == [
        ("a-first-by-handle", "Zulu last by name"),
        ("google-report-xyz", "Google Report XYZ"),
        ("router-bench", "Router Bench"),
    ]
    assert {key: projects[1][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT


@pytest.mark.description("A project ID lookup is scoped to the caller's org.")
def test_project_id_lookup_is_scoped_to_the_callers_org(database_session: Session, router_bench: Project) -> None:
    other = Project(
        org_id=_other_org(database_session).id,
        handle="theirs",
        name="Theirs",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(other)
    database_session.commit()

    response = client.get(f"/projects/by-id/{router_bench.id}")
    assert response.status_code == 200, response.text
    assert response.json() == {"id": str(router_bench.id), "org_id": str(TEST_ORG_ID)}
    for project_id in (other.id, uuid4()):
        assert client.get(f"/projects/by-id/{project_id}").status_code == 404


@pytest.fixture
def gateway_key_login(monkeypatch: MonkeyPatch) -> MagicMock:
    monkeypatch.delitem(app.dependency_overrides, get_current_runner)
    monkeypatch.setattr(tracker_auth, "AUTH_REQUIRED", True)
    monkeypatch.setattr(tracker_auth, "DESCOPE_MANAGEMENT_KEY", "management-key")
    monkeypatch.setattr(
        tracker_auth,
        "_validate_bearer",
        lambda _token: {"sub": "U-alice", "tenants": {"default": {}}},
    )
    descope = MagicMock()
    descope.mgmt.user.load_by_user_id.return_value = {
        "user": {"userId": "U-alice", "userTenants": [{"tenantId": "default", "roleNames": []}]}
    }
    monkeypatch.setattr(tracker_auth, "_descope_client", descope)
    return descope


@pytest.mark.description("Gateway key eligibility needs a live human who is allowed to run.")
def test_gateway_key_eligibility_requires_a_live_human_allowed_to_run(
    database_session: Session, router_bench: Project, gateway_key_login: MagicMock
) -> None:
    path = f"/projects/by-id/{router_bench.id}/gateway-key-eligibility"
    response = client.get(path, headers={"authorization": "Bearer human-session"})
    assert response.status_code == 200, response.text
    assert response.json() == {
        "project_id": str(router_bench.id),
        "org_id": str(TEST_ORG_ID),
        "subject": "U-alice",
    }
    gateway_key_login.mgmt.user.load_by_user_id.assert_any_call("U-alice")
    assert client.get(path, headers={"x-api-key": "non-human-key"}).status_code == 403

    gateway_key_login.mgmt.user.load_by_user_id.return_value["user"]["roleNames"] = ["valkyrie-no-start"]
    assert client.get(path, headers={"authorization": "Bearer human-session"}).status_code == 403

    gateway_key_login.mgmt.user.load_by_user_id.return_value["user"]["roleNames"] = []
    gateway_key_login.mgmt.user.load_by_user_id.return_value["user"]["userTenants"] = [{"tenantId": "other"}]
    assert client.get(path, headers={"authorization": "Bearer human-session"}).status_code == 403


@pytest.mark.description("Gateway key eligibility for a project in another org returns 404.")
def test_gateway_key_eligibility_hides_projects_outside_the_human_org(
    database_session: Session, gateway_key_login: MagicMock
) -> None:
    other = Project(
        org_id=_other_org(database_session).id,
        handle="theirs",
        name="Theirs",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(other)
    database_session.commit()

    for project_id in (other.id, uuid4()):
        response = client.get(
            f"/projects/by-id/{project_id}/gateway-key-eligibility",
            headers={"authorization": "Bearer human-session"},
        )
        assert response.status_code == 404


@pytest.mark.description("Updating a project changes its spend fields and keeps its identity.")
def test_update_changes_spend_fields_without_replacing_project_identity(
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
) -> None:
    stable = (router_bench.id, router_bench.org_id, router_bench.created_at, router_bench.created_by_email)
    benchmark = _saved_run(database_session, contract)
    database_session.add(RunProject(benchmark_id=benchmark.id, project_id=router_bench.id))
    database_session.commit()
    history_before = [(row.id, row.benchmark_id, row.project_id) for row in _link_rows(database_session, benchmark.id)]

    response = client.put(
        "/projects/router-bench",
        json={**_CUSTOMER_PROJECT, "handle": "router-bench"},
    )

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in _CUSTOMER_PROJECT} == {
        **_CUSTOMER_PROJECT,
        "handle": "router-bench",
    }
    database_session.expire_all()
    saved = database_session.get(Project, router_bench.id)
    assert saved is not None
    assert _project_fields(saved) == {**_CUSTOMER_PROJECT, "handle": "router-bench"}
    assert (saved.id, saved.org_id, saved.created_by_email) == (stable[0], stable[1], stable[3])
    assert saved.created_at == stable[2].replace(tzinfo=None)
    assert [
        (row.id, row.benchmark_id, row.project_id) for row in _link_rows(database_session, benchmark.id)
    ] == history_before
    metadata = client.get(f"/fetch-benchmark-metadata/{benchmark.id}").json()
    assert {key: metadata["project"][key] for key in _CUSTOMER_PROJECT} == {
        **_CUSTOMER_PROJECT,
        "handle": "router-bench",
    }


@pytest.mark.description("A rename to another project's name is refused and the old name stays.")
def test_update_refuses_another_projects_name_and_keeps_the_old_one(
    database_session: Session, router_bench: Project, google_report: Project
) -> None:
    response = client.put(
        "/projects/router-bench", json={**_BLOG, "handle": "router-bench", "name": "google report xyz "}
    )

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "A project named 'Google Report XYZ' already exists (handle google-report-xyz). Choose another name."
    )
    database_session.expire_all()
    saved = database_session.get(Project, router_bench.id)
    assert saved is not None and saved.name == "Router Bench"


@pytest.mark.description("A rename that slips past the lookup is refused by the index and named.")
def test_update_racing_a_same_named_write_reports_the_name_from_the_index(
    monkeypatch: MonkeyPatch, database_session: Session, router_bench: Project, google_report: Project
) -> None:
    repository = ProjectRepository(database_session)
    lookups = iter([None, google_report])
    monkeypatch.setattr(repository, "get_by_name", lambda _org_id, _name: next(lookups))
    router_bench.name = "GOOGLE REPORT XYZ"

    with pytest.raises(ProjectNameTakenError, match=r"'Google Report XYZ' already exists \(handle google-report-xyz\)"):
        repository.update(router_bench)


@pytest.mark.description("A project can keep its own name or change only its case.")
@pytest.mark.parametrize("name", ["Router Bench", "router bench"])
def test_update_keeps_or_recases_the_projects_own_name(
    database_session: Session, router_bench: Project, name: str
) -> None:
    response = client.put("/projects/router-bench", json={**_BLOG, "handle": "router-bench", "name": name})

    assert response.status_code == 200, response.text
    assert response.json()["name"] == name


@pytest.mark.description("An update whose body handle differs from the path is rejected.")
def test_update_rejects_a_body_handle_that_differs_from_the_path(
    database_session: Session, router_bench: Project
) -> None:
    project_id = router_bench.id
    before = _project_fields(router_bench)

    response = client.put("/projects/router-bench", json={**_BLOG, "handle": "blog"})

    assert response.status_code == 422
    assert response.json()["detail"] == "Project handle in the body must match the path"
    database_session.expire_all()
    saved = database_session.get(Project, project_id)
    assert saved is not None
    assert _project_fields(saved) == before


@pytest.mark.description("An update applies the same spend validation as creation.")
def test_update_reuses_project_spend_validation(database_session: Session, router_bench: Project) -> None:
    project_id = router_bench.id
    before = _project_fields(router_bench)

    response = client.put(
        "/projects/router-bench",
        json={**_BLOG, "handle": "router-bench", "spend_type": "customer", "customer": None},
    )

    assert response.status_code == 422
    assert "subcategory 'blog' is not allowed for spend type 'customer'" in response.text
    database_session.expire_all()
    saved = database_session.get(Project, project_id)
    assert saved is not None
    assert _project_fields(saved) == before


@pytest.mark.description("Updating another org's project returns 404.")
def test_update_hides_another_orgs_project(database_session: Session) -> None:
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.put("/projects/theirs", json={**_BLOG, "handle": "theirs"})

    assert response.status_code == 404


@pytest.mark.description("Deleting an unused project removes it and returns it.")
def test_delete_returns_and_removes_an_unused_project(database_session: Session, router_bench: Project) -> None:
    project_id = router_bench.id
    expected = _project_fields(router_bench)

    response = client.delete("/projects/router-bench")

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in expected} == expected
    database_session.expire_all()
    assert database_session.get(Project, project_id) is None


@pytest.mark.description("Deleting an unknown or other org's project returns 404.")
def test_delete_hides_unknown_and_other_org_projects(database_session: Session) -> None:
    other = Project(
        org_id=_other_org(database_session).id,
        handle="theirs",
        name="Theirs",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(other)
    database_session.commit()

    for handle in ("missing", "theirs"):
        response = client.delete(f"/projects/{handle}")
        assert response.status_code == 404
    assert database_session.get(Project, other.id) is not None


@pytest.mark.description("A project that was ever linked to a run cannot be deleted.")
def test_delete_rejects_a_project_with_any_historical_run_link(
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
    google_report: Project,
) -> None:
    benchmark = _saved_run(database_session, contract)
    database_session.add_all(
        [
            RunProject(benchmark_id=benchmark.id, project_id=router_bench.id),
            RunProject(benchmark_id=benchmark.id, project_id=google_report.id),
        ]
    )
    database_session.commit()

    response = client.delete("/projects/router-bench")

    assert response.status_code == 409
    assert response.json()["detail"] == "Project 'router-bench' has run history and cannot be deleted"
    assert database_session.get(Project, router_bench.id) is not None


@pytest.mark.usefixtures("active_executor_release")
def test_provision_keeps_supplied_keys_then_automatic_selects_one_project_gateway_secret(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    gateway_api: GatewayAPI,
    vals_org: Org,
) -> None:
    from tracker.aws.secrets import SecretsManagerStore

    writes: list[tuple[str, str, dict[str, str]]] = []

    async def store_secret(_self: SecretsManagerStore, name: str, value: str, *, tags: dict[str, str]) -> None:
        writes.append((name, value, tags))

    monkeypatch.setattr(SecretsManagerStore, "put", store_secret)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "provision")
    gateway_api.status = "pending"
    created = client.post("/projects", json={**_BLOG, **_BUDGET})
    assert created.status_code == 200, created.text
    (project,) = database_session.exec(select(Project)).all()
    assert (project.gateway_request_id, project.gateway_key_id) == (gateway_api.request_id, None)
    supplied_secrets = {
        "MODEL_GATEWAY_URL": "caller-url",
        "MODEL_GATEWAY_API_KEY": "caller-key",
        "UNRELATED_KEY": "caller-other",
    }
    request = _start_request(contract.model_copy(update={"secrets": supplied_secrets}), "blog")
    response = client.post("/start-benchmark", json=request)
    assert response.status_code == 200, response.text
    provision_run_id = UUID(response.json()["benchmark_id"])
    assert gateway_api.collections == []
    assert writes == []
    assert gateway_api.paths == ["/admin/managed-auth/machine/requests"]
    assert len(mock_launcher.queued_calls) == 1
    assert (
        mock_launcher.queued_calls[0]["execution_context_json"]["start_benchmark_request"]["contract"]["secrets"]
        == supplied_secrets
    )

    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")
    gateway_api.status = "approved"
    for _ in range(2):
        response = client.post("/start-benchmark", json=request)
        assert response.status_code == 200, response.text

    reference = f"valkyrie/tenants/{TEST_ORG_ID}/model-gateway-{gateway_api.key_id}"
    expected_secrets = {
        "MODEL_GATEWAY_URL": reference,
        "MODEL_GATEWAY_API_KEY": reference,
        "UNRELATED_KEY": "caller-other",
    }
    assert len(writes) == 1
    assert writes[0][0] == reference
    assert json.loads(writes[0][1]) == {
        "MODEL_GATEWAY_URL": "https://model-gateway.example",
        "MODEL_GATEWAY_API_KEY": "project-gateway-key",
    }
    assert writes[0][2] == {ORG_ID_TAG: str(TEST_ORG_ID)}
    assert gateway_api.collections == [{"expected_revision": 3}]
    assert gateway_api.paths.count(f"/admin/managed-auth/machine/keys/{gateway_api.key_id}/collect") == 1
    database_session.refresh(project)
    assert (project.gateway_request_id, project.gateway_key_id) == (gateway_api.request_id, gateway_api.key_id)
    database_session.expire_all()
    saved_runs = database_session.exec(select(Benchmark)).all()
    assert len(saved_runs) == 3
    provision_run = database_session.get(Benchmark, provision_run_id)
    assert provision_run is not None
    assert provision_run.arguments.contract.secrets == supplied_secrets
    assert provision_run.arguments.selected_project_gateway_secret_ref is None
    assert all(run.arguments.contract.secrets == expected_secrets for run in saved_runs if run.id != provision_run_id)
    assert all(
        run.arguments.selected_project_gateway_secret_ref == reference
        for run in saved_runs
        if run.id != provision_run_id
    )
    assert all("selected_project_gateway_secret_ref" not in run.model_dump_json() for run in saved_runs)
    assert len(mock_launcher.queued_calls) == 3
    for payload in mock_launcher.queued_calls[1:]:
        secrets = payload["execution_context_json"]["start_benchmark_request"]["contract"]["secrets"]
        assert secrets == expected_secrets
        assert "project-gateway-key" not in str(payload)


@pytest.mark.usefixtures("active_executor_release")
def test_other_org_project_creation_and_run_keep_supplied_gateway_references(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    gateway_api: GatewayAPI,
) -> None:
    from tracker.aws.secrets import SecretsManagerStore

    async def no_gateway_storage(_self: SecretsManagerStore, _name: str, _value: str, *, tags: dict[str, str]) -> None:
        raise AssertionError("another organization's project must not store a Gateway key")

    monkeypatch.delenv("MODEL_GATEWAY_URL")
    monkeypatch.delenv("MODEL_GATEWAY_MACHINE_API_KEY")
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")
    monkeypatch.setattr(SecretsManagerStore, "put", no_gateway_storage)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    created = client.post("/projects", json=_BLOG)
    assert created.status_code == 200, created.text
    (project,) = database_session.exec(select(Project)).all()
    assert (project.gateway_request_id, project.gateway_key_id) == (None, None)
    supplied = {"MODEL_GATEWAY_URL": "caller-url", "MODEL_GATEWAY_API_KEY": "caller-key"}

    response = client.post(
        "/start-benchmark",
        json=_start_request(contract.model_copy(update={"secrets": supplied}), "blog"),
    )

    assert response.status_code == 200, response.text
    assert gateway_api.paths == []
    assert len(mock_launcher.queued_calls) == 1
    assert (
        mock_launcher.queued_calls[0]["execution_context_json"]["start_benchmark_request"]["contract"]["secrets"]
        == supplied
    )
    database_session.refresh(project)
    assert (project.gateway_request_id, project.gateway_key_id) == (None, None)


@pytest.mark.usefixtures("active_executor_release")
def test_pending_gateway_approval_does_not_start_a_selected_project(
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    gateway_api: GatewayAPI,
    vals_org: Org,
) -> None:
    gateway_api.status = "pending"
    created = client.post("/projects", json={**_BLOG, **_BUDGET})
    assert created.status_code == 200, created.text
    response = client.post("/start-benchmark", json=_start_request(contract, "blog"))
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "Gateway key request for project 'blog' is pending"
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(Task)).all() == []
    assert mock_launcher.queued_calls == []
    assert gateway_api.collections == []


@pytest.mark.description("An existing project without a gateway binding needs one set explicitly.")
def test_unbound_existing_project_requires_explicit_gateway_binding(
    database_session: Session, contract: AgentContractRequest, vals_org: Org
) -> None:
    project = Project(
        org_id=TEST_ORG_ID,
        handle="old-project",
        name="Old Project",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(project)
    database_session.commit()

    response = client.post("/start-benchmark", json=_start_request(contract, "old-project"))

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == (
        "Project 'old-project' has no Gateway key request; configure its Gateway binding before starting a run"
    )
    assert database_session.exec(select(Benchmark)).all() == []


@pytest.mark.usefixtures("active_executor_release")
@pytest.mark.parametrize("final_status", [BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED])
async def test_start_links_the_project_and_the_link_is_unchanged_when_the_run_status_becomes_error_or_stopped(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    google_report: Project,
    final_status: BenchmarkStatus,
) -> None:
    starter = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: starter)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

    response = client.post("/start-benchmark", json=_start_request(contract, " google-report-xyz "))

    assert response.status_code == 200, response.text
    assert {key: response.json()["project"][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT
    run_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, run_id)
    assert benchmark is not None
    benchmark.status = final_status
    database_session.add(benchmark)
    database_session.commit()

    (row,) = _link_rows(database_session, run_id)
    assert row.project_id == google_report.id
    assert row.created_by_email == benchmark.started_by_email == "alice@vals.ai"
    metadata = client.get(f"/fetch-benchmark-metadata/{run_id}").json()
    assert {key: metadata["project"][key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT


@pytest.mark.usefixtures("active_executor_release")
async def test_start_whose_project_link_cannot_be_inserted_saves_no_run_and_no_tasks(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    original_add = Session.add

    def refuse_run_project(self: Session, instance: object, *args: Any, **kwargs: Any) -> None:
        if isinstance(instance, RunProject):
            raise RuntimeError("project link cannot be inserted")
        original_add(self, instance, *args, **kwargs)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    monkeypatch.setattr(Session, "add", refuse_run_project)

    response = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert response.status_code == 500
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(Task)).all() == []
    assert database_session.exec(select(RunProject)).all() == []


@pytest.mark.description(
    "Starting a run in an unknown project lists the existing handles and the create flags, and saves no run."
)
def test_start_in_an_unknown_project_is_refused_with_the_handles_that_exist(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, google_report: Project
) -> None:
    response = client.post("/start-benchmark", json=_start_request(contract, "router-bnech"))

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Project 'router-bnech' does not exist. Existing projects: google-report-xyz, router-bench. "
        "Create it with: valkyrie project create router-bnech --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY --budget AMOUNT --budget-reason REASON"
    )
    assert database_session.exec(select(Benchmark)).all() == []


@pytest.mark.description("A start cannot use another org's project.")
def test_start_cannot_use_another_orgs_project(database_session: Session, contract: AgentContractRequest) -> None:
    database_session.add(
        Project(
            org_id=_other_org(database_session).id,
            handle="theirs",
            name="Theirs",
            spend_type=SpendType.INTERNAL,
            subcategory="blog",
        )
    )
    database_session.commit()

    response = client.post("/start-benchmark", json=_start_request(contract, "theirs"))

    assert response.status_code == 422
    assert "Existing projects: none." in response.text
    assert database_session.exec(select(Benchmark)).all() == []


@pytest.mark.usefixtures("active_executor_release")
async def test_runs_started_without_a_project_share_the_organizations_default_project(
    monkeypatch: MonkeyPatch, database_session: Session, contract: AgentContractRequest, mock_launcher: Any
) -> None:
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

    first = client.post("/start-benchmark", json=_start_request(contract, None))
    second = client.post("/start-benchmark", json=_start_request(contract, None))

    assert first.status_code == 200, first.text
    assert [response.json()["project"]["handle"] for response in (first, second)] == ["default", "default"]

    defaults = database_session.exec(select(Project).where(col(Project.handle) == "default")).all()
    assert [(project.spend_type, project.subcategory) for project in defaults] == [
        (SpendType.DEVELOPMENT, "new_benchmark_generation")
    ]

    run_id = UUID(first.json()["benchmark_id"])
    assert [link.project_id for link in _link_rows(database_session, run_id)] == [defaults[0].id]
    assert client.get(f"/fetch-benchmark-metadata/{run_id}").json()["project"]["handle"] == "default"


_PROJECT_REQUIRED_DETAIL = (
    "This organization requires a project for every run. Pass --project HANDLE (or project= in the SDK); "
    "create one with: valkyrie project create HANDLE --name NAME "
    "--spend-type development|internal|customer --subcategory SUBCATEGORY"
)


@pytest.fixture
def project_required_org(monkeypatch: MonkeyPatch, database_session: Session) -> Org:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.requires_project = True
    database_session.add(org)
    database_session.commit()
    identity = RequestIdentity(org=org, email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: identity)
    return org


@pytest.mark.usefixtures("project_required_org")
def test_start_without_a_project_in_an_org_that_requires_one_is_refused_and_saves_no_run(
    database_session: Session, contract: AgentContractRequest
) -> None:
    response = client.post("/start-benchmark", json=_start_request(contract, None))

    assert response.status_code == 422
    assert response.json()["detail"] == _PROJECT_REQUIRED_DETAIL
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(Task)).all() == []


@pytest.mark.usefixtures("active_executor_release", "project_required_org")
async def test_start_with_a_project_in_an_org_that_requires_one_links_the_project(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

    response = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert response.status_code == 200, response.text
    run_id = UUID(response.json()["benchmark_id"])
    assert [link.project_id for link in _link_rows(database_session, run_id)] == [router_bench.id]


@pytest.mark.description("Retagging a finished run adds a link, and metadata returns the newest one.")
def test_retag_of_a_finished_run_adds_a_link_and_metadata_returns_the_newest(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
    google_report: Project,
) -> None:
    benchmark = _saved_run(database_session, contract)
    database_session.add(
        RunProject(
            benchmark_id=benchmark.id,
            project_id=router_bench.id,
            created_at=datetime.now(UTC) - timedelta(hours=1),
            created_by_email="alice@vals.ai",
        )
    )
    database_session.commit()
    retagger = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="bob@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: retagger)

    response = client.post(f"/benchmarks/{benchmark.id}/project", json={"project": "google-report-xyz"})

    assert response.status_code == 200, response.text
    assert {key: response.json()[key] for key in _CUSTOMER_PROJECT} == _CUSTOMER_PROJECT
    old, new = _link_rows(database_session, benchmark.id)
    assert (old.project_id, old.created_by_email) == (router_bench.id, "alice@vals.ai")
    assert (new.project_id, new.created_by_email) == (google_report.id, "bob@vals.ai")
    metadata = client.get(f"/fetch-benchmark-metadata/{benchmark.id}").json()
    assert (metadata["project"]["handle"], metadata["project"]["name"]) == ("google-report-xyz", "Google Report XYZ")


@pytest.mark.description("When two project links share a timestamp, the one added last is the run's current project.")
def test_the_last_link_appended_is_the_current_project_when_timestamps_tie(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, google_report: Project
) -> None:
    moved_at = datetime.now(UTC)
    runs = {
        "google-report-xyz": (_saved_run(database_session, contract), [router_bench, google_report]),
        "router-bench": (_saved_run(database_session, contract), [google_report, router_bench]),
    }
    for run, projects in runs.values():
        database_session.add_all(
            RunProject(benchmark_id=run.id, project_id=project.id, created_at=moved_at) for project in projects
        )
    database_session.commit()

    current = {handle: ProjectRepository(database_session).get_for_run(run.id) for handle, (run, _) in runs.items()}

    assert {handle: project.handle if project else None for handle, project in current.items()} == {
        handle: handle for handle in runs
    }


@pytest.mark.description("Retagging into an unknown project is refused and adds no link.")
def test_retag_into_an_unknown_project_is_refused_without_a_link(
    database_session: Session, contract: AgentContractRequest, router_bench: Project
) -> None:
    benchmark = _saved_run(database_session, contract)

    response = client.post(f"/benchmarks/{benchmark.id}/project", json={"project": "router-bnech"})

    assert response.status_code == 422
    assert "Existing projects: router-bench." in response.text
    assert _link_rows(database_session, benchmark.id) == []


@pytest.mark.description("Retagging a missing or other org's run returns 404.")
def test_retag_hides_missing_and_other_org_runs(
    database_session: Session, contract: AgentContractRequest, router_bench: Project
) -> None:
    other_run = _saved_run(database_session, contract, org_id=_other_org(database_session).id)

    for run_id in (uuid4(), other_run.id):
        response = client.post(f"/benchmarks/{run_id}/project", json={"project": "router-bench"})
        assert response.status_code == 404
    assert database_session.exec(select(RunProject)).all() == []


_ARCHIVED_DETAIL = "Project 'router-bench' is archived, so new runs can't use it; unarchive it first"


@pytest.mark.description("Archiving keeps the project and its run links but hides it from the default list.")
def test_archive_keeps_the_project_and_its_run_links_and_hides_it_from_the_default_list(
    database_session: Session,
    contract: AgentContractRequest,
    router_bench: Project,
    google_report: Project,
    gateway_api: GatewayAPI,
) -> None:
    running = _saved_run(database_session, contract)
    running.status = BenchmarkStatus.IN_PROGRESS
    request_id, key_id = uuid4(), uuid4()
    router_bench.gateway_request_id, router_bench.gateway_key_id = request_id, key_id
    database_session.add_all([running, router_bench])
    database_session.add(RunProject(benchmark_id=running.id, project_id=router_bench.id))
    database_session.commit()

    response = client.post("/projects/router-bench/archive")

    assert response.status_code == 200, response.text
    assert response.json()["handle"] == "router-bench"
    assert response.json()["archived_at"] is not None
    assert [project["handle"] for project in client.get("/projects").json()["projects"]] == ["google-report-xyz"]
    archived_list = client.get("/projects", params={"include_archived": True}).json()["projects"]
    assert [(project["handle"], project["archived_at"] is not None) for project in archived_list] == [
        ("google-report-xyz", False),
        ("router-bench", True),
    ]
    (link,) = _link_rows(database_session, running.id)
    assert link.project_id == router_bench.id
    stored = database_session.get(Benchmark, running.id)
    assert stored is not None and stored.status == BenchmarkStatus.IN_PROGRESS
    database_session.expire_all()
    project = database_session.get(Project, router_bench.id)
    assert project is not None and (project.gateway_request_id, project.gateway_key_id) == (request_id, key_id)
    assert gateway_api.paths == []


@pytest.mark.description("Unarchiving lists the project again.")
def test_unarchive_lists_the_project_again(database_session: Session, router_bench: Project) -> None:
    assert client.post("/projects/router-bench/archive").status_code == 200

    response = client.post("/projects/router-bench/unarchive")

    assert response.status_code == 200, response.text
    assert response.json()["archived_at"] is None
    assert [project["handle"] for project in client.get("/projects").json()["projects"]] == ["router-bench"]


@pytest.mark.description("Repeated archive and unarchive calls return the project unchanged.")
def test_archive_and_unarchive_return_the_project_as_it_is_when_repeated(
    database_session: Session, router_bench: Project
) -> None:
    first = client.post("/projects/router-bench/archive").json()["archived_at"]

    assert client.post("/projects/router-bench/archive").json()["archived_at"] == first
    assert client.post("/projects/router-bench/unarchive").json()["archived_at"] is None
    assert client.post("/projects/router-bench/unarchive").json()["archived_at"] is None


@pytest.mark.description("Archive and unarchive return 404 for unknown and other org projects.")
@pytest.mark.parametrize("action", ["archive", "unarchive"])
def test_archive_and_unarchive_hide_unknown_and_other_org_projects(database_session: Session, action: str) -> None:
    other = Project(
        org_id=_other_org(database_session).id,
        handle="theirs",
        name="Theirs",
        spend_type=SpendType.INTERNAL,
        subcategory="blog",
    )
    database_session.add(other)
    database_session.commit()

    for handle in ("missing", "theirs"):
        response = client.post(f"/projects/{handle}/{action}")
        assert response.status_code == 404
    database_session.expire_all()
    stored = database_session.get(Project, other.id)
    assert stored is not None and stored.archived_at is None


@pytest.mark.usefixtures("active_executor_release")
async def test_start_in_an_archived_project_is_refused_with_its_handle_and_accepted_after_unarchive(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    assert client.post("/projects/router-bench/archive").status_code == 200

    refused = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"] == _ARCHIVED_DETAIL
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(RunProject)).all() == []

    assert client.post("/projects/router-bench/unarchive").status_code == 200
    accepted = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert accepted.status_code == 200, accepted.text
    (link,) = _link_rows(database_session, UUID(accepted.json()["benchmark_id"]))
    assert link.project_id == router_bench.id


@pytest.mark.description("Suggestions for an unknown project leave out archived handles.")
def test_start_in_an_unknown_project_leaves_archived_handles_out_of_the_suggestions(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, google_report: Project
) -> None:
    assert client.post("/projects/router-bench/archive").status_code == 200

    response = client.post("/start-benchmark", json=_start_request(contract, "router-bnech"))

    assert response.status_code == 422
    assert "Existing projects: google-report-xyz. " in response.json()["detail"]


@pytest.mark.description("Retagging into an archived project is refused and adds no link.")
def test_retag_into_an_archived_project_is_refused_without_a_link(
    database_session: Session, contract: AgentContractRequest, router_bench: Project
) -> None:
    benchmark = _saved_run(database_session, contract)
    assert client.post("/projects/router-bench/archive").status_code == 200

    response = client.post(f"/benchmarks/{benchmark.id}/project", json={"project": "router-bench"})

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == _ARCHIVED_DETAIL
    assert _link_rows(database_session, benchmark.id) == []


@pytest.mark.description("Retrying or resuming a run in an archived project is refused and leaves the run unchanged.")
@pytest.mark.parametrize("query", ["", "?retry=true"])
def test_retry_or_resume_of_a_run_in_an_archived_project_is_refused_without_changing_the_run(
    database_session: Session, contract: AgentContractRequest, router_bench: Project, query: str
) -> None:
    benchmark = _saved_run(database_session, contract)
    benchmark.status = BenchmarkStatus.STOPPED
    database_session.add(benchmark)
    database_session.add(RunProject(benchmark_id=benchmark.id, project_id=router_bench.id))
    database_session.commit()
    assert client.post("/projects/router-bench/archive").status_code == 200

    response = client.post(f"/retry-or-resume-benchmark/{benchmark.id}{query}")

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == _ARCHIVED_DETAIL
    database_session.expire_all()
    stored = database_session.get(Benchmark, benchmark.id)
    assert stored is not None and stored.status == BenchmarkStatus.STOPPED


def _archive_in_the_background(database_session: Session, project: Project) -> None:
    project.archived_at = datetime.now(UTC)
    database_session.add(project)
    database_session.commit()


@pytest.mark.usefixtures("active_executor_release")
async def test_start_whose_project_is_archived_after_the_lookup_is_refused_and_saves_nothing(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    looked_up = main_module.find_project

    def archive_after_lookup(repository: ProjectRepository, org_id: UUID, handle: str) -> Project:
        project = looked_up(repository, org_id, handle)
        _archive_in_the_background(database_session, router_bench)
        return project

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
    monkeypatch.setattr(main_module, "find_project", archive_after_lookup)

    response = client.post("/start-benchmark", json=_start_request(contract, "router-bench"))

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == _ARCHIVED_DETAIL
    assert database_session.exec(select(Benchmark)).all() == []
    assert database_session.exec(select(Task)).all() == []
    assert database_session.exec(select(RunProject)).all() == []


@pytest.mark.description("A retry whose project is archived mid request is refused and leaves the run unchanged.")
async def test_retry_whose_project_is_archived_after_the_first_check_is_refused_without_changing_the_run(
    monkeypatch: MonkeyPatch,
    database_session: Session,
    contract: AgentContractRequest,
    mock_launcher: Any,
    router_bench: Project,
) -> None:
    benchmark = _saved_run(database_session, contract)
    benchmark.status = BenchmarkStatus.STOPPED
    database_session.add(benchmark)
    database_session.add(RunProject(benchmark_id=benchmark.id, project_id=router_bench.id))
    database_session.commit()

    def archive_after_check(project: Project) -> Project:
        checked = reject_archived_project(project)
        _archive_in_the_background(database_session, router_bench)
        return checked

    monkeypatch.setattr(main_module, "reject_archived_project", archive_after_check)
    monkeypatch.setattr(main_module, "reset_to_in_progress_status", lambda *_args, **_kwargs: ["task_0"])

    response = client.post(f"/retry-or-resume-benchmark/{benchmark.id}?retry=true")

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == _ARCHIVED_DETAIL
    database_session.expire_all()
    stored = database_session.get(Benchmark, benchmark.id)
    assert stored is not None and stored.status == BenchmarkStatus.STOPPED


@pytest.mark.description("Runs without a project get their own default project whatever names other projects hold.")
@pytest.mark.parametrize(
    ("taken", "expected"),
    [
        (["Default"], "Default (default)"),
        (["Default", "Default (default)"], "Default (default 2)"),
    ],
)
def test_runs_without_a_project_get_their_own_default_project_whatever_names_others_hold(
    database_session: Session, taken: list[str], expected: str
) -> None:
    others = [
        Project(
            org_id=TEST_ORG_ID,
            handle=f"team-{index}",
            name=name,
            spend_type=SpendType.DEVELOPMENT,
            subcategory="new_benchmark_generation",
        )
        for index, name in enumerate(taken)
    ]
    database_session.add_all(others)
    database_session.commit()

    project = ProjectRepository(database_session).get_or_create_default(TEST_ORG_ID, "alice@vals.ai")

    assert (project.handle, project.name) == ("default", expected)
    assert ProjectRepository(database_session).get_or_create_default(TEST_ORG_ID, None).id == project.id
