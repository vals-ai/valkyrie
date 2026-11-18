import httpx
import pytest

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import link_run_project, make_benchmark, make_project
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.clients.projects import ProjectServiceClient
from tracker.repositories import project as project_repository
from tracker.domain import BenchmarkStatus
from tracker.services.datasets.ref import HostedDataset


@pytest.mark.description("Filter option responses and authentication.")
class TestFilterOptions:
    @pytest.mark.description(
        (
            "Filter options must collapse repeated benchmark metadata into distinct values.\n"
            "\n"
            "Test cases:\n"
            "- Authenticated results contain each available filter value once.\n"
            '- Runs without a dataset are listed under "default", matching the run list and its dataset filter.\n'
            "- Blank agents, models and starters are left out because the run-list filters cannot select them."
        )
    )
    def test_filter_options_returns_distinct(self, client: TestClient, database_session: Session) -> None:
        for benchmark_name, agent_name, model, dataset, email in [
            ("swebench", "mini_sweagent", "gateway/openai/gpt-5", "verified", "a@vals.ai"),
            ("swebench", "claude_code", "gateway/anthropic/claude-opus-5", None, "b@vals.ai"),
            ("swebench", "claude_code", "gateway/anthropic/claude-opus-5", "default", "b@vals.ai"),
            ("fab", "mini_sweagent", None, "verified", None),
            ("fab", "", "", "", ""),
            ("swebench", "mini_sweagent", "gateway/openai/gpt-5", "lite", "a@vals.ai"),
        ]:
            make_benchmark(
                name=benchmark_name,
                status=BenchmarkStatus.FINISHED,
                agent_name=agent_name,
                model=model,
                dataset=dataset,
                started_by_email=email,
                session=database_session,
            )

        response = client.get(
            "/benchmarks/filter-options",
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        response_body = response.json()
        assert response_body["benchmark_names"] == ["fab", "swebench"]
        assert response_body["agent_names"] == ["claude_code", "mini_sweagent"]
        assert response_body["models"] == ["gateway/anthropic/claude-opus-5", "gateway/openai/gpt-5"]
        assert response_body["datasets"] == ["default", "lite", "verified"]
        assert response_body["started_by_emails"] == ["a@vals.ai", "b@vals.ai"]

    @pytest.mark.description("A run with a blank dataset is listed under `default`.")
    def test_filter_options_lists_blank_dataset_as_default(self, client: TestClient, database_session: Session) -> None:
        make_benchmark(name="fab", status=BenchmarkStatus.FINISHED, dataset="", session=database_session)

        response = client.get("/benchmarks/filter-options", headers={"X-Api-Key": "fake"})

        assert response.status_code == 200, response.text
        assert response.json()["datasets"] == ["default"]

    @pytest.mark.description(
        (
            "Runs match the dataset filter and filter options by the dataset their run row shows.\n"
            "\n"
            "Test cases:\n"
            "- A hosted-dataset run is listed and filtered under its hosted dataset name.\n"
            "- A benchmark-service run is listed and filtered under its `dataset` argument."
        )
    )
    def test_dataset_filter_matches_hosted_and_legacy_runs(self, client: TestClient, database_session: Session) -> None:
        hosted = make_benchmark(
            name="demo",
            status=BenchmarkStatus.FINISHED,
            dataset_ref=HostedDataset(benchmark="demo", dataset="tiny", revision=1),
            session=database_session,
        )
        legacy = make_benchmark(
            name="swebench", status=BenchmarkStatus.FINISHED, dataset="verified", session=database_session
        )

        options = client.get("/benchmarks/filter-options", headers={"X-Api-Key": "fake"})
        hosted_runs = client.get("/fetch-benchmarks?dataset=tiny", headers={"X-Api-Key": "fake"})
        legacy_runs = client.get("/fetch-benchmarks?dataset=verified", headers={"X-Api-Key": "fake"})

        assert options.status_code == 200, options.text
        assert options.json()["datasets"] == ["tiny", "verified"]

        assert hosted_runs.status_code == 200, hosted_runs.text
        assert [(run["id"], run["dataset"]) for run in hosted_runs.json()["benchmarks"]] == [(str(hosted.id), "tiny")]

        assert legacy_runs.status_code == 200, legacy_runs.text
        assert [(run["id"], run["dataset"]) for run in legacy_runs.json()["benchmarks"]] == [
            (str(legacy.id), "verified")
        ]

    @pytest.mark.description(
        (
            "Filter options narrow to the runs whose newest project link is the requested project.\n"
            "\n"
            "Test cases:\n"
            "- Values from runs outside the project are left out.\n"
            "- A run moved to another project contributes its values only to the new one.\n"
            "- An archived project's handle still filters, with one project service request."
        )
    )
    def test_filter_options_narrow_to_project(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        inside, other = make_project(project_service, "p"), make_project(project_service, "q")
        project_service.rows[str(inside.id)]["archived_at"] = "2026-11-01T00:00:00Z"
        linked_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        stayed = make_benchmark(model="a", session=database_session)
        make_benchmark(model="b", session=database_session)
        moved = make_benchmark(model="c", session=database_session)
        link_run_project(database_session, stayed.id, inside, linked_at)
        link_run_project(database_session, moved.id, inside, linked_at)
        link_run_project(database_session, moved.id, other, linked_at + timedelta(minutes=1))
        database_session.commit()
        paths: list[str] = []

        def counted(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            return project_service.handle(request)

        service = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(counted)))
        monkeypatch.setattr(project_repository, "project_service", lambda: service)

        def models(handle: str) -> list[str]:
            response = client.get(
                "/benchmarks/filter-options", params={"project": handle}, headers={"X-Api-Key": "fake"}
            )
            assert response.status_code == 200, response.text
            return response.json()["models"]

        assert models("p") == ["a"]
        assert models("q") == ["c"]
        assert paths == [f"/orgs/{TEST_ORG_ID}/projects"] * 2

    @pytest.mark.description(
        (
            "Benchmark filter metadata must require authentication.\n"
            "\n"
            "Test cases:\n"
            "- A request without a bearer session receives 401."
        )
    )
    def test_filter_options_unauth_401(self, client: TestClient) -> None:
        response = client.get("/benchmarks/filter-options")

        assert response.status_code == 401
