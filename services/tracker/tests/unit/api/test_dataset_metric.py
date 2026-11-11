from collections.abc import Sequence
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

import main as main_module
from main import app
from tests.unit.datasets.support import archive_digest_of, zip_bytes
from tests.unit.services.task_execution_support import MockLauncher
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.types import AgentContractRequest
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Task,
    TaskStatus,
)
from tracker.services.datasets import scoring
from tracker.services.datasets.metric_script import file_digest
from tracker.services.datasets.ref import HostedDataset
from tracker.services.datasets.rewards import RewardMap
from tracker.clients.metric_runner import MetricResult, MetricRunnerError
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.types import StartBenchmarkRequest

pytestmark = pytest.mark.usefixtures("active_executor_release")
_client = TestClient(app, base_url="http://localhost")
_BASE = "/v1/benchmarks/swe-bench/datasets/verified"
_SCRIPT = b"import sys\nprint('metric')\n"
_TASK = zip_bytes(
    {
        "task.toml": '[environment]\ndocker_image = "ubuntu:24.04"\n',
        "instruction.md": "Do it.",
        "tests/test.sh": "#!/bin/bash",
    }
)
_HOSTED = HostedDataset(benchmark="swe-bench", dataset="verified")


class Runner:
    def __init__(self) -> None:
        self.metrics: dict[str, float] = {"reward": 0.25}
        self.error: str | None = None
        self.calls: list[tuple[str, str, list[RewardMap | None]]] = []

    def __call__(self, function_name: str) -> "Runner":
        return self

    async def invoke(self, tenant_id: str, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult:
        self.calls.append((tenant_id, script, list(rewards)))
        if self.error is not None:
            raise MetricRunnerError(self.error)
        return MetricResult(metrics=dict(self.metrics), duration=0.5)


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> Runner:
    fake = Runner()
    monkeypatch.setattr(scoring, "LambdaMetricInvoker", fake)
    monkeypatch.setattr(config, "METRIC_RUNNER_FUNCTION", "valkyrie-metric-runner-test")
    return fake


@pytest.fixture(autouse=True)
def local_tracker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path / "data"))
    _client.post("/v1/benchmarks", json={"name": "swe-bench"})
    assert _client.post("/v1/benchmarks/swe-bench/datasets", json={"name": "verified"}).status_code == 201


@pytest.fixture
def scoring_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "METRIC_RUNNER_FUNCTION", "valkyrie-metric-runner-test")


def put_script(content: bytes = _SCRIPT) -> str:
    digest = file_digest(content)
    stored = _client.put(f"{_BASE}/files/{digest}", content=content)
    assert stored.status_code in {200, 201}, stored.text
    return digest


def put_task() -> str:
    digest = archive_digest_of(_TASK)
    stored = _client.put(f"{_BASE}/blobs/{digest}", content=_TASK, headers={"content-type": "application/zip"})
    assert stored.status_code in {200, 201}, stored.text
    return digest


def publish(metric_digest: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"tasks": {"task-a": put_task(), "task-b": put_task()}, "metric_digest": metric_digest}
    published = _client.post(f"{_BASE}/versions", json=body)
    assert published.status_code in {200, 201}, published.text
    return published.json()


@pytest.mark.description("Storing a metric script with a dataset version.")
class TestMetricFiles:
    @pytest.mark.description("A deployment without a metric runner cannot store or publish a script.")
    def test_a_deployment_without_a_metric_runner_cannot_store_or_publish_a_script(self) -> None:
        digest = file_digest(_SCRIPT)

        stored = _client.put(f"{_BASE}/files/{digest}", content=_SCRIPT)
        published = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": put_task()}, "metric_digest": digest})

        assert stored.status_code == published.status_code == 403
        assert "isn't available" in stored.json()["detail"]

    @pytest.mark.description("A script is stored under its digest and listed on the version.")
    @pytest.mark.usefixtures("scoring_available")
    def test_a_script_is_stored_under_its_digest_and_listed_on_the_version(self, tmp_path: Path) -> None:
        digest = put_script()

        again = _client.put(f"{_BASE}/files/{digest}", content=_SCRIPT)
        version = publish(digest)

        assert again.status_code == 200
        assert version["metric_digest"] == digest
        assert _client.get(f"{_BASE}/versions/1").json()["metric_digest"] == digest
        assert [path.name for path in (tmp_path / "data").rglob("*") if path.is_file() and "files" in path.parts] == [
            digest.removeprefix("sha256:")
        ]

    @pytest.mark.description("The same script is the same revision and a changed script is a new one.")
    @pytest.mark.usefixtures("scoring_available")
    def test_the_same_script_is_the_same_revision_and_a_changed_script_is_a_new_one(self) -> None:
        first = publish(put_script())

        same = publish(put_script())
        changed = publish(put_script(b"print('another')\n"))
        without = publish()

        assert same["revision"] == first["revision"]
        assert len({first["revision"], changed["revision"], without["revision"]}) == 3
        assert without["metric_digest"] is None

    @pytest.mark.description("A script that cannot run is refused and never stored.")
    @pytest.mark.usefixtures("scoring_available")
    @pytest.mark.parametrize(
        ("content", "status", "complaint"),
        [
            pytest.param(b"def broken(:\n", 400, "not valid Python", id="not-python"),
            pytest.param(b"x" * (256 * 1024 + 1), 413, "larger than", id="too-large"),
        ],
    )
    def test_a_script_that_cannot_run_is_refused_and_never_stored(
        self, content: bytes, status: int, complaint: str
    ) -> None:
        refused = _client.put(f"{_BASE}/files/{file_digest(content)}", content=content)

        assert refused.status_code == status
        assert complaint in refused.json()["detail"]

    @pytest.mark.description("A script that does not hash to its digest is refused.")
    @pytest.mark.usefixtures("scoring_available")
    def test_a_script_that_does_not_hash_to_its_digest_is_refused(self) -> None:
        refused = _client.put(f"{_BASE}/files/{file_digest(b'other')}", content=_SCRIPT)

        assert refused.status_code == 422

    @pytest.mark.description("Publishing a script that is not stored or a digest that is not sha256 is refused.")
    @pytest.mark.usefixtures("scoring_available")
    def test_publishing_a_script_that_is_not_stored_or_a_digest_that_is_not_sha256_is_refused(self) -> None:
        digest = file_digest(_SCRIPT)
        tasks = {"task-a": put_task()}

        missing = _client.post(f"{_BASE}/versions", json={"tasks": tasks, "metric_digest": digest})
        other = _client.post(f"{_BASE}/versions", json={"tasks": tasks, "metric_digest": "sha256:not-hex"})

        assert missing.status_code == 409 and "not stored yet" in missing.json()["detail"]
        assert other.status_code == 422


def start_run(contract: AgentContractRequest, version: str = "latest") -> UUID:
    request = StartBenchmarkRequest(
        contract=contract,
        benchmark_name="swe-bench",
        sandbox_provider="docker",
        dataset_ref=_HOSTED.model_copy(update={"ref": version}),
    )
    response = _client.post("/start-benchmark", json=request.model_dump(mode="json"))
    assert response.status_code == 200, response.text
    return UUID(response.json()["benchmark_id"])


def finish_run(session: Session, run_id: UUID, rewards: dict[str, dict[str, float]]) -> None:
    for task in session.exec(select(Task).where(Task.benchmark == run_id)):
        task.status = TaskStatus.FINISHED
        session.add(task)
        session.add(
            EvaluationResult(
                org_id=TEST_ORG_ID,
                task=task.id,
                instance_id=str(uuid4()),
                result={"task_id": task.task_id, "rewards": rewards[task.task_id]},
            )
        )
    run = session.get(Benchmark, run_id)
    assert run is not None
    run.status = BenchmarkStatus.FINISHED
    session.add(run)
    session.commit()


def preview(run_id: UUID, *task_ids: str) -> dict[str, Any]:
    wanted = task_ids or ("task-a", "task-b")
    response = _client.get(
        "/retrieve-results", params=[("benchmark_id", str(run_id)), *[("task_ids", task_id) for task_id in wanted]]
    )
    assert response.status_code == 200, response.text
    return response.json()["final_evaluation"]


@pytest.mark.description("A run on a version that carries a metric script.")
@pytest.mark.usefixtures("scoring_available")
class TestScoredRun:
    @pytest.mark.description("The script's metric is the score of the run, of a repeat request and of a subset.")
    def test_the_scripts_metric_is_the_score_of_the_run_of_a_repeat_request_and_of_a_subset(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish(put_script())
        run_id = start_run(contract)
        finish_run(database_session, run_id, {"task-a": {"reward": 1.0}, "task-b": {"reward": 0.0}})

        first = preview(run_id, "task-a", "task-b")
        second = preview(run_id, "task-a", "task-b")
        subset = preview(run_id, "task-a")

        assert first["final_score"] == 25.0 and second["final_score"] == 25.0
        assert first["properties"]["scorer"]["digest"] == file_digest(_SCRIPT)
        assert subset["final_score"] == 25.0
        assert [(tenant, rewards) for tenant, _script, rewards in runner.calls] == [
            (str(TEST_ORG_ID), [{"reward": 1.0}, {"reward": 0.0}]),
            (str(TEST_ORG_ID), [{"reward": 1.0}, {"reward": 0.0}]),
            (str(TEST_ORG_ID), [{"reward": 1.0}]),
        ]

    @pytest.mark.description("Rescoring runs the script again and replaces the stored score and the stored final view.")
    def test_rescoring_runs_the_script_again_and_replaces_the_stored_score(
        self,
        contract: AgentContractRequest,
        runner: Runner,
        database_session: Session,
        mock_launcher: MockLauncher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        publish(put_script())
        run_id = start_run(contract)
        finish_run(database_session, run_id, {"task-a": {"reward": 1.0}, "task-b": {"reward": 0.0}})
        preview(run_id)
        runner.metrics = {"reward": 0.75}
        uploaded: list[Any] = []

        async def record(final_view: Any, _objects: Any) -> str:
            uploaded.append(final_view)
            return "key"

        monkeypatch.setattr(main_module, "upload_final_view", record)

        rescored = _client.post(f"/rescore-benchmark/{run_id}")

        assert rescored.status_code == 200, rescored.text
        assert rescored.json()["final_score"] == 75.0
        assert [view.final_evaluation.final_score for view in uploaded if view.final_evaluation] == [75.0]
        database_session.expire_all()
        stored = database_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == run_id)).one()
        assert stored.final_score == 75.0

    @pytest.mark.description("A rescore whose script fails returns 502 and keeps the stored score.")
    def test_a_rescore_whose_script_fails_returns_502_and_keeps_the_stored_score(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish(put_script())
        run_id = start_run(contract)
        finish_run(database_session, run_id, {"task-a": {"reward": 1.0}, "task-b": {"reward": 0.0}})
        assert _client.post(f"/rescore-benchmark/{run_id}").status_code == 200
        runner.error = "The script exited with status 1: boom"

        failed = _client.post(f"/rescore-benchmark/{run_id}")

        assert failed.status_code == 502
        assert failed.json()["detail"] == "The metric script failed: The script exited with status 1: boom"
        database_session.expire_all()
        stored = database_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == run_id)).one()
        assert stored.final_score == 25.0

    @pytest.mark.description("A failing script leaves the run with its results and no score.")
    def test_a_failing_script_leaves_the_run_with_its_results_and_no_score(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish(put_script())
        run_id = start_run(contract)
        finish_run(database_session, run_id, {"task-a": {"reward": 1.0}, "task-b": {"reward": 1.0}})
        runner.error = "The script exited with status 1: boom"

        evaluation = preview(run_id)
        results = _client.get("/retrieve-results", params={"benchmark_id": str(run_id)}).json()

        assert evaluation["final_score"] is None
        assert evaluation["properties"]["scorer"]["error"] == "The script exited with status 1: boom"
        assert set(results["evaluation_results"]) == {"task-a", "task-b"}

    @pytest.mark.description("A version without a script is scored by the mean.")
    def test_a_version_without_a_script_is_scored_by_the_mean(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish()
        run_id = start_run(contract)
        finish_run(database_session, run_id, {"task-a": {"reward": 1.0}, "task-b": {"reward": 0.0}})

        evaluation = preview(run_id)

        assert evaluation["final_score"] == 50.0 and "scorer" not in evaluation["properties"]
        assert runner.calls == []

    @pytest.mark.description("A run pinned to an earlier version keeps that versions scoring.")
    def test_a_run_pinned_to_an_earlier_version_keeps_that_versions_scoring(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish()
        publish(put_script())
        old_run = start_run(contract, "1")
        finish_run(database_session, old_run, {"task-a": {"reward": 1.0}, "task-b": {"reward": 0.0}})

        evaluation = preview(old_run)

        assert evaluation["final_score"] == 50.0 and runner.calls == []

    @pytest.mark.description("Only a finished run on a dataset can be rescored.")
    def test_only_a_finished_run_on_a_dataset_can_be_rescored(
        self, contract: AgentContractRequest, runner: Runner, database_session: Session, mock_launcher: MockLauncher
    ) -> None:
        publish()
        run_id = start_run(contract)

        running = _client.post(f"/rescore-benchmark/{run_id}")

        assert running.status_code == 409
        assert _client.post(f"/rescore-benchmark/{uuid4()}").status_code == 404
