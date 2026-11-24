import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from main import app
from tracker.database.models import ExecutorDispatch
from tests.unit.datasets.support import publish_version, zip_bytes
from tests.unit.services.task_execution_support import MockLauncher
from tests.utils import TEST_ORG_ID
from tracker.types import AgentContractRequest
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    Task,
    TaskStatus,
    UserBenchmarkService,
)
from tracker.services.datasets.oracle import ORACLE_CONTRACT
from tracker.services.datasets.ref import DatasetRef, HostedDataset, LocalDataset
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.types import StartBenchmarkRequest
from tracker.secret_access import tenant_secret_prefix

pytestmark = pytest.mark.usefixtures("active_executor_release")
_client = TestClient(app, base_url="http://localhost")
_DATASETS = "/v1/benchmarks/swe-bench/datasets"
_TASK_TOML = '[environment]\ndocker_image = "ubuntu:24.04"\n'
_HOSTED = HostedDataset(benchmark="swe-bench", dataset="verified")


def _task_files(name: str) -> dict[str, str]:
    return {"task.toml": _TASK_TOML, "instruction.md": f"Fix {name}.", "tests/test.sh": "#!/bin/bash\n"}


def _start(
    contract: AgentContractRequest, ref: DatasetRef, benchmark: str = "swe-bench", **fields: object
) -> dict[str, object]:
    request = StartBenchmarkRequest(
        contract=contract, benchmark_name=benchmark, sandbox_provider="docker", dataset_ref=ref
    )
    return {**request.model_dump(mode="json"), **fields}


@pytest.fixture(autouse=True)
def local_tracker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path / "data"))


def _store_tasks(benchmark: str, dataset: str, names: list[str], tags: list[str] | None = None) -> dict[str, Any]:
    assert _client.post("/v1/benchmarks", json={"name": benchmark}).status_code == 201
    assert _client.post(f"/v1/benchmarks/{benchmark}/datasets", json={"name": dataset}).status_code == 201
    return _publish(benchmark, dataset, names, tags)


def _publish(benchmark: str, dataset: str, names: list[str], tags: list[str] | None = None) -> dict[str, Any]:
    archives = {name: zip_bytes(_task_files(name)) for name in names}
    return publish_version(_client, f"/v1/benchmarks/{benchmark}/datasets/{dataset}", archives, tags or [])


@pytest.fixture
def hosted_tasks(local_tracker: None) -> list[str]:
    names = ["task-a", "task-b", "task-c"]
    _store_tasks("swe-bench", "verified", names)
    return names


def _folder(root: Path, names: list[str]) -> Path:
    for name in names:
        for path, text in _task_files(name).items():
            (root / name / path).parent.mkdir(parents=True, exist_ok=True)
            (root / name / path).write_text(text)
    return root


@pytest.mark.description("A run on a dataset stored in the Tracker.")
class TestHostedDatasetStart:
    @pytest.mark.description("The run creates a task for each stored task and queues the resolved dataset.")
    def test_the_run_creates_a_task_for_each_stored_task_and_queues_the_resolved_dataset(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        mock_launcher: MockLauncher,
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED))

        assert response.status_code == 200, response.text
        run = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert run is not None
        assert isinstance(run.arguments.dataset_ref, HostedDataset)
        assert run.arguments.dataset_ref.dataset_id is not None
        assert {task.task_id for task in database_session.exec(select(Task).where(Task.benchmark == run.id))} == set(
            hosted_tasks
        )

        queued = mock_launcher.queued_calls[0]["execution_context_json"]["start_benchmark_request"]
        assert queued["dataset_ref"]["dataset_id"] == str(run.arguments.dataset_ref.dataset_id)

    @pytest.mark.description("Task ids choose the tasks to run.")
    def test_task_ids_choose_the_tasks_to_run(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, task_ids=["task-b"]))

        assert response.status_code == 200, response.text
        tasks = database_session.exec(select(Task).where(Task.benchmark == UUID(response.json()["benchmark_id"])))
        assert [task.task_id for task in tasks] == ["task-b"]

    @pytest.mark.description("An unknown task id is refused before the run is created.")
    def test_an_unknown_task_id_is_refused_before_the_run_is_created(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, task_ids=["missing"]))

        assert response.status_code == 400
        assert "missing" in response.json()["detail"]
        assert database_session.exec(select(Benchmark)).all() == []

    @pytest.mark.description("A run pins the version it names.")
    def test_a_run_pins_the_version_it_names(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        _publish("swe-bench", "verified", ["task-a"], tags=["small"])

        latest = _client.post("/start-benchmark", json=_start(contract, _HOSTED))
        first = _client.post("/start-benchmark", json=_start(contract, _HOSTED.model_copy(update={"ref": "1"})))
        tagged = _client.post("/start-benchmark", json=_start(contract, _HOSTED.model_copy(update={"ref": "small"})))

        runs = {
            name: database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
            for name, response in {"latest": latest, "first": first, "tagged": tagged}.items()
        }
        pinned = {
            name: (
                run.arguments.dataset_ref.revision,
                sorted(task.task_id for task in database_session.exec(select(Task).where(Task.benchmark == run.id))),
            )
            for name, run in runs.items()
            if run is not None and isinstance(run.arguments.dataset_ref, HostedDataset)
        }
        assert pinned == {
            "latest": (2, ["task-a"]),
            "first": (1, ["task-a", "task-b", "task-c"]),
            "tagged": (2, ["task-a"]),
        }
        assert [
            (response.json()["dataset_version"], response.json()["dataset_version_warning"])
            for response in (latest, first)
        ] == [
            ({"id": "swe-bench/verified@2", "label": None}, None),
            ({"id": "swe-bench/verified@1", "label": None}, None),
        ]
        reported = _client.get(f"/fetch-benchmark-metadata/{first.json()['benchmark_id']}").json()[
            "benchmark_arguments"
        ]
        assert (reported["dataset"], reported["dataset_version"]["id"]) == ("verified", "swe-bench/verified@1")

    @pytest.mark.description("An unknown version is not found.")
    @pytest.mark.parametrize("ref", ["9", "nope"])
    def test_an_unknown_version_is_not_found(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session, ref: str
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED.model_copy(update={"ref": ref})))

        assert response.status_code == 404
        assert f"Version '{ref}'" in response.json()["detail"]
        assert database_session.exec(select(Benchmark)).all() == []

    @pytest.mark.description("An unknown benchmark or dataset is not found.")
    @pytest.mark.parametrize("dataset", ["nope", "verified"])
    def test_an_unknown_benchmark_or_dataset_is_not_found(
        self, contract: AgentContractRequest, local_tracker: None, dataset: str
    ) -> None:
        _client.post("/v1/benchmarks", json={"name": "swe-bench"})

        response = _client.post(
            "/start-benchmark", json=_start(contract, HostedDataset(benchmark="swe-bench", dataset=dataset))
        )

        assert response.status_code == 404
        assert f"swe-bench/{dataset}" in response.json()["detail"]

    @pytest.mark.description("An archived dataset cannot start a run.")
    def test_an_archived_dataset_cannot_start_a_run(
        self, contract: AgentContractRequest, hosted_tasks: list[str]
    ) -> None:
        assert _client.delete(f"{_DATASETS}/verified").status_code == 200

        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED))

        assert response.status_code == 409
        assert "restore it first" in response.json()["detail"]


@pytest.mark.description("What a start request for a dataset run may and may not say.")
class TestDatasetRunRequest:
    @pytest.mark.description("A run on a benchmark whose name is not a hostname can start and resume.")
    def test_a_run_on_a_benchmark_whose_name_is_not_a_hostname_can_start_and_resume(
        self, contract: AgentContractRequest, local_tracker: None, database_session: Session
    ) -> None:
        name = "terminal-bench-2.1"
        _store_tasks(name, "all", ["task-a"])
        ref = HostedDataset(benchmark=name, dataset="all")

        response = _client.post("/start-benchmark", json=_start(contract, ref, name))

        assert response.status_code == 200, response.text
        run = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert run is not None
        assert run.name == name
        run.status = BenchmarkStatus.STOPPED
        database_session.add(run)
        for task in database_session.exec(select(Task).where(Task.benchmark == run.id)):
            task.status = TaskStatus.STOPPED
            database_session.add(task)
        database_session.commit()

        resumed = _client.post(f"/retry-or-resume-benchmark/{run.id}")

        assert resumed.status_code == 200, resumed.text

    @pytest.mark.description("Options that belong to a benchmark service are refused.")
    @pytest.mark.parametrize(
        "fields",
        [{"dataset": "default"}, {"dataset_version": "v1"}, {"service_headers": {"X-Key": "value"}}],
        ids=["dataset", "version", "headers"],
    )
    def test_options_that_belong_to_a_benchmark_service_are_refused(
        self, contract: AgentContractRequest, hosted_tasks: list[str], fields: dict[str, object]
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, **fields))

        assert response.status_code == 422
        assert "benchmark service" in response.text

    @pytest.mark.description("An oracle run needs no agent in the library and runs the tasks solutions.")
    def test_an_oracle_run_needs_no_agent_in_the_library_and_runs_the_tasks_solutions(
        self, hosted_tasks: list[str], mock_launcher: MockLauncher
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(AgentContractRequest(name="oracle"), _HOSTED))

        assert response.status_code == 200, response.text

        queued = mock_launcher.queued_calls[0]["execution_context_json"]["start_benchmark_request"]
        assert queued["contract"]["run_cmd"] == ORACLE_CONTRACT.run_cmd

    @pytest.mark.description("An oracle run requires a dataset and cannot replace its built-in agent with a release.")
    @pytest.mark.parametrize("exact_release", [False, True])
    def test_an_oracle_run_needs_a_dataset(self, exact_release: bool) -> None:
        body = StartBenchmarkRequest(contract=AgentContractRequest(name="oracle"), benchmark_name="swebench")
        payload = body.model_dump(mode="json")
        if exact_release:
            payload["contract"]["agent_release"] = {"reference": "release-1", "archive_sha256": "a" * 64}

        response = _client.post("/start-benchmark", json=payload)

        assert response.status_code == 400
        assert ("cannot select an agent release" if exact_release else "runs only on a dataset") in response.text

    @pytest.mark.description("An ordinary run still needs a hostname for its benchmark.")
    @pytest.mark.parametrize("name", ["Bad Name", "a.b", "a" * 65])
    def test_an_ordinary_run_still_needs_a_hostname_for_its_benchmark(
        self, contract: AgentContractRequest, name: str
    ) -> None:
        body = StartBenchmarkRequest(contract=contract, benchmark_name="swebench").model_dump(mode="json")

        response = _client.post("/start-benchmark", json={**body, "benchmark_name": name})

        assert response.status_code == 422
        assert "Invalid benchmark name" in response.text

    @pytest.mark.description("A name that is not even a resource name is refused at the field.")
    def test_a_name_that_is_not_even_a_resource_name_is_refused_at_the_field(
        self, contract: AgentContractRequest
    ) -> None:
        body = _start(contract, _HOSTED)

        response = _client.post("/start-benchmark", json={**body, "benchmark_name": "Bad Name"})

        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["body", "benchmark_name"]

    @pytest.mark.description("A service saved under the benchmark name is not used.")
    def test_a_service_saved_under_the_benchmark_name_is_not_used(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        database_session.merge(
            UserBenchmarkService(
                org_id=TEST_ORG_ID, user_id="local", benchmark="swe-bench", url="http://localhost:8001"
            )
        )
        database_session.commit()

        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED))

        assert response.status_code == 200, response.text
        run = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert run is not None
        assert run.custom_benchmark_service is None


@pytest.mark.description("A Valkyrie regrade validates task compatibility and grading credentials before admission.")
class TestRegradeSecrets:
    @pytest.mark.description("Relative artifacts require a known working directory before regrade admission.")
    @pytest.mark.parametrize("workdir", [None, "/app"])
    @pytest.mark.parametrize("artifact", ["out", "/out"])
    def test_image_workdir_artifacts_are_validated_before_admission(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        workdir: str | None,
        artifact: str,
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, task_ids=["task-a"]))
        assert response.status_code == 200, response.text
        source = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert source is not None
        source.status = BenchmarkStatus.FINISHED
        task = database_session.exec(select(Task).where(Task.benchmark == source.id)).one()
        task.status = TaskStatus.FINISHED
        database_session.add_all([source, task])
        database_session.commit()
        files = _task_files("task-a")
        if workdir:
            files["task.toml"] += f"workdir = '{workdir}'\n"
        files["task.toml"] += f"[verifier]\nenvironment_mode = 'separate'\n[[artifacts]]\nsource = '{artifact}'\n"
        publish_version(_client, f"{_DATASETS}/verified", {"task-a": zip_bytes(files)})
        capture = Mock()
        monkeypatch.setattr("sentry_sdk.capture_exception", capture)

        response = _client.post(f"/regrade-benchmark/{source.id}", json={"dataset": "swe-bench/verified"})

        if workdir is None and not artifact.startswith("/"):
            assert response.status_code == 400, response.text
            detail = response.json()["detail"]
            assert "Valkyrie" in detail and "swe-bench/verified" in detail and "Task 'task-a'" in detail
            assert "relative artifact path" in detail and "working directory from its image" in detail
            assert "valkyrie run start" in detail
            assert len(database_session.exec(select(Benchmark)).all()) == 1
            assert len(database_session.exec(select(ExecutorDispatch)).all()) == 1
        else:
            assert response.status_code == 200, response.text
        capture.assert_not_called()

    @pytest.mark.description(
        "A verifier using models requires an eligible selected project key before regrade admission."
    )
    @pytest.mark.parametrize("binding", ["missing", "untrusted", "valid", "regraded"])
    @pytest.mark.parametrize("target_benchmark", ["swe-bench", "my_benchmark"])
    def test_grading_requires_a_selected_project_key(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        binding: str,
        target_benchmark: str,
    ) -> None:
        monkeypatch.setattr("tracker.config.GATEWAY_KEY_ORGS", {"default"})
        credentials = tmp_path / "credentials.env"
        credentials.write_text("MODEL_GATEWAY_URL=https://gateway.example\nMODEL_GATEWAY_API_KEY=grading-token\n")
        monkeypatch.setattr(
            local_config, "resources", LocalResources(data_root=tmp_path / "data", secrets_file=credentials)
        )
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, task_ids=["task-a"]))
        assert response.status_code == 200, response.text
        source = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert source is not None
        reference = f"{tenant_secret_prefix(TEST_ORG_ID)}model-gateway-00000000-0000-0000-0000-000000000001"
        source.status = BenchmarkStatus.FINISHED
        source.arguments = source.arguments.model_copy(
            update={
                "selected_project_gateway_secret_ref": None if binding == "missing" else reference,
                "contract": source.arguments.contract.model_copy(
                    update={
                        "secrets": {
                            "MODEL_GATEWAY_URL": reference,
                            "MODEL_GATEWAY_API_KEY": reference,
                        }
                    }
                ),
            }
        )
        for task in database_session.exec(select(Task).where(Task.benchmark == source.id)):
            task.status = TaskStatus.FINISHED
            database_session.add(task)
        database_session.add(source)
        database_session.commit()
        if binding == "untrusted":
            monkeypatch.setattr("tracker.config.GATEWAY_KEY_ORGS", set())
        if target_benchmark != "swe-bench":
            _store_tasks(target_benchmark, "verified", ["task-a"])
        target_dataset = f"{target_benchmark}/verified"
        target_path = f"/v1/benchmarks/{target_benchmark}/datasets/verified"
        files = _task_files("task-a")
        files["task.toml"] += "[verifier]\nenvironment_mode = 'separate'\nmodels = ['grader']\n"
        if binding == "regraded":
            initial_files = _task_files("task-a")
            initial_files["task.toml"] += "[verifier]\nenvironment_mode = 'separate'\n"
            publish_version(_client, target_path, {"task-a": zip_bytes(initial_files)})
            response = _client.post(f"/regrade-benchmark/{source.id}", json={"dataset": target_dataset})
            assert response.status_code == 200, response.text
            source = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
            assert source is not None
            assert source.arguments.contract.secrets == {}
            assert source.arguments.selected_project_gateway_secret_ref == reference
            source.status = BenchmarkStatus.FINISHED
            task = database_session.exec(select(Task).where(Task.benchmark == source.id)).one()
            task.status = TaskStatus.FINISHED
            database_session.add_all([source, task])
            database_session.commit()
        publish_version(_client, target_path, {"task-a": zip_bytes(files)})
        capture = Mock()
        monkeypatch.setattr("sentry_sdk.capture_exception", capture)

        response = _client.post(f"/regrade-benchmark/{source.id}", json={"dataset": target_dataset})

        if binding in {"valid", "regraded"}:
            assert response.status_code == 200, response.text
            result = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
            assert result is not None
            assert result.arguments.contract.secrets == {
                "MODEL_GATEWAY_URL": reference,
                "MODEL_GATEWAY_API_KEY": reference,
            }
        else:
            assert response.status_code == 400, response.text
            detail = response.json()["detail"]
            assert "Valkyrie" in detail and "selected project Gateway key" in detail
            assert "valkyrie run start" in detail
            assert len(database_session.exec(select(Benchmark)).all()) == 1
            assert len(database_session.exec(select(ExecutorDispatch)).all()) == 1
        capture.assert_not_called()

    @pytest.mark.description("Incompatible dataset tasks are named and refused before a regrade run is created.")
    @pytest.mark.parametrize("configuration", ["shared", "steps", "mixed"])
    def test_incompatible_tasks_are_rejected_before_admission(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        configuration: str,
    ) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED))
        assert response.status_code == 200, response.text
        source = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert source is not None
        source.status = BenchmarkStatus.FINISHED
        for task in database_session.exec(select(Task).where(Task.benchmark == source.id)):
            task.status = TaskStatus.FINISHED
            database_session.add(task)
        database_session.add(source)
        database_session.commit()
        archives: dict[str, bytes] = {}
        for name in hosted_tasks:
            files = _task_files(name)
            if configuration == "steps" or configuration == "mixed" and name != "task-b":
                files["task.toml"] += "[verifier]\nenvironment_mode = 'separate'\n"
                if configuration == "steps" or name == "task-c":
                    files["task.toml"] += "[[steps]]\nname = 'one'\n"
                    files["steps/one/instruction.md"] = "Complete step one."
            archives[name] = zip_bytes(files)
        publish_version(_client, f"{_DATASETS}/verified", archives)
        capture = Mock()
        monkeypatch.setattr("sentry_sdk.capture_exception", capture)

        response = _client.post(f"/regrade-benchmark/{source.id}", json={"dataset": "swe-bench/verified"})

        assert response.status_code == 400, response.text
        detail = response.json()["detail"]
        assert "Valkyrie" in detail and "Tracker" not in detail
        assert "swe-bench/verified" in detail
        assert "steps or grades in the agent's sandbox" in detail
        assert "valkyrie run start" in detail
        for name in hosted_tasks:
            assert (f"Task '{name}'" in detail) == (configuration != "mixed" or name != "task-a")
        assert len(database_session.exec(select(Benchmark)).all()) == 1
        assert len(database_session.exec(select(ExecutorDispatch)).all()) == 1
        capture.assert_not_called()

    @pytest.mark.description(
        "A deleted agent credential does not block regrade and a missing verifier credential creates no run."
    )
    @pytest.mark.parametrize("verifier_secret", ["available", "missing", "missing_environment", "unused"])
    def test_only_grading_secrets_are_required(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        verifier_secret: str,
    ) -> None:
        credentials = tmp_path / "credentials.env"
        credentials.write_text("GRADER_KEY=grading-value\n" if verifier_secret == "available" else "")
        monkeypatch.setattr(
            local_config, "resources", LocalResources(data_root=tmp_path / "data", secrets_file=credentials)
        )
        task_files = _task_files("task-a")
        task_files["task.toml"] += (
            "env = { AGENT_KEY = '${deleted-agent-key}' }\n[verifier]\nenvironment_mode = 'separate'\n"
        )
        if verifier_secret == "missing_environment":
            task_files["task.toml"] += "[verifier.environment.env]\nKEY = '${GRADER_KEY}'\n"
        elif verifier_secret != "unused":
            task_files["task.toml"] += "[verifier.env]\nKEY = '${GRADER_KEY}'\n"
        publish_version(_client, f"{_DATASETS}/verified", {"task-a": zip_bytes(task_files)})
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED))
        assert response.status_code == 200, response.text
        source = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert source is not None
        source.status = BenchmarkStatus.FINISHED
        source.arguments = source.arguments.model_copy(
            update={
                "contract": source.arguments.contract.model_copy(
                    update={
                        "secrets": {
                            "AGENT_KEY": "deleted-agent-key",
                            "MODEL_GATEWAY_URL": "deleted-unused-gateway",
                            "MODEL_GATEWAY_API_KEY": "deleted-unused-gateway",
                        }
                    }
                )
            }
        )
        task = database_session.exec(select(Task).where(Task.benchmark == source.id)).one()
        task.status = TaskStatus.FINISHED
        database_session.add_all([source, task])
        database_session.commit()
        capture = Mock()
        monkeypatch.setattr("sentry_sdk.capture_exception", capture)

        response = _client.post(f"/regrade-benchmark/{source.id}", json={"dataset": "swe-bench/verified"})

        if verifier_secret.startswith("missing"):
            assert response.status_code == 400, response.text
            detail = response.json()["detail"]
            assert "Valkyrie" in detail and "Tracker" not in detail
            assert "GRADER_KEY" in detail and "valkyrie secret set GRADER_KEY" in detail
            assert len(database_session.exec(select(Benchmark)).all()) == 1
            assert len(database_session.exec(select(ExecutorDispatch)).all()) == 1
        else:
            assert response.status_code == 200, response.text
            run = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
            assert run is not None and run.arguments.contract.secrets == {}
            assert response.json()["task_ids"] == ["task-a"]
        capture.assert_not_called()
        assert source.arguments.contract.secrets["AGENT_KEY"] == "deleted-agent-key"


@pytest.mark.description("A run on a folder of tasks on the machine of a local Tracker.")
class TestLocalFolderStart:
    @pytest.mark.description("The run creates a task for each folder in the directory.")
    def test_the_run_creates_a_task_for_each_folder_in_the_directory(
        self, contract: AgentContractRequest, tmp_path: Path, database_session: Session
    ) -> None:
        folder = _folder(tmp_path / "tasks", ["alpha", "beta"])

        response = _client.post("/start-benchmark", json=_start(contract, LocalDataset(path=str(folder))))

        assert response.status_code == 200, response.text
        run = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert run is not None
        assert run.arguments.dataset_ref == LocalDataset(path=str(folder))
        metadata = _client.get(f"/fetch-benchmark-metadata/{run.id}")
        assert metadata.status_code == 200, metadata.text
        assert metadata.json()["benchmark_arguments"]["dataset"] == str(folder)
        assert {task.task_id for task in database_session.exec(select(Task).where(Task.benchmark == run.id))} == {
            "alpha",
            "beta",
        }

    @pytest.mark.description("Invalid TOML in a selected local task is rejected with the task name before launch.")
    def test_malformed_task_config_is_rejected(self, contract: AgentContractRequest, tmp_path: Path) -> None:
        folder = _folder(tmp_path / "tasks", ["alpha", "beta"])
        (folder / "beta" / "task.toml").write_text("[environment\n")

        response = _client.post("/start-benchmark", json=_start(contract, LocalDataset(path=str(folder))))

        assert response.status_code == 400
        assert "Task 'beta' is not valid: task.toml is not valid TOML" in response.json()["detail"]

    @pytest.mark.description("A missing folder is refused.")
    def test_a_missing_folder_is_refused(self, contract: AgentContractRequest, tmp_path: Path) -> None:
        response = _client.post("/start-benchmark", json=_start(contract, LocalDataset(path=str(tmp_path / "gone"))))

        assert response.status_code == 400
        assert "is not a folder" in response.json()["detail"]

    @pytest.mark.description("A hosted tracker refuses a folder.")
    def test_a_hosted_tracker_refuses_a_folder(
        self, contract: AgentContractRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(local_config, "resources", None)

        response = _client.post("/start-benchmark", json=_start(contract, LocalDataset(path=str(tmp_path))))

        assert response.status_code == 400
        assert "local Tracker" in response.json()["detail"]


@pytest.mark.description("What a dataset run does once it has started.")
class TestRunAfterStart:
    def _start(self, contract: AgentContractRequest) -> UUID:
        response = _client.post("/start-benchmark", json=_start(contract, _HOSTED, task_ids=["task-a", "task-b"]))
        assert response.status_code == 200, response.text
        return UUID(response.json()["benchmark_id"])

    @pytest.mark.description("Results are scored from the tasks rewards.")
    def test_results_are_scored_from_the_tasks_rewards(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        run_id = self._start(contract)
        task = database_session.exec(select(Task).where(Task.benchmark == run_id, Task.task_id == "task-a")).one()
        task.status = TaskStatus.FINISHED
        database_session.add(task)
        database_session.add(
            EvaluationResult(
                org_id=TEST_ORG_ID,
                task=task.id,
                instance_id=str(uuid4()),
                result={
                    "task_id": "task-a",
                    "rewards": {"reward": 1.0},
                    "verifier_result": {"rewards": {"reward": 1.0}},
                },
            )
        )
        database_session.commit()

        response = _client.get(
            "/retrieve-results", params=[("benchmark_id", str(run_id)), ("task_ids", "task-a"), ("task_ids", "task-b")]
        )

        assert response.status_code == 200, response.text
        assert response.json()["final_evaluation"]["final_score"] == 50.0

    @pytest.mark.description(
        "Rewards with several keys and none named reward finish with every mean and no headline score."
    )
    def test_rewards_with_several_keys_and_none_named_reward_finish_with_every_mean_and_no_headline_score(
        self, contract: AgentContractRequest, hosted_tasks: list[str], database_session: Session
    ) -> None:
        run_id = self._start(contract)
        for task_id, rewards in (
            ("task-a", {"accuracy": 1.0, "speed": 0.5}),
            ("task-b", {"accuracy": 0.0, "speed": 1.5}),
        ):
            task = database_session.exec(select(Task).where(Task.benchmark == run_id, Task.task_id == task_id)).one()
            task.status = TaskStatus.FINISHED
            database_session.add(task)
            database_session.add(
                EvaluationResult(
                    org_id=TEST_ORG_ID,
                    task=task.id,
                    instance_id=str(uuid4()),
                    result={"task_id": task_id, "rewards": rewards, "verifier_result": {"rewards": rewards}},
                )
            )
        database_session.commit()

        response = _client.get(
            "/retrieve-results", params=[("benchmark_id", str(run_id)), ("task_ids", "task-a"), ("task_ids", "task-b")]
        )

        assert response.status_code == 200, response.text
        evaluation = response.json()["final_evaluation"]
        assert evaluation["final_score"] is None
        assert evaluation["properties"]["metrics"] == [{"accuracy": 0.5, "speed": 1.0}]
        assert evaluation["properties"]["score_key"] is None

    @pytest.mark.description("A resume cannot send a dataset run to a benchmark service.")
    @pytest.mark.parametrize(
        "options",
        [{"json": {"service_headers": {"X-Key": "value"}}}, {"json": {"benchmark_url": "https://other.example"}}],
        ids=["headers", "url"],
    )
    def test_a_resume_cannot_send_a_dataset_run_to_a_benchmark_service(
        self, contract: AgentContractRequest, hosted_tasks: list[str], options: dict[str, Any]
    ) -> None:
        run_id = self._start(contract)

        response = _client.post(f"/retry-or-resume-benchmark/{run_id}", **options)

        assert response.status_code == 400
        assert "no benchmark service" in response.json()["detail"]

    @pytest.mark.description("Resuming keeps the version the run started on.")
    def test_resuming_keeps_the_version_the_run_started_on(
        self,
        contract: AgentContractRequest,
        hosted_tasks: list[str],
        database_session: Session,
        mock_launcher: MockLauncher,
    ) -> None:
        run_id = self._start(contract)
        started = mock_launcher.queued_calls[0]["execution_context_json"]["start_benchmark_request"]["dataset_ref"]
        _publish("swe-bench", "verified", ["task-b", "task-d"])

        def stop_run() -> None:
            run = database_session.get(Benchmark, run_id)
            assert run is not None
            run.status = BenchmarkStatus.STOPPED
            database_session.add(run)
            for task in database_session.exec(select(Task).where(Task.benchmark == run_id)):
                task.status = TaskStatus.STOPPED
                database_session.add(task)
            database_session.commit()

        stop_run()

        resumed = _client.post(f"/retry-or-resume-benchmark/{run_id}")

        assert resumed.status_code == 200, resumed.text
        queued = [call["execution_context_json"]["start_benchmark_request"] for call in mock_launcher.queued_calls]
        assert queued[1]["dataset_ref"] == started
        assert started["revision"] == 1

        stop_run()

        added = _client.post(f"/retry-or-resume-benchmark/{run_id}", json={"task_ids": ["task-d"]})

        assert added.status_code == 400
        assert "task-d" in added.json()["detail"]


@pytest.mark.description("The stored arguments of a run.")
class TestRunArguments:
    @pytest.mark.description("A run without a dataset stores no dataset ref.")
    def test_a_run_without_a_dataset_stores_no_dataset_ref(
        self, example_benchmark_object: Benchmark, database_session: Session
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        stored = database_session.connection().exec_driver_sql(
            "select arguments from benchmark where id = ?", (example_benchmark_object.id.hex,)
        )

        assert "dataset_ref" not in json.loads(stored.scalar_one())
