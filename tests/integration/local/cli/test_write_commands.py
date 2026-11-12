import pytest

import json
import base64
import hashlib
import io
import zipfile
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import HealthCheckResponse, VerifyTaskIdsResponse, VersionResponse
from services.tracker import main as tracker_main
from tracker.database.models import Org, OrgProviderSecret
from tracker.runtime.storage import StoredObject
from pathlib import Path


from click.testing import CliRunner
from sqlmodel import Session, select
from tracker.database.models import Benchmark, Task
from tracker.domain import BenchmarkStatus, TaskStatus


from valkyrie.cli.main import cli


@pytest.mark.description("A project can be created, updated, and deleted.")
def test_project_create_update_and_delete_round_trip(cli_runner: CliRunner) -> None:
    created = cli_runner.invoke(
        cli,
        [
            "project",
            "create",
            "local-project",
            "--name",
            "Local Project",
            "--spend-type",
            "development",
            "--subcategory",
            "new_benchmark_generation",
            "--tag",
            "owner=research",
            "--tag",
            "reporting=q4",
        ],
    )
    assert created.exit_code == 0, created.output

    updated = cli_runner.invoke(cli, ["project", "update", "local-project", "--name", "Renamed Project"])
    assert updated.exit_code == 0, updated.output

    listed = cli_runner.invoke(cli, ["project", "list", "--format", "json"])
    assert listed.exit_code == 0, listed.output

    (project,) = json.loads(listed.output)["projects"]
    assert project["name"] == "Renamed Project"
    assert project["spend_type"] == "development"
    assert project["subcategory"] == "new_benchmark_generation"
    assert project["extra_tags"] == {"owner": "research", "reporting": "q4"}

    replaced = cli_runner.invoke(cli, ["project", "update", "local-project", "--tag", "team=platform"])
    assert replaced.exit_code == 0, replaced.output

    after_replacement = cli_runner.invoke(cli, ["project", "list", "--format", "json"])
    assert after_replacement.exit_code == 0, after_replacement.output
    (project,) = json.loads(after_replacement.output)["projects"]
    assert project["name"] == "Renamed Project"
    assert project["extra_tags"] == {"team": "platform"}

    deleted = cli_runner.invoke(cli, ["project", "delete", "local-project"], input="y\n")
    assert deleted.exit_code == 0, deleted.output

    after_delete = cli_runner.invoke(cli, ["project", "list", "--format", "json"])
    assert after_delete.exit_code == 0, after_delete.output
    assert json.loads(after_delete.output)["projects"] == []


@pytest.mark.description("An agent library entry can be saved and read back.")
def test_agent_library_round_trip(
    cli_runner: CliRunner,
    agent_library: dict[str, bytes],
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "contract.yaml").write_text("name: demo\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n")
    (source / "run.py").write_text("first version")
    (source / "run.py").chmod(0o755)
    output = tmp_path / "download"

    result = cli_runner.invoke(cli, ["agent", "push", str(source)])

    assert result.exit_code == 0, result.output
    assert "Agent 'demo' pushed successfully!" in result.output
    assert "agents/demo.zip" in agent_library
    result = cli_runner.invoke(cli, ["agent", "remove", "demo"], input="y\n")
    assert result.exit_code == 0, result.output

    result = cli_runner.invoke(cli, ["agent", "push", str(source), "--name", "alias"])

    assert result.exit_code == 0, result.output
    assert "agents/alias.zip" in agent_library

    result = cli_runner.invoke(cli, ["agent", "list"])

    assert result.exit_code == 0, result.output
    assert "alias" in result.output

    result = cli_runner.invoke(cli, ["agent", "list", "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"agents": [{"name": "alias", "last_modified": None}]}

    result = cli_runner.invoke(cli, ["agent", "download", "alias", "--output-dir", str(output)])

    assert result.exit_code == 0, result.output
    assert (output / "alias" / "run.py").read_text() == "first version"
    assert (output / "alias" / "run.py").stat().st_mode & 0o111

    (source / "run.py").write_text("replacement")
    result = cli_runner.invoke(cli, ["agent", "push", str(source), "--name", "alias"])

    assert result.exit_code == 0, result.output

    result = cli_runner.invoke(cli, ["agent", "download", "alias", "--output-dir", str(output)])

    assert result.exit_code == 1
    assert (output / "alias" / "run.py").read_text() == "first version"

    result = cli_runner.invoke(cli, ["agent", "download", "alias", "--output-dir", str(output), "--overwrite"])

    assert result.exit_code == 0, result.output
    assert (output / "alias" / "run.py").read_text() == "replacement"

    cancelled = cli_runner.invoke(cli, ["agent", "remove", "alias"], input="n\n")

    assert cancelled.exit_code == 0
    assert "agents/alias.zip" in agent_library

    result = cli_runner.invoke(cli, ["agent", "remove", "alias"], input="y\n")

    assert result.exit_code == 0, result.output
    assert not agent_library

    missing = cli_runner.invoke(cli, ["agent", "remove", "alias"], input="y\n")

    assert missing.exit_code == 1
    assert "404" in missing.output


@pytest.mark.description(
    (
        "A selective stop must preserve the run and every unselected task.\n"
        "\n"
        "Test cases:\n"
        "- The CLI sends selected task IDs through the FastAPI request body.\n"
        "- The tracker stops the selected pending task without stopping the run.\n"
        "- An unselected active task keeps its current state."
    )
)
def test_cli_stops_only_selected_pending_tasks(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    running, _finished = seeded_runs

    result = cli_runner.invoke(
        cli,
        ["run", "stop", str(running.id), "--task-ids", "pending"],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    assert "Selected tasks are stopping" in result.output

    database_session.expire_all()
    task_rows = database_session.exec(select(Task).where(Task.benchmark == running.id)).all()
    task_statuses = {task.task_id: task.status for task in task_rows}
    assert task_statuses["pending"] == TaskStatus.STOPPED
    assert task_statuses["active"] == TaskStatus.IN_PROGRESS

    stored_run = database_session.get(Benchmark, running.id)
    assert stored_run is not None
    assert stored_run.status == BenchmarkStatus.IN_PROGRESS


@pytest.mark.description(
    (
        "The public CLI must persist live concurrency through the production HTTP path.\n"
        "\n"
        "Test cases:\n"
        "- A live update travels through Click, the SDK, and the FastAPI route.\n"
        "- Repeating the same update is idempotent.\n"
        "- A subsequent CLI read and direct database read return the new limit.\n"
        "- Updating concurrency preserves the rest of the stored benchmark arguments."
    )
)
def test_cli_updates_active_run_concurrency_through_tracker(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    running, _finished = seeded_runs
    original_arguments = running.arguments

    for _ in range(2):
        result = cli_runner.invoke(cli, ["run", "update", str(running.id), "--concurrency", "4"])

        assert result.exit_code == 0, result.output
        assert result.output == "✓ Run concurrency updated to 4.\n"

    fetch_result = cli_runner.invoke(cli, ["run", "fetch", str(running.id), "--format", "json"])

    assert fetch_result.exit_code == 0, fetch_result.output
    assert json.loads(fetch_result.output)["metadata"]["benchmark_arguments"]["concurrency"] == 4

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, running.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments.model_copy(update={"concurrency": 4})


@pytest.mark.description("A terminal run must reject updates without changing its stored arguments.")
def test_cli_rejects_concurrency_update_for_finished_run(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    database_session: Session,
) -> None:
    _running, finished = seeded_runs
    original_arguments = finished.arguments

    result = cli_runner.invoke(cli, ["run", "update", str(finished.id), "--concurrency", "4"])

    assert result.exit_code == 1
    assert "Valkyrie API request failed (409)" in result.output
    assert "currently in the FINISHED state." in result.output

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, finished.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments


@pytest.mark.description(
    (
        "Result export must preserve tracker data without persisting private agent configuration.\n"
        "\n"
        "Test cases:\n"
        "- The CLI accepts the FastAPI final-view response and writes its evaluation and final score.\n"
        "- Agent secrets and runtime kwargs are absent from the saved JSON."
    )
)
def test_cli_exports_tracker_results_without_private_contract_values(
    cli_runner: CliRunner,
    seeded_runs: tuple[Benchmark, Benchmark],
    tmp_path: Path,
) -> None:
    _running, finished = seeded_runs
    output_path = tmp_path / "results.json"

    result = cli_runner.invoke(cli, ["run", "results", str(finished.id), "--path", str(output_path)])

    assert result.exit_code == 0, result.output
    saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved_payload["benchmark_id"] == str(finished.id)
    assert saved_payload["final_evaluation"]["final_score"] == 0.75
    assert saved_payload["evaluation_results"]["complete"]["score"] == 1
    assert "secrets" not in saved_payload["benchmark_arguments"]["contract"]
    assert "kwargs" not in saved_payload["benchmark_arguments"]["contract"]
    assert "finished-secret-must-not-leak" not in output_path.read_text(encoding="utf-8")


@pytest.mark.description("The SDK updates a run's saved concurrency.")
async def test_sdk_updates_persisted_concurrency(
    seeded_runs: tuple[Benchmark, Benchmark], database_session: Session, local_tracker_app
) -> None:
    import httpx
    from valkyrie.sdk import ValkyrieClient

    running, _ = seeded_runs
    async with ValkyrieClient(transport=httpx.ASGITransport(app=local_tracker_app)) as client:
        result = await client.runs.update_concurrency(running.id, concurrency=3)
    database_session.refresh(running)
    assert running.arguments.concurrency == result.concurrency == 3


@pytest.mark.description("`valkyrie run output` saves a task's artifacts from the Tracker under the `-o` folder.")
def test_cli_downloads_run_artifacts_through_tracker(
    cli_runner: CliRunner, seeded_runs: tuple[Benchmark, Benchmark], monkeypatch, tmp_path: Path
) -> None:
    import httpx
    from unittest.mock import AsyncMock
    from tracker.aws.clients import AWSClientProvider

    running, _ = seeded_runs
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.list_objects_v2.return_value = {"Contents": [{"Key": f"benchmarks/{running.id}/task/result.txt", "Size": 6}]}
    client.head_object.return_value = {"ContentLength": 6}
    client.generate_presigned_url.return_value = "https://download.test/artifact"

    monkeypatch.setattr(AWSClientProvider, "s3_client", lambda _: client)

    tracker = httpx.ASGITransport.handle_async_request

    async def download(transport: httpx.ASGITransport, request: httpx.Request) -> httpx.Response:
        if request.url.host != "download.test":
            return await tracker(transport, request)
        assert "x-api-key" not in request.headers
        return httpx.Response(200, content=b"result")

    monkeypatch.setattr(httpx.ASGITransport, "handle_async_request", download)
    listing = cli_runner.invoke(cli, ["run", "artifacts", str(running.id), "--format", "json"])
    assert listing.exit_code == 0, listing.output
    assert json.loads(listing.output)["artifacts"][0]["path"] == "task/result.txt"
    destination = tmp_path / "download"
    result = cli_runner.invoke(cli, ["run", "output", str(running.id), "task", "-o", str(destination)])
    assert result.exit_code == 0, result.output
    assert (destination / "task/result.txt").read_text() == "result"


@pytest.mark.description("The CLI starts and retries a run with the same saved agent version.")
@pytest.mark.parametrize("explicit_digest", [False, True])
def test_cli_selects_and_retries_exact_agent(
    explicit_digest: bool,
    cli_runner: CliRunner,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = database_session.exec(select(Org)).one()
    database_session.add(OrgProviderSecret(org_id=org.id, provider="daytona", secret_name="provider-secret"))
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    monkeypatch.delenv("EXECUTOR_PAYLOAD_KMS_KEY_ID", raising=False)
    monkeypatch.setattr(tracker_main, "launch_dispatch", AsyncMock())
    monkeypatch.setattr(
        BenchmarkServiceClient, "health_check", AsyncMock(return_value=HealthCheckResponse(status="ok"))
    )
    monkeypatch.setattr(
        BenchmarkServiceClient, "version", AsyncMock(return_value=VersionResponse(framework_version="test"))
    )
    monkeypatch.setattr(
        BenchmarkServiceClient, "verify_task_ids", AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["one"]))
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "exact/contract.yaml",
            "name: exact\ninstall_cmd: echo setup\nrun_cmd: echo {problem_statement_path}\nkwargs:\n  budget:\n    type: int\n    required: false\n    default: 1\n",
        )
    content = buffer.getvalue()
    digest = hashlib.sha256(content).hexdigest()
    objects = {
        f"agent-releases/exact/A1/{digest}.zip": content,
        "agent-releases/exact/A1/release.json": json.dumps(
            {"schema_version": 1, "agent": "exact", "archive": "exact.zip", "sha256": digest}
        ).encode(),
    }

    async def get_bytes(_store: object, key: str) -> bytes:
        return objects[key]

    async def put_bytes(_store: object, key: str, content: bytes) -> None:
        objects[key] = content

    async def exists(_store: object, key: str) -> bool:
        return key in objects

    async def list_objects(_store: object, _prefix: str) -> AsyncIterator[StoredObject]:
        for item in tuple[StoredObject, ...]():
            yield item

    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.get_bytes", get_bytes)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.put_bytes", put_bytes)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", exists)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.list_objects", list_objects)
    project = cli_runner.invoke(
        cli,
        [
            "project",
            "create",
            "exact-project",
            "--name",
            "Exact",
            "--spend-type",
            "development",
            "--subcategory",
            "new_benchmark_generation",
        ],
    )
    assert project.exit_code == 0, project.output

    started = cli_runner.invoke(
        cli,
        [
            "run",
            "start",
            "--agent",
            "exact",
            "--agent-release",
            "A1",
            *(["--agent-sha256", digest] if explicit_digest else []),
            "--benchmark",
            "swebench",
            "--project",
            "exact-project",
            "--kwarg",
            "budget",
            "5",
        ],
    )
    assert started.exit_code == 0, started.output
    benchmark = database_session.exec(select(Benchmark)).one()
    assert benchmark.arguments.contract.agent_release is not None
    assert benchmark.arguments.contract.agent_release.reference == "A1"
    assert benchmark.arguments.contract.agent_release.archive_sha256 == digest
    assert benchmark.arguments.contract.kwargs["budget"] == "5"
    benchmark.status = BenchmarkStatus.STOPPED
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.status = TaskStatus.STOPPED
    database_session.add_all([benchmark, task])
    database_session.commit()

    receipt = objects["agent-releases/exact/A1/release.json"]
    objects["agent-releases/exact/A1/release.json"] = receipt.replace(digest.encode(), b"0" * 64)
    retried = cli_runner.invoke(cli, ["run", "retry", str(benchmark.id)])
    assert retried.exit_code == 0, retried.output
    inspected = cli_runner.invoke(cli, ["run", "fetch", str(benchmark.id), "--format", "json"])
    assert inspected.exit_code == 0, inspected.output
    snapshot = json.loads(inspected.output)["metadata"]["benchmark_arguments"]
    assert snapshot["contract"]["agent_release"]["reference"] == "A1"
    assert [attempt["agent_release"]["reference"] for attempt in snapshot["agent_attempts"]] == ["A1", "A1"]
    assert "effective_configuration" not in inspected.output
    assert "budget" not in inspected.output

    database_session.expire_all()
    benchmark = database_session.get(Benchmark, benchmark.id)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.status = TaskStatus.STOPPED
    database_session.add_all([benchmark, task])
    database_session.commit()
    objects[f"agent-releases/exact/A2/{digest}.zip"] = content
    objects["agent-releases/exact/A2/release.json"] = receipt

    updated = cli_runner.invoke(
        cli,
        [
            "run",
            "retry",
            str(benchmark.id),
            "--agent-release",
            "A2",
            *(["--agent-sha256", digest] if explicit_digest else []),
            "--update-agent",
            "--from-scratch",
        ],
    )

    assert updated.exit_code == 0, updated.output
    database_session.expire_all()
    benchmark = database_session.get(Benchmark, benchmark.id)
    assert benchmark is not None and benchmark.arguments.contract.agent_release is not None
    assert benchmark.arguments.contract.agent_release.reference == "A2"
    assert benchmark.arguments.contract.agent_release.archive_sha256 == digest
    assert benchmark.arguments.contract.kwargs["budget"] == "1"
