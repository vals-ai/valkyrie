from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tracker.aws.s3 import S3ObjectStore
from tracker.runtime.storage import StoredObject
from tracker import auth
from tracker.auth import get_current_org
from tracker.local.resources import LocalResources
from main import app
from tests.factories import make_error_result, make_evaluation_result, make_task
from tracker.database.models import (
    Benchmark,
    Org,
    TaskBreakdown,
)
from tracker.types import LocalBenchmarkArguments
from tracker.domain import AgentCausedExitReason, FailureCategory, TaskStatus

_client = TestClient(app)


@pytest.mark.description(
    (
        "Task detail must expose the latest terminal result without leaking another organization.\n"
        "\n"
        "Test cases:\n"
        "- Finished, error, and pending tasks return status-appropriate result fields.\n"
        "- The newest terminal result wins when a newer scheduled-retry row exists.\n"
        "- A benchmark from another organization returns 404."
    )
)
def test_single_task_returns_latest_terminal_result_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()

    finished_task = make_task(
        benchmark,
        "finished-task",
        status=TaskStatus.FINISHED,
        finished_at=now,
    )
    task_breakdown = TaskBreakdown(
        sandbox_build_duration=1.0,
        agent_run_duration=2.0,
        evaluation_run_duration=3.0,
        sandbox_run_duration=4.0,
        accounting_id="accounting-session",
        base_generation_allowance_seconds=300.0,
        cumulative_time_credit_cap_seconds=120.0,
        external_service_overhead_seconds=45.0,
        external_service_credit_applied_seconds=45.0,
        external_service_credit_revision=2,
        generation_elapsed_seconds=20.0,
    )
    finished_task.task_breakdown = task_breakdown.id
    database_session.add(task_breakdown)
    error_task = make_task(
        benchmark,
        "error-task",
        status=TaskStatus.ERROR,
        finished_at=now,
    )
    pending_task = make_task(benchmark, "pending-task")
    database_session.add_all([finished_task, error_task, pending_task])
    database_session.flush()
    database_session.add_all(
        [
            make_evaluation_result(
                finished_task,
                "old-attempt",
                {"score": 0.0},
                now - timedelta(minutes=1),
            ),
            make_evaluation_result(
                finished_task,
                "new-attempt",
                {"score": 1.0},
                now,
                exit_reason=AgentCausedExitReason.TIMEOUT,
            ),
            make_error_result(error_task, "old failure", now - timedelta(minutes=1)),
            make_error_result(
                error_task,
                "latest failure",
                now,
                producer="tracker",
                operation="process_task",
                error_type="AgentRunFailedError",
                category=FailureCategory.AGENT,
            ),
            make_error_result(
                error_task,
                "scheduled retry",
                now + timedelta(minutes=1),
                producer="sandbox_provider",
                operation="setup",
                error_type="SandboxSetupError",
                retry_scheduled=True,
                failed_attempt_number=1,
            ),
        ]
    )

    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(
        org_id=other_org.id,
        name=benchmark.name,
        arguments=benchmark.arguments,
    )
    database_session.add_all([other_org, other_benchmark])
    database_session.commit()

    finished_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}")
    error_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}")
    pending_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{pending_task.task_id}")
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}/tasks/unknown")

    assert finished_response.status_code == 200
    assert finished_response.json()["evaluation_result"] == {"score": 1.0}
    assert finished_response.json()["agent_caused_exit_reason"] == "TIMEOUT"
    assert finished_response.json()["error_message"] is None
    assert finished_response.json()["task_breakdown"] == {
        "sandbox_build_duration": 1.0,
        "agent_run_duration": 2.0,
        "evaluation_run_duration": 3.0,
        "sandbox_run_duration": 4.0,
        "accounting_id": "accounting-session",
        "base_generation_allowance_seconds": 300.0,
        "cumulative_time_credit_cap_seconds": 120.0,
        "external_service_overhead_seconds": 45.0,
        "external_service_credit_applied_seconds": 45.0,
        "external_service_credit_revision": 2,
        "generation_elapsed_seconds": 20.0,
    }
    assert error_response.status_code == 200
    assert error_response.json()["error_message"] == "latest failure"
    assert error_response.json()["failure_category"] == "agent"
    assert error_response.json()["evaluation_result"] is None
    assert error_response.json()["task_breakdown"] is None
    assert pending_response.status_code == 200
    assert pending_response.json()["error_message"] is None
    assert pending_response.json()["evaluation_result"] is None
    assert pending_response.json()["task_breakdown"] is None
    assert other_org_response.status_code == 404


@pytest.mark.description(
    (
        "Artifact detail must return useful links without signing a missing output archive.\n"
        "\n"
        "Test cases:\n"
        "- Existing output receives a five-minute presigned URL and CloudWatch link.\n"
        "- Missing output returns no S3 URL and does not call the signer again.\n"
        "- Renamed task streams link to the run instead of guessing the historical encoding."
    )
)
@pytest.mark.parametrize("task_id", ["task-with-output", "task:with-output", "task*with-output", "task%with-output"])
def test_task_artifacts_only_presign_existing_output(
    task_id: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, task_id)
    database_session.add_all([benchmark, task])
    database_session.commit()

    object_exists = AsyncMock(return_value=True)
    create_presigned_url = AsyncMock(return_value="https://example.test/presigned")

    async def no_objects(_store: S3ObjectStore, _prefix: str) -> AsyncIterator[StoredObject]:
        return
        yield

    monkeypatch.setattr(S3ObjectStore, "list_objects", no_objects)
    monkeypatch.setattr(S3ObjectStore, "exists", object_exists)
    monkeypatch.setattr(S3ObjectStore, "temporary_download_url", create_presigned_url)

    found_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts",
    )
    object_exists.return_value = False
    missing_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts",
    )

    expected_key = f"benchmarks/{benchmark.id}/{task.task_id}/agent_output.tar.gz"
    assert found_response.status_code == 200
    assert found_response.json()["agent_output_url"] == "https://example.test/presigned"
    assert found_response.json()["agent_output_expires_in"] == 300
    cloudwatch_url = found_response.json()["cloudwatch_url"]
    assert "logsV2:log-groups/log-group/" in cloudwatch_url
    assert str(benchmark.id) in cloudwatch_url
    assert ("/log-events/" in cloudwatch_url) is (task_id == "task-with-output")
    object_exists.assert_awaited_with(expected_key)
    create_presigned_url.assert_awaited_once_with(
        expected_key,
        expires_in=300,
    )
    assert missing_response.status_code == 200
    assert missing_response.json()["agent_output_url"] is None
    assert missing_response.json()["agent_output_expires_in"] is None
    assert create_presigned_url.await_count == 1


@pytest.mark.description("Task artifacts link the newest step output when the task has none of its own.")
@pytest.mark.parametrize(
    ("status", "root_present"),
    [
        (TaskStatus.FINISHED, False),
        (TaskStatus.ERROR, False),
        (TaskStatus.IN_PROGRESS, False),
        (TaskStatus.FINISHED, True),
    ],
)
def test_task_artifacts_link_the_newest_steps_output_when_the_task_has_none_of_its_own(
    status: TaskStatus,
    root_present: bool,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "stepped-task", status=status)
    database_session.add_all([benchmark, task])
    database_session.commit()
    prefix = f"benchmarks/{benchmark.id}/{task.task_id}/"
    now = datetime.now(ZoneInfo("UTC"))
    stored = [
        StoredObject(f"{prefix}steps/beta/agent_output.tar.gz", now - timedelta(minutes=1), size=1),
        StoredObject(f"{prefix}steps/alpha/agent_output.tar.gz", now - timedelta(minutes=3), size=1),
        StoredObject(f"{prefix}steps/epsilon/agent_output.tar.gz", None, size=1),
        StoredObject(f"{prefix}steps/beta/artifacts/agent_output.tar.gz", now, size=1),
        StoredObject(f"{prefix}steps/delta/agent_output.tar.gz.old", now, size=1),
        StoredObject(f"{prefix}steps/gamma/agent_output.tar.gz", now - timedelta(minutes=2), size=1),
    ]

    async def exists(_store: S3ObjectStore, _key: str) -> bool:
        return root_present

    async def list_objects(_store: S3ObjectStore, listed: str) -> AsyncIterator[StoredObject]:
        assert listed == f"{prefix}steps/"
        for item in stored:
            yield item

    create_presigned_url = AsyncMock(return_value="https://example.test/presigned")
    monkeypatch.setattr(S3ObjectStore, "exists", exists)
    monkeypatch.setattr(S3ObjectStore, "list_objects", list_objects)
    monkeypatch.setattr(S3ObjectStore, "temporary_download_url", create_presigned_url)

    response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")

    expected = f"{prefix}agent_output.tar.gz" if root_present else f"{prefix}steps/beta/agent_output.tar.gz"
    assert response.status_code == 200
    assert response.json()["agent_output_url"] == "https://example.test/presigned"
    create_presigned_url.assert_awaited_once_with(expected, expires_in=300)


@pytest.mark.description("Run artifacts are scoped to the run, and storage errors map to HTTP statuses.")
@pytest.mark.parametrize("task_id", ["task", "task:one"])
def test_run_artifacts_are_scoped_and_storage_errors_are_mapped(
    task_id: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from botocore.exceptions import ClientError
    from tracker.aws.clients import AWSClientProvider

    benchmark = example_benchmark_object
    other_org = Org(id=uuid4(), name="other-artifacts")
    other = Benchmark(org_id=other_org.id, name="other", arguments=benchmark.arguments)
    database_session.add_all([benchmark, other_org, other])
    database_session.commit()
    root = f"benchmarks/{benchmark.id}/"
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": root + f"{task_id}/result.json", "Size": 2},
            {"Key": root + f"{task_id}-other/file", "Size": 1},
        ],
        "NextContinuationToken": "next",
    }
    client.head_object.return_value = {"ContentLength": 2}
    client.generate_presigned_url.return_value = "https://download.test/file"
    monkeypatch.setattr(AWSClientProvider, "s3_client", lambda _: client)
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts",
        params={"prefix": task_id, "cursor": "previous", "limit": 2},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "artifacts": [{"path": f"{task_id}/result.json", "size": 2, "last_modified": None}],
        "next_cursor": "next",
    }
    client.list_objects_v2.assert_awaited_once_with(
        Bucket="test-bucket",
        Prefix=root + task_id,
        MaxKeys=2,
        ExpectedBucketOwner="123456789012",
        ContinuationToken="previous",
    )
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts/download-url",
        params={"path": f"{task_id}/result.json"},
    )
    assert response.status_code == 200
    assert response.json()["download_url"] == "https://download.test/file"
    client.generate_presigned_url.assert_awaited_once_with(
        "get_object",
        Params={"Bucket": "test-bucket", "Key": root + f"{task_id}/result.json"},
        ExpiresIn=300,
    )
    for endpoint, params in (("artifacts", {}), ("artifacts/download-url", {"path": "file"})):
        assert _client.get(f"/benchmarks/{other.id}/{endpoint}", params=params).status_code == 404
    for path in ("../other", "/outside", "task/../file", "a\\b"):
        assert (
            _client.get(f"/benchmarks/{benchmark.id}/artifacts/download-url", params={"path": path}).status_code == 400
        )
    for code, status in (("NoSuchKey", 404), ("AccessDenied", 403), ("InternalError", 502)):
        client.head_object.side_effect = ClientError({"Error": {"Code": code}}, "HeadObject")
        assert (
            _client.get(f"/benchmarks/{benchmark.id}/artifacts/download-url", params={"path": "file"}).status_code
            == status
        )


@pytest.mark.description(
    (
        "Serve local output over HTTP while enforcing organization and path boundaries.\n"
        "\n"
        "Test cases:\n"
        "- Task and run download links return the existing artifact bytes.\n"
        "- Traversal and symlink escapes fail without exposing files.\n"
        "- Other organizations, unauthenticated requests, and missing files cannot download."
    )
)
def test_local_task_artifacts_use_existing_files(
    database_session: Session, example_benchmark_object: Benchmark, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    benchmark = example_benchmark_object
    benchmark.arguments = LocalBenchmarkArguments.model_validate(
        {
            **benchmark.arguments.model_dump(),
            "environment": "local",
            "properties": LocalResources(data_root=tmp_path),
            "sandbox_provider": "docker",
        }
    )
    task = make_task(benchmark, "local task:one")
    database_session.add_all([benchmark, task])
    database_session.commit()
    root = tmp_path / "orgs" / str(benchmark.org_id)
    output = root / "objects" / "benchmarks" / str(benchmark.id) / task.task_id / "agent_output.tar.gz"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"archive")
    endpoint = f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts"
    response = _client.get(endpoint)
    assert response.status_code == 200, response.text
    download_url = response.json()["agent_output_url"]
    assert download_url.startswith("http://testserver/")
    assert _client.get(download_url).content == b"archive"
    path = f"{task.task_id}/agent_output.tar.gz"
    artifact_endpoint = f"/benchmarks/{benchmark.id}/artifacts/download-url"
    artifact_link = _client.get(artifact_endpoint, params={"path": path})
    assert artifact_link.status_code == 200, artifact_link.text
    assert artifact_link.json()["expires_in"] == 0
    assert _client.get(artifact_link.json()["download_url"]).content == b"archive"

    for invalid in ("../outside", "/outside", "task/../file", "a\\b"):
        assert _client.get(artifact_endpoint, params={"path": invalid, "download": "true"}).status_code == 400
    outside = tmp_path / "outside"
    outside.write_bytes(b"private")
    (output.parent / "escape").symlink_to(outside)
    assert (
        _client.get(artifact_endpoint, params={"path": f"{task.task_id}/escape", "download": "true"}).status_code == 400
    )

    with monkeypatch.context() as scoped:
        scoped.setitem(app.dependency_overrides, get_current_org, lambda: Org(id=uuid4(), name="other"))
        assert _client.get(download_url).status_code == 404
    with monkeypatch.context() as scoped:
        scoped.delitem(app.dependency_overrides, get_current_org)
        scoped.setattr(auth, "AUTH_REQUIRED", True)
        assert _client.get(download_url).status_code == 401
    assert response.json()["agent_output_expires_in"] == 0
    assert response.json()["cloudwatch_url"].startswith("http://testserver/")
    assert _client.get(response.json()["cloudwatch_url"]).status_code == 200
    output.unlink()
    assert _client.get(endpoint).json()["agent_output_url"] is None
    assert _client.get(download_url).status_code == 404
    step_output = output.parent / "steps" / "v1.2" / "agent_output.tar.gz"
    step_output.parent.mkdir(parents=True)
    step_output.write_bytes(b"step archive")
    assert _client.get(_client.get(endpoint).json()["agent_output_url"]).content == b"step archive"
