import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlmodel import Session

from tracker.database.models import Benchmark, ExecutorDispatch, ExecutorRelease
from tracker.domain import ExecutorDispatchKind
from tracker.executor import launcher


@pytest.fixture
def ecs_launcher_env(monkeypatch: pytest.MonkeyPatch, ecs_runner_env: None) -> None:
    monkeypatch.setattr(
        launcher, "_task_definition", lambda _dispatch: "arn:aws:ecs:us-east-1:123456789012:task-definition/runner:7"
    )


@pytest.mark.description(
    "After an ambiguous ECS error, `RunTask` is retried with the same client token and one task ARN is recorded."
)
@pytest.mark.asyncio
async def test_ecs_retries_ambiguous_error_with_same_token_and_records_one_arn(
    monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
) -> None:
    dispatch_id = uuid4()
    observed: list[dict[str, Any]] = []
    recorded: list[tuple[object, str]] = []

    def run_task(**kwargs: Any) -> dict[str, Any]:
        observed.append(kwargs)
        if len(observed) == 1:
            raise RuntimeError("ambiguous transport failure")
        return {"tasks": [{"taskArn": "arn:task:runner"}], "failures": []}

    monkeypatch.setattr(launcher.boto3, "client", lambda *_args, **_kwargs: SimpleNamespace(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", lambda key, arn: recorded.append((key, arn)))
    monkeypatch.setattr(launcher.asyncio, "sleep", AsyncMock())

    await launcher.launch_dispatch(SimpleNamespace(id=dispatch_id))

    assert len(observed) == 2
    assert observed[0] == observed[1]
    assert observed[0]["clientToken"] == str(dispatch_id)
    assert observed[0]["count"] == 1
    assert observed[0]["taskDefinition"].endswith("runner:7")
    assert observed[0]["overrides"]["containerOverrides"][0]["command"] == [
        "/app/.venv/bin/python",
        "-m",
        "tracker.executor.watchdog",
        "--dispatch-id",
        str(dispatch_id),
    ]
    assert recorded == [(dispatch_id, "arn:task:runner")]


@pytest.mark.description("A definite ECS launch failure records no ARN.")
@pytest.mark.asyncio
async def test_ecs_rejects_definitive_failure_without_recording_arn(
    monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
) -> None:
    run_task = Mock(return_value={"tasks": [], "failures": [{"reason": "RESOURCE:CPU"}]})
    record = Mock()
    monkeypatch.setattr(launcher.boto3, "client", lambda *_args, **_kwargs: SimpleNamespace(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", record)

    with pytest.raises(launcher.DefinitiveLaunchFailure):
        await launcher.launch_dispatch(SimpleNamespace(id=uuid4()))

    run_task.assert_called_once()
    record.assert_not_called()


@pytest.mark.description("An accepted ECS launch survives a failure to save its ARN.")
@pytest.mark.asyncio
async def test_ecs_accepted_launch_survives_arn_write_failure(
    monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
) -> None:
    run_task = Mock(return_value={"tasks": [{"taskArn": "arn:task:accepted"}], "failures": []})
    record = Mock(side_effect=RuntimeError("database unavailable"))
    log_exception = Mock()
    monkeypatch.setattr(launcher.boto3, "client", lambda *_args, **_kwargs: SimpleNamespace(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", record)
    monkeypatch.setattr(launcher.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(launcher.logger, "exception", log_exception)

    await launcher.launch_dispatch(SimpleNamespace(id=uuid4()))

    run_task.assert_called_once()
    assert record.call_count == 3
    log_exception.assert_called_once()
    assert "arn:task:accepted" in log_exception.call_args.args


@pytest.mark.description(
    "The local launcher starts one detached executor process with the Tracker environment off the event loop thread."
)
@pytest.mark.asyncio
async def test_local_launcher_spawns_one_detached_watchdog(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("DB_PASSWORD", "must-not-enter-executor")
    monkeypatch.setenv("DATABASE_URL", "postgresql://must-not-enter-executor")
    monkeypatch.setenv("EXECUTOR_CONTROL_URL", "http://control.test")
    spawn_threads: list[int] = []
    popen = Mock(side_effect=lambda *_args, **_kwargs: spawn_threads.append(threading.get_ident()))
    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    dispatch_id = uuid4()

    await launcher.launch_dispatch(SimpleNamespace(id=dispatch_id))

    popen.assert_called_once()
    command = popen.call_args.args[0]
    options = popen.call_args.kwargs
    assert command == [launcher.sys.executable, "-m", "tracker.executor.watchdog", "--dispatch-id", str(dispatch_id)]
    assert options["start_new_session"] is True
    assert options["env"]["EXECUTOR_CONTROL_URL"] == "http://control.test"
    assert "DB_PASSWORD" not in options["env"]
    assert "DATABASE_URL" not in options["env"]
    assert spawn_threads != [threading.get_ident()]


@pytest.mark.description("The launched ECS task ARN is stored in `ExecutorDispatch.ecs_task_arn`.")
@pytest.mark.asyncio
async def test_ecs_task_arn_persists_on_dispatch_row(
    monkeypatch: pytest.MonkeyPatch,
    ecs_launcher_env: None,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = ExecutorRelease(
        id="arn-test-release",
        artifact_uri=f"123456789012.dkr.ecr.us-east-1.amazonaws.com/valkyrie-executor@sha256:{'a' * 64}",
        artifact_digest="a" * 64,
        protocol_version="3",
        task_definition_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/runner:8",
    )
    benchmark = example_benchmark_object
    dispatch = ExecutorDispatch(
        benchmark_id=benchmark.id,
        kind=ExecutorDispatchKind.START,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
    )
    database_session.add_all([release, benchmark, dispatch])
    database_session.commit()
    monkeypatch.setattr(launcher, "engine", database_session.get_bind())
    monkeypatch.setattr(
        launcher.boto3,
        "client",
        lambda *_args, **_kwargs: SimpleNamespace(
            run_task=lambda **_params: {"tasks": [{"taskArn": "arn:task:persisted"}], "failures": []}
        ),
    )

    await launcher.launch_dispatch(dispatch)

    with Session(database_session.get_bind()) as session:
        stored = session.get(ExecutorDispatch, dispatch.id)
        assert stored is not None
        assert stored.ecs_task_arn == "arn:task:persisted"


@pytest.mark.description("A launch needs the release's task definition.")
def test_launch_requires_the_release_task_definition(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
) -> None:
    monkeypatch.setattr(launcher, "engine", database_session.get_bind())
    source = ExecutorRelease(
        id="source-release",
        artifact_uri="source:///checkout",
        artifact_digest="a" * 64,
        protocol_version="3",
    )
    image = ExecutorRelease(
        id="image-release",
        artifact_uri=f"123456789012.dkr.ecr.us-east-1.amazonaws.com/valkyrie-executor@sha256:{'b' * 64}",
        artifact_digest="b" * 64,
        protocol_version="3",
        task_definition_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/runner:8",
    )
    database_session.add_all([source, image])
    database_session.commit()

    def dispatch(release: ExecutorRelease) -> ExecutorDispatch:
        return ExecutorDispatch(
            benchmark_id=uuid4(),
            kind=ExecutorDispatchKind.START,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
        )

    assert launcher._task_definition(dispatch(image)).endswith("runner:8")
    with pytest.raises(launcher.DefinitiveLaunchFailure, match="has no task definition"):
        launcher._task_definition(dispatch(source))
