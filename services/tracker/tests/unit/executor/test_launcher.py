import asyncio
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock, call
from uuid import uuid4

import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError
from sqlmodel import Session

from tracker.aws import clients as aws_clients
from tracker.database.models import Benchmark, ExecutorDispatch, ExecutorRelease
from tracker.domain import ExecutorDispatchKind
from tracker.executor import launcher


@pytest.fixture(autouse=True)
def isolated_ecs_client() -> Iterator[None]:
    getattr(aws_clients, "_ecs_client").cache_clear()
    yield
    getattr(aws_clients, "_ecs_client").cache_clear()


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

    monkeypatch.setattr(aws_clients.boto3, "client", lambda *_args, **_kwargs: Mock(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", lambda key, arn: recorded.append((key, arn)))
    monkeypatch.setattr(launcher.asyncio, "sleep", AsyncMock())

    await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=dispatch_id))

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
    monkeypatch.setattr(aws_clients.boto3, "client", lambda *_args, **_kwargs: Mock(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", record)

    with pytest.raises(launcher.DefinitiveLaunchFailure, match="RESOURCE:CPU"):
        await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=uuid4()))

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
    monkeypatch.setattr(aws_clients.boto3, "client", lambda *_args, **_kwargs: Mock(run_task=run_task))
    monkeypatch.setattr(launcher, "_record_task_arn", record)
    monkeypatch.setattr(launcher.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(launcher.logger, "exception", log_exception)

    await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=uuid4()))

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

    await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=dispatch_id))

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
        aws_clients.boto3,
        "client",
        lambda *_args, **_kwargs: Mock(
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


@pytest.mark.description("ECS launch retries preserve dispatch identity and stop within the acknowledgement budget.")
class TestECSLaunchRetries:
    @pytest.mark.description("Throttled launches back off with jitter and record one task using the same client token.")
    @pytest.mark.parametrize(
        "message",
        [
            "Rate exceeded",
            "Account has been throttled on ec2:DescribeSecurityGroups. (ErrorCode: RequestLimitExceeded)",
        ],
    )
    async def test_throttled_launch_succeeds(
        self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None, message: str
    ) -> None:
        error = ClientError({"Error": {"Code": "ThrottlingException", "Message": message}}, "RunTask")
        run_task = Mock(side_effect=[error] * 4 + [{"tasks": [{"taskArn": "arn:task:accepted"}], "failures": []}])
        client = Mock(run_task=run_task)
        factory = Mock(return_value=client)
        record = Mock()
        sleep = AsyncMock()
        jitter = Mock(return_value=0.5)
        monkeypatch.setattr(aws_clients.boto3, "client", factory)
        monkeypatch.setattr(launcher, "_record_task_arn", record)
        monkeypatch.setattr(launcher.asyncio, "sleep", sleep)
        monkeypatch.setattr(launcher.random, "uniform", jitter)
        dispatch = ExecutorDispatch.model_construct(id=uuid4())

        await launcher.launch_dispatch(dispatch)

        assert run_task.call_count == 5
        assert all(item.kwargs == run_task.call_args.kwargs for item in run_task.call_args_list)
        assert run_task.call_args.kwargs["clientToken"] == str(dispatch.id)
        record.assert_called_once_with(dispatch.id, "arn:task:accepted")
        assert jitter.call_args_list == [call(0, 1), call(0, 2), call(0, 4), call(0, 8)]
        assert sleep.await_args_list == [call(0.5)] * 4
        assert aws_clients.ecs_client() is client
        factory.assert_called_once_with("ecs", config=aws_clients.ECS_RUN_TASK_CONFIG)
        client.meta.events.register_last.assert_called_once_with(
            "before-send", getattr(aws_clients, "_check_ecs_launch_deadline")
        )
        assert getattr(aws_clients.ECS_RUN_TASK_CONFIG, "retries") == {"mode": "standard", "total_max_attempts": 1}
        assert getattr(aws_clients.ECS_RUN_TASK_CONFIG, "max_pool_connections") == 200

    @pytest.mark.description(
        "ECS server errors retry with stable tokens and preserve their cause after three attempts."
    )
    @pytest.mark.parametrize(
        ("code", "status"),
        [
            ("ServerException", None),
            ("ServiceUnavailableException", None),
            ("InternalFailure", None),
            ("UnknownError", 500),
            ("UnknownError", 503),
        ],
    )
    @pytest.mark.parametrize("exhausted", [False, True])
    async def test_server_errors_are_bounded(
        self,
        monkeypatch: pytest.MonkeyPatch,
        ecs_launcher_env: None,
        code: str,
        status: int | None,
        exhausted: bool,
    ) -> None:
        response: dict[str, Any] = {"Error": {"Code": code, "Message": "ECS backend unavailable"}}
        if status is not None:
            response["ResponseMetadata"] = {"HTTPStatusCode": status}
        error = ClientError(response, "RunTask")
        run_task = Mock(
            side_effect=error if exhausted else [error, {"tasks": [{"taskArn": "arn:task:accepted"}], "failures": []}]
        )
        record = Mock()
        sleep = AsyncMock()
        jitter = Mock(return_value=0.5)
        monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=Mock(run_task=run_task)))
        monkeypatch.setattr(launcher, "_record_task_arn", record)
        monkeypatch.setattr(launcher.asyncio, "sleep", sleep)
        monkeypatch.setattr(launcher.random, "uniform", jitter)
        dispatch = ExecutorDispatch.model_construct(id=uuid4())

        if exhausted:
            with pytest.raises(ClientError, match=f"{code}.*ECS backend unavailable") as raised:
                await launcher.launch_dispatch(dispatch)
            assert raised.value is error
            record.assert_not_called()
        else:
            await launcher.launch_dispatch(dispatch)
            record.assert_called_once_with(dispatch.id, "arn:task:accepted")

        assert run_task.call_count == (3 if exhausted else 2)
        assert all(item.kwargs == run_task.call_args.kwargs for item in run_task.call_args_list)
        assert run_task.call_args.kwargs["clientToken"] == str(dispatch.id)
        assert jitter.call_args_list == ([call(0, 1), call(0, 2)] if exhausted else [call(0, 1)])
        assert sleep.await_args_list == [call(0.5)] * (2 if exhausted else 1)

    @pytest.mark.description("Nonretryable ECS errors fail on the first call without sleeping or recording a task.")
    @pytest.mark.parametrize("status", [400, 403, 404, 429])
    async def test_nonretryable_error_fails_fast(
        self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None, status: int
    ) -> None:
        error = ClientError(
            {
                "Error": {"Code": "AccessDeniedException", "Message": "Denied"},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            "RunTask",
        )
        run_task = Mock(side_effect=error)
        record = Mock()
        sleep = AsyncMock()
        monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=Mock(run_task=run_task)))
        monkeypatch.setattr(launcher, "_record_task_arn", record)
        monkeypatch.setattr(launcher.asyncio, "sleep", sleep)

        with pytest.raises(ClientError, match="AccessDeniedException"):
            await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=uuid4()))

        run_task.assert_called_once()
        sleep.assert_not_awaited()
        record.assert_not_called()

    @pytest.mark.description(
        "A launch timeout prevents the worker from sending further SDK retries after its deadline."
    )
    async def test_launch_deadline_blocks_late_send(
        self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
    ) -> None:
        client = Mock()
        monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=client))
        monkeypatch.setattr(launcher, "_LAUNCH_TIMEOUT_SECONDS", 0)

        with pytest.raises(TimeoutError):
            await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=uuid4()))

        deadline = aws_clients.ECS_LAUNCH_DEADLINE
        check_deadline = getattr(aws_clients, "_check_ecs_launch_deadline")
        token = deadline.set(10)
        monkeypatch.setattr(aws_clients, "monotonic", lambda: 11)
        try:
            with pytest.raises(TimeoutError, match="deadline expired"):
                check_deadline()
        finally:
            deadline.reset(token)

        assert deadline.get() is None
        check_deadline()

    @pytest.mark.description("Persistent throttling exhausts the attempt budget without recording an executor task.")
    async def test_throttling_attempts_are_bounded(
        self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
    ) -> None:
        error = ClientError({"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "RunTask")
        run_task = Mock(side_effect=error)
        record = Mock()
        monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=Mock(run_task=run_task)))
        monkeypatch.setattr(launcher, "_record_task_arn", record)
        monkeypatch.setattr(launcher.asyncio, "sleep", AsyncMock())
        jitter = Mock(return_value=0)
        monkeypatch.setattr(launcher.random, "uniform", jitter)

        with pytest.raises(RuntimeError, match="ECS RunTask throttled for .*ThrottlingException"):
            await launcher.launch_dispatch(ExecutorDispatch.model_construct(id=uuid4()))

        assert run_task.call_count == 20
        assert all(item.kwargs == run_task.call_args.kwargs for item in run_task.call_args_list)
        assert jitter.call_args_list[-1] == call(0, 8)
        record.assert_not_called()

    @pytest.mark.description("Accepted launches finish ARN recording outside the launch acknowledgement timeout.")
    async def test_arn_recording_finishes_outside_launch_timeout(
        self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None
    ) -> None:
        run_task = Mock(return_value={"tasks": [{"taskArn": "arn:task:accepted"}], "failures": []})
        record = Mock()
        timeout = Mock(wraps=launcher.asyncio.timeout)
        monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=Mock(run_task=run_task)))
        monkeypatch.setattr(launcher, "_record_task_arn", record)
        monkeypatch.setattr(launcher.asyncio, "timeout", timeout)

        dispatch = ExecutorDispatch.model_construct(id=uuid4())
        await launcher.launch_dispatch(dispatch)

        run_task.assert_called_once()
        record.assert_called_once_with(dispatch.id, "arn:task:accepted")
        timeout.assert_called_once_with(180)

    @pytest.mark.description("Recovery stops after three SDK attempts when ECS keeps throttling the replacement task.")
    def test_relaunch_sdk_retries_are_bounded(self, monkeypatch: pytest.MonkeyPatch, ecs_launcher_env: None) -> None:
        client = getattr(aws_clients.boto3, "client")(
            "ecs",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            config=aws_clients.ECS_CONFIG,
        )
        try:
            raw = Mock()
            raw.stream.return_value = [b'{"__type":"ThrottlingException","message":"Rate exceeded"}']
            response = AWSResponse(
                "https://ecs.us-east-1.amazonaws.com/",
                400,
                {"content-type": "application/x-amz-json-1.1"},
                raw,
            )
            send = Mock(return_value=response)
            sleep = Mock()
            monkeypatch.setattr(client._endpoint.http_session, "send", send)
            monkeypatch.setattr("botocore.endpoint.time.sleep", sleep)
            monkeypatch.setattr(aws_clients.boto3, "client", Mock(return_value=client))
            dispatch = ExecutorDispatch.model_construct(id=uuid4())

            with pytest.raises(ClientError, match="ThrottlingException"):
                launcher.relaunch_dispatch(dispatch)

            assert send.call_count == 3
            assert sleep.call_count == 2
            assert 0 <= sleep.call_args_list[0].args[0] <= 1
            assert 0 <= sleep.call_args_list[1].args[0] <= 2
            assert getattr(client.meta.config, "connect_timeout") == 3
            assert getattr(client.meta.config, "read_timeout") == 8
        finally:
            client.close()


class TestRunTaskPacing:
    @pytest.mark.description("RunTask concurrency stays bounded even when a waiting launch is cancelled.")
    @pytest.mark.parametrize("cancel_first", [False, True])
    async def test_concurrency_cap(self, monkeypatch: pytest.MonkeyPatch, cancel_first: bool) -> None:
        semaphore = asyncio.Semaphore(2)
        monkeypatch.setattr(launcher, "_run_task_semaphore", semaphore)
        monkeypatch.setattr(launcher, "_run_task_slots", threading.BoundedSemaphore(2))
        capacity, release = threading.Event(), threading.Event()
        lock = threading.Lock()
        active = peak = started = 0

        def run_task(**_params: Any) -> dict[str, Any]:
            nonlocal active, peak, started
            with lock:
                active += 1
                started += 1
                peak = max(peak, active)
                if active == 2:
                    capacity.set()
            try:
                assert release.wait(timeout=5)
                return {"tasks": [{"taskArn": "arn:task:paced"}], "failures": []}
            finally:
                with lock:
                    active -= 1

        client = Mock(run_task=run_task)
        tasks = [asyncio.create_task(launcher._run_task(client, {})) for _ in range(6)]
        tasks.append(asyncio.create_task(asyncio.to_thread(launcher._call_run_task, client, {})))
        try:
            assert await asyncio.to_thread(capacity.wait, 2)
            if cancel_first:
                tasks[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await tasks[0]
            assert started == peak == 2
        finally:
            release.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(*launcher._run_task_calls)

        assert peak == 2
        assert started == 7
        assert sum(isinstance(result, dict) for result in results) == (6 if cancel_first else 7)

    @pytest.mark.description("The launch budget reserves claim time and never exceeds 180 seconds.")
    @pytest.mark.parametrize(("remaining", "expected"), [(None, 180), (300, 180), (150, 60), (60, 0)])
    def test_claim_allowance(self, monkeypatch: pytest.MonkeyPatch, remaining: int | None, expected: int) -> None:
        now = datetime(2026, 11, 14, tzinfo=UTC)
        clock = Mock()
        clock.now.return_value = now
        monkeypatch.setattr(launcher, "datetime", clock)
        deadline = now + timedelta(seconds=remaining) if remaining is not None else None
        dispatch = ExecutorDispatch.model_construct(id=uuid4(), claim_deadline_at=deadline)

        assert launcher._launch_budget(dispatch) == expected
