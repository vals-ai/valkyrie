from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest.mock import Mock
from uuid import uuid4

import pytest
from pytest import MonkeyPatch
from sqlmodel import Session, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    FailureCategory,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Task,
    TaskStatus,
)
from tracker.egress_proxy.policy import PolicyStore
from tracker.executor import dispatch_recovery
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release


@pytest.mark.description("Dispatch recovery sweeps only payloads outside the queue.")
@pytest.mark.parametrize(
    "terminal_status",
    [ExecutorDispatchStatus.FAILED, ExecutorDispatchStatus.FINISHED, ExecutorDispatchStatus.RUNNING],
)
def test_recovery_sweeps_only_nonqueued_payloads(
    terminal_status: ExecutorDispatchStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
) -> None:
    release = ExecutorRelease(
        id="payload-sweep-release",
        artifact_uri="source:///artifacts/runner.pex",
        artifact_digest="a" * 64,
        protocol_version="3",
    )
    benchmark = example_benchmark_object
    database_session.add_all([release, benchmark])
    database_session.flush()
    dispatches = [
        ExecutorDispatch(
            id=uuid4(),
            benchmark_id=benchmark.id,
            kind=ExecutorDispatchKind.START,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
            status=status,
        )
        for status in (ExecutorDispatchStatus.QUEUED, terminal_status)
    ]
    for dispatch in dispatches:
        sealed = seal_payload(dispatch.id, {"sensitive": "secret-marker"}, generate_payload_key(dispatch.id))
        database_session.add(dispatch)
        database_session.add(
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            )
        )
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())

    assert dispatch_recovery.reconcile_dispatches_once() == 0

    database_session.expire_all()
    queued, nonqueued = dispatches
    kept = database_session.get(ExecutorDispatchPayload, queued.id)
    assert kept is not None
    assert b"secret-marker" not in kept.ciphertext
    assert database_session.get(ExecutorDispatchPayload, nonqueued.id) is None


@pytest.mark.description("The recovery loop tries again after a failed pass.")
def test_recovery_loop_retries_after_a_failed_pass(monkeypatch: MonkeyPatch) -> None:
    stop_event = Event()
    attempts = 0

    def reconcile() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database unavailable")
        stop_event.set()
        return 1

    logger = Mock()

    def skip_policy_sweep(_store: PolicyStore) -> int:
        return 0

    monkeypatch.setattr(dispatch_recovery, "reconcile_dispatches_once", reconcile)

    monkeypatch.setattr(dispatch_recovery, "report_unowned_pending_tasks", lambda: None)
    monkeypatch.setattr(PolicyStore, "revoke_finished", skip_policy_sweep)
    monkeypatch.setattr(dispatch_recovery, "_logger", logger)

    dispatch_recovery.run_dispatch_recovery_loop(stop_event, interval_seconds=0)

    assert attempts == 2
    logger.exception.assert_called_once_with(
        "executor_dispatch_recovery_failed",
        extra={"event": "automatic_dispatch_recovery_failed"},
    )


@pytest.mark.description("Automatic recovery starts and stops its own thread.")
def test_automatic_recovery_starts_and_stops_owned_thread(monkeypatch: MonkeyPatch) -> None:
    events: list[str] = []

    class FakeThread:
        def __init__(
            self,
            *,
            target: Callable[..., None],
            args: tuple[Event],
            kwargs: dict[str, float],
            name: str,
            daemon: bool,
        ) -> None:
            assert target is dispatch_recovery.run_dispatch_recovery_loop
            assert kwargs == {"interval_seconds": 60}
            assert name == "executor-dispatch-recovery"
            assert daemon
            self.stop_event = args[0]

        def start(self) -> None:
            events.append("started")

        def join(self, timeout: float) -> None:
            assert timeout == 5
            assert self.stop_event.is_set()
            events.append("joined")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(dispatch_recovery, "Thread", FakeThread)
    recovery = dispatch_recovery.AutomaticDispatchRecovery(interval_seconds=60)

    recovery.start()
    recovery.stop()

    assert events == ["started", "joined"]


@pytest.mark.description("Automatic recovery logs when its thread does not stop.")
def test_automatic_recovery_logs_if_owned_thread_does_not_stop(monkeypatch: MonkeyPatch) -> None:
    logger = Mock()

    class NeverStops:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            pass

        def join(self, timeout: float) -> None:
            assert timeout == 5

        def is_alive(self) -> bool:
            return True

    monkeypatch.setattr(dispatch_recovery, "Thread", NeverStops)
    monkeypatch.setattr(dispatch_recovery, "_logger", logger)

    dispatch_recovery.AutomaticDispatchRecovery().stop()

    logger.error.assert_called_once_with(
        "executor_dispatch_recovery_shutdown_timeout",
        extra={"event": "automatic_dispatch_recovery_shutdown_timeout"},
    )


@pytest.mark.description(
    "Recovery fails a dispatch whose ECS task stopped and saves the stop reason, such as an out of memory kill."
)
@pytest.mark.parametrize(
    ("dispatch_status", "last_status"),
    [
        (ExecutorDispatchStatus.QUEUED, "STOPPED"),
        (ExecutorDispatchStatus.QUEUED, "STOPPED_OLD"),
        (ExecutorDispatchStatus.QUEUED, "RUNNING"),
        (ExecutorDispatchStatus.QUEUED, "MISSING"),
        (ExecutorDispatchStatus.RUNNING, "STOPPED"),
        (ExecutorDispatchStatus.RUNNING, "PENDING"),
        (ExecutorDispatchStatus.RUNNING, "MISSING"),
        (ExecutorDispatchStatus.RUNNING, "MISSING_OLD"),
    ],
)
def test_recovery_fails_dispatches_whose_task_stopped(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
    dispatch_status: ExecutorDispatchStatus,
    last_status: str,
) -> None:
    release = ExecutorRelease(
        id="ecs-stopped-release",
        artifact_uri="source:///artifacts/runner",
        artifact_digest="a" * 64,
        protocol_version="3",
    )
    benchmark = example_benchmark_object
    register_release(database_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    dispatch.ecs_task_arn = "arn:aws:ecs:us-east-1:123456789012:task/cluster/task"
    dispatch.status = dispatch_status
    if last_status.endswith("_OLD"):
        dispatch.created_at -= timedelta(minutes=11)
    running = dispatch_status == ExecutorDispatchStatus.RUNNING
    if running:
        dispatch.started_at = datetime.now(UTC)
    task = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id="task-1",
        status=TaskStatus.IN_PROGRESS if running else TaskStatus.PENDING,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add_all([benchmark, dispatch, task])
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())
    ecs = Mock()
    described = {
        "taskArn": dispatch.ecs_task_arn,
        "lastStatus": last_status.removesuffix("_OLD"),
        "stoppedReason": "Essential container in task exited",
        "containers": [{"reason": "OutOfMemoryError: Container killed due to memory usage"}],
    }
    if last_status.startswith("MISSING"):
        ecs.describe_tasks.return_value = {
            "tasks": [],
            "failures": [{"arn": dispatch.ecs_task_arn, "reason": "MISSING"}],
        }
    else:
        ecs.describe_tasks.return_value = {"tasks": [described], "failures": []}
    monkeypatch.setattr(dispatch_recovery.boto3, "client", lambda *_args, **_kwargs: ecs)

    recovered = last_status in {"STOPPED", "STOPPED_OLD", "MISSING_OLD"}
    assert dispatch_recovery.reconcile_dispatches_once() == (1 if recovered else 0)
    ecs.describe_tasks.assert_called_once_with(cluster="cluster", tasks=[dispatch.ecs_task_arn])
    database_session.expire_all()
    stored = database_session.get(ExecutorDispatch, dispatch.id)
    assert stored is not None
    if not recovered:
        assert stored.status == dispatch_status
        assert database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.IN_PROGRESS
        return
    assert stored.status == ExecutorDispatchStatus.FAILED
    assert stored.failure_reason == "ECS_TASK_STOPPED"
    assert database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.ERROR
    error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
    assert error.category == FailureCategory.INFRASTRUCTURE
    assert error.error_type == ("ExecutorTaskStopped" if running else "ExecutorTaskStoppedBeforeClaim")
    if last_status.startswith("MISSING"):
        assert "ECS no longer reports the task" in error.error_message
    else:
        assert "OutOfMemoryError: Container killed due to memory usage" in error.error_message


@pytest.mark.description("An expired launch with no saved ARN recovers during an ECS outage.")
def test_expired_launch_without_saved_arn_recovers_during_ecs_outage(
    database_session: Session, example_benchmark_object: Benchmark, monkeypatch: MonkeyPatch
) -> None:
    benchmark = example_benchmark_object
    release = ExecutorRelease(
        id="lost-launch-release",
        artifact_uri="source:///local",
        artifact_digest="a" * 64,
        protocol_version="5",
    )
    register_release(database_session, release)
    pin_benchmark_to_release(benchmark, release)
    lost_launch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    lost_launch.created_at -= timedelta(seconds=121)
    other_launch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    other_launch.ecs_task_arn = "arn:aws:ecs:us-east-1:123456789012:task/cluster/other"
    database_session.add_all([benchmark, lost_launch, other_launch])
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())
    ecs = Mock()
    ecs.describe_tasks.side_effect = RuntimeError("ECS unavailable")
    monkeypatch.setattr(dispatch_recovery.boto3, "client", lambda *_args, **_kwargs: ecs)

    with pytest.raises(RuntimeError, match="ECS unavailable"):
        dispatch_recovery.reconcile_dispatches_once()

    database_session.refresh(lost_launch)
    database_session.refresh(other_launch)
    assert lost_launch.status == ExecutorDispatchStatus.FAILED
    assert lost_launch.failure_reason == "CLAIM_DEADLINE_EXPIRED"
    assert other_launch.status == ExecutorDispatchStatus.QUEUED


@pytest.mark.description(
    "A PENDING task whose retry dispatch failed is reported every pass. One a live dispatch lists is not."
)
def test_recovery_flags_pending_tasks_whose_dispatch_failed(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
) -> None:
    release = ExecutorRelease(
        id="stranded-release",
        artifact_uri="source:///artifacts/runner",
        artifact_digest="a" * 64,
        protocol_version="3",
    )
    benchmark = example_benchmark_object
    register_release(database_session, release)
    pin_benchmark_to_release(benchmark, release)
    start = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-a", "task-b"]
    )
    start.status = ExecutorDispatchStatus.RUNNING
    start.started_at = datetime.now(UTC)
    start.lease_expires_at = datetime.now(UTC) + timedelta(hours=1)
    retry = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.RETRY, dispatch_id=uuid4(), task_ids=["task-a"]
    )
    retry.created_at = start.created_at + timedelta(minutes=5)
    retry.status = ExecutorDispatchStatus.FAILED
    retry.failure_reason = "CLAIM_DEADLINE_EXPIRED"
    tasks = [
        Task(org_id=benchmark.org_id, benchmark=benchmark.id, task_id="task-a", status=TaskStatus.PENDING),
        Task(org_id=benchmark.org_id, benchmark=benchmark.id, task_id="task-b", status=TaskStatus.PENDING),
    ]
    database_session.add_all([benchmark, start, retry, *tasks])
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setattr(dispatch_recovery, "engine", database_session.get_bind())

    warning = Mock()
    monkeypatch.setattr(dispatch_recovery._logger, "warning", warning)  # pyright: ignore[reportPrivateUsage]

    assert dispatch_recovery.reconcile_dispatches_once() == 0
    assert warning.call_count == 0

    dispatch_recovery.report_unowned_pending_tasks()
    assert warning.call_count == 1
    message, *args = warning.call_args.args
    assert message % tuple(args) == (
        f"Run {benchmark.id} has 1 PENDING task(s) that no active executor dispatch will run: task-a"
    )
    assert warning.call_args.kwargs["extra"]["event"] == "pending_tasks_without_executor"
