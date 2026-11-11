from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from threading import Event, Thread
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from sqlmodel import Session

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import ExecutorDispatch
from tracker.database.session import engine
from tracker.domain import FailureCategory
from tracker.egress_proxy.policy import PolicyStore
from tracker.executor.dispatch_control import DISPATCH_CLAIM_WINDOW, record_dispatch_failure, unowned_pending_task_ids
from tracker.executor.launcher import ECS_CONFIG, DefinitiveLaunchFailure, relaunch_dispatch
from tracker.executor.release_control import share_executor_admission
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.run_access_keys import delete_finished_run_keys

_ECS_MISSING_GRACE = timedelta(minutes=10)
_RELAUNCH_CLAIM_ALLOWANCE = timedelta(seconds=90)
_RECONCILIATION_INTERVAL_SECONDS = 60.0
_SHUTDOWN_TIMEOUT_SECONDS = 5.0
_ECS_ERROR_CODE = re.compile(r"\w+(?=:)")
_logger = logging.getLogger(__name__)


_STOPPED_TASK_FAILURES = {
    ExecutorDispatchStatus.QUEUED: ("Executor task stopped before claim", "ExecutorTaskStoppedBeforeClaim"),
    ExecutorDispatchStatus.RUNNING: ("Executor task stopped without finishing", "ExecutorTaskStopped"),
}


def _record_replacement(
    session: Session, dispatch: ExecutorDispatch, stopped_task_arn: str, replacement_task_arn: str, stopped_reason: str
) -> bool:
    dispatch_id = dispatch.id
    benchmark_id = dispatch.benchmark_id
    error_code = _ECS_ERROR_CODE.match(stopped_reason)
    relaunch_reason = error_code.group() if error_code else None
    share_executor_admission(session)
    recorded = ExecutorDispatchRepository(session).record_relaunch(
        dispatch_id, stopped_task_arn, replacement_task_arn, relaunch_reason
    )
    session.commit()
    if recorded:
        _logger.warning(
            "executor_dispatch_relaunched",
            extra={
                "event": "executor_dispatch_relaunched",
                "executor_dispatch_id": str(dispatch_id),
                "benchmark_id": str(benchmark_id),
                "relaunch_reason": relaunch_reason,
                "stopped_task_arn": stopped_task_arn,
                "stopped_reason": stopped_reason,
                "ecs_task_arn": replacement_task_arn,
            },
        )

    return recorded


def describe_executor_tasks(task_arns: list[str]) -> dict[str, Any]:
    ecs = boto3.client("ecs", config=ECS_CONFIG)
    return ecs.describe_tasks(cluster=os.environ["EXECUTOR_RUNNER_CLUSTER"], tasks=task_arns)


def _reconcile_stopped_tasks(session: Session) -> int:
    repository = ExecutorDispatchRepository(session)
    now = repository.now()
    cutoff = now - _ECS_MISSING_GRACE
    relaunch_cutoff = now - DISPATCH_CLAIM_WINDOW + _RELAUNCH_CLAIM_ALLOWANCE
    candidates = repository.list_with_task_arn()
    if not candidates:
        return 0
    recovered = 0
    for offset in range(0, len(candidates), 100):
        batch = {dispatch.ecs_task_arn: dispatch for dispatch in candidates[offset : offset + 100]}
        session.commit()
        response = describe_executor_tasks(list(batch))
        stopped = {task["taskArn"]: task for task in response["tasks"] if task["lastStatus"] == "STOPPED"}
        stopped |= {
            failure["arn"]: {}
            for failure in response["failures"]
            if failure["reason"] == "MISSING" and batch[failure["arn"]].created_at <= cutoff
        }
        failures: list[tuple[str, ExecutorDispatch, str]] = []
        for task_arn, task in stopped.items():
            dispatch = batch[task_arn]
            reasons = [task.get("stoppedReason"), *(c.get("reason") for c in task.get("containers", []))]
            details = "; ".join(reason for reason in reasons if reason) or "ECS no longer reports the task"
            if not (
                dispatch.status == ExecutorDispatchStatus.QUEUED
                and task.get("stopCode") == "TaskFailedToStart"
                and dispatch.created_at > relaunch_cutoff
                and dispatch.relaunch_count == 0
            ):
                failures.append((task_arn, dispatch, details))
                continue
            try:
                replacement_task_arn = relaunch_dispatch(dispatch)
            except (BotoCoreError, ClientError, DefinitiveLaunchFailure, RuntimeError):
                _logger.exception(
                    "executor_dispatch_relaunch_failed",
                    extra={"event": "executor_dispatch_relaunch_failed", "executor_dispatch_id": str(dispatch.id)},
                )
                continue
            if _record_replacement(session, dispatch, task_arn, replacement_task_arn, details):
                recovered += 1
        if failures:
            share_executor_admission(session)
        for task_arn, dispatch, details in failures:
            message, error_type = _STOPPED_TASK_FAILURES[dispatch.status]
            if record_dispatch_failure(
                session,
                benchmark_id=dispatch.benchmark_id,
                dispatch_id=dispatch.id,
                task_ids=dispatch.assigned_task_ids or [],
                error_message=f"{message}: {details}",
                producer="executor_dispatch",
                operation="dispatch_reconciliation",
                error_type=error_type,
                category=FailureCategory.INFRASTRUCTURE,
                cause_code="ECS_TASK_STOPPED",
                failure_reason="ECS_TASK_STOPPED",
                dispatch_status=dispatch.status,
                ecs_task_arn=task_arn,
            ):
                recovered += 1
    return recovered


def _reconcile_expired_dispatches(session: Session) -> int:
    repository = ExecutorDispatchRepository(session)
    now = repository.now()
    expired = repository.list_expired(now)
    return sum(
        record_dispatch_failure(
            session,
            benchmark_id=dispatch.benchmark_id,
            dispatch_id=dispatch.id,
            task_ids=dispatch.assigned_task_ids or [],
            error_message="Executor dispatch did not claim or renew its local lease before the deadline",
            producer="executor_dispatch",
            operation="dispatch_reconciliation",
            error_type="ExecutorDispatchDeadlineExpired",
            category=FailureCategory.INFRASTRUCTURE,
            cause_code="CLAIM_DEADLINE_EXPIRED"
            if dispatch.status == ExecutorDispatchStatus.QUEUED
            else "LEASE_EXPIRED",
            failure_reason="CLAIM_DEADLINE_EXPIRED"
            if dispatch.status == ExecutorDispatchStatus.QUEUED
            else "LEASE_EXPIRED",
            dispatch_status=dispatch.status,
            expired_before=now if dispatch.status == ExecutorDispatchStatus.RUNNING else None,
            claim_expired_before=now if dispatch.status == ExecutorDispatchStatus.QUEUED else None,
        )
        for dispatch in expired
    )


def report_unowned_pending_tasks() -> None:
    with Session(engine) as session:
        report_unowned_pending_tasks_in(session)


def report_unowned_pending_tasks_in(session: Session) -> None:
    unowned = {
        benchmark.id: task_ids
        for benchmark in BenchmarkRepository(session).list_in_progress_with_pending_tasks()
        if (task_ids := unowned_pending_task_ids(session, benchmark))
    }
    for benchmark_id, task_ids in unowned.items():
        _logger.warning(
            "Run %s has %d PENDING task(s) that no active executor dispatch will run: %s",
            benchmark_id,
            len(task_ids),
            ", ".join(task_ids),
            extra={"event": "pending_tasks_without_executor", "benchmark_id": str(benchmark_id)},
        )


def reconcile_dispatches_once() -> int:
    with Session(engine) as session:
        ExecutorDispatchRepository(session).delete_terminal_payloads()
        session.commit()
    with Session(engine) as session:
        recovered = 0
        try:
            if os.environ["EXECUTOR_LAUNCHER"] == "ecs":
                recovered += _reconcile_stopped_tasks(session)
                session.commit()
        finally:
            share_executor_admission(session)
            recovered += _reconcile_expired_dispatches(session)
            session.commit()
    return recovered


def run_dispatch_recovery_loop(
    stop_event: Event,
    *,
    interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
) -> None:
    while not stop_event.is_set():
        try:
            reconcile_dispatches_once()
        except Exception:
            _logger.exception(
                "executor_dispatch_recovery_failed",
                extra={"event": "automatic_dispatch_recovery_failed"},
            )
        try:
            report_unowned_pending_tasks()
        except Exception:
            _logger.exception("pending_tasks_without_executor_report_failed")
        try:
            delete_finished_run_keys(engine)
        except Exception:
            _logger.exception("run_access_key_cleanup_failed")
        try:
            PolicyStore(engine).revoke_finished()
        except Exception:
            _logger.exception("egress_proxy_policy_cleanup_failed")
        stop_event.wait(interval_seconds)


class AutomaticDispatchRecovery:
    def __init__(
        self,
        *,
        interval_seconds: float = _RECONCILIATION_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_dispatch_recovery_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="executor-dispatch-recovery",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            _logger.error(
                "executor_dispatch_recovery_shutdown_timeout",
                extra={"event": "automatic_dispatch_recovery_shutdown_timeout"},
            )
