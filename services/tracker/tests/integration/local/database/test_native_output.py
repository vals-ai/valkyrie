from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from benchmark_service import ComposeSource, SnapshotSource
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    ExecutorDispatch,
    Org,
)
from executor_protocol import ExecutorDispatchStatus
from tracker.types import ActiveNativeOutput
from tracker.domain import TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.control_protocol import AttemptRef, DispatchRef
from tracker.executor.control_tasks import NativeOutputRequest, TaskControl
from tracker.executor.native_output import clear_native_output, publish_native_output
from tracker.services.runs.stop import apply_shed_benchmark, apply_stop_benchmark

_STARTED_AT = datetime(2026, 7, 1, tzinfo=UTC)


def _descriptor(sandbox_id: str) -> ActiveNativeOutput:
    return ActiveNativeOutput(
        attempt_started_at=_STARTED_AT,
        sandbox_id=sandbox_id,
        runtime_source=ComposeSource(
            outer=SnapshotSource(snapshot="original-snapshot"),
            service="agent",
            compose_command="docker compose -f /run.yml",
        ),
        output_path="/agent/claude_code.log",
    )


@pytest.mark.description("Publish under current authority and revoke only the original generation.")
class TestNativeOutputPersistence:
    @pytest.mark.description(
        "An old cleanup keeps the replacement reference, and a revoked reference can still clean itself up."
    )
    @pytest.mark.parametrize("status", [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS])
    def test_old_cleanup_preserves_replacement_and_revocation_allows_own_cleanup(
        self, database_session: Session, status: TaskStatus
    ) -> None:
        benchmark = make_benchmark(session=database_session)
        task = make_task(benchmark, "task", status=status, started_at=_STARTED_AT)
        database_session.add(task)
        database_session.commit()
        old = _descriptor("old-sandbox")
        current = _descriptor("replacement-sandbox")

        publish_native_output(database_session, task, old)
        database_session.commit()
        database_session.refresh(task)

        assert task.active_native_output is not None
        assert task.active_native_output == old
        assert task.active_native_output.attempt_started_at == old.attempt_started_at

        publish_native_output(database_session, task, current)
        clear_native_output(database_session, task, old)
        database_session.commit()
        database_session.refresh(task)

        assert task.active_native_output == current
        assert current.sandbox_id != old.sandbox_id

        task.status = TaskStatus.STOPPED
        database_session.add(task)
        database_session.commit()

        clear_native_output(database_session, task, current)
        database_session.commit()
        database_session.refresh(task)

        assert task.active_native_output is None
        assert task.status == TaskStatus.STOPPED

    @pytest.mark.description("A stop revokes only the selected native output references.")
    @pytest.mark.parametrize(
        ("operation", "stopped"), [("force-stop", {"older"}), ("stop", {"newer", "evaluating"}), ("shed", {"newer"})]
    )
    def test_stop_revokes_selected_references_only(
        self, database_session: Session, operation: str, stopped: set[str]
    ) -> None:
        benchmark = make_benchmark(session=database_session)
        tasks = [
            make_task(benchmark, name, status=status, started_at=_STARTED_AT + timedelta(seconds=index))
            for index, (name, status) in enumerate(
                [
                    ("older", TaskStatus.IN_PROGRESS),
                    ("newer", TaskStatus.BUILDING),
                    ("evaluating", TaskStatus.EVALUATING),
                ]
            )
        ]
        for task in tasks:
            task.active_native_output = _descriptor(task.task_id).model_copy(
                update={"attempt_started_at": task.started_at}
            )
            database_session.add(task)
        database_session.commit()
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        if operation == "shed":
            apply_shed_benchmark(benchmark, 2, database_session, org)
        else:
            apply_stop_benchmark(
                benchmark,
                database_session,
                operation == "force-stop",
                org,
                task_ids=["older"] if operation == "force-stop" else None,
            )
        database_session.commit()

        for task in tasks:
            database_session.refresh(task)
            assert (task.status == TaskStatus.STOPPED) == (task.task_id in stopped)
            if task.task_id in stopped:
                assert task.active_native_output is None
            else:
                assert task.active_native_output is not None
                assert task.active_native_output.sandbox_id == task.task_id

    @pytest.mark.description("A stale native output publish is rejected.")
    @pytest.mark.parametrize("changed", ["attempt", "status", "evaluating", "dispatch", "benchmark"])
    async def test_stale_publish_is_rejected(
        self, control_postgres_session: Session, bound_controls: Callable[..., dict[str, object]], changed: str
    ) -> None:
        session = control_postgres_session
        benchmark = make_benchmark(session=session)
        task = make_task(benchmark, "task", status=TaskStatus.BUILDING, started_at=_STARTED_AT)
        session.add(task)
        session.commit()
        controls = bound_controls(benchmark, session=session)
        session.refresh(task)
        dispatch = session.get(ExecutorDispatch, UUID(str(controls["executor_dispatch_id"])))
        assert dispatch is not None
        request = NativeOutputRequest(
            ref=DispatchRef(benchmark_id=benchmark.id, dispatch_id=dispatch.id),
            attempt=AttemptRef(
                org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at
            ),
            descriptor=_descriptor("stale-sandbox").model_copy(update={"attempt_started_at": task.started_at}),
        )
        if changed == "attempt":
            task.started_at += timedelta(seconds=1)
        elif changed in ("status", "evaluating"):
            task.status = TaskStatus.EVALUATING if changed == "evaluating" else TaskStatus.STOPPED
        elif changed == "dispatch":
            dispatch.status = ExecutorDispatchStatus.FINISHED
            session.add(dispatch)
        else:
            other = make_benchmark(session=session)
            task.benchmark = other.id
        session.add(task)
        session.commit()

        task_control = controls["task_control"]
        assert isinstance(task_control, TaskControl)
        if changed in ("status", "evaluating"):
            assert await task_control.publish_native_output(request) is False
        elif changed in ("attempt", "dispatch"):
            with pytest.raises(ExecutionAuthorityRevoked):
                await task_control.publish_native_output(request)
        else:
            with pytest.raises(httpx.HTTPStatusError) as caught:
                await task_control.publish_native_output(request)
            assert caught.value.response.status_code == 409
        session.refresh(task)
        assert task.active_native_output is None
