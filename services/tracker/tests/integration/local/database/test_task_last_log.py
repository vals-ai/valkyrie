from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from unittest.mock import Mock

import pytest
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import Org
from tracker.domain import RetryMode, TaskStatus
from tracker.executor.control_protocol import AttemptRef
from tracker.executor.control_tasks import TaskControl
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.services.runs.retry import reset_to_in_progress_status


@pytest.mark.description(
    "The executor records the latest log for the current attempt and rejects writes from an old attempt."
)
async def test_report_task_last_log(
    control_postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = control_postgres_session
    benchmark = make_benchmark(session=session)
    started_at = datetime(2026, 9, 17, 12, tzinfo=UTC)
    task = make_task(benchmark, "task", status=TaskStatus.IN_PROGRESS, started_at=started_at)
    session.add(task)
    session.commit()
    controls = bound_controls(benchmark, session=session)
    control = controls["task_control"]
    assert isinstance(control, TaskControl)
    session.refresh(task)
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    last_log_at = started_at + timedelta(seconds=5)
    monkeypatch.setattr("tracker.runtime.task_logs.time.time", last_log_at.timestamp)

    async with TaskLogBuffer(Mock(), "run:stream", partial(control.record_last_log, attempt)) as logs:
        logs.write_line("task output")
    await control.monitor([attempt.task_id])
    control.record_last_log(attempt, started_at)
    await control.flush_last_logs()

    session.refresh(task)
    assert task.last_log_at == last_log_at

    task.status = TaskStatus.ERROR
    session.add(task)
    session.commit()
    org = session.get(Org, task.org_id)
    assert org is not None
    reset_to_in_progress_status(benchmark, session, True, RetryMode.FROM_SCRATCH, [task.task_id], org, [task.task_id])
    session.commit()
    session.refresh(task)
    assert task.started_at > attempt.started_at
    assert task.last_log_at is None

    control.record_last_log(attempt, last_log_at)
    tick = await control.monitor([attempt.task_id])
    assert tick.tasks[attempt.task_id].started_at == task.started_at

    session.refresh(task)
    assert task.last_log_at is None

    next_log_at = task.started_at.replace(tzinfo=UTC) + timedelta(seconds=5)
    control.record_last_log(tick.tasks[attempt.task_id].ref, next_log_at)
    await control.flush_last_logs()

    session.refresh(task)
    assert task.last_log_at == next_log_at
