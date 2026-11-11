import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, Org, Task
from tracker.domain import TaskStatus
from tracker.exceptions import SandboxError, SandboxSetupError
from tracker.executor.control_protocol import DispatchRef, TaskRef
from tracker.executor.control_tasks import TaskControl, TaskState
from tracker.services.tasks.execute import ResizableLimiter, TaskMonitor, TrackedTask, TrackedTaskStatus
import tracker.services.tasks.execute as task_execution
from tracker.services.runs.stop import apply_shed_benchmark
from tracker.services.tasks.execute import _exception_message as exception_message


@pytest.mark.description("Persisted task error text keeps the original error behind tracker wrappers.")
class TestExceptionMessage:
    @pytest.mark.description("`exception_message` formats a plain error as its type and message.")
    def test_direct_error_keeps_type_and_message(self) -> None:
        assert exception_message(ValueError("bad task input")) == "ValueError: bad task input"

    @pytest.mark.description("An error with an empty message keeps its type.")
    def test_empty_message_keeps_type(self) -> None:
        assert exception_message(TimeoutError()) == "TimeoutError"

    @pytest.mark.description(
        "A wrapping error's message ends with `caused by` and the original error's type and message."
    )
    def test_wrapper_with_new_text_appends_original_type_and_message(self) -> None:
        try:
            try:
                raise TimeoutError("egress rule update exceeded 30s")
            except TimeoutError as e:
                raise SandboxSetupError("Failed to clear egress rules") from e
        except SandboxSetupError as wrapped:
            message = exception_message(wrapped)
        assert message == (
            "SandboxSetupError: Sandbox error: Failed to clear egress rules "
            "(caused by TimeoutError: egress rule update exceeded 30s)"
        )

    @pytest.mark.description("The original error type is still shown when the wrapper repeats the original message.")
    def test_cause_type_survives_when_wrapper_contains_its_message(self) -> None:
        try:
            try:
                raise TimeoutError("provider unavailable")
            except TimeoutError as e:
                raise SandboxError(str(e)) from e
        except SandboxError as wrapped:
            message = exception_message(wrapped)
        assert message == (
            "SandboxError: Sandbox error: provider unavailable (caused by TimeoutError: provider unavailable)"
        )

    @pytest.mark.description("An error raised while handling another adds `during handling of` and the first error.")
    def test_implicit_context_survives(self) -> None:
        try:
            try:
                raise ValueError("bad task input")
            except ValueError:
                raise RuntimeError("setup failed")
        except RuntimeError as wrapped:
            message = exception_message(wrapped)
        assert message == "RuntimeError: setup failed (during handling of ValueError: bad task input)"

    @pytest.mark.description("An error raised `from None` shows only its own message.")
    def test_suppressed_context_stays_suppressed(self) -> None:
        try:
            try:
                raise ValueError("hidden detail")
            except ValueError:
                raise RuntimeError("public detail") from None
        except RuntimeError as wrapped:
            message = exception_message(wrapped)
        assert message == "RuntimeError: public detail"

    @pytest.mark.description("An explicit cause wins over implicit context.")
    def test_explicit_cause_takes_precedence_over_context(self) -> None:
        exc = RuntimeError("setup failed")
        exc.__context__ = ValueError("unrelated context")
        exc.__cause__ = TimeoutError("provider unavailable")
        assert exception_message(exc) == "RuntimeError: setup failed (caused by TimeoutError: provider unavailable)"

    @pytest.mark.description("A cause chain that refers to itself ends.")
    def test_self_referential_cause_chain_terminates(self) -> None:
        exc = RuntimeError("stream closed")
        exc.__cause__ = exc
        assert exception_message(exc) == "RuntimeError: stream closed"

    @pytest.mark.description("A context cycle ends.")
    def test_context_cycle_terminates(self) -> None:
        exc = RuntimeError("outer")
        cause = ValueError("inner")
        exc.__context__ = cause
        cause.__context__ = exc
        assert exception_message(exc) == "RuntimeError: outer (during handling of ValueError: inner)"


@pytest.mark.description("Task monitoring and tracked task state transitions.")
class TestTaskExecution:
    _test_org = Org(id=TEST_ORG_ID, name="default")

    @pytest.mark.description("Shedding keeps direct tasks that are waiting.")
    @pytest.mark.parametrize("shed_first", [False, True])
    async def test_shed_preserves_waiting_direct_tasks(
        self,
        shed_first: bool,
        control_postgres_session: Session,
        bound_controls: Any,
        example_benchmark_object: Benchmark,
    ) -> None:
        benchmark = example_benchmark_object
        benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": 3})
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        controls = bound_controls(benchmark, session=control_postgres_session)
        control = cast(TaskControl, controls["task_control"])
        tasks = [
            Task(
                org_id=TEST_ORG_ID,
                benchmark=benchmark.id,
                task_id=f"task-{index}",
                started_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index),
            )
            for index in range(4)
        ]
        control_postgres_session.add_all(tasks)
        control_postgres_session.commit()

        if shed_first:
            assert apply_shed_benchmark(benchmark, 1, control_postgres_session, self._test_org) == []
            control_postgres_session.commit()
        for task in tasks[: 1 if shed_first else 3]:
            state = (await control.read(TaskRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id))).task
            assert (await control.admit(state.ref)).admitted

        control_postgres_session.expire_all()
        statuses = {task.task_id: task.status for task in control_postgres_session.exec(select(Task)).all()}
        assert list(statuses.values()).count(TaskStatus.BUILDING) == (1 if shed_first else 3)
        assert statuses["task-3"] == TaskStatus.PENDING
        if not shed_first:
            assert apply_shed_benchmark(benchmark, 1, control_postgres_session, self._test_org) == ["task-2", "task-1"]
            control_postgres_session.commit()
            control_postgres_session.expire_all()
            statuses = {task.task_id: task.status for task in control_postgres_session.exec(select(Task)).all()}
            assert statuses["task-0"] == TaskStatus.BUILDING
            assert statuses["task-1"] == statuses["task-2"] == TaskStatus.STOPPED
            assert statuses["task-3"] == TaskStatus.PENDING

    @pytest.mark.description("Raising the limiter wakes waiting admissions.")
    async def test_resizable_limiter_increase_wakes_waiting_admission(self) -> None:
        limiter = ResizableLimiter(limit=1)
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_attempted = asyncio.Event()
        second_started = asyncio.Event()

        async def worker(
            started: asyncio.Event,
            release: asyncio.Event | None = None,
            attempting: asyncio.Event | None = None,
        ) -> None:
            if attempting is not None:
                attempting.set()
            async with limiter:
                started.set()
                if release is not None:
                    await release.wait()

        first = asyncio.create_task(worker(first_started, release_first))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second = asyncio.create_task(worker(second_started, attempting=second_attempted))
        await asyncio.wait_for(second_attempted.wait(), timeout=1)
        assert not second_started.is_set()

        await limiter.resize(2)
        await asyncio.wait_for(second_started.wait(), timeout=1)

        release_first.set()
        await asyncio.gather(first, second)

    @pytest.mark.description("Lowering the limiter does not stop running tasks.")
    async def test_resizable_limiter_decrease_is_non_preemptive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        limiter = ResizableLimiter(limit=2)
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        third_started = asyncio.Event()
        release_first = asyncio.Event()
        release_second = asyncio.Event()
        third_wait_attempted = asyncio.Event()
        third_rewait_attempted = asyncio.Event()
        condition = getattr(limiter, "_condition")
        condition_wait = getattr(condition, "wait")
        wait_attempts = 0

        async def observed_condition_wait() -> bool:
            nonlocal wait_attempts
            wait_attempts += 1
            if wait_attempts == 1:
                third_wait_attempted.set()
            elif wait_attempts == 2:
                third_rewait_attempted.set()
            return await condition_wait()

        monkeypatch.setattr(condition, "wait", observed_condition_wait)

        async def worker(started: asyncio.Event, release: asyncio.Event | None = None) -> None:
            async with limiter:
                started.set()
                if release is not None:
                    await release.wait()

        first = asyncio.create_task(worker(first_started, release_first))
        second = asyncio.create_task(worker(second_started, release_second))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.wait_for(second_started.wait(), timeout=1)

        await limiter.resize(1)
        third = asyncio.create_task(worker(third_started))
        await asyncio.wait_for(third_wait_attempted.wait(), timeout=1)
        assert not first.done()
        assert not second.done()
        assert not third_started.is_set()

        release_first.set()
        await first
        await asyncio.wait_for(third_rewait_attempted.wait(), timeout=1)
        assert not third_started.is_set()

        release_second.set()
        await second
        await asyncio.wait_for(third_started.wait(), timeout=1)
        await third

    @pytest.mark.description("The monitor removes completed work and cancels invalid attempts once.")
    async def test_task_monitor(
        self,
        control_postgres_session: Session,
        example_benchmark_object: Benchmark,
        monkeypatch: pytest.MonkeyPatch,
        bound_controls: Any,
    ) -> None:
        benchmark = example_benchmark_object
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        stopped_row = Task(org_id=TEST_ORG_ID, task_id="stopped", benchmark=benchmark.id, status=TaskStatus.STOPPED)
        superseded_row = Task(org_id=TEST_ORG_ID, task_id="superseded", benchmark=benchmark.id)
        control_postgres_session.add_all([stopped_row, superseded_row])
        control_postgres_session.commit()

        done = TrackedTask(asyncio.sleep(0), control, stopped_row.started_at)
        stopped = TrackedTask(asyncio.sleep(0), control, stopped_row.started_at)
        superseded = TrackedTask(asyncio.sleep(0), control, superseded_row.started_at)
        setattr(done, "_status", TrackedTaskStatus.DONE)
        setattr(stopped, "_status", TrackedTaskStatus.RUNNING)
        setattr(superseded, "_status", TrackedTaskStatus.RUNNING)
        stopped_cancel = Mock()
        superseded_cancel = Mock()
        setattr(stopped, "_task", Mock(cancel=stopped_cancel, done=lambda: False))
        setattr(superseded, "_task", Mock(cancel=superseded_cancel, done=lambda: False))
        superseded_row.started_at += timedelta(seconds=1)
        control_postgres_session.add(superseded_row)
        control_postgres_session.commit()
        tracking = {"done": done, "stopped": stopped, "superseded": superseded}
        monitor = TaskMonitor(tracking, limiter=ResizableLimiter(limit=1), control=control)
        sleep_count = 0

        async def complete_cancelled_tasks(_delay: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count == 2:
                setattr(stopped, "_status", TrackedTaskStatus.DONE)
                setattr(superseded, "_status", TrackedTaskStatus.DONE)

        monkeypatch.setattr(task_execution.asyncio, "sleep", complete_cancelled_tasks)
        try:
            await monitor.track_tasks()
        finally:
            for tracked in (done, stopped, superseded):
                getattr(tracked, "_coro").close()
        stopped_cancel.assert_called_once()
        superseded_cancel.assert_called_once()
        assert tracking == {}

    def _monitor_for_task(
        self,
        session: Session,
        benchmark: Benchmark,
        bound_controls: Any,
        *,
        status: TaskStatus,
    ) -> tuple[TaskMonitor, TrackedTask, Mock, TaskControl]:
        session.add(benchmark)
        session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=session)["task_control"])
        row = Task(org_id=TEST_ORG_ID, task_id="monitored", benchmark=benchmark.id, status=status)
        session.add(row)
        session.commit()
        tracked = TrackedTask(asyncio.sleep(0), control, row.started_at)
        setattr(tracked, "_status", TrackedTaskStatus.RUNNING)
        cancellation = Mock()
        setattr(tracked, "_task", Mock(cancel=cancellation, done=lambda: False))

        monitor = TaskMonitor({"monitored": tracked}, limiter=None, control=control)
        return monitor, tracked, cancellation, control

    @pytest.mark.description("A Control database failure propagates without cancelling a monitored task.")
    async def test_task_monitor_operational_error_propagates_without_cancelling(
        self,
        control_postgres_session: Session,
        example_benchmark_object: Benchmark,
        bound_controls: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monitor, tracked, cancellation, control = self._monitor_for_task(
            control_postgres_session,
            example_benchmark_object,
            bound_controls,
            status=TaskStatus.STOPPED,
        )

        async def fail_monitor(_task_ids: list[str]) -> Any:
            raise OperationalError("SELECT", {}, Exception("connection lost"))

        monkeypatch.setattr(control, "monitor", fail_monitor)
        try:
            with pytest.raises(OperationalError):
                await monitor.track_tasks()
        finally:
            getattr(tracked, "_coro").close()
        cancellation.assert_not_called()

    @pytest.mark.description("Unexpected Control monitor errors propagate without cancelling a monitored task.")
    async def test_task_monitor_unexpected_control_error_propagates(
        self,
        control_postgres_session: Session,
        example_benchmark_object: Benchmark,
        bound_controls: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monitor, tracked, cancellation, control = self._monitor_for_task(
            control_postgres_session,
            example_benchmark_object,
            bound_controls,
            status=TaskStatus.STOPPED,
        )

        async def fail_monitor(_task_ids: list[str]) -> Any:
            raise ValueError("invalid benchmark state")

        monkeypatch.setattr(control, "monitor", fail_monitor)
        try:
            with pytest.raises(ValueError, match="invalid benchmark state"):
                await monitor.track_tasks()
        finally:
            getattr(tracked, "_coro").close()
        cancellation.assert_not_called()

    @pytest.mark.description(
        (
            "Tracked task states must match semaphore scheduling and cancellation.\n"
            "\n"
            "Test cases:\n"
            "- A new task stays waiting until its semaphore slot is available.\n"
            "- A running task returns its result and becomes done.\n"
            "- Cancellation returns the task-shaped empty result and becomes done."
        )
    )
    async def test_tracked_task(self) -> None:

        async def controlled_result(
            task_id: str,
            started: asyncio.Event,
            release: asyncio.Event,
        ) -> dict[str, dict[str, str]]:
            started.set()
            await release.wait()
            return {task_id: {"result": task_id}}

        limiter = ResizableLimiter(limit=1)
        running_started = asyncio.Event()
        release_running = asyncio.Event()
        waiting_attempted = asyncio.Event()
        waiting_started = asyncio.Event()
        release_waiting = asyncio.Event()

        def task_state(task_id: str) -> TaskState:
            return TaskState(
                id=uuid4(),
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=uuid4(),
                started_at=datetime.now(UTC),
                status=TaskStatus.PENDING,
                eval_resume_state=None,
                credited_wall_deadline_at=None,
                build_phase=None,
                evaluation_phase=None,
            )

        running_row = task_state("task_id_1")
        waiting_row = task_state("task_id_2")
        control = Mock(spec=TaskControl)
        running = TrackedTask(
            controlled_result("task_id_1", running_started, release_running), control, running_row.started_at
        )
        waiting = TrackedTask(
            controlled_result("task_id_2", waiting_started, release_waiting), control, waiting_row.started_at
        )

        assert running.status == TrackedTaskStatus.WAITING
        assert running.task is None

        running_call = asyncio.create_task(running.run(limiter, running_row))
        await running_started.wait()

        async def run_waiting() -> dict[str, dict[str, Any] | None]:
            waiting_attempted.set()
            return await waiting.run(limiter, waiting_row)

        waiting_call = asyncio.create_task(run_waiting())
        await waiting_attempted.wait()

        assert running.status == TrackedTaskStatus.RUNNING
        assert waiting.status == TrackedTaskStatus.WAITING
        assert not waiting_started.is_set()

        assert waiting.task is not None
        waiting.task.cancel()
        release_running.set()
        results = await asyncio.gather(running_call, waiting_call)

        assert results == [{"task_id_1": {"result": "task_id_1"}}, {"task_id_2": None}]
        assert running.status == TrackedTaskStatus.DONE
        assert waiting.status == TrackedTaskStatus.DONE
        assert running.task is not None
        assert running.task.result() == {"task_id_1": {"result": "task_id_1"}}
        assert waiting.task is not None
        with pytest.raises(asyncio.CancelledError):
            waiting.task.result()


def _terminal_task_state() -> TaskState:
    return TaskState(
        id=uuid4(),
        org_id=TEST_ORG_ID,
        task_id="task_0",
        benchmark=uuid4(),
        started_at=datetime.now(UTC),
        status=TaskStatus.PENDING,
        eval_resume_state=None,
        credited_wall_deadline_at=None,
        build_phase=None,
        evaluation_phase=None,
    )


@pytest.mark.description("A tracked task that is cancelled or raises an unhandled error logs that stable event.")
@pytest.mark.parametrize(
    "error,level,expected_event",
    [
        (asyncio.CancelledError(), "warning", "task.cancelled"),
        (RuntimeError("boom"), "error", "task.unhandled_error"),
    ],
)
async def test_tracked_task_logs_terminal_event(
    monkeypatch: pytest.MonkeyPatch, error: BaseException, level: str, expected_event: str
) -> None:
    async def fail() -> dict[str, dict[str, Any] | None]:
        raise error

    state = _terminal_task_state()
    control = Mock(spec=TaskControl)
    control.ref = DispatchRef(benchmark_id=state.benchmark, dispatch_id=uuid4())
    control.fail = AsyncMock(return_value=True)
    log = Mock()
    monkeypatch.setattr(task_execution.logger, level, log)
    monkeypatch.setattr(task_execution, "capture_exception", Mock())
    tracked = TrackedTask(fail(), control, state.started_at)

    assert await tracked.run(None, state) == {"task_0": None}
    assert [call.kwargs["extra"]["event"] for call in log.call_args_list] == [expected_event]


@pytest.mark.description(
    (
        "A tracked task counts one task outcome only for the terminal state it reaches.\n"
        "\n"
        "Test cases:\n"
        "- A cancelled task counts as cancelled.\n"
        "- An unhandled error counts once the ERROR write succeeds.\n"
        "- A refused ERROR write counts nothing.\n"
        "- A cancellation during the ERROR write counts nothing."
    )
)
@pytest.mark.parametrize(
    "error,write_result,expected_outcomes",
    [
        (asyncio.CancelledError(), None, ["cancelled"]),
        (RuntimeError("boom"), True, ["unhandled_error"]),
        (RuntimeError("boom"), False, []),
        (RuntimeError("boom"), asyncio.CancelledError(), []),
    ],
)
async def test_tracked_task_records_terminal_outcome_metric(
    monkeypatch: pytest.MonkeyPatch,
    error: BaseException,
    write_result: bool | BaseException | None,
    expected_outcomes: list[str],
) -> None:
    outcome_tags: list[dict[str, Any]] = []

    def record_outcome_metric(name: str, _value: float = 1, tags: dict[str, Any] | None = None) -> None:
        if name == "valkyrie.task.outcome":
            outcome_tags.append(dict(tags or {}))

    async def fail() -> dict[str, dict[str, Any] | None]:
        raise error

    control = Mock(spec=TaskControl)
    state = _terminal_task_state()
    control.ref = DispatchRef(benchmark_id=state.benchmark, dispatch_id=uuid4())
    if isinstance(write_result, BaseException):
        control.fail = AsyncMock(side_effect=write_result)
    else:
        control.fail = AsyncMock(return_value=write_result)

    monkeypatch.setattr("tracker.observability.metrics.incr", record_outcome_metric)
    monkeypatch.setattr(task_execution, "capture_exception", Mock())
    tracked = TrackedTask(fail(), control, state.started_at)

    if isinstance(write_result, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await tracked.run(None, state)
    else:
        assert await tracked.run(None, state) == {"task_0": None}
    assert [tags["outcome"] for tags in outcome_tags] == expected_outcomes
