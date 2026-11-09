from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest  # pyright: ignore[reportMissingImports]
from botocore.exceptions import ClientError  # pyright: ignore[reportMissingImports]
from fastapi.testclient import TestClient  # pyright: ignore[reportMissingImports]
from sqlalchemy.exc import OperationalError  # pyright: ignore[reportMissingImports]
from sqlmodel import Session  # pyright: ignore[reportMissingImports]

from main import app
from tests.factories import make_task
from tracker.api import logs as logs_api
from tracker.api.dependencies import get_run_runtime
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import CloudWatchLogProvider
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    LocalBenchmarkArguments,
    Org,
    RunMonitoring,
    RunMonitoringRules,
    TaskStatus,
)
from tracker.local.logs import FilesystemLogs
from tracker.runtime.logs import (
    LogEvent,
    LogPage,
    LogProvider,
    RunLogReference,
    RunTaskLogReference,
    TaskLogLine,
    TaskLogReference,
)
from tracker.services.tasks.execute import task_attempt_id

_client = TestClient(app)


class MockLogProvider(LogProvider):
    def __init__(self, session: Session | None = None) -> None:
        self.session = session
        self.stream_transaction_open: bool | None = None
        self.task_reference: TaskLogReference | None = None
        self.run_reference: RunLogReference | None = None
        self.query: str | None = None
        self.stream_release: asyncio.Event | None = None
        self.stream_closed = False

    async def fetch(
        self,
        reference: RunLogReference | TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        self.query = query
        if isinstance(reference, TaskLogReference):
            self.task_reference = reference
            return LogPage(
                events=[
                    LogEvent(
                        timestamp=start_time or reference.started_at,
                        message="task log",
                        task_id=reference.task_id,
                    )
                ],
                next_cursor=cursor,
            )

        self.run_reference = reference
        timestamp = start_time or datetime(2026, 1, 1, tzinfo=timezone.utc)
        return LogPage(
            events=[
                LogEvent(timestamp=timestamp, message="first", task_id="task-1"),
                LogEvent(timestamp=timestamp, message="second", task_id="task-2"),
            ]
        )

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        self.task_reference = reference
        self.query = query
        if self.session is not None:
            self.stream_transaction_open = self.session.in_transaction()
        try:
            if self.stream_release is not None:
                await self.stream_release.wait()
            yield LogEvent(timestamp=start_time or reference.started_at, message="streamed", task_id=reference.task_id)
        finally:
            self.stream_closed = True


class FailingClients:
    def cloudwatch_logs_client(self) -> object:
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "FilterLogEvents",
        )


def _override_provider(monkeypatch: pytest.MonkeyPatch, provider: LogProvider) -> None:
    monkeypatch.setitem(app.dependency_overrides, get_run_runtime, lambda: SimpleNamespace(log_reader=provider))


@pytest.mark.description("Client construction failures must remain provider-neutral in snapshots and streams.")
def test_client_construction_failure_uses_snapshot_and_sse_error_paths(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = CloudWatchLogProvider(cast(AWSClientProvider, FailingClients()), "benchmarks")
    _override_provider(monkeypatch, provider)

    snapshot_response = _client.get(f"/benchmarks/{benchmark.id}/logs")
    stream_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs/stream",
        params={"task_id": task.task_id},
    )

    assert snapshot_response.status_code == 502
    assert snapshot_response.json() == {"detail": "CloudWatch log request failed: AccessDeniedException"}
    assert stream_response.status_code == 200
    assert "event: error" in stream_response.text
    assert '"detail": "CloudWatch log request failed: AccessDeniedException"' in stream_response.text


@pytest.mark.description("Task and aggregate endpoints must resolve scoped identities without exposing AWS locations.")
def test_task_and_run_logs_use_scoped_provider_references(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    first_task = make_task(benchmark, "provider/model:fast")
    second_task = make_task(benchmark, "task-2")
    database_session.add_all([benchmark, first_task, second_task])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)

    task_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "task_id": first_task.task_id,
            "query": "needle",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-01T01:00:00+00:00",
        },
    )
    run_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={"query": "aggregate"},
    )

    assert task_response.status_code == 200
    assert task_response.json()["events"][0]["task_id"] == first_task.task_id
    assert provider.task_reference == TaskLogReference(
        run_id=benchmark.id,
        task_id=first_task.task_id,
        started_at=first_task.started_at,
        siblings=(RunTaskLogReference(task_id=second_task.task_id, started_at=second_task.started_at),),
    )
    assert run_response.status_code == 200
    assert [event["task_id"] for event in run_response.json()["events"]] == ["task-1", "task-2"]
    assert provider.run_reference == RunLogReference(
        run_id=benchmark.id,
        tasks=(
            RunTaskLogReference(task_id=first_task.task_id, started_at=first_task.started_at),
            RunTaskLogReference(task_id=second_task.task_id, started_at=second_task.started_at),
        ),
    )
    assert task_response.request.url.params["task_id"] == first_task.task_id
    assert "log_group" not in task_response.request.url.params
    assert "log_stream" not in task_response.request.url.params


@pytest.mark.description("Log endpoints must reject ambiguous bounds and runs outside the active organization.")
def test_log_routes_validate_time_and_prevent_cross_org_access(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    other_org = Org(id=uuid4(), name="other")
    other_benchmark = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    database_session.add_all([benchmark, task, other_org, other_benchmark])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)

    naive_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={"start_time": "2026-01-01T00:00:00"},
    )
    reversed_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "start_time": "2026-01-01T01:00:00+00:00",
            "end_time": "2026-01-01T00:00:00+00:00",
        },
    )
    equal_response = _client.get(
        f"/benchmarks/{benchmark.id}/logs",
        params={
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-01T00:00:00+00:00",
        },
    )
    other_response = _client.get(f"/benchmarks/{other_benchmark.id}/logs")

    assert naive_response.status_code == 422
    assert reversed_response.status_code == 422
    assert equal_response.status_code == 422
    assert other_response.status_code == 404
    assert provider.run_reference is None


@pytest.mark.description(
    (
        "The task stream must release its transaction before yielding typed log events.\n"
        "\n"
        "Test cases:\n"
        "- Literal queries and slash-containing task IDs reach the provider.\n"
        "- CloudWatch polling starts without an open database transaction."
    )
)
def test_task_log_stream_returns_sse_events(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "provider/model:fast")
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = MockLogProvider(database_session)
    _override_provider(monkeypatch, provider)

    response = _client.get(
        f"/benchmarks/{benchmark.id}/logs/stream",
        params={"task_id": task.task_id, "query": "needle"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert "event: log" in response.text
    assert '"message":"streamed"' in response.text
    assert "event: end" in response.text
    assert provider.query == "needle"
    assert response.request.url.params["task_id"] == task.task_id
    assert provider.stream_transaction_open is False


@pytest.mark.description("Idle streams must emit comments and cancel the pending provider read only when closed.")
async def test_task_log_stream_sends_keep_alives_without_closing_pending_provider() -> None:
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="provider/model:fast",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    events = logs_api._stream_events(  # pyright: ignore[reportPrivateUsage]
        provider,
        reference,
        query=None,
        start_time=None,
        end_time=None,
        keep_alive_interval=0,
    )

    assert await anext(events) == ": connected\n\n"
    assert await anext(events) == ": keep-alive\n\n"
    assert await anext(events) == ": keep-alive\n\n"
    assert not provider.stream_closed

    await events.aclose()

    assert provider.stream_closed


class QueuedLogProvider(MockLogProvider):
    def __init__(self) -> None:
        super().__init__()
        self.events: asyncio.Queue[LogEvent] = asyncio.Queue()

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        try:
            while True:
                yield await self.events.get()
        finally:
            self.stream_closed = True


def _follow(
    provider: LogProvider, attempt_end: logs_api._AttemptEnd | None
) -> tuple[AsyncIterator[str], list[logs_api._AttemptEnd | None]]:  # pyright: ignore[reportPrivateUsage]
    reference = TaskLogReference(
        run_id=uuid4(),
        task_id="provider/model:fast",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    state = [attempt_end]

    async def current_end() -> logs_api._AttemptEnd | None:  # pyright: ignore[reportPrivateUsage]
        return state[0]

    events = logs_api._stream_events(  # pyright: ignore[reportPrivateUsage]
        provider,
        reference,
        query=None,
        start_time=None,
        end_time=None,
        attempt_end=current_end,
        keep_alive_interval=0,
        finished_check_interval=0,
    )
    return events, state


@pytest.mark.description(
    "A follow stream must end once its task attempt finished, even while the provider keeps polling."
)
async def test_task_log_stream_ends_after_task_finishes() -> None:
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    events, state = _follow(provider, None)

    assert await anext(events) == ": connected\n\n"
    assert await anext(events) == ": keep-alive\n\n"
    state[0] = logs_api._AttemptEnd(  # pyright: ignore[reportPrivateUsage]
        ended_at=datetime.now(timezone.utc) - timedelta(minutes=1)
    )
    assert await anext(events) == "event: end\ndata: {}\n\n"
    assert provider.stream_closed


@pytest.mark.description(
    "Lines that arrive after the task ended are still sent, and the stream ends only once the grace has passed since the last one."
)
async def test_task_log_stream_keeps_lines_that_arrive_after_the_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_LOG_GRACE", timedelta(seconds=0.3))
    provider = QueuedLogProvider()
    ended_at = datetime.now(timezone.utc)
    events, _ = _follow(provider, logs_api._AttemptEnd(ended_at=ended_at))  # pyright: ignore[reportPrivateUsage]

    assert await anext(events) == ": connected\n\n"
    await asyncio.sleep(0.2)
    provider.events.put_nowait(LogEvent(timestamp=ended_at, message="[ERROR] boom", task_id="provider/model:fast"))
    received: list[str] = []
    started = time.monotonic()
    async for chunk in events:
        received.append(chunk)

    assert any("[ERROR] boom" in chunk for chunk in received)
    assert received[-1] == "event: end\ndata: {}\n\n"
    assert time.monotonic() - started >= 0.3
    assert provider.stream_closed


@pytest.mark.description("The stream route must end the SSE stream for a finished, failed, or stopped task.")
@pytest.mark.parametrize("status", [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED])
def test_task_log_stream_route_ends_for_ended_task(
    status: TaskStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    started_at = datetime.now(timezone.utc) - timedelta(hours=1)
    task = make_task(benchmark, "provider/model:fast", status=status, started_at=started_at)
    database_session.add_all([benchmark, task])
    database_session.commit()
    assert (task.finished_at is None) == (status is TaskStatus.STOPPED)
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    _override_provider(monkeypatch, provider)
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_CHECK_INTERVAL", 0.01)
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_LOG_GRACE", timedelta(seconds=0.05))

    response = _client.get(f"/benchmarks/{benchmark.id}/logs/stream", params={"task_id": task.task_id})

    assert response.status_code == 200
    assert response.text.endswith("event: end\ndata: {}\n\n")
    assert provider.stream_closed


@pytest.mark.description(
    "A brief database outage in the stream end check is retried, and the stream still ends with `event: end`."
)
def test_task_log_stream_route_retries_end_check_after_database_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "provider/model:fast", status=TaskStatus.FINISHED)
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    _override_provider(monkeypatch, provider)
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_CHECK_INTERVAL", 0.01)
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_LOG_GRACE", timedelta(seconds=0.05))
    attempt_end = logs_api._attempt_end
    calls: list[int] = []

    captured: list[BaseException] = []

    def failing_briefly(*args: object) -> object:
        calls.append(1)
        if len(calls) < logs_api._END_CHECK_MAX_FAILURES:
            raise OperationalError("SELECT 1", {}, Exception("connection reset"))
        return attempt_end(*args)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(logs_api, "_attempt_end", failing_briefly)
    monkeypatch.setattr(logs_api, "capture_exception", captured.append)

    response = _client.get(f"/benchmarks/{benchmark.id}/logs/stream", params={"task_id": task.task_id})

    assert response.status_code == 200
    assert len(calls) == logs_api._END_CHECK_MAX_FAILURES
    assert "event: error" not in response.text
    assert response.text.endswith("event: end\ndata: {}\n\n")
    assert not captured
    assert provider.stream_closed


@pytest.mark.description(
    "A database error that persists through every end check ends the stream with `event: error` and reaches Sentry."
)
def test_task_log_stream_route_reports_persistent_end_check_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "provider/model:fast", status=TaskStatus.FINISHED)
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = MockLogProvider()
    provider.stream_release = asyncio.Event()
    _override_provider(monkeypatch, provider)
    monkeypatch.setattr(logs_api, "_FINISHED_TASK_CHECK_INTERVAL", 0.01)
    calls: list[int] = []
    captured: list[BaseException] = []
    error = OperationalError("SELECT 1", {}, Exception("connection refused"))

    def always_failing(*args: object) -> object:
        calls.append(1)
        raise error

    monkeypatch.setattr(logs_api, "_attempt_end", always_failing)
    monkeypatch.setattr(logs_api, "capture_exception", captured.append)

    response = _client.get(f"/benchmarks/{benchmark.id}/logs/stream", params={"task_id": task.task_id})

    assert response.status_code == 200
    assert len(calls) == logs_api._END_CHECK_MAX_FAILURES
    assert captured == [error]
    assert response.text.endswith('event: error\ndata: {"detail": "Task status is unavailable"}\n\n')
    assert "event: end" not in response.text
    assert provider.stream_closed


@pytest.mark.description("Local logs come from the filesystem without resolving AWS.")
def test_local_logs_use_filesystem_without_aws_resolution(
    tmp_path: Path, database_session: Session, example_benchmark_object: Benchmark, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tracker.local.resources import LocalResources
    from tracker.runtime.logs import task_log_stream_name

    benchmark = example_benchmark_object
    benchmark.arguments = LocalBenchmarkArguments.model_validate(
        {
            **benchmark.arguments.model_dump(),
            "environment": "local",
            "properties": LocalResources(data_root=tmp_path),
            "sandbox_provider": "docker",
        }
    )
    task = make_task(benchmark, "local-task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    logs = FilesystemLogs(tmp_path / "orgs" / str(benchmark.org_id) / "logs")
    asyncio.run(logs.create_benchmark(str(benchmark.id), retention_days=0))
    logs.write(
        f"{benchmark.id}:{task_log_stream_name(task.task_id, task.started_at)}",
        [TaskLogLine(received_at=time.time(), message="local execution output", fields={})],
    )

    def unexpected_aws(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Local log access resolved AWS credentials")

    monkeypatch.setattr("tracker.api.dependencies.resolve_run_aws_runtime", unexpected_aws)
    for params in ({}, {"task_id": task.task_id}):
        response = _client.get(f"/benchmarks/{benchmark.id}/logs", params=params)
        assert response.status_code == 200, response.text
        assert response.json()["events"][0]["message"] == "local execution output"


@pytest.mark.description("A retried task keeps its `task_row_id` in the monitor task list but gets a new `attempt_id`.")
def test_monitor_task_identities_are_scoped_and_change_on_retry(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    other_org = Org(id=uuid4(), name="other")
    foreign_run = Benchmark(org_id=other_org.id, name=benchmark.name, arguments=benchmark.arguments)
    foreign_task = make_task(benchmark, "foreign-task")
    foreign_task.org_id = other_org.id
    database_session.add_all([benchmark, task, other_org, foreign_run, foreign_task])
    database_session.commit()

    response = _client.get(f"/benchmarks/{benchmark.id}/monitor/tasks")
    assert response.status_code == 200
    identities = response.json()["tasks"]
    assert len(identities) == 1
    assert identities[0]["task_id"] == task.task_id
    assert identities[0]["task_row_id"] == str(task.id)
    assert identities[0]["attempt_id"] == task_attempt_id(task.id, task.started_at)
    assert datetime.fromisoformat(identities[0]["started_at"]).utcoffset() == timedelta(0)
    assert _client.get(f"/benchmarks/{foreign_run.id}/monitor/tasks").status_code == 404

    task.started_at += timedelta(seconds=1)
    database_session.add(task)
    database_session.commit()
    retried = _client.get(f"/benchmarks/{benchmark.id}/monitor/tasks").json()["tasks"][0]
    assert retried["task_row_id"] == identities[0]["task_row_id"]
    assert retried["attempt_id"] != identities[0]["attempt_id"]


@pytest.mark.description("The log attempt guard rejects a stale attempt before fetching from the provider.")
@pytest.mark.parametrize("suffix", ["logs", "logs/stream"])
def test_log_attempt_guard_rejects_stale_before_provider_fetch(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    provider = MockLogProvider()
    _override_provider(monkeypatch, provider)
    url = f"/benchmarks/{benchmark.id}/{suffix}"
    response = _client.get(url, params={"task_id": task.task_id, "expected_attempt_id": "stale"})
    assert response.status_code == 409
    assert provider.task_reference is None
    assert provider.run_reference is None
    valid = _client.get(
        url, params={"task_id": task.task_id, "expected_attempt_id": task_attempt_id(task.id, task.started_at)}
    )
    assert valid.status_code == 200
    assert provider.task_reference is not None
    if suffix == "logs":
        assert _client.get(url, params={"expected_attempt_id": "stale"}).status_code == 422


@pytest.mark.description("Guarded logs reject a request with no source.")
@pytest.mark.parametrize("suffix", ["logs", "logs/stream"])
def test_guarded_logs_reject_missing_source(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    suffix: str,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    _override_provider(monkeypatch, FilesystemLogs(tmp_path))
    response = _client.get(
        f"/benchmarks/{benchmark.id}/{suffix}",
        params={"task_id": task.task_id, "expected_attempt_id": task_attempt_id(task.id, task.started_at)},
    )
    assert response.status_code == 503
    if suffix == "logs":
        legacy = _client.get(f"/benchmarks/{benchmark.id}/logs", params={"task_id": task.task_id})
        assert legacy.status_code == 200
        assert legacy.json()["events"] == []
        assert legacy.json()["available"] is False


@pytest.mark.description("The monitored runs list holds only the org's in progress runs that opted in.")
def test_monitored_runs_list_only_opted_in_in_progress_runs_of_the_org(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    monitoring = RunMonitoring()
    monitored = example_benchmark_object
    rules = RunMonitoringRules(
        policy="Stay in scope.",
        watcher_policy_pack="core+web",
        allowed_cidrs=["10.0.0.0/8"],
        allowed_hosts=["range.test"],
    )
    monitored.arguments = monitored.arguments.model_copy(update={"monitoring": monitoring, "monitoring_rules": rules})
    unmonitored = Benchmark(org_id=monitored.org_id, name=monitored.name, arguments=example_benchmark_object.arguments)
    unmonitored.arguments = unmonitored.arguments.model_copy(update={"monitoring": None})
    finished = Benchmark(
        org_id=monitored.org_id,
        name=monitored.name,
        arguments=monitored.arguments,
        status=BenchmarkStatus.FINISHED,
        finished_at=datetime.now(timezone.utc),
    )
    other_org = Org(id=uuid4(), name="other")
    foreign = Benchmark(org_id=other_org.id, name=monitored.name, arguments=monitored.arguments)
    database_session.add_all([monitored, unmonitored, finished, other_org, foreign])
    database_session.commit()

    response = _client.get("/monitor/runs")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "runs": [
            {
                "benchmark_id": str(monitored.id),
                "benchmark_name": monitored.name,
                "agent_name": monitored.arguments.contract.name,
                "policy": "Stay in scope.",
                "watcher_policy_pack": "core+web",
                "allowed_cidrs": ["10.0.0.0/8"],
                "allowed_hosts": ["range.test"],
            }
        ]
    }
