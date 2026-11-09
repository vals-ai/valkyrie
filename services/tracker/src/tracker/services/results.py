import asyncio
from asyncio import CancelledError
import base64
import binascii
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial
from typing import Any, Sequence
from uuid import UUID

from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.database.models import (
    FINISHED_BENCHMARK_STATUSES,
    Benchmark,
    BenchmarkStatus,
    Org,
    TaskStatus,
)
from tracker.database.session import database_call
from tracker.repositories.benchmark import BenchmarkRepository, FinishedEvaluationRow
from tracker.logging import get_logger
from tracker.observability.sentry import capture_exception
from tracker.types import (
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
)

logger = get_logger(__name__)


_FINISHED_STATUSES = (TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)


def benchmark_details(benchmark_row: Benchmark, breakdown: dict[TaskStatus, int]) -> BenchmarkDetails:
    return BenchmarkDetails(
        status=benchmark_row.status,
        started_at=benchmark_row.started_at,
        total_tasks=sum(breakdown.values()),
        finished_tasks=sum(breakdown.get(status, 0) for status in _FINISHED_STATUSES),
        task_breakdown=breakdown,
        docent_reading_status=benchmark_row.docent_reading_status,
        docent_reading_url=benchmark_row.docent_reading_url,
    )


def benchmark_response(
    benchmark_row: Benchmark,
    breakdown: dict[TaskStatus, int],
    s3_bucket_url: str,
    storage_bucket: str | None,
) -> FetchBenchmarkResponse:
    return FetchBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        benchmark_id=benchmark_row.id,
        details=benchmark_details(benchmark_row, breakdown),
        s3_bucket_url=s3_bucket_url,
        storage_bucket=storage_bucket,
        label=benchmark_row.label,
        executor_release_id=benchmark_row.executor_release_id,
        current_execution_release_id=benchmark_row.current_execution_release_id,
        executor_artifact_digest=benchmark_row.executor_artifact_digest,
        executor_protocol_version=benchmark_row.executor_protocol_version,
        final_score=benchmark_row.final_evaluation.final_score if benchmark_row.final_evaluation else None,
        error_message=benchmark_row.error_message if benchmark_row.status == BenchmarkStatus.ERROR else None,
    )


def fetch_benchmark_response(
    benchmark_row: Benchmark,
    session: Session,
    s3_bucket_url: str,
    storage_bucket: str | None,
) -> FetchBenchmarkResponse:
    breakdown = BenchmarkRepository(session).task_state_counts([benchmark_row.id]).get(benchmark_row.id, {})
    return benchmark_response(benchmark_row, breakdown, s3_bucket_url, storage_bucket)


def _fetch_result_histories(
    session: Session,
    task_row_ids: Sequence[UUID],
    evaluation_rows: Sequence[tuple[UUID, datetime, dict[str, Any]]],
    org_id: UUID,
) -> dict[UUID, list[dict[str, Any]]]:
    error_rows = BenchmarkRepository(session).final_errors(org_id, task_row_ids)

    histories: dict[UUID, list[dict[str, Any]]] = {}
    for task_row_id, created_at, result in evaluation_rows:
        histories.setdefault(task_row_id, []).append({"created_at": created_at.isoformat(), "result": dict(result)})
    for task_row_id, created_at, error_message, category in error_rows:
        histories.setdefault(task_row_id, []).append(
            {
                "created_at": created_at.isoformat(),
                "error_message": error_message,
                "failure_category": category.value if category else None,
            }
        )

    for entries in histories.values():
        entries.sort(key=lambda entry: entry["created_at"], reverse=True)
    return histories


def fetch_evaluation_results(benchmark_id: UUID, session: Session, org_id: UUID) -> dict[str, dict[str, Any]]:
    results = BenchmarkRepository(session).finished_evaluations(org_id, benchmark_id)

    latest_results: list[FinishedEvaluationRow] = []
    historical_results: list[tuple[UUID, datetime, dict[str, Any]]] = []
    seen_task_row_ids: set[UUID] = set()
    for row in results:
        _evaluation_id, created_at, result, _exit_reason, task_row_id, _task_id, _task_breakdown = row
        if task_row_id not in seen_task_row_ids:
            latest_results.append(row)
            seen_task_row_ids.add(task_row_id)
        else:
            historical_results.append((task_row_id, created_at, result))

    latest_results.sort(
        key=lambda row: (row[1], row[0]),
        reverse=True,
    )

    histories = _fetch_result_histories(
        session,
        list(seen_task_row_ids),
        historical_results,
        org_id,
    )

    evaluation_results: dict[str, dict[str, Any]] = {}
    for _evaluation_id, _created_at, result, exit_reason, task_row_id, task_id, task_breakdown in latest_results:
        result_data = dict(result)
        result_data["agent_caused_exit_reason"] = exit_reason
        if task_breakdown is not None:
            result_data["task_breakdown"] = task_breakdown.model_dump()
        task_history = histories.get(task_row_id, [])
        result_data["attempts"] = len(task_history) + 1
        if task_history:
            result_data["history"] = task_history
        evaluation_results[task_id] = result_data

    return evaluation_results


def fetch_average_task_breakdown(benchmark_id: UUID, session: Session, org_id: UUID) -> AverageTaskBreakdown | None:
    row = BenchmarkRepository(session).average_task_breakdown(org_id, benchmark_id)

    if all(v is None for v in row):
        return None

    return AverageTaskBreakdown(
        sandbox_build_duration=row[0],
        agent_run_duration=row[1],
        evaluation_run_duration=row[2],
        sandbox_run_duration=row[3],
    )


RESULT_POLL_INTERVAL = 5

_RunSnapshot = dict[UUID, tuple[Benchmark, dict[TaskStatus, int]]]


def _read_runs(bind: Engine | Connection | None, benchmark_ids: Sequence[UUID]) -> _RunSnapshot:
    with Session(bind=bind) as session:
        repository = BenchmarkRepository(session)
        breakdowns = repository.task_state_counts(benchmark_ids)
        return {
            row.id: (row, breakdowns.get(row.id, {})) for row in repository.list_with_final_evaluation(benchmark_ids)
        }


class _ResultPoller:
    def __init__(self, bind: Engine | Connection | None) -> None:
        self._bind = bind
        self._subscribers: dict[UUID, set[asyncio.Queue[_RunSnapshot]]] = {}
        self._task: asyncio.Task[None] | None = None

    @asynccontextmanager
    async def subscribe(self, benchmark_id: UUID) -> AsyncIterator[asyncio.Queue[_RunSnapshot]]:
        updates: asyncio.Queue[_RunSnapshot] = asyncio.Queue(maxsize=1)
        self._subscribers.setdefault(benchmark_id, set()).add(updates)
        if self._task is None:
            self._task = asyncio.create_task(self._poll())
        try:
            yield updates
        finally:
            watchers = self._subscribers[benchmark_id]
            watchers.discard(updates)
            if not watchers:
                del self._subscribers[benchmark_id]
            if not self._subscribers and self._task is not None:
                self._task.cancel()
                self._task = None

    async def _poll(self) -> None:
        while True:
            await asyncio.sleep(RESULT_POLL_INTERVAL)
            benchmark_ids = list(self._subscribers)
            try:
                snapshot = await database_call(partial(_read_runs, self._bind, benchmark_ids))
            except Exception as error:
                logger.exception("Result stream poll failed for %d runs", len(benchmark_ids))
                capture_exception(error)
                continue
            for benchmark_id in benchmark_ids:
                for updates in self._subscribers.get(benchmark_id, ()):
                    if updates.full():
                        updates.get_nowait()
                    updates.put_nowait(snapshot)


_result_pollers: dict[Engine | Connection | None, _ResultPoller] = {}


async def stream_benchmark_results(
    benchmark_id: UUID,
    bind: Engine | Connection | None,
    s3_bucket_url: str,
    org_id: UUID,
    *,
    storage_bucket: str | None,
) -> AsyncGenerator[str]:
    EVENT_COMPLETE = "event: complete\n\n"
    EVENT_ERROR = "event: error\ndata:"
    DATA_PREFIX = "data:"
    DISCONNECT = "event: disconnect\n\n"

    def stream_event(snapshot: _RunSnapshot) -> tuple[str, bool]:
        run = snapshot.get(benchmark_id)
        if run is None or run[0].org_id != org_id:
            return f"{EVENT_ERROR} {json.dumps({'error': 'Run not found'})}\n\n", True
        benchmark_row, breakdown = run
        response = benchmark_response(benchmark_row, breakdown, s3_bucket_url, storage_bucket)
        data = f"{DATA_PREFIX} {response.model_dump_json()}\n\n"
        if benchmark_row.status in FINISHED_BENCHMARK_STATUSES:
            return data + EVENT_COMPLETE, True
        return data, False

    try:
        snapshot = await database_call(partial(_read_runs, bind, [benchmark_id]))
        async with _result_pollers.setdefault(bind, _ResultPoller(bind)).subscribe(benchmark_id) as updates:
            while True:
                event, complete = stream_event(snapshot)
                yield event
                if complete:
                    break
                snapshot = await updates.get()
    except CancelledError:
        logger.info("Client disconnected from benchmark %s stream", benchmark_id)
        yield DISCONNECT


class InvalidCursorError(ValueError):
    pass


def encode_cursor(started_at: datetime, row_id: UUID) -> str:
    payload = json.dumps({"started_at": started_at.isoformat(), "id": str(row_id)})
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        return datetime.fromisoformat(payload["started_at"]), UUID(payload["id"])
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise InvalidCursorError("Invalid benchmark cursor") from exc


def fetch_filtered_benchmark_rows(
    request: FetchBenchmarksRequest, session: Session, org: Org
) -> tuple[Sequence[Benchmark], int | None, str | None]:

    repository = BenchmarkRepository(session)
    if request.cursor is not None:
        after = decode_cursor(request.cursor) if request.cursor else None
        benchmark_rows = repository.list_filtered(request, org.id, limit=request.limit + 1, after=after)

        next_cursor: str | None = None
        if len(benchmark_rows) > request.limit:
            benchmark_rows = benchmark_rows[: request.limit]
            last_row = benchmark_rows[-1]
            next_cursor = encode_cursor(last_row.started_at, last_row.id)

        return benchmark_rows, None, next_cursor

    total_count = repository.count_filtered(request, org.id)

    if not total_count:
        return [], 0, None

    benchmark_rows = repository.list_filtered(request, org.id, limit=request.limit, offset=request.offset)

    return benchmark_rows, total_count, None


def build_benchmark_table_rows(benchmarks: Sequence[Benchmark], session: Session) -> list[BenchmarkTableRow]:
    if not benchmarks:
        return []

    counts_by_bench = BenchmarkRepository(session).task_state_counts([b.id for b in benchmarks])

    rows: list[BenchmarkTableRow] = []
    for b in benchmarks:
        counts = counts_by_bench.get(b.id, {})
        rows.append(
            BenchmarkTableRow(
                id=b.id,
                name=b.name,
                agent_name=b.arguments.contract.name,
                label=b.label,
                model=b.arguments.contract.model,
                dataset=b.arguments.dataset or "default",
                executor_release_id=b.executor_release_id,
                current_execution_release_id=b.current_execution_release_id,
                executor_artifact_digest=b.executor_artifact_digest,
                executor_protocol_version=b.executor_protocol_version,
                error_message=b.error_message if b.status == BenchmarkStatus.ERROR else None,
                started_by_email=b.started_by_email,
                started_at=b.started_at,
                finished_at=b.finished_at,
                status=b.status,
                total_tasks=sum(counts.values()),
                finished_tasks=sum(counts.get(status, 0) for status in _FINISHED_STATUSES),
                task_state_counts={k.value: v for k, v in counts.items()},
                final_score=b.final_evaluation.final_score if b.final_evaluation else None,
            )
        )
    return rows
