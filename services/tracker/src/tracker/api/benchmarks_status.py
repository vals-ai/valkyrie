from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from tracker.api.parsing import parse_csv
from tracker.auth import get_current_org
from tracker.database.models import Org, TaskStatus
from tracker.repositories.benchmark import BenchmarkRepositoryDependency
from tracker.types import BenchmarkStatusEntry, BenchmarkStatusResponse

router = APIRouter(prefix="/benchmarks")


@router.get(
    "/status",
    response_model=BenchmarkStatusResponse,
    description="Return the status and task counts of each listed run.",
)
def get_benchmarks_status(
    repository: BenchmarkRepositoryDependency,
    ids: str = Query(default=""),
    org: Org = Depends(get_current_org),
) -> BenchmarkStatusResponse:
    try:
        parsed_ids = parse_csv(ids, UUID)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="ids must be a comma-separated list of run UUIDs") from error
    if not parsed_ids:
        return BenchmarkStatusResponse(entries=[])

    benchmarks = repository.list_by_ids(org.id, parsed_ids)
    counts_by_benchmark_id = repository.task_state_counts([benchmark.id for benchmark in benchmarks])

    entries: list[BenchmarkStatusEntry] = []
    for benchmark in benchmarks:
        counts = counts_by_benchmark_id.get(benchmark.id, {})
        entries.append(
            BenchmarkStatusEntry(
                id=benchmark.id,
                status=benchmark.status,
                finished_at=benchmark.finished_at,
                executor_release_id=benchmark.executor_release_id,
                current_execution_release_id=benchmark.current_execution_release_id,
                executor_artifact_digest=benchmark.executor_artifact_digest,
                executor_protocol_version=benchmark.executor_protocol_version,
                total_tasks=sum(counts.values()),
                finished_tasks=(
                    counts.get(TaskStatus.FINISHED, 0)
                    + counts.get(TaskStatus.ERROR, 0)
                    + counts.get(TaskStatus.STOPPED, 0)
                ),
                task_state_counts={status.value: count for status, count in counts.items()},
            )
        )

    return BenchmarkStatusResponse(entries=entries)
