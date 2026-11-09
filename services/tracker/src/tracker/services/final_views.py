from sqlmodel import Session
from tracker.services.results import fetch_average_task_breakdown
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX
from tracker.database.models import (
    Benchmark,
    Org,
    TaskStatus,
)
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.runtime.artifacts import benchmark_prefix
from tracker.runtime.storage import ObjectStore
from tracker.types import (
    FinalViewResponse,
)


def create_final_view(benchmark_row: Benchmark, session: Session, org: Org) -> FinalViewResponse:
    repository = BenchmarkRepository(session)
    tasks_stopped = repository.count_tasks(org.id, benchmark_row.id, [TaskStatus.STOPPED])
    task_failures = repository.task_failures(benchmark_row)
    return FinalViewResponse(
        benchmark_name=benchmark_row.name,
        status=benchmark_row.status,
        error_message=benchmark_row.error_message,
        benchmark_id=benchmark_row.id,
        benchmark_arguments=benchmark_row.arguments,
        started_at=benchmark_row.started_at,
        finished_at=benchmark_row.finished_at,
        tasks_stopped=tasks_stopped or None,
        final_evaluation=benchmark_row.final_evaluation,
        evaluation_results=benchmark_row.fetch_evaluation_results(session),
        task_errors={task_id: message for task_id, (message, _) in task_failures.items()} if task_failures else None,
        task_failure_categories=(
            {task_id: category for task_id, (_, category) in task_failures.items() if category is not None}
            if task_failures
            else None
        ),
        average_task_breakdown=fetch_average_task_breakdown(benchmark_row.id, session, org.id),
    )


def final_view_s3_key(benchmark_row: Benchmark) -> str:
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_row.id}/{benchmark_row.name}.json"


async def upload_final_view(final_view: FinalViewResponse, object_store: ObjectStore) -> str:
    key = f"{benchmark_prefix(str(final_view.benchmark_id))}{final_view.benchmark_name}.json"
    await object_store.put_bytes(key, final_view.model_dump_json(indent=4, exclude_none=True).encode())
    return key
