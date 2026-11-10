from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from benchmark_service.schemas import DatasetVersion

from tracker.auth import RequestIdentity
from tracker.database.models import Benchmark, ErrorResult, Task, TaskStatus


@dataclass(frozen=True)
class RegradeTasks:
    eval_resume_states: dict[str, dict[str, Any]]
    skipped_task_ids: list[str]

    @property
    def task_ids(self) -> list[str]:
        return list(self.eval_resume_states)


def regrade_tasks(source_tasks: Sequence[Task]) -> RegradeTasks:
    return RegradeTasks(
        eval_resume_states={
            task.task_id: task.eval_resume_state for task in source_tasks if task.eval_resume_state is not None
        },
        skipped_task_ids=[task.task_id for task in source_tasks if task.eval_resume_state is None],
    )


def regrade_run(
    source: Benchmark,
    tasks: RegradeTasks,
    run_starter: RequestIdentity,
    *,
    dataset: str,
    dataset_version: DatasetVersion | None,
    concurrency: int | None = None,
) -> Benchmark:
    return Benchmark(
        org_id=source.org_id,
        name=source.name,
        label=source.label,
        custom_benchmark_service=source.custom_benchmark_service,
        arguments=source.arguments.model_copy(
            update={
                "dataset": dataset,
                "dataset_version": dataset_version,
                "task_ids": tasks.task_ids,
                "slice_str": None,
                "concurrency": source.arguments.concurrency if concurrency is None else concurrency,
                "lambda_function": None,
                "regraded_from_run_id": source.id,
                "regrade_skipped_task_ids": tasks.skipped_task_ids,
            }
        ),
        started_by_id=run_starter.access_key_id or run_starter.user_id,
        started_by_email=run_starter.email,
    )


def regrade_task_rows(
    run: Benchmark, tasks: RegradeTasks, started_at: datetime
) -> tuple[list[Task], list[ErrorResult]]:
    copied = [
        Task(
            org_id=run.org_id,
            benchmark=run.id,
            task_id=task_id,
            status=TaskStatus.EVALUATING,
            started_at=started_at,
            eval_resume_state=state,
        )
        for task_id, state in tasks.eval_resume_states.items()
    ]
    skipped = [
        Task(
            org_id=run.org_id,
            benchmark=run.id,
            task_id=task_id,
            status=TaskStatus.ERROR,
            started_at=started_at,
            finished_at=started_at,
        )
        for task_id in tasks.skipped_task_ids
    ]
    errors = [
        ErrorResult(
            org_id=run.org_id,
            task=task.id,
            error_message=f"Source run {run.arguments.regraded_from_run_id} saved no eval_resume_state for this task.",
            producer="tracker",
            operation="regrade",
            transient=False,
        )
        for task in skipped
    ]
    return [*copied, *skipped], errors
