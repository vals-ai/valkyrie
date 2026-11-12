from collections.abc import Collection, Sequence
from datetime import datetime
from typing import Annotated, Any, Literal, NamedTuple, cast
from uuid import UUID

from sqlalchemy import JSON, case, literal, tuple_, type_coerce
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, selectinload
from fastapi import Depends
from sqlmodel import Session, SQLModel, asc, col, desc, func, or_, select, update
from sqlmodel.sql.expression import Select, SelectOfScalar

from tracker.database.session import get_session
from tracker.repositories.cost import later_of
from tracker.database.models import (
    AgentCausedExitReason,
    TaskBreakdown,
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    ErrorResult,
    EvaluationResult,
    FailureCategory,
    FinalEvaluation,
    SandboxRecord,
    Task,
    TaskStatus,
)
from tracker.types import BenchmarkFilters, Order

TaskSort = Literal["task_id", "started_at", "duration", "status"]

_STATUS_SORT_PRIORITY = case(
    {
        TaskStatus.ERROR: 6,
        TaskStatus.STOPPED: 5,
        TaskStatus.FINISHED: 4,
        TaskStatus.EVALUATING: 3,
        TaskStatus.IN_PROGRESS: 2,
        TaskStatus.BUILDING: 1,
        TaskStatus.PENDING: 0,
    },
    value=col(Task.status),
    else_=-1,
)


_RUNNABLE_TASK_STATUSES = (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


FinishedEvaluationRow = tuple[
    UUID, datetime, dict[str, Any], AgentCausedExitReason | None, UUID, str, TaskBreakdown | None
]


class FilterRow(NamedTuple):
    benchmark_name: str
    agent_name: str
    model: str | None
    dataset: str | None
    email: str | None


class BenchmarkRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get(self, org_id: UUID, benchmark_id: UUID) -> Benchmark | None:
        benchmark = self._session.get(Benchmark, benchmark_id)
        return benchmark if benchmark is not None and benchmark.org_id == org_id else None

    def get_by_id(self, benchmark_id: UUID, *, for_update: bool = False) -> Benchmark | None:
        return self._session.get(
            Benchmark,
            benchmark_id,
            populate_existing=for_update,
            with_for_update=for_update or None,
        )

    def lock_by_id(self, benchmark_id: UUID) -> Benchmark:
        return self._session.exec(
            select(Benchmark)
            .where(Benchmark.id == benchmark_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one()

    def status_by_id(self, benchmark_id: UUID) -> BenchmarkStatus | None:
        return self._session.exec(select(Benchmark.status).where(col(Benchmark.id) == benchmark_id)).one_or_none()

    def lock_pool(self, pool_id: str) -> Sequence[Benchmark]:
        return self._session.exec(
            select(Benchmark)
            .where(type_coerce(col(Benchmark.arguments), JSON)["queue_pool_id"].as_string() == pool_id)
            .order_by(col(Benchmark.id))
            .with_for_update()
        ).all()

    def lock(self, org_id: UUID, benchmark_id: UUID) -> Benchmark | None:
        return self._session.exec(
            select(Benchmark)
            .where(Benchmark.id == benchmark_id)
            .where(Benchmark.org_id == org_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).one_or_none()

    def _filtered(self, filters: BenchmarkFilters, org_id: UUID) -> SelectOfScalar[Benchmark]:
        query = select(Benchmark).where(Benchmark.org_id == org_id).options(selectinload(Benchmark.final_evaluation))
        arguments_json = type_coerce(col(Benchmark.arguments), JSON)

        if filters.agent_name:
            query = query.where(arguments_json["contract"]["name"].as_string().in_(filters.agent_name))

        if filters.model:
            query = query.where(arguments_json["contract"]["model"].as_string() == filters.model)

        if filters.dataset:
            dataset_value = arguments_json["dataset"].as_string()
            if filters.dataset == "default":
                query = query.where(or_(dataset_value == "default", dataset_value.is_(None)))
            else:
                query = query.where(dataset_value == filters.dataset)

        if filters.label is not None:
            query = query.where(func.lower(Benchmark.label) == filters.label.lower())

        if filters.benchmark_name:
            query = query.where(col(Benchmark.name).in_(filters.benchmark_name))

        if filters.status:
            query = query.where(col(Benchmark.status).in_(filters.status))

        if filters.started_after is not None:
            query = query.where(Benchmark.started_at > filters.started_after)

        if filters.started_before is not None:
            query = query.where(Benchmark.started_at < filters.started_before)

        if filters.started_by:
            normalized_emails = [s.strip().lower() for s in filters.started_by if s and s.strip()]
            if normalized_emails:
                query = query.where(col(Benchmark.started_by_email).in_(normalized_emails))

        direction = desc if filters.order_by == Order.DESC else asc
        return query.order_by(direction(Benchmark.started_at), direction(Benchmark.id))

    def list_filtered(
        self,
        filters: BenchmarkFilters,
        org_id: UUID,
        *,
        limit: int,
        offset: int = 0,
        after: tuple[datetime, UUID] | None = None,
    ) -> Sequence[Benchmark]:
        query = self._filtered(filters, org_id)
        if after is not None:
            position = tuple_(col(Benchmark.started_at), col(Benchmark.id))
            cursor_position = tuple_(literal(after[0]), literal(str(after[1])))
            query = query.where(
                position < cursor_position if filters.order_by == Order.DESC else position > cursor_position
            )
        return self._session.exec(query.limit(limit).offset(offset)).all()

    def count_filtered(self, filters: BenchmarkFilters, org_id: UUID) -> int:
        return self._session.exec(select(func.count()).select_from(self._filtered(filters, org_id).subquery())).one()

    def list_by_ids(self, org_id: UUID, benchmark_ids: Sequence[UUID]) -> list[Benchmark]:
        return list(
            self._session.exec(
                select(Benchmark).where(Benchmark.org_id == org_id).where(col(Benchmark.id).in_(benchmark_ids))
            ).all()
        )

    def list_with_final_evaluation(self, benchmark_ids: Sequence[UUID]) -> Sequence[Benchmark]:
        return self._session.exec(
            select(Benchmark)
            .where(col(Benchmark.id).in_(benchmark_ids))
            .options(selectinload(Benchmark.final_evaluation))
        ).all()

    def list_by_status(self, statuses: Sequence[BenchmarkStatus]) -> Sequence[Benchmark]:
        return self._session.exec(
            select(Benchmark)
            .where(col(Benchmark.status).in_(statuses))
            .order_by(col(Benchmark.started_at), col(Benchmark.id))
        ).all()

    def lock_by_status(self, statuses: Sequence[BenchmarkStatus]) -> Sequence[Benchmark]:
        return self._session.exec(
            select(Benchmark).where(col(Benchmark.status).in_(statuses)).order_by(col(Benchmark.id)).with_for_update()
        ).all()

    def list_in_progress(self, org_id: UUID) -> list[Benchmark]:
        return list(
            self._session.exec(
                select(Benchmark)
                .where(col(Benchmark.org_id) == org_id)
                .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
                .order_by(col(Benchmark.started_at))
            ).all()
        )

    def count_active(self, org_id: UUID, *, excluding: UUID) -> int:
        return self._session.exec(
            select(func.count())
            .select_from(Benchmark)
            .where(col(Benchmark.org_id) == org_id)
            .where(col(Benchmark.status).in_([BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING]))
            .where(col(Benchmark.id) != excluding)
        ).one()

    def task_state_counts(self, benchmark_ids: Sequence[UUID]) -> dict[UUID, dict[TaskStatus, int]]:
        rows = self._session.exec(
            select(Task.benchmark, Task.status, func.count())
            .where(col(Task.benchmark).in_(benchmark_ids))
            .group_by(col(Task.benchmark), col(Task.status))
        ).all()
        counts: dict[UUID, dict[TaskStatus, int]] = {}
        for benchmark_id, status, count in rows:
            counts.setdefault(benchmark_id, {})[TaskStatus(status)] = count
        return counts

    def final_score(self, benchmark: Benchmark) -> float | None:
        return self._session.exec(
            select(FinalEvaluation.final_score)
            .where(col(FinalEvaluation.benchmark) == benchmark.id)
            .where(col(FinalEvaluation.org_id) == benchmark.org_id)
        ).first()

    def task_failures(self, benchmark: Benchmark) -> dict[str, tuple[str, FailureCategory | None]]:
        error_rows = self._session.exec(
            select(Task.task_id, col(ErrorResult.error_message), col(ErrorResult.category))
            .outerjoin(
                ErrorResult,
                (col(ErrorResult.task) == col(Task.id))
                & (col(ErrorResult.org_id) == benchmark.org_id)
                & col(ErrorResult.retry_scheduled).is_(False),
            )
            .where(Task.benchmark == benchmark.id)
            .where(Task.org_id == benchmark.org_id)
            .where(Task.status == TaskStatus.ERROR)
            .order_by(col(Task.id), col(ErrorResult.created_at).desc())
        ).all()
        failures: dict[str, tuple[str, FailureCategory | None]] = {}
        for task_id, error_message, category in error_rows:
            failures.setdefault(task_id, (error_message or "No error message was provided", category))
        return failures

    def filter_rows(self, org_id: UUID) -> Sequence[FilterRow]:
        arguments = type_coerce(col(Benchmark.arguments), JSON)
        statement = (
            cast(Any, select)(
                col(Benchmark.name).label("benchmark_name"),
                arguments["contract"]["name"].as_string().label("agent_name"),
                arguments["contract"]["model"].as_string().label("model"),
                arguments["dataset"].as_string().label("dataset"),
                col(Benchmark.started_by_email).label("email"),
            )
            .where(Benchmark.org_id == org_id)
            .distinct()
        )
        return cast(Sequence[FilterRow], cast(Any, self._session.exec(statement)).all())

    def get_task(self, org_id: UUID, benchmark_id: UUID, task_id: str, *, refresh: bool = False) -> Task | None:
        return self._session.exec(
            select(Task)
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .where(Task.task_id == task_id)
            .execution_options(populate_existing=refresh)
        ).first()

    def list_tasks(
        self,
        org_id: UUID,
        benchmark_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        statuses: Sequence[TaskStatus] | None = None,
    ) -> list[Task]:
        query = select(Task).where(col(Task.benchmark) == benchmark_id).where(col(Task.org_id) == org_id)
        if task_ids is not None:
            query = query.where(col(Task.task_id).in_(task_ids))
        if statuses is not None:
            query = query.where(col(Task.status).in_(statuses))
        return list(self._session.exec(query.order_by(col(Task.task_id))).all())

    def list_tasks_by_start(
        self, org_id: UUID, benchmark_id: UUID, statuses: Sequence[TaskStatus], *, for_update: bool = False
    ) -> list[Task]:
        query = (
            select(Task)
            .where(Task.benchmark == benchmark_id, Task.org_id == org_id)
            .where(col(Task.status).in_(statuses))
            .order_by(asc(Task.started_at), asc(Task.id))
        )
        if for_update:
            query = query.with_for_update()
        return list(self._session.exec(query).all())

    def retry_candidates(
        self, benchmark: Benchmark, retry: bool, rerun_task_ids: Sequence[str], *, for_update: bool = False
    ) -> list[Task]:
        query = select(Task).where(col(Task.benchmark) == benchmark.id, col(Task.org_id) == benchmark.org_id)
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            query = query.where(col(Task.status).in_([TaskStatus.ERROR, TaskStatus.STOPPED]))
            if rerun_task_ids:
                query = query.where(col(Task.task_id).in_(rerun_task_ids))
        elif retry and rerun_task_ids:
            query = query.where(col(Task.task_id).in_(rerun_task_ids))
        else:
            retry_statuses = [TaskStatus.STOPPED, TaskStatus.ERROR] if retry else [TaskStatus.STOPPED]
            query = query.where(or_(col(Task.status).in_(retry_statuses), col(Task.task_id).in_(rerun_task_ids)))
        query = query.order_by(asc(Task.started_at), asc(Task.id))
        if for_update:
            query = query.with_for_update()
        return list(self._session.exec(query).all())

    def lock_newest_tasks(
        self, org_id: UUID, benchmark_id: UUID, statuses: Sequence[TaskStatus], limit: int
    ) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id, col(Task.org_id) == org_id)
            .where(col(Task.status).in_(statuses))
            .order_by(desc(Task.started_at), desc(Task.id))
            .limit(limit)
            .with_for_update()
        ).all()

    def page_tasks(
        self,
        org_id: UUID,
        benchmark_id: UUID,
        statuses: Sequence[TaskStatus],
        task_id_search: str | None,
        sort: TaskSort,
        sort_dir: Literal["asc", "desc"],
        limit: int,
        offset: int,
    ) -> tuple[Sequence[tuple[Task, str | None, FailureCategory | None]], int]:
        base_filters = [
            col(Task.benchmark) == benchmark_id,
            col(Task.org_id) == org_id,
        ]
        if statuses:
            base_filters.append(col(Task.status).in_(statuses))

        if task_id_search:
            base_filters.append(col(Task.task_id).icontains(task_id_search, autoescape=True))

        latest_error = aliased(ErrorResult)
        latest_error_id = (
            select(latest_error.id)
            .where(latest_error.task == Task.id)
            .where(latest_error.org_id == org_id)
            .where(col(latest_error.retry_scheduled).is_(False))
            .order_by(desc(latest_error.created_at))
            .limit(1)
            .scalar_subquery()
        )
        terminal_error_id = case(
            (col(Task.status) == TaskStatus.ERROR, latest_error_id),
            else_=None,
        )
        sort_expr = {
            "task_id": col(Task.task_id),
            "started_at": col(Task.started_at),
            "duration": func.coalesce(col(Task.finished_at), func.now()) - col(Task.started_at),
            "status": _STATUS_SORT_PRIORITY,
        }[sort]
        primary = sort_expr.asc() if sort_dir == "asc" else sort_expr.desc()
        order_by = [primary, col(Task.started_at).desc()]

        rows = self._session.exec(
            select(Task, col(ErrorResult.error_message), col(ErrorResult.category))
            .outerjoin(ErrorResult, col(ErrorResult.id) == terminal_error_id)
            .where(*base_filters)
            .order_by(*order_by)
            .limit(limit)
            .offset(offset)
        ).all()
        total = self._session.exec(select(func.count()).select_from(Task).where(*base_filters)).one()
        return rows, total

    def get_task_by_id(self, task_row_id: UUID) -> Task | None:
        return self._session.get(Task, task_row_id)

    def lock_task_by_row_id(self, task_row_id: UUID) -> Task:
        return self._session.exec(
            select(Task).where(col(Task.id) == task_row_id).execution_options(populate_existing=True).with_for_update()
        ).one()

    def task_by_row_id(self, task_row_id: UUID) -> Task:
        return self._session.exec(
            select(Task).where(col(Task.id) == task_row_id).execution_options(populate_existing=True)
        ).one()

    def tasks_by_row_ids(self, task_row_ids: Collection[UUID]) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.id).in_(task_row_ids))
            .order_by(col(Task.id))
            .execution_options(populate_existing=True)
        ).all()

    def list_tasks_by_id_for_benchmark(self, benchmark_id: UUID, task_ids: Sequence[str]) -> Sequence[Task]:
        return self._session.exec(
            select(Task).where(col(Task.benchmark) == benchmark_id, col(Task.task_id).in_(task_ids))
        ).all()

    def record_sandbox_link(self, provider: str, sandbox_id: str, benchmark_id: UUID, seen_at: datetime) -> None:
        statement = insert(SandboxRecord).values(
            provider=provider, sandbox_id=sandbox_id, benchmark_id=benchmark_id, last_seen_at=seen_at
        )
        self._session.exec(
            statement.on_conflict_do_update(
                index_elements=["provider", "sandbox_id"],
                set_={"last_seen_at": later_of(col(SandboxRecord.last_seen_at), statement.excluded.last_seen_at)},
                where=col(SandboxRecord.benchmark_id) == statement.excluded.benchmark_id,
            )
        )

    def task_ids(self, org_id: UUID, benchmark_id: UUID, task_ids: Sequence[str] | None = None) -> set[str]:
        query = select(Task.task_id).where(col(Task.benchmark) == benchmark_id).where(col(Task.org_id) == org_id)
        if task_ids is not None:
            query = query.where(col(Task.task_id).in_(task_ids))
        return set(self._session.exec(query).all())

    def count_tasks(self, org_id: UUID, benchmark_id: UUID, statuses: Sequence[TaskStatus]) -> int:
        return self._session.exec(
            select(func.count(col(Task.id)))
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status).in_(statuses))
        ).one()

    def has_tasks(self, org_id: UUID, benchmark_id: UUID, statuses: Sequence[TaskStatus]) -> bool:
        return (
            self._session.exec(
                select(Task.id)
                .where(Task.benchmark == benchmark_id)
                .where(Task.org_id == org_id)
                .where(col(Task.status).in_(statuses))
                .limit(1)
            ).first()
            is not None
        )

    def lock_tasks(
        self,
        org_id: UUID,
        benchmark_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
        row_ids: Sequence[UUID] | None = None,
        statuses: Sequence[TaskStatus] | None = None,
    ) -> Sequence[Task]:
        query = select(Task).where(col(Task.benchmark) == benchmark_id).where(col(Task.org_id) == org_id)
        if task_ids is not None:
            query = query.where(col(Task.task_id).in_(task_ids))
        if row_ids is not None:
            query = query.where(col(Task.id).in_(row_ids))
        if statuses is not None:
            query = query.where(col(Task.status).in_(statuses))
        return self._session.exec(
            query.order_by(col(Task.started_at), col(Task.id))
            .with_for_update()
            .execution_options(populate_existing=True)
        ).all()

    def lock_nonterminal_tasks(self, org_id: UUID, benchmark_id: UUID, task_ids: Sequence[str]) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id, col(Task.org_id) == org_id)
            .where(col(Task.status).notin_((TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)))
            .where(col(Task.task_id).in_(task_ids))
            .with_for_update()
        ).all()

    def lock_owned_attempts(self, org_id: UUID, benchmark_id: UUID, row_ids: Sequence[UUID]) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id, col(Task.org_id) == org_id)
            .where(col(Task.id).in_(row_ids))
            .with_for_update()
        ).all()

    def lock_tasks_for_benchmark_by_ids(self, benchmark_id: UUID, task_ids: Sequence[str]) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id, col(Task.task_id).in_(task_ids))
            .order_by(col(Task.id))
            .with_for_update()
        ).all()

    def has_build_in_pool(self, pool_id: str) -> bool:
        return (
            self._session.exec(
                select(Task.id)
                .join(Benchmark, col(Task.benchmark) == col(Benchmark.id))
                .where(col(Task.status) == TaskStatus.BUILDING)
                .where(type_coerce(col(Benchmark.arguments), JSON)["queue_pool_id"].as_string() == pool_id)
            ).first()
            is not None
        )

    def final_errors(
        self, org_id: UUID, task_row_ids: Sequence[UUID]
    ) -> Sequence[tuple[UUID, datetime, str | None, FailureCategory | None]]:
        return self._session.exec(
            select(ErrorResult.task, ErrorResult.created_at, col(ErrorResult.error_message), col(ErrorResult.category))
            .where(col(ErrorResult.task).in_(task_row_ids))
            .where(col(ErrorResult.org_id) == org_id)
            .where(col(ErrorResult.retry_scheduled).is_(False))
        ).all()

    def finished_evaluations(self, org_id: UUID, benchmark_id: UUID) -> Sequence[FinishedEvaluationRow]:
        statement = cast(
            Select[FinishedEvaluationRow],
            sa_select(
                col(EvaluationResult.id),
                col(EvaluationResult.created_at),
                col(EvaluationResult.result),
                col(EvaluationResult.agent_caused_exit_reason),
                col(Task.id),
                col(Task.task_id),
                TaskBreakdown,
            )
            .join(Task, col(EvaluationResult.task) == col(Task.id))
            .outerjoin(TaskBreakdown, col(Task.task_breakdown) == col(TaskBreakdown.id))
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(EvaluationResult.org_id) == org_id)
            .where(col(Task.status) == TaskStatus.FINISHED)
            .order_by(col(EvaluationResult.task), desc(EvaluationResult.created_at), desc(EvaluationResult.id)),
        )
        return self._session.exec(statement).all()

    def average_task_breakdown(
        self, org_id: UUID, benchmark_id: UUID
    ) -> tuple[float | None, float | None, float | None, float | None]:
        return self._session.exec(
            select(
                func.avg(TaskBreakdown.sandbox_build_duration),
                func.avg(TaskBreakdown.agent_run_duration),
                func.avg(TaskBreakdown.evaluation_run_duration),
                func.avg(TaskBreakdown.sandbox_run_duration),
            )
            .join(Task, col(Task.task_breakdown) == col(TaskBreakdown.id))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
        ).one()

    def task_states(
        self, org_id: UUID, benchmark_id: UUID, task_ids: Sequence[str]
    ) -> Sequence[tuple[str, str, datetime]]:
        return self._session.exec(
            select(Task.task_id, Task.status, Task.started_at)
            .where(col(Task.task_id).in_(task_ids))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
        ).all()

    def lock_task(
        self,
        org_id: UUID,
        benchmark_id: UUID,
        task_row_id: UUID,
        *,
        started_at: datetime | None = None,
        statuses: Sequence[TaskStatus] | None = None,
    ) -> Task | None:
        query = (
            select(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.benchmark) == benchmark_id)
        )
        if started_at is not None:
            query = query.where(col(Task.started_at) == started_at)
        if statuses is not None:
            query = query.where(col(Task.status).in_(statuses))
        return self._session.exec(query.with_for_update()).one_or_none()

    def update_task(
        self,
        task_row_id: UUID,
        org_id: UUID,
        values: dict[str, Any],
        *,
        expected_started_at: datetime | None = None,
        expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
    ) -> bool:
        task_update = update(Task).where(col(Task.id) == task_row_id).where(col(Task.org_id) == org_id)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)
        if expected_status is None:
            task_update = task_update.where(
                col(Task.status).not_in((TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED))
            )
        elif isinstance(expected_status, tuple):
            task_update = task_update.where(col(Task.status).in_(expected_status))
        else:
            task_update = task_update.where(col(Task.status) == expected_status)
        return self._session.exec(task_update.values(**values)).rowcount > 0

    def lock_runnable_tasks(
        self, benchmark: Benchmark, task_ids: Sequence[str], dispatch_created_at: datetime
    ) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark.id)
            .where(col(Task.org_id) == benchmark.org_id)
            .where(col(Task.task_id).in_(task_ids))
            .where(col(Task.status).in_(_RUNNABLE_TASK_STATUSES))
            .where(col(Task.started_at) <= dispatch_created_at)
            .with_for_update()
        ).all()

    def pending_tasks(self, benchmark: Benchmark) -> Sequence[Task]:
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark.id)
            .where(col(Task.org_id) == benchmark.org_id)
            .where(col(Task.status) == TaskStatus.PENDING)
            .order_by(col(Task.task_id))
        ).all()

    def final_score_inputs(self, org_id: UUID, benchmark_id: UUID) -> dict[str, dict[str, Any] | None]:
        task_rows = self._session.exec(
            select(Task.id, Task.task_id, Task.status)
            .where(col(Task.benchmark) == benchmark_id, col(Task.org_id) == org_id)
            .order_by(col(Task.id))
        ).all()
        latest_results = {
            task_row_id: result
            for task_row_id, _result_id, result in self.latest_evaluation_results(org_id, benchmark_id)
        }
        return {
            task_id: latest_results[task_row_id]
            if status == TaskStatus.FINISHED and task_row_id in latest_results
            else None
            for task_row_id, task_id, status in task_rows
        }

    def list_in_progress_with_pending_tasks(self) -> Sequence[Benchmark]:
        return self._session.exec(
            select(Benchmark)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            .where(col(Benchmark.id).in_(select(col(Task.benchmark)).where(col(Task.status) == TaskStatus.PENDING)))
            .order_by(col(Benchmark.id))
        ).all()

    def lock_all_tasks(self, statuses: Sequence[TaskStatus]) -> list[Task]:
        return list(
            self._session.exec(
                select(Task).where(col(Task.status).in_(statuses)).order_by(col(Task.id)).with_for_update()
            ).all()
        )

    def clear_native_output(self, task_row_id: UUID, sandbox_id: str, attempt_started_at: str) -> None:
        stored = type_coerce(col(Task.active_native_output), JSON)
        self._session.exec(
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(stored["sandbox_id"].as_string() == sandbox_id)
            .where(stored["attempt_started_at"].as_string() == attempt_started_at)
            .values(active_native_output=None)
            .execution_options(synchronize_session=False)
        )

    def attach_task_breakdown(self, task: Task, task_breakdown: TaskBreakdown) -> None:
        self._session.add(task_breakdown)
        self._session.flush()
        task.task_breakdown = task_breakdown.id

    def get_task_breakdown(self, task_breakdown_id: UUID) -> TaskBreakdown | None:
        return self._session.get(TaskBreakdown, task_breakdown_id)

    def latest_evaluation(self, org_id: UUID, task_id: UUID) -> EvaluationResult | None:
        return self._session.exec(
            select(EvaluationResult)
            .where(EvaluationResult.task == task_id, EvaluationResult.org_id == org_id)
            .order_by(desc(EvaluationResult.created_at))
            .limit(1)
        ).first()

    def latest_error(self, org_id: UUID, task_id: UUID) -> tuple[str, FailureCategory | None] | None:
        return cast(
            tuple[str, FailureCategory | None] | None,
            self._session.exec(
                select(ErrorResult.error_message, ErrorResult.category)
                .where(col(ErrorResult.retry_scheduled).is_(False))
                .where(ErrorResult.task == task_id, ErrorResult.org_id == org_id)
                .order_by(desc(ErrorResult.created_at))
                .limit(1)
            ).first(),
        )

    def save(self, *rows: SQLModel) -> None:
        self._session.add_all(rows)

    def add_tasks_with_errors(self, tasks: Sequence[Task], errors: Sequence[ErrorResult]) -> None:
        self._session.add_all(tasks)
        self._session.flush()
        self._session.add_all(errors)

    def add_tasks(self, org_id: UUID, benchmark_id: UUID, task_ids: Sequence[str]) -> None:
        self._session.add_all(Task(org_id=org_id, benchmark=benchmark_id, task_id=task_id) for task_id in task_ids)

    def delete_final_evaluation(self, benchmark: Benchmark) -> None:
        evaluation = benchmark.final_evaluation
        if evaluation is not None:
            benchmark.final_evaluation = None
            self._session.delete(evaluation)

    def replace_final_evaluation(self, benchmark: Benchmark, final_evaluation: FinalEvaluation) -> None:
        if benchmark.final_evaluation:
            self._session.delete(benchmark.final_evaluation)
            self._session.flush()
        self._session.add(final_evaluation)

    def task_attempts(
        self, org_id: UUID, benchmark_id: UUID, *, for_update: bool = False
    ) -> Sequence[tuple[UUID, str, datetime, TaskStatus]]:
        query = (
            select(Task.id, Task.task_id, Task.started_at, Task.status)
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .order_by(col(Task.id))
        )
        if for_update:
            query = query.with_for_update()
        return cast(Sequence[tuple[UUID, str, datetime, TaskStatus]], self._session.exec(query).all())

    def latest_evaluation_results(
        self, org_id: UUID, benchmark_id: UUID
    ) -> Sequence[tuple[UUID, UUID, dict[str, Any]]]:
        return cast(
            Sequence[tuple[UUID, UUID, dict[str, Any]]],
            self._session.exec(
                select(EvaluationResult.task, EvaluationResult.id, EvaluationResult.result)  # pyright: ignore[reportUnknownArgumentType]
                .join(Task, col(EvaluationResult.task) == col(Task.id))
                .where(col(Task.benchmark) == benchmark_id)
                .where(col(Task.org_id) == org_id)
                .where(col(EvaluationResult.org_id) == org_id)
                .distinct(col(EvaluationResult.task))
                .order_by(col(EvaluationResult.task), desc(EvaluationResult.created_at), desc(EvaluationResult.id))
            ).all(),
        )

    def stop_tasks(
        self,
        org_id: UUID,
        benchmark_id: UUID,
        statuses: Sequence[TaskStatus],
        task_ids: Sequence[str] | None = None,
    ) -> int:
        statement = (
            update(Task)
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status).in_(statuses))
        )
        if task_ids:
            statement = statement.where(col(Task.task_id).in_(task_ids))
        return self._session.exec(statement.values(status=TaskStatus.STOPPED, active_native_output=None)).rowcount

    def restart_evaluations(self, task_ids: Sequence[UUID], started_at: datetime) -> int:
        return self._session.exec(
            update(Task)
            .where(col(Task.id).in_(task_ids))
            .where(col(Task.status) == TaskStatus.EVALUATING)
            .values(
                started_at=started_at,
                evaluation_phase_dispatch_id=None,
                evaluation_phase_token=None,
            )
        ).rowcount

    def has_active_evaluation_owner(self, dispatch_id: UUID | None) -> bool:
        if dispatch_id is None:
            return False
        return (
            self._session.exec(
                select(ExecutorDispatch.id)
                .where(col(ExecutorDispatch.id) == dispatch_id)
                .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
                .where(
                    col(ExecutorDispatch.ecs_task_arn).is_not(None)
                    | (col(ExecutorDispatch.lease_expires_at) > func.clock_timestamp())
                )
            ).first()
            is not None
        )


BenchmarkRepositoryDependency = Annotated[BenchmarkRepository, Depends(BenchmarkRepository)]
