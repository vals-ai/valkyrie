from collections.abc import Sequence
from itertools import batched
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, NamedTuple, cast
from uuid import UUID

from dataclasses import dataclass

from sqlalchemy import JSON, ColumnElement, and_, case, extract, func, or_, type_coerce, update
from sqlalchemy.dialects.postgresql import insert
from fastapi import Depends
from sqlmodel import Session, col, select

from tracker.database.session import get_session
from tracker.search_cost import SearchCharge, attempt_totals
from tracker.repositories.project import ProjectRepository
from tracker.database.models import (
    FINISHED_BENCHMARK_STATUSES,
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    Project,
    RunCost,
    RunCostUsageEndBackfill,
    RunProject,
    SandboxCostCursor,
    SandboxUnmatchedUsage,
    SandboxRecord,
    Task,
)

SANDBOX_COST_TYPE = "sandbox"
SEARCH_COST_TYPE = "search"
_HOUR = timedelta(hours=1)
_LAST_MOMENT_OF_HOUR = _HOUR - timedelta(microseconds=1)
_SANDBOX_HOUR_COST_BATCH = 1000


def sandbox_hour_source_id(sandbox_id: str, hour: datetime) -> str:
    return f"{sandbox_id}:{hour.isoformat()}"


DAYTONA_PROVIDER = "daytona"
MODAL_PROVIDER = "modal"
_PRICED_PROVIDERS = (DAYTONA_PROVIDER,)


class EmailCostRow(NamedTuple):
    email: str
    cost_type: str
    provider: str
    amount_usd: float
    run_count: int


class RunProjectRow(NamedTuple):
    run_id: UUID
    started_at: datetime
    handle: str | None
    name: str | None
    spend_type: Any
    subcategory: str | None
    customer: str | None


class PlacedCostRow(NamedTuple):
    run_id: UUID
    provider: str
    amount_usd: float
    placed_at: datetime
    handle: str | None
    name: str | None
    spend_type: Any
    subcategory: str | None
    customer: str | None


class SandboxCostRow(NamedTuple):
    source_id: str | None
    hourly: bool | None
    placed_at: datetime
    amount_usd: float
    usage_end: datetime | None
    run_id: UUID
    handle: str | None


class SandboxDayTotalRow(NamedTuple):
    day: date
    hourly: bool
    amount_usd: float
    row_count: int


class ProjectDaytonaTotalRow(NamedTuple):
    project_id: UUID
    total_usd: float


class ProjectSandboxTotalRow(NamedTuple):
    project_id: UUID
    provider: str
    total_usd: float


class MissingUsageEndRow(NamedTuple):
    id: UUID
    source_id: str
    amount_usd: float
    placed_at: datetime


class UnpricedSandbox(NamedTuple):
    org_id: UUID
    provider: str
    secret_name: str
    benchmark_id: UUID
    sandbox_id: str
    last_seen_at: datetime | None


class SandboxAccountUse(NamedTuple):
    org_id: UUID
    secret_name: str
    last_finished_at: datetime | None


@dataclass(frozen=True)
class SandboxUsage:
    sandbox_id: str
    total_price: float
    last_end: datetime | None


_EPOCH_DAY = date(1970, 1, 1)
_SECONDS_PER_DAY = 86400


def _labels(projects: dict[UUID, Project], project_id: UUID | None) -> tuple[Any, ...]:
    project = None if project_id is None else projects.get(project_id)
    if project is None:
        return (None, None, None, None, None)
    return (project.handle, project.name, project.spend_type, project.subcategory, project.customer)


def _placed_at() -> Any:
    return func.coalesce(col(RunCost.usage_end), col(Benchmark.finished_at), col(Benchmark.started_at))


def _is_hourly_source() -> Any:
    return col(RunCost.source_id).contains(":")


def later_of(current: ColumnElement[Any], candidate: Any) -> ColumnElement[Any]:
    return case((or_(current.is_(None), current < candidate), candidate), else_=current)


def _run_last_active_at() -> ColumnElement[datetime | None]:
    last_task_at = (
        select(func.max(func.coalesce(col(Task.finished_at), col(Task.started_at))))
        .where(col(Task.benchmark) == col(Benchmark.id))
        .scalar_subquery()
    )
    return func.coalesce(col(Benchmark.finished_at), last_task_at, col(Benchmark.started_at))


class CostRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def sandbox_ids(self, benchmark_id: UUID, provider_type: str) -> list[str]:
        return list(
            self._session.exec(
                select(SandboxRecord.sandbox_id).where(
                    SandboxRecord.benchmark_id == benchmark_id, SandboxRecord.provider == provider_type
                )
            )
        )

    def _projects(self, org_id: UUID) -> dict[UUID, Project]:
        return {project.id: project for project in ProjectRepository(self._session).list_all(org_id, True, True)}

    def _current_projects(self, org_id: UUID, *filters: Any) -> Any:
        ranked = (
            cast(Any, select)(
                col(RunProject.benchmark_id).label("benchmark_id"),
                col(RunProject.project_id).label("project_id"),
                func.row_number()
                .over(partition_by=col(RunProject.benchmark_id), order_by=col(RunProject.id).desc())
                .label("newest"),
            )
            .join(Benchmark, col(Benchmark.id) == col(RunProject.benchmark_id))
            .where(Benchmark.org_id == org_id, *filters)
        ).subquery()
        return cast(Any, select)(ranked.c.benchmark_id, ranked.c.project_id).where(ranked.c.newest == 1).subquery()

    def by_email(self, org_id: UUID, start: datetime, end: datetime) -> Sequence[EmailCostRow]:
        email = func.lower(func.trim(col(Benchmark.started_by_email)))
        statement = (
            cast(Any, select)(
                email.label("email"),
                col(RunCost.cost_type).label("cost_type"),
                col(RunCost.provider).label("provider"),
                func.sum(col(RunCost.amount_usd)).label("amount_usd"),
                func.count(func.distinct(col(Benchmark.id))).label("run_count"),
            )
            .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
            .where(Benchmark.org_id == org_id)
            .where(col(Benchmark.started_by_email).is_not(None))
            .where(col(Benchmark.started_at) >= start)
            .where(col(Benchmark.started_at) < end)
            .group_by(email, col(RunCost.cost_type), col(RunCost.provider))
            .order_by(email, col(RunCost.cost_type), col(RunCost.provider))
        )
        return cast(Sequence[EmailCostRow], cast(Any, self._session.exec(statement)).all())

    def by_project(
        self, org_id: UUID, start: datetime, end: datetime
    ) -> tuple[Sequence[RunProjectRow], Sequence[PlacedCostRow]]:
        unfinished = col(Benchmark.finished_at).is_(None)
        alive_in_window = (col(Benchmark.started_at) < end) & or_(
            col(Benchmark.finished_at) >= start,
            unfinished & col(Benchmark.status).in_([BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING]),
            unfinished & (col(Benchmark.started_at) >= start),
        )
        current = self._current_projects(org_id)
        placed_at = _placed_at()
        runs_with_costs = (
            select(col(RunCost.benchmark_id))
            .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
            .where(Benchmark.org_id == org_id)
            .where(placed_at >= start)
            .where(placed_at < end)
        )
        runs_statement = (
            cast(Any, select)(
                col(Benchmark.id).label("run_id"),
                col(Benchmark.started_at).label("started_at"),
                current.c.project_id.label("project_id"),
            )
            .select_from(Benchmark)
            .outerjoin(current, current.c.benchmark_id == col(Benchmark.id))
            .where(Benchmark.org_id == org_id)
            .where(or_(alive_in_window, col(Benchmark.id).in_(runs_with_costs)))
            .order_by(col(Benchmark.started_at), col(Benchmark.id))
        )
        costs_statement = (
            cast(Any, select)(
                col(RunCost.benchmark_id).label("run_id"),
                col(RunCost.provider).label("provider"),
                col(RunCost.amount_usd).label("amount_usd"),
                placed_at.label("placed_at"),
                current.c.project_id.label("project_id"),
            )
            .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
            .outerjoin(current, current.c.benchmark_id == col(Benchmark.id))
            .where(Benchmark.org_id == org_id)
            .where(placed_at >= start)
            .where(placed_at < end)
        )
        projects = self._projects(org_id)
        runs = cast(Any, self._session.exec(runs_statement)).all()
        costs = cast(Any, self._session.exec(costs_statement)).all()
        return (
            [RunProjectRow(row.run_id, row.started_at, *_labels(projects, row.project_id)) for row in runs],
            [
                PlacedCostRow(
                    row.run_id, row.provider, row.amount_usd, row.placed_at, *_labels(projects, row.project_id)
                )
                for row in costs
            ],
        )

    def _placed_sandbox_costs(self, statement: Any, org_id: UUID, provider: str, start: datetime, end: datetime) -> Any:
        return (
            statement.join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
            .where(Benchmark.org_id == org_id)
            .where(col(RunCost.cost_type) == SANDBOX_COST_TYPE)
            .where(col(RunCost.provider) == provider)
            .where(_placed_at() >= start)
            .where(_placed_at() < end)
        )

    def sandbox_rows(
        self, org_id: UUID, provider: str, start: datetime, end: datetime, *, limit: int, offset: int
    ) -> Sequence[SandboxCostRow]:
        placed_at = _placed_at()
        current = self._current_projects(org_id)
        statement = (
            self._placed_sandbox_costs(
                cast(Any, select)(
                    col(RunCost.source_id).label("source_id"),
                    _is_hourly_source().label("hourly"),
                    placed_at.label("placed_at"),
                    col(RunCost.amount_usd).label("amount_usd"),
                    col(RunCost.usage_end).label("usage_end"),
                    col(RunCost.benchmark_id).label("run_id"),
                    current.c.project_id.label("project_id"),
                ),
                org_id,
                provider,
                start,
                end,
            )
            .outerjoin(current, current.c.benchmark_id == col(Benchmark.id))
            .order_by(placed_at, col(RunCost.id))
            .limit(limit)
            .offset(offset)
        )
        projects = self._projects(org_id)
        return [
            SandboxCostRow(
                row.source_id,
                row.hourly,
                row.placed_at,
                row.amount_usd,
                row.usage_end,
                row.run_id,
                _labels(projects, row.project_id)[0],
            )
            for row in cast(Any, self._session.exec(statement)).all()
        ]

    def sandbox_day_totals(
        self, org_id: UUID, provider: str, start: datetime, end: datetime
    ) -> Sequence[SandboxDayTotalRow]:
        epoch_day = func.floor(extract("epoch", _placed_at()) / _SECONDS_PER_DAY)
        hourly = _is_hourly_source()
        statement = self._placed_sandbox_costs(
            cast(Any, select)(
                epoch_day.label("epoch_day"),
                hourly.label("hourly"),
                func.sum(col(RunCost.amount_usd)).label("amount_usd"),
                func.count().label("row_count"),
            ),
            org_id,
            provider,
            start,
            end,
        ).group_by(epoch_day, hourly)
        rows = cast(Sequence[tuple[float, bool | None, float, int]], cast(Any, self._session.exec(statement)).all())
        return sorted(
            SandboxDayTotalRow(
                day=_EPOCH_DAY + timedelta(days=int(day)),
                hourly=bool(is_hourly),
                amount_usd=amount_usd,
                row_count=row_count,
            )
            for day, is_hourly, amount_usd, row_count in rows
        )

    def project_daytona_totals(self, org_id: UUID, start: datetime, end: datetime) -> Sequence[ProjectDaytonaTotalRow]:
        return [
            ProjectDaytonaTotalRow(project_id=row.project_id, total_usd=row.total_usd)
            for row in self.project_sandbox_totals(org_id, start, end)
            if row.provider == DAYTONA_PROVIDER
        ]

    def project_sandbox_totals(self, org_id: UUID, start: datetime, end: datetime) -> Sequence[ProjectSandboxTotalRow]:
        current = self._current_projects(org_id, col(Benchmark.started_at) >= start, col(Benchmark.started_at) < end)
        statement = (
            cast(Any, select)(
                current.c.project_id.label("project_id"),
                col(RunCost.provider).label("provider"),
                func.sum(col(RunCost.amount_usd)).label("total_usd"),
            )
            .select_from(current)
            .join(RunCost, col(RunCost.benchmark_id) == current.c.benchmark_id)
            .where(col(RunCost.cost_type) == SANDBOX_COST_TYPE)
            .group_by(current.c.project_id, col(RunCost.provider))
            .order_by(current.c.project_id, col(RunCost.provider))
        )
        projects = self._projects(org_id)
        rows = cast(Sequence[ProjectSandboxTotalRow], cast(Any, self._session.exec(statement)).all())
        return [ProjectSandboxTotalRow(*row) for row in rows if row.project_id in projects]

    def missing_usage_end_rows(self, org_id: UUID, provider: str, source_ids: list[str]) -> list[MissingUsageEndRow]:
        if not source_ids:
            return []
        statement = (
            cast(Any, select)(col(RunCost.id), col(RunCost.source_id), col(RunCost.amount_usd), _placed_at())
            .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
            .where(
                col(Benchmark.org_id) == org_id,
                col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                col(RunCost.provider) == provider,
                col(RunCost.usage_end).is_(None),
                col(RunCost.source_id).in_(source_ids),
            )
            .order_by(col(RunCost.id))
        )
        return [MissingUsageEndRow(*row) for row in cast(Any, self._session.exec(statement)).all()]

    def backfill_usage_end(self, usage_ends: dict[UUID, datetime], backfilled_at: datetime) -> list[UUID]:
        changed: list[UUID] = []
        for run_cost_id, usage_end in usage_ends.items():
            result = self._session.exec(
                update(RunCost)
                .where(col(RunCost.id) == run_cost_id, col(RunCost.usage_end).is_(None))
                .values(usage_end=usage_end)
            )
            if result.rowcount == 1:
                changed.append(run_cost_id)
        self._session.add_all(
            RunCostUsageEndBackfill(run_cost_id=run_cost_id, backfilled_at=backfilled_at) for run_cost_id in changed
        )
        self._session.commit()
        return changed

    def record_sandbox(self, provider: str, sandbox_id: str, benchmark_id: UUID) -> None:
        self._session.exec(
            insert(SandboxRecord)
            .values(provider=provider, sandbox_id=sandbox_id, benchmark_id=benchmark_id)
            .on_conflict_do_nothing(index_elements=["provider", "sandbox_id"])
        )

    def add_sandbox_hour_costs(
        self, *, provider: str, hour: datetime, usages: Sequence[tuple[UUID, SandboxUsage]]
    ) -> int:
        last_moment = hour + _LAST_MOMENT_OF_HOUR
        rows = [
            {
                "benchmark_id": benchmark_id,
                "cost_type": SANDBOX_COST_TYPE,
                "source_id": sandbox_hour_source_id(usage.sandbox_id, hour),
                "provider": provider,
                "amount_usd": usage.total_price,
                "usage_end": min(usage.last_end, last_moment) if usage.last_end else last_moment,
            }
            for benchmark_id, usage in usages
        ]
        written = 0
        for batch in batched(rows, _SANDBOX_HOUR_COST_BATCH):
            result = self._session.exec(
                insert(RunCost)
                .values(list(batch))
                .on_conflict_do_nothing(
                    index_elements=["provider", "source_id"],
                    index_where=col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                )
            )
            written += result.rowcount
        return written

    def add_sandbox_cost(
        self, *, benchmark_id: UUID, provider: str, sandbox_id: str, amount_usd: float, usage_end: datetime
    ) -> bool:
        result = self._session.exec(
            insert(RunCost)
            .values(
                benchmark_id=benchmark_id,
                cost_type=SANDBOX_COST_TYPE,
                source_id=sandbox_id,
                provider=provider,
                amount_usd=amount_usd,
                usage_end=usage_end,
            )
            .on_conflict_do_nothing(
                index_elements=["provider", "source_id"],
                index_where=col(RunCost.cost_type) == SANDBOX_COST_TYPE,
            )
        )
        return result.rowcount == 1

    def price_sandbox_up_to(
        self, *, benchmark_id: UUID, provider: str, sandbox_id: str, amount_usd: float, usage_end: datetime
    ) -> bool:
        statement = insert(RunCost).values(
            benchmark_id=benchmark_id,
            cost_type=SANDBOX_COST_TYPE,
            source_id=sandbox_id,
            provider=provider,
            amount_usd=amount_usd,
            usage_end=usage_end,
        )
        result = self._session.exec(
            statement.on_conflict_do_update(
                index_elements=["provider", "source_id"],
                index_where=col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                set_={"amount_usd": statement.excluded.amount_usd, "usage_end": statement.excluded.usage_end},
                where=col(RunCost.usage_end) < statement.excluded.usage_end,
            )
        )
        return result.rowcount == 1

    def upsert_modal_run_cost(self, *, benchmark_id: UUID, amount_usd: float, usage_end: datetime) -> bool:
        result = self._session.exec(
            insert(RunCost)
            .values(
                benchmark_id=benchmark_id,
                cost_type=SANDBOX_COST_TYPE,
                source_id=str(benchmark_id),
                provider=MODAL_PROVIDER,
                amount_usd=amount_usd,
                usage_end=usage_end,
            )
            .on_conflict_do_update(
                index_elements=["provider", "source_id"],
                index_where=col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                set_={"amount_usd": amount_usd, "usage_end": usage_end},
            )
        )
        return result.rowcount == 1

    def add_search_costs(self, *, benchmark_id: UUID, attempt_id: str, charges: list[SearchCharge]) -> int:
        totals = attempt_totals(charges, attempt_id)
        saved = set(
            self._session.exec(
                select(RunCost.provider, RunCost.source_id).where(
                    RunCost.benchmark_id == benchmark_id,
                    RunCost.cost_type == SEARCH_COST_TYPE,
                    col(RunCost.source_id).in_([charge.source_id for charge in totals]),
                )
            ).all()
        )
        new = [charge for charge in totals if (charge.provider, charge.source_id) not in saved]
        saved_at = datetime.now(UTC)
        self._session.add_all(
            RunCost(
                benchmark_id=benchmark_id,
                cost_type=SEARCH_COST_TYPE,
                source_id=charge.source_id,
                provider=charge.provider,
                amount_usd=float(charge.amount_usd),
                usage_end=saved_at,
            )
            for charge in new
        )
        return len(new)

    def modal_runs_to_price(self) -> list[Benchmark]:
        used_modal = select(SandboxRecord.benchmark_id).where(col(SandboxRecord.provider) == MODAL_PROVIDER)
        latest_dispatch_finish = (
            select(func.max(col(ExecutorDispatch.finished_at)))
            .where(col(ExecutorDispatch.benchmark_id) == col(Benchmark.id))
            .scalar_subquery()
        )
        effective_finish = func.coalesce(col(Benchmark.finished_at), latest_dispatch_finish)
        rows = self._session.exec(
            cast(Any, select)(Benchmark, effective_finish.label("effective_finish"))
            .outerjoin(
                RunCost,
                and_(
                    col(RunCost.benchmark_id) == col(Benchmark.id),
                    col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                    col(RunCost.provider) == MODAL_PROVIDER,
                ),
            )
            .where(col(Benchmark.id).in_(used_modal))
            .where(col(Benchmark.status).in_(FINISHED_BENCHMARK_STATUSES))
            .where(effective_finish.is_not(None))
            .where(
                or_(
                    col(RunCost.id).is_(None),
                    col(RunCost.usage_end).is_(None),
                    col(RunCost.usage_end) < effective_finish,
                )
            )
        ).all()
        runs: list[Benchmark] = []
        for run, finish in rows:
            runs.append(run.model_copy(update={"finished_at": finish}))
        return runs

    def priced_source_ids(self, provider: str, source_ids: list[str]) -> set[str]:
        if not source_ids:
            return set()
        return {
            source_id
            for source_id in self._session.exec(
                select(RunCost.source_id).where(
                    col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                    col(RunCost.provider) == provider,
                    col(RunCost.source_id).in_(source_ids),
                )
            )
            if source_id is not None
        }

    def sandbox_owners(self, provider: str, sandbox_ids: list[str]) -> dict[str, UUID]:
        if not sandbox_ids:
            return {}
        rows = self._session.exec(
            select(SandboxRecord.sandbox_id, SandboxRecord.benchmark_id).where(
                col(SandboxRecord.provider) == provider, col(SandboxRecord.sandbox_id).in_(sandbox_ids)
            )
        )
        return dict(rows.all())

    def sandbox_accounts_in_use(self, provider: str, since: datetime) -> list[SandboxAccountUse]:
        secret_name = type_coerce(col(Benchmark.arguments), JSON)["sandbox_provider_secret_name"].as_string()
        active = col(Benchmark.status).not_in(FINISHED_BENCHMARK_STATUSES)
        ended_at = _run_last_active_at()
        has_sandbox = (
            select(SandboxRecord.sandbox_id)
            .where(col(SandboxRecord.provider) == provider, col(SandboxRecord.benchmark_id) == col(Benchmark.id))
            .exists()
        )
        statement = (
            cast(Any, select)(
                col(Benchmark.org_id),
                secret_name,
                func.sum(case((active, 1), else_=0)),
                func.max(ended_at),
            )
            .where(has_sandbox, secret_name.is_not(None), or_(active, ended_at >= since))
            .group_by(col(Benchmark.org_id), secret_name)
        )
        rows = cast(Sequence[tuple[UUID, str, int, datetime | None]], cast(Any, self._session.exec(statement)).all())
        return [
            SandboxAccountUse(org_id=org_id, secret_name=name, last_finished_at=None if active_runs else last_end)
            for org_id, name, active_runs, last_end in rows
        ]

    def unpriced_sandboxes(self, providers: Sequence[str], since: datetime) -> list[UnpricedSandbox]:
        secret_name = type_coerce(col(Benchmark.arguments), JSON)["sandbox_provider_secret_name"].as_string()
        priced = (
            select(RunCost.id)
            .where(
                col(RunCost.cost_type) == SANDBOX_COST_TYPE,
                col(RunCost.provider) == col(SandboxRecord.provider),
                col(RunCost.source_id) == col(SandboxRecord.sandbox_id),
                or_(
                    col(SandboxRecord.last_seen_at).is_(None), col(RunCost.usage_end) >= col(SandboxRecord.last_seen_at)
                ),
            )
            .exists()
        )
        statement = (
            cast(Any, select)(
                col(Benchmark.org_id),
                col(SandboxRecord.provider),
                secret_name,
                col(SandboxRecord.benchmark_id),
                col(SandboxRecord.sandbox_id),
                func.coalesce(col(SandboxRecord.last_seen_at), _run_last_active_at()),
            )
            .join(Benchmark, col(Benchmark.id) == col(SandboxRecord.benchmark_id))
            .where(
                col(SandboxRecord.provider).in_(providers),
                secret_name.is_not(None),
                or_(
                    col(Benchmark.status).not_in(FINISHED_BENCHMARK_STATUSES),
                    col(Benchmark.finished_at) >= since,
                    col(Benchmark.started_at) >= since,
                ),
                ~priced,
            )
            .order_by(col(SandboxRecord.benchmark_id), col(SandboxRecord.sandbox_id))
        )
        return [UnpricedSandbox(*row) for row in cast(Any, self._session.exec(statement)).all()]

    def mark_sandbox_seen(self, provider: str, sandbox_id: str, seen_at: datetime) -> None:
        self._session.exec(
            update(SandboxRecord)
            .where(col(SandboxRecord.provider) == provider, col(SandboxRecord.sandbox_id) == sandbox_id)
            .values(last_seen_at=later_of(col(SandboxRecord.last_seen_at), seen_at))
        )

    def usage_cursor(self, org_id: UUID, secret_name: str, start: datetime) -> datetime:
        self._session.exec(
            insert(SandboxCostCursor)
            .values(org_id=org_id, secret_name=secret_name, next_hour=start)
            .on_conflict_do_nothing(index_elements=["org_id", "secret_name"])
        )
        return self._session.exec(
            select(SandboxCostCursor.next_hour).where(
                col(SandboxCostCursor.org_id) == org_id, col(SandboxCostCursor.secret_name) == secret_name
            )
        ).one()

    def advance_usage_cursor(self, org_id: UUID, secret_name: str, hour: datetime, next_hour: datetime) -> bool:
        result = self._session.exec(
            update(SandboxCostCursor)
            .where(
                col(SandboxCostCursor.org_id) == org_id,
                col(SandboxCostCursor.secret_name) == secret_name,
                col(SandboxCostCursor.next_hour) == hour,
            )
            .values(next_hour=next_hour)
        )
        return result.rowcount == 1

    def record_unmatched_usage(
        self, org_id: UUID, secret_name: str, hour: datetime, usage: Sequence[SandboxUsage | None]
    ) -> None:
        priced = [entry for entry in usage if entry is not None]
        self._session.exec(
            insert(SandboxUnmatchedUsage)
            .values(
                org_id=org_id,
                secret_name=secret_name,
                hour=hour,
                amount_usd=sum(entry.total_price for entry in priced),
                sandboxes=len(usage),
                unpriced_sandboxes=len(usage) - len(priced),
            )
            .on_conflict_do_nothing(index_elements=["org_id", "secret_name", "hour"])
        )

    def unmatched_usage(self, org_id: UUID, start: datetime, end: datetime) -> tuple[float, int]:
        total_usd, unpriced_sandboxes = self._session.exec(
            select(
                func.coalesce(func.sum(SandboxUnmatchedUsage.amount_usd), 0.0),
                func.coalesce(func.sum(SandboxUnmatchedUsage.unpriced_sandboxes), 0),
            ).where(
                col(SandboxUnmatchedUsage.org_id) == org_id,
                col(SandboxUnmatchedUsage.hour) >= start,
                col(SandboxUnmatchedUsage.hour) < end,
            )
        ).one()
        return float(total_usd), int(unpriced_sandboxes)


CostRepositoryDependency = Annotated[CostRepository, Depends(CostRepository)]
