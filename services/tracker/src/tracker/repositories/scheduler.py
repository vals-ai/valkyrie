from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Annotated, Any, TypeVar, cast
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxReservationDemand
from fastapi import Depends
from sqlalchemy import JSON, Subquery, and_, case, or_, text, tuple_, type_coerce, union
from sqlalchemy import select as sa_select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import Session, col, delete, func, select, update
from sqlmodel.sql.expression import Select, SelectOfScalar

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Org,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    SandboxGradingReservation,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.executor.control_queue import queued_sandbox_name
from tracker.repositories.advisory_lock import (
    advisory_lock_key,
    task_build_lock_resource_id,
    task_evaluation_lock_resource_id,
)
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.scheduler.store import (
    GRANT_ACCEPTANCE_TIMEOUT,
    READINESS_CHECK_TIMEOUT,
    ActiveReservations,
    fits_in_capacity,
    queue_pool_lock_id,
)

_Statement = TypeVar("_Statement", Select[Any], SelectOfScalar[Any])
_ADMISSION_SCAN_CHUNK = 256
_ACTIVE_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)

_ARGUMENTS = type_coerce(col(Benchmark.arguments), JSON)
_IS_QUEUED = col(Benchmark.queue_pool_id).is_not(None)


def _open_runs(pool_id: str):
    concurrency = _ARGUMENTS["concurrency"].as_integer()
    active_task = aliased(Task)
    active = func.count(col(active_task.id))
    return (
        select(col(Benchmark.id).label("benchmark_id"), (concurrency - active).label("slots"))
        .outerjoin(
            active_task,
            and_(col(active_task.benchmark) == col(Benchmark.id), col(active_task.status).in_(_ACTIVE_STATUSES)),
        )
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(col(Benchmark.queue_pool_id) == pool_id)
        .where(
            col(Benchmark.id).in_(
                select(col(ExecutorDispatch.benchmark_id)).where(
                    col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING
                )
            )
        )
        .group_by(col(Benchmark.id))
        .having(active < concurrency)
        .subquery()
    )


def _with_demand(reserved: ActiveReservations, demand: SandboxReservationDemand) -> ActiveReservations:
    return replace(
        reserved,
        count=reserved.count + 1,
        vcpu=reserved.vcpu + demand.vcpu,
        memory=reserved.memory + demand.memory,
        disk=reserved.disk + demand.disk,
        gpu=reserved.gpu + demand.gpu,
    )


def _grading_attempt_in(statuses: Sequence[TaskStatus]):
    return (
        select(col(Task.id))
        .where(col(Task.id) == col(SandboxGradingReservation.task_row_id))
        .where(col(Task.started_at) == col(SandboxGradingReservation.attempt_started_at))
        .where(col(Task.status).in_(statuses))
        .correlate(SandboxGradingReservation)
        .exists()
    )


def _pending_tasks(statement: _Statement, open_runs: Subquery) -> _Statement:
    return (
        statement.join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .join(open_runs, open_runs.c.benchmark_id == col(Benchmark.id))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(~_blocking_reservation_exists())
    )


def _eligible_tasks(pool_id: str):
    return _pending_tasks(select(col(Task.id)), _open_runs(pool_id))


def _blocking_reservation_exists():
    return (
        select(col(SandboxBuildReservation.task_row_id))
        .where(col(SandboxBuildReservation.task_row_id) == col(Task.id))
        .where(
            col(SandboxBuildReservation.released_at).is_(None)
            | (col(SandboxBuildReservation.attempt_started_at) == col(Task.started_at))
        )
        .correlate(Task)
        .exists()
    )


def _admissible_ids(pool_id: str, limit: int | None):
    eligible = _eligible_tasks(pool_id)
    if limit is None:
        return eligible
    priority = type_coerce(col(Benchmark.arguments), JSON)["priority"].as_integer()
    return eligible.order_by(priority, col(Task.started_at), col(Task.id)).limit(limit)


def _current_candidate_attempt():
    return (
        select(col(Task.id))
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .join(
            ExecutorDispatch,
            and_(
                col(ExecutorDispatch.id) == col(SandboxAdmissionCandidate.executor_dispatch_id),
                col(ExecutorDispatch.benchmark_id) == col(Benchmark.id),
            ),
        )
        .where(col(Task.id) == col(SandboxAdmissionCandidate.task_row_id))
        .where(col(Task.started_at) == col(SandboxAdmissionCandidate.attempt_started_at))
        .where(col(Task.status) == TaskStatus.PENDING)
        .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
    )


@dataclass(frozen=True)
class PoolProviderReference:
    provider_type: str | None
    secret_name: str | None


class SchedulerRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def eligible_task_ids(self, pool_id: str, task_row_ids: Sequence[UUID]) -> set[UUID]:
        return set(self._session.exec(_eligible_tasks(pool_id).where(col(Task.id).in_(task_row_ids))).all())

    def eligible_task_is(
        self, pool_id: str, task_row_id: UUID, expected_started_at: datetime, limit: int | None
    ) -> bool:
        return (
            self._session.exec(
                select(col(Task.id))
                .where(col(Task.id) == task_row_id)
                .where(col(Task.started_at) == expected_started_at)
                .where(col(Task.id).in_(_admissible_ids(pool_id, limit)))
            ).first()
            is not None
        )

    def claim_eligible_task(self, pool_id: str, task_row_id: UUID, expected_started_at: datetime) -> bool:
        result = self._session.exec(
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.id).in_(_admissible_ids(pool_id, 1)))
            .values(status=TaskStatus.BUILDING)
        )
        return result.rowcount == 1

    def reserved_builds(self, pool_id: str) -> dict[UUID, SandboxReservationDemand]:
        rows = self._session.exec(
            select(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.released_at).is_(None))
        ).all()
        return {
            row.task_row_id: SandboxReservationDemand(
                target_id=row.target_id,
                sandbox_class=row.sandbox_class,
                vcpu=row.requested_vcpu,
                memory=row.requested_memory,
                disk=row.requested_disk,
                gpu=row.requested_gpu,
            )
            for row in rows
        }

    def waiting_rows(
        self,
        *,
        org_id: UUID,
        limit: int,
        offset: int = 0,
    ) -> tuple[list[tuple[Task, Benchmark, int, str, int]], dict[str, int]]:
        priority = _ARGUMENTS["priority"].as_integer()
        pool_id = col(Benchmark.queue_pool_id)
        queued_tasks = (
            sa_select(
                cast(ColumnElement[UUID], col(Task.id).label("task_id")),
                cast(ColumnElement[UUID], col(Task.org_id).label("task_org_id")),
                cast(ColumnElement[UUID], col(Benchmark.org_id).label("benchmark_org_id")),
                priority.label("priority"),
                pool_id.label("pool_id"),
                func.row_number()
                .over(
                    partition_by=pool_id,
                    order_by=(priority.asc(), col(Task.started_at).asc(), col(Task.id).asc()),
                )
                .label("position"),
            )
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(
                col(Task.status) == TaskStatus.PENDING,
                col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
                _IS_QUEUED,
            )
            .subquery()
        )
        scope = (queued_tasks.c.task_org_id == org_id, queued_tasks.c.benchmark_org_id == org_id)
        pool_counts_statement = cast(
            Select[tuple[str, int]],
            sa_select(queued_tasks.c.pool_id, func.count(queued_tasks.c.task_id))
            .where(*scope)
            .group_by(queued_tasks.c.pool_id),
        )
        pool_counts = dict(self._session.exec(pool_counts_statement).all())
        rows_statement = cast(
            Select[tuple[Task, Benchmark, int, str, int]],
            sa_select(
                Task,
                Benchmark,
                queued_tasks.c.priority,
                queued_tasks.c.pool_id,
                queued_tasks.c.position,
            )
            .join(queued_tasks, col(Task.id) == queued_tasks.c.task_id)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(*scope)
            .order_by(queued_tasks.c.priority.asc(), col(Task.started_at).asc(), col(Task.id).asc())
            .offset(offset)
            .limit(limit + 1),
        )
        rows = list(self._session.exec(rows_statement).all())

        return rows, pool_counts

    def pool_references(
        self,
        *,
        org_id: UUID,
    ) -> dict[str, set[PoolProviderReference]]:
        pool_id = col(Benchmark.queue_pool_id)
        provider_type = _ARGUMENTS["sandbox_provider"].as_string()
        secret_name = _ARGUMENTS["sandbox_provider_secret_name"].as_string()
        lifecycle_scope = or_(
            and_(
                col(Task.status) == TaskStatus.PENDING,
                col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            ),
            and_(
                col(Task.status).in_(_ACTIVE_STATUSES),
                col(Benchmark.status).in_((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)),
            ),
        )
        statement = cast(
            Select[tuple[str, str | None, str | None]],
            sa_select(
                pool_id,
                provider_type,
                secret_name,
            )
            .select_from(Task)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(
                col(Task.org_id) == org_id,
                col(Benchmark.org_id) == org_id,
                lifecycle_scope,
                _IS_QUEUED,
            )
            .distinct(),
        )
        references: dict[str, set[PoolProviderReference]] = {}
        for stored_pool_id, stored_provider_type, stored_secret_name in self._session.exec(statement).all():
            references.setdefault(stored_pool_id, set()).add(
                PoolProviderReference(
                    provider_type=stored_provider_type,
                    secret_name=stored_secret_name,
                )
            )

        return references

    def active_rows(
        self,
        *,
        org_id: UUID,
        limit: int,
        offset: int = 0,
    ) -> tuple[list[tuple[Task, Benchmark]], dict[TaskStatus, int]]:
        scope = (
            col(Task.org_id) == org_id,
            col(Benchmark.org_id) == org_id,
            col(Benchmark.status).in_((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)),
            col(Task.status).in_(_ACTIVE_STATUSES),
            _IS_QUEUED,
        )
        counts = dict(
            self._session.exec(
                select(col(Task.status), func.count(col(Task.id)))
                .select_from(Task)
                .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
                .where(*scope)
                .group_by(col(Task.status))
            ).all()
        )
        rows = self._session.exec(
            select(Task, Benchmark)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(*scope)
            .order_by(col(Task.started_at).asc(), col(Task.id).asc())
            .offset(offset)
            .limit(limit + 1)
            .execution_options(populate_existing=True)
        ).all()

        return list(rows), counts

    def task_readiness(
        self, org_id: UUID, benchmark_id: UUID, dispatch_id: UUID
    ) -> Sequence[tuple[Task, datetime | None]]:
        return self._session.exec(
            select(Task, col(SandboxAdmissionCandidate.readiness_requested_at))
            .outerjoin(
                SandboxAdmissionCandidate,
                (col(SandboxAdmissionCandidate.task_row_id) == col(Task.id))
                & (col(SandboxAdmissionCandidate.attempt_started_at) == col(Task.started_at))
                & (col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id),
            )
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
        ).all()

    def candidate_state(
        self, task_row_id: UUID, expected_started_at: datetime, dispatch_id: UUID
    ) -> tuple[TaskStatus | None, datetime | None]:
        row = self._session.exec(
            select(Task.status, SandboxAdmissionCandidate.readiness_requested_at)
            .outerjoin(
                SandboxAdmissionCandidate,
                (col(SandboxAdmissionCandidate.task_row_id) == col(Task.id))
                & (col(SandboxAdmissionCandidate.attempt_started_at) == col(Task.started_at))
                & (col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id),
            )
            .where(col(Task.id) == task_row_id, col(Task.started_at) == expected_started_at)
        ).first()
        return (row[0], row[1]) if row is not None else (None, None)

    def readiness_requests(
        self, attempts: Collection[tuple[UUID, datetime]], dispatch_id: UUID
    ) -> dict[tuple[UUID, datetime], datetime | None]:
        rows = self._session.exec(
            select(
                SandboxAdmissionCandidate.task_row_id,
                SandboxAdmissionCandidate.attempt_started_at,
                SandboxAdmissionCandidate.readiness_requested_at,
            )
            .where(
                tuple_(
                    col(SandboxAdmissionCandidate.task_row_id), col(SandboxAdmissionCandidate.attempt_started_at)
                ).in_(list(attempts))
            )
            .where(col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id)
        ).all()
        return {(row_id, started_at): requested_at for row_id, started_at, requested_at in rows}

    def reserved_task_row_ids(self, task_row_ids: Collection[UUID]) -> set[UUID]:
        return set(
            self._session.exec(
                select(SandboxBuildReservation.task_row_id)
                .where(col(SandboxBuildReservation.task_row_id).in_(task_row_ids))
                .where(col(SandboxBuildReservation.released_at).is_(None))
            ).all()
        )

    def try_task_evaluation_transaction_lock(self, task_row_id: UUID) -> bool:
        return bool(
            self._session.connection()
            .execute(
                text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                {"lock_key": advisory_lock_key(task_evaluation_lock_resource_id(task_row_id))},
            )
            .scalar_one()
        )

    def prepare_admission_candidate(
        self,
        pool_id: str,
        task_row_id: UUID,
        expected_started_at: datetime,
        dispatch_id: UUID,
        demand: SandboxReservationDemand,
        *,
        admission_ready: bool,
        grading_demand: SandboxReservationDemand | None = None,
    ) -> None:
        values = {
            "task_row_id": task_row_id,
            "attempt_started_at": expected_started_at,
            "executor_dispatch_id": dispatch_id,
            "admission_ready": admission_ready,
            "readiness_checked_at": self.database_now(),
            "readiness_requested_at": None,
            "pool_id": queue_pool_lock_id(pool_id),
            "target_id": demand.target_id,
            "sandbox_class": demand.sandbox_class,
            "requested_vcpu": demand.vcpu,
            "requested_memory": demand.memory,
            "requested_disk": demand.disk,
            "requested_gpu": demand.gpu,
        }
        self._session.exec(
            insert(SandboxAdmissionCandidate)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["task_row_id", "attempt_started_at"],
                set_=values,
            )
        )
        if grading_demand is None:
            return
        grading = {
            "task_row_id": task_row_id,
            "attempt_started_at": expected_started_at,
            "pool_id": queue_pool_lock_id(pool_id),
            "target_id": grading_demand.target_id,
            "sandbox_class": grading_demand.sandbox_class,
            "requested_vcpu": grading_demand.vcpu,
            "requested_memory": grading_demand.memory,
            "requested_disk": grading_demand.disk,
            "requested_gpu": grading_demand.gpu,
        }
        self._session.exec(
            insert(SandboxGradingReservation)
            .values(**grading)
            .on_conflict_do_update(index_elements=["task_row_id", "attempt_started_at"], set_=grading)
        )

    def clear_admission_candidate(self, task_row_id: UUID, expected_started_at: datetime, dispatch_id: UUID) -> None:
        self._session.exec(
            delete(SandboxAdmissionCandidate)
            .where(col(SandboxAdmissionCandidate.task_row_id) == task_row_id)
            .where(col(SandboxAdmissionCandidate.attempt_started_at) == expected_started_at)
            .where(col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id)
        )

    def request_readiness_checks(
        self,
        pool_id: str,
        *,
        limit: int,
        retry_delay: timedelta,
    ) -> int:
        now = self.database_now()
        cutoff = now - READINESS_CHECK_TIMEOUT
        outstanding = self._session.exec(
            select(func.count(col(SandboxAdmissionCandidate.task_row_id)))
            .where(col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxAdmissionCandidate.admission_ready).is_(False))
            .where(col(SandboxAdmissionCandidate.readiness_requested_at) > cutoff)
        ).one()
        available = limit - outstanding
        if available <= 0:
            return 0
        last_activity = case(
            (
                col(SandboxAdmissionCandidate.readiness_requested_at).is_(None),
                col(SandboxAdmissionCandidate.readiness_checked_at),
            ),
            else_=col(SandboxAdmissionCandidate.readiness_requested_at),
        )
        selected = (
            select(col(SandboxAdmissionCandidate.task_row_id), col(SandboxAdmissionCandidate.attempt_started_at))
            .where(col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxAdmissionCandidate.admission_ready).is_(False))
            .where(col(SandboxAdmissionCandidate.readiness_checked_at) <= now - retry_delay)
            .where(
                col(SandboxAdmissionCandidate.readiness_requested_at).is_(None)
                | (col(SandboxAdmissionCandidate.readiness_requested_at) <= cutoff)
            )
            .where(_current_candidate_attempt().exists())
            .where(col(SandboxAdmissionCandidate.task_row_id).in_(_eligible_tasks(pool_id)))
            .order_by(last_activity, col(SandboxAdmissionCandidate.task_row_id))
            .limit(available)
        )
        result = self._session.exec(
            update(SandboxAdmissionCandidate)
            .where(
                tuple_(
                    col(SandboxAdmissionCandidate.task_row_id), col(SandboxAdmissionCandidate.attempt_started_at)
                ).in_(selected)
            )
            .where(col(SandboxAdmissionCandidate.admission_ready).is_(False))
            .values(readiness_requested_at=now)
        )
        return result.rowcount

    def begin_readiness_check(
        self,
        task_row_id: UUID,
        expected_started_at: datetime,
        dispatch_id: UUID,
        requested_at: datetime,
    ) -> bool:
        now = self.database_now()
        result = self._session.exec(
            update(SandboxAdmissionCandidate)
            .where(col(SandboxAdmissionCandidate.task_row_id) == task_row_id)
            .where(col(SandboxAdmissionCandidate.attempt_started_at) == expected_started_at)
            .where(col(SandboxAdmissionCandidate.executor_dispatch_id) == dispatch_id)
            .where(col(SandboxAdmissionCandidate.admission_ready).is_(False))
            .where(col(SandboxAdmissionCandidate.readiness_requested_at) == requested_at)
            .where(col(SandboxAdmissionCandidate.readiness_requested_at) > now - READINESS_CHECK_TIMEOUT)
            .where(_current_candidate_attempt().exists())
            .values(
                readiness_requested_at=case(
                    (
                        col(SandboxAdmissionCandidate.readiness_requested_at) >= now,
                        col(SandboxAdmissionCandidate.readiness_requested_at) + timedelta(microseconds=1),
                    ),
                    else_=now,
                )
            )
        )
        return result.rowcount == 1

    def purge_stale_admission_candidates(self, pool_id: str) -> None:
        self._session.exec(
            delete(SandboxAdmissionCandidate)
            .where(col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id))
            .where(~_current_candidate_attempt().exists())
        )

    def packed_admission_candidates(
        self,
        pool_id: str,
        capacities: Mapping[tuple[str, str], SandboxCapacity],
        capacity_read_at: datetime,
        limit: int | None,
    ) -> Iterator[SandboxAdmissionCandidate]:
        arguments = type_coerce(col(Benchmark.arguments), JSON)
        priority = arguments["priority"].as_integer()
        open_runs = _open_runs(pool_id)
        candidates = self._session.exec(
            _pending_tasks(
                select(
                    col(Task.benchmark),
                    SandboxAdmissionCandidate,
                    SandboxGradingReservation,
                    priority,
                    open_runs.c.slots,
                ),
                open_runs,
            )
            .outerjoin(
                SandboxAdmissionCandidate,
                and_(
                    col(SandboxAdmissionCandidate.task_row_id) == col(Task.id),
                    col(SandboxAdmissionCandidate.attempt_started_at) == col(Task.started_at),
                    col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id),
                    _current_candidate_attempt().correlate(SandboxAdmissionCandidate).exists(),
                ),
            )
            .outerjoin(
                SandboxGradingReservation,
                and_(
                    col(SandboxGradingReservation.task_row_id) == col(Task.id),
                    col(SandboxGradingReservation.attempt_started_at) == col(Task.started_at),
                ),
            )
            .order_by(priority, col(Task.started_at), col(Task.id))
            .execution_options(yield_per=_ADMISSION_SCAN_CHUNK)
        )
        charged = {key: ActiveReservations(count=0, vcpu=0, memory=0, disk=0, gpu=0) for key in capacities}
        charged.update(self.charged_reservations_by_domain(pool_id, capacity_read_at))
        slots: dict[UUID, int] = {}
        selected = 0
        unknown_priority: int | None = None
        for benchmark_id, joined_candidate, joined_grading, rank, available_slots in candidates:
            if limit is not None and selected >= limit:
                return
            if unknown_priority is not None and rank > unknown_priority:
                return
            candidate = cast(SandboxAdmissionCandidate | None, joined_candidate)
            if benchmark_id not in slots:
                slots[benchmark_id] = available_slots
            if slots[benchmark_id] <= 0:
                continue
            if candidate is None:
                unknown_priority = rank
                continue
            if not candidate.admission_ready:
                continue
            domain = candidate.target_id, candidate.sandbox_class
            if domain not in capacities:
                continue
            demand = SandboxReservationDemand(
                target_id=candidate.target_id,
                sandbox_class=candidate.sandbox_class,
                vcpu=candidate.requested_vcpu,
                memory=candidate.requested_memory,
                disk=candidate.requested_disk,
                gpu=candidate.requested_gpu,
            )
            if not fits_in_capacity(demand, capacities[domain], charged[domain]):
                continue
            grading = cast(SandboxGradingReservation | None, joined_grading)
            grading_demand = (
                None
                if grading is None
                else SandboxReservationDemand(
                    target_id=grading.target_id,
                    sandbox_class=grading.sandbox_class,
                    vcpu=grading.requested_vcpu,
                    memory=grading.requested_memory,
                    disk=grading.requested_disk,
                    gpu=grading.requested_gpu,
                )
            )
            if grading_demand is not None:
                grading_domain = grading_demand.target_id, grading_demand.sandbox_class
                if grading_domain not in capacities:
                    continue
                reserved = charged[grading_domain]
                if grading_domain == domain:
                    reserved = _with_demand(reserved, demand)
                if not fits_in_capacity(grading_demand, capacities[grading_domain], reserved):
                    continue
            slots[benchmark_id] -= 1
            selected += 1
            charged[domain] = _with_demand(charged[domain], demand)
            if grading_demand is not None:
                grading_domain = grading_demand.target_id, grading_demand.sandbox_class
                charged[grading_domain] = _with_demand(charged[grading_domain], grading_demand)
            yield candidate

    def grant_prepared_tasks(
        self,
        pool_id: str,
        capacities: Mapping[tuple[str, str], SandboxCapacity],
        capacity_read_at: datetime,
        limit: int,
    ) -> int:
        candidates = list(self.packed_admission_candidates(pool_id, capacities, capacity_read_at, limit))
        if not candidates:
            return 0

        task_ids = [candidate.task_row_id for candidate in candidates]
        benchmark_ids = select(col(Task.benchmark)).where(col(Task.id).in_(task_ids))
        locked_benchmarks = self._session.exec(
            select(col(Benchmark.id))
            .where(col(Benchmark.id).in_(benchmark_ids))
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            .order_by(col(Benchmark.id))
            .with_for_update()
        ).all()
        dispatch_ids = [candidate.executor_dispatch_id for candidate in candidates]
        locked_dispatches = set(
            self._session.exec(
                select(col(ExecutorDispatch.id))
                .where(col(ExecutorDispatch.id).in_(dispatch_ids))
                .where(col(ExecutorDispatch.benchmark_id).in_(locked_benchmarks))
                .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
                .order_by(col(ExecutorDispatch.id))
                .with_for_update()
            ).all()
        )
        selected_tasks = set(task_ids)
        eligible = [
            candidate
            for candidate in self.packed_admission_candidates(pool_id, capacities, capacity_read_at, limit)
            if candidate.task_row_id in selected_tasks and candidate.executor_dispatch_id in locked_dispatches
        ]
        if not eligible:
            return 0

        claimed = {
            (task_id, started_at)
            for task_id, started_at in self._session.exec(
                update(Task)
                .where(
                    tuple_(col(Task.id), col(Task.started_at)).in_(
                        [(candidate.task_row_id, candidate.attempt_started_at) for candidate in eligible]
                    )
                )
                .where(col(Task.status) == TaskStatus.PENDING)
                .values(status=TaskStatus.BUILDING)
                .returning(col(Task.id), col(Task.started_at))
                .execution_options(synchronize_session="fetch")
            ).all()
        }
        granted = [
            candidate for candidate in eligible if (candidate.task_row_id, candidate.attempt_started_at) in claimed
        ]
        if not granted:
            return 0

        reserved_at = self.database_now()
        self._session.exec(
            insert(SandboxBuildReservation).values(
                [
                    {
                        "task_row_id": candidate.task_row_id,
                        "attempt_started_at": candidate.attempt_started_at,
                        "executor_dispatch_id": candidate.executor_dispatch_id,
                        "reserved_at": reserved_at,
                        "pool_id": queue_pool_lock_id(pool_id),
                        "target_id": candidate.target_id,
                        "sandbox_class": candidate.sandbox_class,
                        "requested_vcpu": candidate.requested_vcpu,
                        "requested_memory": candidate.requested_memory,
                        "requested_disk": candidate.requested_disk,
                        "requested_gpu": candidate.requested_gpu,
                    }
                    for candidate in granted
                ]
            )
        )
        self._session.exec(
            delete(SandboxAdmissionCandidate)
            .where(
                tuple_(
                    col(SandboxAdmissionCandidate.task_row_id),
                    col(SandboxAdmissionCandidate.attempt_started_at),
                    col(SandboxAdmissionCandidate.executor_dispatch_id),
                ).in_(
                    [
                        (candidate.task_row_id, candidate.attempt_started_at, candidate.executor_dispatch_id)
                        for candidate in granted
                    ]
                )
            )
            .execution_options(synchronize_session="fetch")
        )
        return len(granted)

    def accept_grant(
        self,
        task_row_id: UUID,
        expected_started_at: datetime,
        dispatch_id: UUID,
    ) -> bool:
        now = self.database_now()
        building_attempt = (
            select(col(Task.id))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        )
        result = self._session.exec(
            update(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.task_row_id).in_(building_attempt))
            .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
            .where(col(SandboxBuildReservation.executor_dispatch_id) == dispatch_id)
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .where(col(SandboxBuildReservation.accepted_at).is_(None))
            .where(col(SandboxBuildReservation.reserved_at) > now - GRANT_ACCEPTANCE_TIMEOUT)
            .values(accepted_at=now)
        )
        return result.rowcount == 1

    def expire_unaccepted_grants(self, pool_id: str) -> None:
        now = self.database_now()
        live_owner = (
            select(col(Task.id))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .join(ExecutorDispatch, col(ExecutorDispatch.benchmark_id) == col(Benchmark.id))
            .where(col(Task.id) == col(SandboxBuildReservation.task_row_id))
            .where(col(Task.started_at) == col(SandboxBuildReservation.attempt_started_at))
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            .where(col(ExecutorDispatch.id) == col(SandboxBuildReservation.executor_dispatch_id))
            .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
            .correlate(SandboxBuildReservation)
            .exists()
        )
        revocable = and_(
            col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id),
            col(SandboxBuildReservation.released_at).is_(None),
            col(SandboxBuildReservation.accepted_at).is_(None),
            (col(SandboxBuildReservation.reserved_at) <= now - GRANT_ACCEPTANCE_TIMEOUT) | ~live_owner,
        )
        candidates = self._session.exec(select(col(SandboxBuildReservation.task_row_id)).where(revocable)).all()
        claimed = self.try_task_build_transaction_locks(candidates)
        if not claimed:
            return
        locked = self._session.exec(
            select(col(Task.id))
            .where(col(Task.id).in_(claimed))
            .order_by(col(Task.id))
            .with_for_update(skip_locked=True)
        ).all()
        if not locked:
            return

        revoked = self._session.exec(
            update(SandboxBuildReservation)
            .where(revocable)
            .where(col(SandboxBuildReservation.task_row_id).in_(locked))
            .values(released_at=now)
            .returning(col(SandboxBuildReservation.task_row_id), col(SandboxBuildReservation.attempt_started_at))
        ).all()
        if not revoked:
            return

        queued_benchmarks = select(col(Benchmark.id)).where(
            col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            col(Benchmark.queue_pool_id) == pool_id,
        )
        reset = self._session.exec(
            update(Task)
            .where(tuple_(col(Task.id), col(Task.started_at)).in_(revoked))
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Task.benchmark).in_(queued_benchmarks))
            .values(
                status=TaskStatus.PENDING,
                started_at=case(
                    (col(Task.started_at) >= now, col(Task.started_at) + timedelta(microseconds=1)),
                    else_=now,
                ),
            )
            .returning(col(Task.id))
        ).all()
        previous = dict(revoked)
        for (task_row_id,) in reset:
            attempt_id = queued_sandbox_name(task_row_id, previous[task_row_id]).removeprefix("queued-")
            RemoteGraderRepository(self._session).close_attempt(task_row_id, attempt_id)

    def database_now(self) -> datetime:
        return self._session.connection().execute(sa_select(func.clock_timestamp())).scalar_one()

    def active_reservations(self, pool_id: str) -> ActiveReservations:
        count, vcpu, memory, disk, gpu = (
            self._session.connection()
            .execute(
                sa_select(
                    func.count(col(SandboxBuildReservation.task_row_id)),
                    func.coalesce(func.sum(col(SandboxBuildReservation.requested_vcpu)), 0),
                    func.coalesce(func.sum(col(SandboxBuildReservation.requested_memory)), 0),
                    func.coalesce(func.sum(col(SandboxBuildReservation.requested_disk)), 0),
                    func.coalesce(func.sum(col(SandboxBuildReservation.requested_gpu)), 0),
                )
                .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
                .where(col(SandboxBuildReservation.released_at).is_(None))
            )
            .one()
        )
        return ActiveReservations(count=count, vcpu=vcpu, memory=memory, disk=disk, gpu=gpu)

    def charged_reservations_by_domain(
        self, pool_id: str, capacity_read_at: datetime
    ) -> dict[tuple[str, str], ActiveReservations]:
        rows = self._session.connection().execute(
            sa_select(
                col(SandboxBuildReservation.target_id),
                col(SandboxBuildReservation.sandbox_class),
                func.count(col(SandboxBuildReservation.task_row_id)),
                func.sum(col(SandboxBuildReservation.requested_vcpu)),
                func.sum(col(SandboxBuildReservation.requested_memory)),
                func.sum(col(SandboxBuildReservation.requested_disk)),
                func.sum(col(SandboxBuildReservation.requested_gpu)),
            )
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(
                col(SandboxBuildReservation.released_at).is_(None)
                | (col(SandboxBuildReservation.released_at) > capacity_read_at)
            )
            .group_by(col(SandboxBuildReservation.target_id), col(SandboxBuildReservation.sandbox_class))
        )
        grading_rows = self._session.connection().execute(
            sa_select(
                col(SandboxGradingReservation.target_id),
                col(SandboxGradingReservation.sandbox_class),
                func.count(col(SandboxGradingReservation.task_row_id)),
                func.sum(col(SandboxGradingReservation.requested_vcpu)),
                func.sum(col(SandboxGradingReservation.requested_memory)),
                func.sum(col(SandboxGradingReservation.requested_disk)),
                func.sum(col(SandboxGradingReservation.requested_gpu)),
            )
            .where(col(SandboxGradingReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(_grading_attempt_in(_ACTIVE_STATUSES))
            .where(
                col(SandboxGradingReservation.released_at).is_(None)
                | (col(SandboxGradingReservation.released_at) > capacity_read_at)
            )
            .group_by(col(SandboxGradingReservation.target_id), col(SandboxGradingReservation.sandbox_class))
        )
        charged: dict[tuple[str, str], ActiveReservations] = {}
        for target_id, sandbox_class, count, vcpu, memory, disk, gpu in [*rows, *grading_rows]:
            prior = charged.get(
                (target_id, sandbox_class), ActiveReservations(count=0, vcpu=0, memory=0, disk=0, gpu=0)
            )
            charged[(target_id, sandbox_class)] = ActiveReservations(
                count=prior.count + count,
                vcpu=prior.vcpu + vcpu,
                memory=prior.memory + memory,
                disk=prior.disk + disk,
                gpu=prior.gpu + gpu,
            )
        return charged

    def admissions_since(self, pool_id: str, since: datetime) -> int:
        return (
            self._session.connection()
            .execute(
                sa_select(func.count(col(SandboxBuildReservation.task_row_id)))
                .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
                .where(col(SandboxBuildReservation.reserved_at) >= since)
            )
            .scalar_one()
        )

    def release_reservation(self, task_row_id: UUID, expected_started_at: datetime) -> None:
        self._session.exec(
            update(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
            .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .values(released_at=self.database_now())
        )

    def release_grading_reservation(self, task_row_id: UUID, expected_started_at: datetime) -> None:
        self._session.exec(
            update(SandboxGradingReservation)
            .where(col(SandboxGradingReservation.task_row_id) == task_row_id)
            .where(col(SandboxGradingReservation.attempt_started_at) == expected_started_at)
            .where(col(SandboxGradingReservation.released_at).is_(None))
            .values(released_at=self.database_now())
        )

    def purge_released_reservations(self, pool_id: str, released_before: datetime) -> None:
        self._session.exec(
            delete(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.released_at) < released_before)
        )
        self._session.exec(
            delete(SandboxGradingReservation)
            .where(col(SandboxGradingReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(
                (col(SandboxGradingReservation.released_at) < released_before)
                | ~_grading_attempt_in((TaskStatus.PENDING, *_ACTIVE_STATUSES))
            )
        )

    def handoff_dispatch_phases(self, dispatch_id: UUID) -> None:
        self._session.exec(
            update(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.phase_dispatch_id) == dispatch_id)
            .where(col(SandboxBuildReservation.accepted_at).is_not(None))
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .values(phase_dispatch_id=None, phase_token=None)
        )

    def pending_owned_ecs_task_arns(self, pool_id: str) -> dict[UUID, str]:
        rows = self._session.exec(
            select(col(ExecutorDispatch.id), col(ExecutorDispatch.ecs_task_arn))
            .join(
                SandboxBuildReservation,
                col(SandboxBuildReservation.phase_dispatch_id) == col(ExecutorDispatch.id),
            )
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.accepted_at).is_not(None))
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .where(col(ExecutorDispatch.status) != ExecutorDispatchStatus.RUNNING)
            .where(col(ExecutorDispatch.ecs_task_arn).is_not(None))
            .distinct()
        ).all()
        return {dispatch_id: cast(str, task_arn) for dispatch_id, task_arn in rows}

    def lock_abandoned_reservations(self, pool_id: str) -> list[SandboxBuildReservation]:
        candidates = self._session.exec(
            select(SandboxBuildReservation.task_row_id)
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .where(col(SandboxBuildReservation.accepted_at).is_not(None))
            .where(col(SandboxBuildReservation.phase_dispatch_id).is_(None))
        ).all()
        abandoned = self.try_task_build_transaction_locks(candidates)
        if not abandoned:
            return []

        return list(
            self._session.exec(
                select(SandboxBuildReservation)
                .where(col(SandboxBuildReservation.task_row_id).in_(abandoned))
                .where(col(SandboxBuildReservation.released_at).is_(None))
                .where(col(SandboxBuildReservation.accepted_at).is_not(None))
                .where(col(SandboxBuildReservation.phase_dispatch_id).is_(None))
            ).all()
        )

    def abandoned_reservation(
        self,
        pool_id: str,
        task_row_id: UUID,
        expected_started_at: datetime,
        dispatch_id: UUID,
        reserved_at: datetime,
        phase_dispatch_id: UUID | None,
        phase_token: UUID | None,
    ) -> SandboxBuildReservation | None:
        return self._session.exec(
            select(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(pool_id))
            .where(col(SandboxBuildReservation.task_row_id) == task_row_id)
            .where(col(SandboxBuildReservation.attempt_started_at) == expected_started_at)
            .where(col(SandboxBuildReservation.executor_dispatch_id) == dispatch_id)
            .where(col(SandboxBuildReservation.reserved_at) == reserved_at)
            .where(col(SandboxBuildReservation.phase_dispatch_id) == phase_dispatch_id)
            .where(col(SandboxBuildReservation.phase_token) == phase_token)
            .where(col(SandboxBuildReservation.accepted_at).is_not(None))
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .where(col(SandboxBuildReservation.phase_dispatch_id).is_(None))
        ).one_or_none()

    def try_task_build_transaction_locks(self, task_row_ids: Sequence[UUID]) -> set[UUID]:
        if not task_row_ids:
            return set()
        keys = {
            advisory_lock_key(task_build_lock_resource_id(task_row_id)): task_row_id for task_row_id in task_row_ids
        }
        locked = self._session.connection().execute(
            text("SELECT key FROM unnest(CAST(:keys AS bigint[])) AS key WHERE pg_try_advisory_xact_lock(key)"),
            {"keys": list(keys)},
        )
        return {keys[key] for (key,) in locked}

    def reset_abandoned_builds(self, pool_id: str, now: datetime) -> None:
        queued_benchmarks = select(col(Benchmark.id)).where(
            col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            col(Benchmark.queue_pool_id) == pool_id,
        )
        unhanded_reservations = select(col(SandboxBuildReservation.task_row_id)).where(
            col(SandboxBuildReservation.released_at).is_(None),
            col(SandboxBuildReservation.accepted_at).is_(None)
            | col(SandboxBuildReservation.phase_dispatch_id).is_not(None),
        )
        building = (
            select(col(Task.id))
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Task.benchmark).in_(queued_benchmarks))
            .where(col(Task.id).not_in(unhanded_reservations))
        )
        abandoned = self.try_task_build_transaction_locks(self._session.exec(building).all())
        if not abandoned:
            return
        current = dict(
            self._session.exec(
                select(col(Task.id), col(Task.started_at))
                .where(col(Task.id).in_(abandoned))
                .where(col(Task.status) == TaskStatus.BUILDING)
                .where(col(Task.benchmark).in_(queued_benchmarks))
                .where(col(Task.id).not_in(unhanded_reservations))
                .order_by(col(Task.id))
                .with_for_update(skip_locked=True)
            ).all()
        )
        if not current:
            return

        reset = self._session.exec(
            update(Task)
            .where(col(Task.id).in_(current))
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Task.benchmark).in_(queued_benchmarks))
            .where(col(Task.id).not_in(unhanded_reservations))
            .values(
                status=TaskStatus.PENDING,
                started_at=case(
                    (col(Task.started_at) >= now, col(Task.started_at) + timedelta(microseconds=1)),
                    else_=now,
                ),
            )
            .returning(col(Task.id))
        ).all()
        for (task_row_id,) in reset:
            attempt_id = queued_sandbox_name(task_row_id, current[task_row_id]).removeprefix("queued-")
            RemoteGraderRepository(self._session).close_attempt(task_row_id, attempt_id)

    def is_waiting(self, task_row_id: UUID, expected_started_at: datetime) -> bool:
        return (
            self._session.exec(
                select(col(Task.id))
                .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
                .where(col(Task.id) == task_row_id)
                .where(col(Task.started_at) == expected_started_at)
                .where(col(Task.status) == TaskStatus.PENDING)
                .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            ).first()
            is not None
        )

    def start_building_task(self, task_row_id: UUID, expected_started_at: datetime) -> bool:
        active_benchmarks = select(col(Benchmark.id)).where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
        result = self._session.exec(
            update(Task)
            .where(col(Task.id) == task_row_id)
            .where(col(Task.started_at) == expected_started_at)
            .where(col(Task.status) == TaskStatus.BUILDING)
            .where(col(Task.benchmark).in_(active_benchmarks))
            .values(status=TaskStatus.IN_PROGRESS)
        )
        return result.rowcount == 1

    def contributing_runs(self) -> Sequence[tuple[Benchmark, Org]]:
        pool_id = col(Benchmark.queue_pool_id)
        queued_task_ids = union(
            select(col(SandboxAdmissionCandidate.task_row_id)),
            select(col(SandboxBuildReservation.task_row_id)).where(col(SandboxBuildReservation.released_at).is_(None)),
        )
        queued_runs = select(col(Task.benchmark)).where(col(Task.id).in_(queued_task_ids))
        return self._session.exec(
            select(Benchmark, Org)
            .join(Org, col(Org.id) == col(Benchmark.org_id))
            .where(col(Benchmark.id).in_(queued_runs))
            .where(pool_id.is_not(None))
            .order_by(pool_id, col(Benchmark.id))
        ).all()


SchedulerRepositoryDependency = Annotated[SchedulerRepository, Depends(SchedulerRepository)]
