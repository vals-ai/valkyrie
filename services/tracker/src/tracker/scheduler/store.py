from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxReservationDemand
from sqlalchemy.engine import Engine

from tracker.repositories.advisory_lock import PostgresAdvisoryLock

_RESERVED_QUEUE_POOL_SUFFIX = ".reserved"
GRANT_ACCEPTANCE_TIMEOUT = timedelta(seconds=30)
READINESS_CHECK_TIMEOUT = timedelta(seconds=30)


@dataclass(frozen=True)
class ActiveReservations:
    count: int
    vcpu: float
    memory: float
    disk: float
    gpu: float


def queue_pool_id(provider_pool_id: str) -> str:
    return f"pool_{sha256(provider_pool_id.encode()).hexdigest()[:24]}"


def reserved_queue_pool_id(pool_id: str) -> str:
    return f"{queue_pool_lock_id(pool_id)}{_RESERVED_QUEUE_POOL_SUFFIX}"


def queue_pool_lock_id(pool_id: str) -> str:
    return pool_id.removesuffix(_RESERVED_QUEUE_POOL_SUFFIX)


def is_reserved_queue_pool_id(pool_id: str) -> bool:
    return pool_id.endswith(_RESERVED_QUEUE_POOL_SUFFIX)


def fits_in_capacity(demand: SandboxReservationDemand, capacity: SandboxCapacity, reserved: ActiveReservations) -> bool:
    return (
        (demand.vcpu == 0 or demand.vcpu <= capacity.cpu.available - reserved.vcpu - capacity.cpu.total * 0.10)
        and (
            demand.memory == 0
            or demand.memory <= capacity.memory.available - reserved.memory - capacity.memory.total * 0.10
        )
        and (demand.disk == 0 or demand.disk <= capacity.disk.available - reserved.disk - capacity.disk.total * 0.10)
        and (demand.gpu == 0 or (capacity.gpu is not None and demand.gpu <= capacity.gpu.available - reserved.gpu))
    )


def queued_sandbox_name(task_row_id: UUID, started_at: datetime) -> str:
    attempt = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at.astimezone(UTC)
    return f"queued-{task_row_id.hex}-{int(attempt.timestamp() * 1_000_000):x}"


def queue_pool_lock(engine: Engine, pool_id: str) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=queue_pool_lock_id(pool_id))
