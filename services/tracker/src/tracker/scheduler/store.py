from dataclasses import dataclass
from datetime import timedelta

from benchmark_service import SandboxCapacity, SandboxReservationDemand

from tracker.executor.control_queue import queue_pool_lock_id as queue_pool_lock_id

GRANT_ACCEPTANCE_TIMEOUT = timedelta(seconds=30)
READINESS_CHECK_TIMEOUT = timedelta(seconds=30)


@dataclass(frozen=True)
class ActiveReservations:
    count: int
    vcpu: float
    memory: float
    disk: float
    gpu: float


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
