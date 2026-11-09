from __future__ import annotations
from enum import Enum
from pydantic import Field, FiniteFloat
from typing import Annotated
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime
from uuid import UUID


class SchedulerActiveStatus(str, Enum):
    """Task states included in the scheduler's active entries."""

    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"


class SchedulerResourceCapacityResponse(ResponseModel):
    """Available and total capacity for one resource."""

    available: Annotated[FiniteFloat, Field(ge=0)]
    total: Annotated[FiniteFloat, Field(ge=0)]


class SchedulerSummaryResponse(ResponseModel):
    """Total waiting and active task counts, independent of entry limits."""

    waiting: int = 0
    building: int = 0
    in_progress: int = 0
    evaluating: int = 0


class SchedulerWaitingEntryResponse(ResponseModel):
    """One waiting task with its priority and position within its pool."""

    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    pool_id: str
    position: int
    priority: int
    enqueued_at: UTCDateTime


class SchedulerActiveEntryResponse(ResponseModel):
    """One task currently building, running, or evaluating."""

    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    status: SchedulerActiveStatus
    started_at: UTCDateTime


class SchedulerCapacityResponse(ResponseModel):
    """CPU, memory, disk, and optional GPU capacity for one target and sandbox class."""

    cpu: SchedulerResourceCapacityResponse
    memory: SchedulerResourceCapacityResponse
    disk: SchedulerResourceCapacityResponse
    gpu: SchedulerResourceCapacityResponse | None = None
    allowed_gpu_types: list[str] | None = None


class SchedulerCapacityDomainResponse(ResponseModel):
    """Capacity scoped to one provider target and sandbox class."""

    target_id: Annotated[str, Field(min_length=1)]
    sandbox_class: Annotated[str, Field(min_length=1)]
    capacity: SchedulerCapacityResponse


class SchedulerPoolResponse(ResponseModel):
    """Waiting task count and optional capacity for one sandbox admission pool."""

    pool_id: str
    waiting: int
    provider: str | None = None
    capacity_domains: list[SchedulerCapacityDomainResponse] | None = None


class SchedulerOverviewResponse(ResponseModel):
    """Scheduler totals and bounded task entries for the authenticated organization."""

    observed_at: UTCDateTime
    summary: SchedulerSummaryResponse
    pools: list[SchedulerPoolResponse]
    waiting_entries: list[SchedulerWaitingEntryResponse]
    active_entries: list[SchedulerActiveEntryResponse]
    waiting_capped: bool
    active_capped: bool
    waiting_next_offset: Annotated[
        int | None,
        Field(description="Next waiting offset, or null when this list is exhausted."),
    ]
    active_next_offset: Annotated[
        int | None,
        Field(description="Next active offset, or null when this list is exhausted."),
    ]
