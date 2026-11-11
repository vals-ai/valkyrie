from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from tracker.domain import FailureCategory

AUTHORITY_REVOKED_CODE = "EXECUTION_AUTHORITY_REVOKED"
MAINTENANCE_ADMISSION_PAUSED = "MAINTENANCE_ADMISSION_PAUSED"


class DispatchRef(BaseModel):
    benchmark_id: UUID
    dispatch_id: UUID


class TaskRef(BaseModel):
    org_id: UUID
    task_row_id: UUID
    task_id: str


class AttemptRef(TaskRef):
    started_at: datetime


class PhaseRef(BaseModel):
    token: UUID


class ClaimDispatchRequest(BaseModel):
    dispatch_id: UUID


class ClaimDispatchResponse(BaseModel):
    claimed: bool
    payload: dict[str, Any] | None


class AckDispatchRequest(BaseModel):
    ref: DispatchRef


class AckDispatchResponse(BaseModel):
    acknowledged: bool


class RenewDispatchRequest(BaseModel):
    ref: DispatchRef


class RenewDispatchResponse(BaseModel):
    renewed: bool
    live: bool | None
    stopped: bool | None


class FinishDispatchRequest(BaseModel):
    ref: DispatchRef
    succeeded: Literal[True]


class FailedFinishDispatchRequest(BaseModel):
    ref: DispatchRef
    succeeded: Literal[False]
    error_message: str
    error_type: str
    category: FailureCategory


class FinishDispatchResponse(BaseModel):
    finished: bool


class BuildPhaseSnapshot(BaseModel):
    scope: Literal["build"]
    task: TaskRef
    started_at: datetime | None
    status: str
    phase: PhaseRef | None


class EvaluationPhaseSnapshot(BaseModel):
    scope: Literal["evaluation"]
    task: TaskRef
    started_at: datetime
    status: str
    phase: PhaseRef | None


class PublicationPhaseSnapshot(BaseModel):
    scope: Literal["publication"]
    status: str
    phase: PhaseRef | None


AttemptPhaseSnapshot = Annotated[
    BuildPhaseSnapshot | EvaluationPhaseSnapshot | PublicationPhaseSnapshot,
    Field(discriminator="scope"),
]
