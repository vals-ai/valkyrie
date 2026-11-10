"""Request and response models for run lifecycle operations."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, Field

from valkyrie.sdk.models._base import ResponseModel
from valkyrie.sdk.models._generated.runs import (
    AnalyzeBenchmarkRequest,
    AverageTaskBreakdown,
    AWSBenchmarkArguments,
    BenchmarkStatus,
    BenchmarkTableRow,
    DatasetVersion,
    FailureCategory,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarksResponse,
    FilterOptionsResponse,
    FinalEvaluation,
    FinalViewResponse,
    LocalBenchmarkArguments,
    Order,
    RetryMode,
    RegradeBenchmarkRequest,
    RegradeBenchmarkResponse,
    RetryOrResumeBenchmarkResponse,
    RunMonitoring,
    RunMonitoringRules,
    S3UploadResultsResponse,
    ShedBenchmarkRequest,
    ShedBenchmarkResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StatusResponse,
    StopBenchmarkResponse,
    TaskStatus,
    UpdateBenchmarkConcurrencyRequest,
    UpdateBenchmarkConcurrencyResponse,
)
from valkyrie.sdk.models._validation import default_environment

__all__ = [
    "AWSBenchmarkArguments",
    "AnalyzeBenchmarkRequest",
    "AnalyzeEvent",
    "AverageTaskBreakdown",
    "BenchmarkArguments",
    "BenchmarkDetails",
    "BenchmarkStatus",
    "BenchmarkTableRow",
    "DatasetVersion",
    "DocentReadingStatus",
    "FailureCategory",
    "FetchBenchmarkMetadataResponse",
    "FetchBenchmarkResponse",
    "FetchBenchmarksRequest",
    "FetchBenchmarksResponse",
    "FilterOptionsResponse",
    "FinalEvaluation",
    "FinalViewResponse",
    "LocalBenchmarkArguments",
    "Order",
    "ResultsExistResponse",
    "RetrieveResultsResponse",
    "RetryMode",
    "RegradeBenchmarkRequest",
    "RegradeBenchmarkResponse",
    "RetryOrResumeBenchmarkResponse",
    "RunMonitoring",
    "RunMonitoringRules",
    "RunStreamEvent",
    "S3UploadResultsResponse",
    "ShedBenchmarkRequest",
    "ShedBenchmarkResponse",
    "StartBenchmarkRequest",
    "StartBenchmarkResponse",
    "StatusResponse",
    "StopBenchmarkResponse",
    "TaskStatus",
    "UpdateBenchmarkConcurrencyRequest",
    "UpdateBenchmarkConcurrencyResponse",
]


class DocentReadingStatus(str, Enum):
    """Lifecycle states for generated Docent analysis."""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"


class FetchBenchmarksRequest(BaseModel):
    """Filters and pagination for listing benchmark runs."""

    agent_name: list[str] | None = None
    benchmark_name: list[str] | None = None
    model: str | None = None
    dataset: str | None = None
    label: str | None = None
    status: list[BenchmarkStatus] | None = None
    started_by: list[str] | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None
    order_by: Order = Order.DESC
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class AnalyzeEvent(ResponseModel):
    """One progress or completion event from run analysis."""

    event: str
    data: dict[str, Any]


class BenchmarkDetails(ResponseModel):
    """Detailed progress for a fetched run."""

    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]
    docent_reading_status: DocentReadingStatus
    docent_reading_url: str | None = None


class FetchBenchmarkResponse(ResponseModel):
    """Current state of one benchmark run."""

    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails
    s3_bucket_url: str
    storage_bucket: str | None = None
    label: str | None = None
    final_score: float | None = None
    error_message: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    regraded_from_run_id: UUID | None = None
    regrade_skipped_task_ids: list[str] | None = None


class RunStreamEvent(ResponseModel):
    """Represents one message on a run's live update stream, as yielded by `runs.events()`.

    The stream sends an `update` every few seconds while the run is going, then ends with one
    `complete`, `error` or `disconnect`.
    """

    event: Literal["update", "complete", "error", "disconnect"] = Field(
        description=(
            '"complete" means the run reached FINISHED, ERROR or STOPPED (the last update shows which), '
            '"error" that the run can no longer be found, and "disconnect" that the server closed the stream first.'
        )
    )
    run: FetchBenchmarkResponse | None = Field(
        default=None, description="The run's current state, set only on update events."
    )
    detail: str | None = Field(default=None, description="The server's message, set only on error events.")


BenchmarkArguments = Annotated[
    AWSBenchmarkArguments | LocalBenchmarkArguments,
    Field(discriminator="environment"),
    BeforeValidator(default_environment),
]


class ResultsExistResponse(ResponseModel):
    """Whether the canonical result file already exists in S3."""

    exists: bool


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse
