from __future__ import annotations
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime
from uuid import UUID
from . import runs
from typing import Any


class TaskArtifactsResponse(ResponseModel):
    """Temporary artifact and log links for one task."""

    cloudwatch_url: str | None
    agent_output_url: str | None
    agent_output_expires_in: int | None


class TaskBreakdown(ResponseModel):
    """Per-task runtime and generation accounting metadata."""

    sandbox_build_duration: float | None
    agent_run_duration: float | None
    evaluation_run_duration: float | None
    sandbox_run_duration: float | None
    accounting_id: str | None
    base_generation_allowance_seconds: float | None
    cumulative_time_credit_cap_seconds: float | None
    external_service_overhead_seconds: float | None
    external_service_credit_applied_seconds: float | None
    external_service_credit_revision: int | None
    generation_elapsed_seconds: float | None


class BenchmarkStatusEntry(ResponseModel):
    """Lightweight status and task counts for one run."""

    id: UUID
    status: runs.BenchmarkStatus
    finished_at: UTCDateTime | None
    total_tasks: int
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    finished_tasks: int
    task_state_counts: dict[str, int] = {}


class BenchmarkStatusResponse(ResponseModel):
    """Status entries returned for a group of runs."""

    entries: list[BenchmarkStatusEntry]


class SingleBenchmarkResponse(ResponseModel):
    """Hosted run detail used by benchmark inspection views."""

    id: UUID
    name: str
    agent_name: str
    model: str | None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    started_at: UTCDateTime
    finished_at: UTCDateTime | None
    status: runs.BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = {}
    started_by_email: str | None = None
    final_score: float | None = None
    error_message: str | None = None
    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None
    storage_bucket: str | None = None


class SingleTaskResponse(ResponseModel):
    """Detailed state and evaluation output for one task."""

    id: UUID
    task_id: str
    status: runs.TaskStatus
    started_at: UTCDateTime
    finished_at: UTCDateTime | None
    error_message: str | None
    failure_category: runs.FailureCategory | None = None
    evaluation_result: dict[str, Any] | None
    agent_caused_exit_reason: str | None
    task_breakdown: TaskBreakdown | None = None


class TaskSummary(ResponseModel):
    """Summary information for one task in a paginated run."""

    id: UUID
    task_id: str
    status: runs.TaskStatus
    started_at: UTCDateTime
    finished_at: UTCDateTime | None
    error_message: str | None = None
    failure_category: runs.FailureCategory | None = None


class TasksResponse(ResponseModel):
    """Paginated task summaries for one run."""

    tasks: list[TaskSummary]
    total_count: int
