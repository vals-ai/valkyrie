from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime
from enum import Enum
from uuid import UUID
from typing import Annotated, Any, Literal
from valkyrie.sdk.models._validation import (
    FinalEvaluationBase,
    StartBenchmarkRequestBase,
    StoredArgumentsBase,
)
from . import agents, config, datasets as datasets_1, projects
from datetime import datetime


class AnalyzeBenchmarkRequest(BaseModel):
    """Wire payload used to trigger Docent analysis for a run."""

    no_cache: bool = False
    lambda_function: str | None = None


class AverageTaskBreakdown(ResponseModel):
    """Average timing breakdown across completed tasks."""

    sandbox_build_duration: float | None
    agent_run_duration: float | None
    evaluation_run_duration: float | None
    sandbox_run_duration: float | None


class BenchmarkStatus(str, Enum):
    """Lifecycle states for a benchmark run."""

    IN_PROGRESS = "IN_PROGRESS"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class BenchmarkTableRow(ResponseModel):
    """Summary row returned by the run-list endpoint."""

    id: UUID
    name: str
    agent_name: str
    label: str | None = None
    model: str | None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    dataset: str = "default"
    started_by_email: str | None
    started_at: UTCDateTime
    finished_at: UTCDateTime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = {}
    final_score: float | None = None
    error_message: str | None = None


class DatasetVersion(ResponseModel):
    """Exact dataset release saved for a run."""

    id: Annotated[str, Field(max_length=1024, min_length=1, pattern="^[!-~]+$")]
    label: Annotated[str | None, Field(max_length=256)]


class FailureCategory(str, Enum):
    """Coarse classification of a task or run failure."""

    INFRASTRUCTURE = "infrastructure"
    BENCHMARK_SERVICE = "benchmark_service"
    AGENT = "agent"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class FetchBenchmarksResponse(ResponseModel):
    """Page of benchmark runs."""

    benchmarks: list[BenchmarkTableRow]
    total_count: int | None = None
    next_cursor: str | None = None


class FilterOptionsResponse(ResponseModel):
    """Values available for run filters in the caller's organization."""

    benchmark_names: list[str]
    agent_names: list[str]
    models: list[str]
    datasets: list[str]
    started_by_emails: list[str]


class FinalEvaluation(FinalEvaluationBase):
    """Final aggregate evaluation stored for a run."""

    org_id: str
    benchmark: str
    final_score: float | None = None
    properties: dict[str, Any] = {}


class Order(str, Enum):
    """Sort order for run listings."""

    ASC = "asc"
    DESC = "desc"


class RescoreBenchmarkResponse(ResponseModel):
    """The score a finished dataset run has after its metric script ran again."""

    benchmark_id: UUID
    final_score: float | None
    properties: dict[str, Any]


class RetryMode(str, Enum):
    """Supported retry strategies."""

    AUTO = "auto"
    FROM_SCRATCH = "from_scratch"


class StatusResponse(ResponseModel):
    """Generic status response."""

    status: str


class RegradeBenchmarkRequest(BaseModel):
    """The dataset to regrade a finished run's saved task states against."""

    dataset: Annotated[str, Field(max_length=1024, min_length=1)]
    dataset_version: Annotated[str | None, Field(max_length=1024, min_length=1)] = None
    concurrency: Annotated[int | None, Field(ge=1)] = None
    service_headers: Annotated[dict[str, str], Field(repr=False)] = {}


class RegradeBenchmarkResponse(ResponseModel):
    """The new run and which source tasks it regrades or skips."""

    benchmark_id: UUID
    regraded_from_run_id: UUID
    task_ids: list[str]
    skipped_task_ids: list[str]


class RetryOrResumeBenchmarkResponse(StatusResponse):
    """Response returned after retrying or resuming a run."""


class RunMonitoring(BaseModel):
    """Opt-in external monitoring under the monitor's built-in rules."""

    model_config = ConfigDict(
        extra="forbid",
    )


class RunMonitoringRules(ResponseModel):
    """Monitor rules the benchmark declares for its runs."""

    policy: str | None = None
    watcher_policy_pack: str | None = None
    allowed_cidrs: list[str] = []
    allowed_hosts: list[str] = []


class S3UploadResultsResponse(ResponseModel):
    """URLs returned when final results are uploaded to S3."""

    s3_url: str
    presigned_url: str
    console_url: str
    expires_in: int = 86400


class ShedBenchmarkRequest(BaseModel):
    """A concurrency limit below the run's current one; ``dry_run`` only previews the tasks to stop."""

    concurrency: Annotated[int, Field(ge=1, strict=True)]
    dry_run: bool = False


class ShedBenchmarkResponse(ResponseModel):
    """The new limit and the newest tasks stopped (or, on a dry run, selected) to get under it."""

    benchmark_id: UUID
    concurrency: int
    task_ids: list[str]


class StopBenchmarkResponse(StatusResponse):
    """Response returned after stopping a run."""


class TaskStatus(str, Enum):
    """Lifecycle states for one benchmark task."""

    PENDING = "PENDING"
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class UpdateBenchmarkConcurrencyRequest(BaseModel):
    """A positive concurrency limit for an active run."""

    concurrency: Annotated[int, Field(ge=1, strict=True)]


class UpdateBenchmarkConcurrencyResponse(ResponseModel):
    """The run state after changing its concurrency limit."""

    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


class AWSBenchmarkArguments(ResponseModel):
    """Stored arguments for an AWS run."""

    contract: agents.AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    dataset_version: DatasetVersion | None = None
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    monitoring: RunMonitoring | None = None
    monitoring_rules: RunMonitoringRules | None = None
    environment: Literal["aws"] = "aws"
    properties: config.AWSResources | None = None


class LocalBenchmarkArguments(ResponseModel):
    """Stored arguments for a local run."""

    contract: agents.AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    dataset_version: DatasetVersion | None = None
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    monitoring: RunMonitoring | None = None
    monitoring_rules: RunMonitoringRules | None = None
    environment: Literal["local"] = "local"
    properties: config.LocalResources


class StartBenchmarkRequest(StartBenchmarkRequestBase):
    """Wire payload used to start a benchmark run."""

    model_config = ConfigDict(
        extra="forbid",
    )
    benchmark_name: str
    dataset: str | None = None
    ignore_custom_services: bool = False
    service_headers: Annotated[dict[str, str], Field(repr=False)] = {}
    environment: Literal["aws", "local"] = "aws"
    contract: agents.AgentContractRequest
    concurrency: int = 5
    priority: Annotated[int | None, Field(ge=0, le=4, strict=True)] = None
    label: str | None = None
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset_version: Annotated[str | None, Field(max_length=1024, min_length=1)] = None
    dataset_ref: Annotated[
        datasets_1.HostedDataset | datasets_1.LocalDataset | None,
        Field(discriminator="kind"),
    ] = None
    sandbox_provider: str | None = None
    project: Annotated[str | None, Field(min_length=1)] = None
    monitoring: RunMonitoring | None = None


class StartBenchmarkResponse(ResponseModel):
    """Response returned after starting a run."""

    benchmark_name: str
    agent_name: str
    benchmark_id: UUID
    concurrency: int
    started_at: datetime
    task_count: int
    dataset_version: DatasetVersion | None = None
    dataset_version_warning: str | None = None
    cloudwatch_url: str
    s3_bucket_url: str
    storage_bucket: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    project: projects.ProjectResponse | None = None


class FetchBenchmarkMetadataResponse(StoredArgumentsBase):
    """Stored launch metadata for one run."""

    benchmark_id: UUID
    benchmark_name: str
    benchmark_arguments: Annotated[
        AWSBenchmarkArguments | LocalBenchmarkArguments,
        Field(discriminator="environment"),
    ]
    storage_bucket: str | None = None
    started_by_email: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_uri: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    regraded_from_run_id: UUID | None = None
    regrade_skipped_task_ids: list[str] | None = None
    project: projects.ProjectResponse | None = None


class FinalViewResponse(StoredArgumentsBase):
    """Inline final results for a benchmark run."""

    benchmark_id: UUID
    benchmark_name: str
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    error_message: str | None
    benchmark_arguments: Annotated[
        AWSBenchmarkArguments | LocalBenchmarkArguments,
        Field(discriminator="environment"),
    ]
    tasks_stopped: int | None
    final_evaluation: FinalEvaluation | None
    average_task_breakdown: AverageTaskBreakdown | None
    evaluation_results: dict[str, dict[str, Any]] | None
    task_errors: dict[str, str] | None
    task_failure_categories: dict[str, FailureCategory] | None = None
