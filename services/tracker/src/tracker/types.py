from __future__ import annotations

import re
from datetime import UTC, datetime, timezone
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self, TypeAliasType, cast
from uuid import UUID, uuid4

from benchmark_service import SandboxSource
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import DatasetVersion
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    PlainSerializer,
    SerializerFunctionWrapHandler,
    StringConstraints,
    TypeAdapter,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

from tracker.aws.runtime import AWSResources
from tracker.local.resources import LocalResources, reads_secrets_file
from tracker.domain import BenchmarkStatus, DocentReadingStatus, FailureCategory, SpendType, TaskStatus
from tracker.egress import EgressPolicy
from tracker.runtime.artifacts import TASK_ID_PATTERN

from tracker.services.datasets.digest import DIGEST_PATTERN
from tracker.services.datasets.ref import DatasetRef, HostedDataset
from tracker.services.datasets.version_ref import MAX_VERSION_TAGS, MAX_VERSION_TASKS, validate_tag

if TYPE_CHECKING:
    from tracker.database.models import BenchmarkDefinition, Dataset, Project
from tracker.outbound_security import validate_benchmark_name, validate_service_header_name, validate_service_url_syntax

MAX_OUTPUT_ARTIFACT_BYTES = 250 * 1024 * 1024
MAX_OUTPUT_ARTIFACT_COUNT = 10


def _source_has_glob(source: str) -> bool:
    return any(char in source for char in "*?[")


def _source_glob_root(source: str) -> str:
    glob_indices = [source.find(char) for char in "*?[" if source.find(char) != -1]
    first_glob_index = min(glob_indices)
    root = source[:first_glob_index].rsplit("/", 1)[0]
    return root or "/"


class OutputArtifact(BaseModel):
    path: str
    source: str | None = None
    required: bool = True

    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.required:
            data.pop("required", None)
        return data

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str | None) -> str | None:
        if not value:
            return None
        if not value.startswith("/"):
            raise ValueError("output_artifacts source paths must be absolute sandbox paths")

        path = PurePosixPath(value)
        if not path.parts or ".." in path.parts or "." in path.parts:
            raise ValueError("output_artifacts source paths cannot contain empty, '.', or '..' path parts")
        if _source_has_glob(value) and _source_glob_root(value) == "/":
            raise ValueError("output_artifacts glob sources must include a non-root directory prefix")

        return value


OutputArtifactSpec = str | OutputArtifact


class AgentTurns(BaseModel):
    continue_cmd: str | None = None
    interrupt_grace_seconds: float | None = None
    container_name: str | None = None


class AgentReleaseSelection(BaseModel):
    model_config = {"extra": "forbid", "frozen": True}
    reference: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    archive_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    prescribed_configuration: dict[str, str] = Field(default_factory=dict)
    overrides: dict[str, str] = Field(default_factory=dict)


class AgentContractRequest(BaseModel):
    name: str
    model: str | None = None
    install_cmd: str = ""
    run_cmd: str = ""
    turns: AgentTurns | None = None
    final_output: str | None = None
    output_artifacts: list[OutputArtifactSpec] = []
    egress_allowlist: list[str] = []
    install_egress: EgressPolicy | None = None
    allow_provider_tools: bool = False
    isolate_agent_uid: bool = False
    secrets: dict[str, str] = {}
    kwargs: dict[str, str] = {}
    inference_settings_attested: bool = False
    agent_release: AgentReleaseSelection | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        if isinstance(value, dict) and "egress" in value:
            raise ValueError("'egress' is no longer supported; use 'install_egress'")
        return value

    @field_validator("output_artifacts")
    @classmethod
    def validate_output_artifacts(cls, value: list[OutputArtifactSpec]) -> list[OutputArtifactSpec]:
        if len(value) > MAX_OUTPUT_ARTIFACT_COUNT:
            raise ValueError(f"output_artifacts cannot contain more than {MAX_OUTPUT_ARTIFACT_COUNT} entries")

        normalized_artifacts: list[OutputArtifactSpec] = []
        normalized_paths: set[str] = set()
        for artifact in value:
            artifact_path = artifact if isinstance(artifact, str) else artifact.path
            path = PurePosixPath(artifact_path)
            if path.is_absolute():
                raise ValueError("output_artifacts paths must be relative paths")
            if not path.parts or ".." in path.parts or "." in path.parts:
                raise ValueError("output_artifacts paths cannot contain empty, '.', or '..' path parts")
            normalized_path = str(path)
            if normalized_path in normalized_paths:
                raise ValueError(f"output_artifacts cannot contain duplicate paths: {normalized_path}")
            normalized_paths.add(normalized_path)
            if isinstance(artifact, str):
                normalized_artifacts.append(normalized_path)
            else:
                normalized_artifacts.append(artifact.model_copy(update={"path": normalized_path}))

        return normalized_artifacts


class RunMonitoring(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"description": "Opt-in external monitoring requested by whoever starts the run."},
    )


class RunMonitoringRules(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"description": "Monitor rules the benchmark service declares for its runs."},
    )

    policy: str | None = None
    watcher_policy_pack: str | None = None
    allowed_cidrs: list[str] = []
    allowed_hosts: list[str] = []


class AgentExecutionAttempt(BaseModel):
    dispatch_id: str | None = None
    successor_dispatch_ids: list[str] = Field(default_factory=list)
    agent_release: dict[str, Any] | None = None
    model: str | None = None
    effective_configuration: dict[str, str]
    task_ids: list[str]
    retained_outputs: str | None = None
    results: dict[str, Any] | None = None
    final_evaluation: dict[str, Any] | None = None


class _BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract: AgentContractRequest
    agent_attempts: list[AgentExecutionAttempt] = Field(default_factory=list, exclude_if=lambda attempts: not attempts)
    concurrency: int
    priority: int | None = Field(default=None, exclude=True, strict=True, ge=0, le=4)
    queue_pool_id: str | None = Field(default=None, exclude=True)
    selected_project_gateway_secret_ref: str | None = Field(default=None, exclude=True)
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    dataset_version: DatasetVersion | None = None
    dataset_ref: DatasetRef | None = Field(default=None, exclude=True)
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    monitoring: RunMonitoring | None = None
    monitoring_rules: RunMonitoringRules | None = None
    regraded_from_run_id: UUID | None = Field(default=None, exclude=True)
    regrade_skipped_task_ids: list[str] | None = Field(default=None, exclude=True)

    def with_hosted_dataset(self) -> Self:
        ref = self.dataset_ref
        if not isinstance(ref, HostedDataset) or ref.revision is None:
            return self
        version = DatasetVersion(id=f"{ref.benchmark}/{ref.dataset}@{ref.revision}", label=None)
        return self.model_copy(update={"dataset": ref.dataset, "dataset_version": version})


class AWSBenchmarkArguments(_BenchmarkArguments):
    model_config = ConfigDict(
        json_schema_extra={"description": "Stored arguments for an AWS run, including legacy rows without resources."}
    )

    environment: Literal["aws"] = "aws"
    properties: AWSResources | None = None


class LocalBenchmarkArguments(_BenchmarkArguments):
    model_config = ConfigDict(
        json_schema_extra={"description": "Stored arguments with the filesystem resources selected at admission."}
    )

    environment: Literal["local"] = "local"
    properties: LocalResources


BenchmarkArguments = Annotated[
    AWSBenchmarkArguments | LocalBenchmarkArguments,
    Field(discriminator="environment"),
]
benchmark_arguments_adapter: TypeAdapter[BenchmarkArguments] = TypeAdapter(BenchmarkArguments)


class ActiveNativeOutput(BaseModel):
    attempt_started_at: datetime
    sandbox_id: str
    runtime_source: SandboxSource
    output_path: str

    @property
    def generation(self) -> str:
        started_at = self.attempt_started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        identity = f"{started_at.astimezone(UTC).isoformat()}\0{self.sandbox_id}"
        return sha256(identity.encode()).hexdigest()

    def matches_attempt(self, started_at: datetime) -> bool:
        def utc(value: datetime) -> datetime:
            return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

        return utc(self.attempt_started_at) == utc(started_at)


class FinalEvaluation(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID = Field(default_factory=uuid4)
    org_id: UUID
    benchmark: UUID
    final_score: float | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)


class OrgContext(BaseModel):
    id: UUID
    name: str
    storage_key: str | None


BenchmarkName = Annotated[str, AfterValidator(validate_benchmark_name)]
ServiceUrl = Annotated[str, AfterValidator(validate_service_url_syntax)]
ServiceHeaderName = Annotated[str, AfterValidator(validate_service_header_name)]
ProviderName = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
SecretReference = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_+=.@/-]{1,512}$")]


def _serialize_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _serialize_required_utc(value: datetime) -> str:
    result = _serialize_utc(value)
    assert result is not None
    return result


UTCDateTime = Annotated[datetime, PlainSerializer(_serialize_required_utc, return_type=str)]


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]
    docent_reading_status: DocentReadingStatus
    docent_reading_url: str | None = None


class FetchBenchmarkTasksRequest(BaseModel):
    benchmark_name: BenchmarkName
    dataset: str | None = None
    custom_benchmark_service: ServiceUrl | None = None
    ignore_custom_services: bool = False
    service_headers: dict[str, str] = Field(default_factory=dict, repr=False)

    def use_saved_service(self, settings: RunSettingsResponse) -> tuple[str, str] | None:
        saved = None if self.ignore_custom_services else settings.benchmark_services.get(self.benchmark_name)
        self.custom_benchmark_service = saved.url if saved else None
        if saved and saved.auth_header_name and saved.auth_secret_name:
            return saved.auth_header_name, saved.auth_secret_name
        return None


SPEND_SUBCATEGORIES: dict[SpendType, tuple[str, ...]] = {
    SpendType.DEVELOPMENT: (
        "new_benchmark_generation",
        "new_benchmark_launch_runs",
        "post_launch_benchmark_update",
    ),
    SpendType.CUSTOMER: ("report", "paid_run", "eval_server", "platform"),
    SpendType.INTERNAL: (
        "unpaid_leaderboard_run",
        "other_unpaid_run",
        "fellows_interns",
        "blog",
        "miscellaneous",
    ),
}

PROJECT_HANDLE_PATTERN = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
PROJECT_HANDLE_MAX_LENGTH = 64


def validate_project_handle(handle: str) -> str:
    if len(handle) > PROJECT_HANDLE_MAX_LENGTH or not PROJECT_HANDLE_PATTERN.fullmatch(handle):
        raise ValueError(
            f"must be lowercase letters, digits and hyphens, at most {PROJECT_HANDLE_MAX_LENGTH} characters, "
            "e.g. my-project"
        )
    return handle


NonBlankText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
ProjectHandle = Annotated[str, StringConstraints(strip_whitespace=True), AfterValidator(validate_project_handle)]


class ProjectCreateRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "description": "A new project: its immutable handle, its name, spend type and subcategory, and optional customer and extra tags."
        }
    )
    handle: ProjectHandle
    name: NonBlankText
    spend_type: SpendType
    subcategory: str
    customer: Annotated[str, StringConstraints(strip_whitespace=True)] | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_subcategory_and_customer(self) -> "ProjectCreateRequest":
        allowed = SPEND_SUBCATEGORIES[self.spend_type]
        if self.subcategory not in allowed:
            choices = ", ".join(allowed)
            raise ValueError(
                f"subcategory '{self.subcategory}' is not allowed for spend type '{self.spend_type.value}'; "
                f"allowed: {choices}"
            )
        if self.spend_type is SpendType.CUSTOMER and not self.customer:
            raise ValueError("spend type 'customer' requires customer")
        shadowed = sorted(set(self.extra_tags) & (set(type(self).model_fields) - {"extra_tags"}))
        if shadowed:
            raise ValueError(f"extra tag keys {shadowed} name core fields; set them as fields instead")
        return self


class NewProjectRequest(ProjectCreateRequest):
    model_config = ConfigDict(
        json_schema_extra={"description": "A project to create with its main key's budget and reason."}
    )
    budget: Decimal | None = Field(default=None, gt=0)
    budget_reason: NonBlankText | None = None


class ProjectResponse(BaseModel):
    id: UUID
    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)
    created_at: UTCDateTime
    created_by_email: str | None = None
    archived_at: UTCDateTime | None = None

    @classmethod
    def from_row(cls, row: Project) -> "ProjectResponse":
        return cls.model_validate(row, from_attributes=True)


class ProjectsResponse(BaseModel):
    projects: list[ProjectResponse]


class ProjectIdResponse(BaseModel):
    id: UUID
    org_id: UUID


class GatewayKeyEligibilityResponse(BaseModel):
    project_id: UUID
    org_id: UUID
    subject: str


RESOURCE_NAME_PATTERN = r"[a-z0-9]+(?:[._-][a-z0-9]+)*"
RESOURCE_NAME_MAX_LENGTH = 64


def is_resource_name(value: str) -> bool:
    return len(value) <= RESOURCE_NAME_MAX_LENGTH and re.fullmatch(RESOURCE_NAME_PATTERN, value) is not None


def validate_run_name(value: str) -> str:
    return value if is_resource_name(value) else validate_benchmark_name(value)


ResourceName = Annotated[
    str, StringConstraints(pattern=f"^{RESOURCE_NAME_PATTERN}$", max_length=RESOURCE_NAME_MAX_LENGTH)
]
ResourceDescription = Annotated[str, StringConstraints(strip_whitespace=True, max_length=2000)]


class ResourceCreateRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "description": "A new benchmark or dataset: its name, unique among its siblings, and an optional description."
        }
    )
    name: ResourceName
    description: ResourceDescription = ""


class ResourceUpdateRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "The description a benchmark or dataset now has; the name never changes."}
    )
    description: ResourceDescription


class ResourceResponse(BaseModel):
    name: str
    description: str
    created_by_email: str | None = None
    created_at: UTCDateTime
    updated_at: UTCDateTime
    archived_at: UTCDateTime | None = None


class BenchmarkDefinitionResponse(ResourceResponse):
    @classmethod
    def from_row(cls, row: BenchmarkDefinition) -> "BenchmarkDefinitionResponse":
        return cls.model_validate(row, from_attributes=True)


class BenchmarkDefinitionsResponse(BaseModel):
    benchmarks: list[BenchmarkDefinitionResponse]


class DatasetResponse(ResourceResponse):
    benchmark: str

    @classmethod
    def from_row(cls, row: Dataset, benchmark: str) -> "DatasetResponse":
        return cls.model_validate({**row.model_dump(), "benchmark": benchmark})


class DatasetsResponse(BaseModel):
    datasets: list[DatasetResponse]


TaskId = Annotated[str, StringConstraints(pattern=f"^{TASK_ID_PATTERN}$")]


Digest = Annotated[str, StringConstraints(pattern=f"^{DIGEST_PATTERN}$")]
ListedDigest = TypeAliasType("Digest", Digest)


class BlobDigestsRequest(BaseModel):
    digests: list[ListedDigest] = Field(max_length=MAX_VERSION_TASKS)


class MissingBlobsResponse(BaseModel):
    missing: list[ListedDigest]


class BlobResponse(BaseModel):
    digest: Digest
    size: int
    warnings: list[str] = Field(default_factory=list)


class ManifestAuthor(BaseModel):
    name: str
    email: str | None = None


class ManifestInfo(BaseModel):
    name: str
    version: str | None = None
    description: str = ""
    authors: list[ManifestAuthor] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ManifestTask(BaseModel):
    name: str
    digest: Digest


class ManifestFile(BaseModel):
    path: str
    digest: str = ""


class VersionManifest(ManifestInfo):
    tasks: dict[TaskId, ManifestTask] = Field(max_length=MAX_VERSION_TASKS)
    files: list[ManifestFile] = Field(default_factory=list)

    @field_validator("files")
    @classmethod
    def no_files(cls, files: list[ManifestFile]) -> list[ManifestFile]:
        if files:
            raise ValueError("dataset.toml pins dataset files, which Valkyrie can't score with yet")
        return files


class VersionCreateRequest(BaseModel):
    tasks: dict[TaskId, Digest] = Field(min_length=1, max_length=MAX_VERSION_TASKS)
    tags: list[str] = Field(default_factory=list, max_length=MAX_VERSION_TAGS)
    manifest: VersionManifest | None = None
    metric_digest: Digest | None = None

    @field_validator("tags")
    @classmethod
    def valid_tags(cls, tags: list[str]) -> list[str]:
        return [validate_tag(tag) for tag in tags]


class TagRequest(BaseModel):
    ref: NonBlankText
    force: bool = False


class DatasetTaskResponse(BaseModel):
    task_id: str
    digest: Digest
    size: int
    source_name: str | None = None


class DatasetVersionResponse(BaseModel):
    benchmark: str
    dataset: str
    revision: int
    digest: Digest
    task_count: int
    tags: list[str]
    created_by_email: str | None = None
    created_at: UTCDateTime


class DatasetTasksPage(BaseModel):
    tasks: list[DatasetTaskResponse]
    next_cursor: str | None = None


class DatasetVersionDetailResponse(DatasetVersionResponse):
    metric_digest: Digest | None = None
    manifest: ManifestInfo | None = None


class DatasetVersionsResponse(BaseModel):
    versions: list[DatasetVersionResponse]


class DatasetTaskDownloadResponse(DatasetTaskResponse):
    download_url: str
    expires_in: int


class RunProjectRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "The handle of the project a run now belongs to."})
    project: NonBlankText


class StartBenchmarkRequest(FetchBenchmarkTasksRequest):
    model_config = ConfigDict(hide_input_in_errors=True)

    benchmark_name: Annotated[str, AfterValidator(validate_run_name)]
    environment: Literal["aws", "local"] = "aws"
    properties: AWSResources | LocalResources | None = None
    contract: AgentContractRequest
    concurrency: int = 5
    priority: int | None = Field(default=None, strict=True, ge=0, le=4)
    label: str | None = None
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset_version: str | None = Field(default=None, min_length=1, max_length=1024)
    admission_id: UUID | None = None
    dataset_ref: DatasetRef | None = None
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    service_auth_header_name: str | None = None
    service_auth_secret_name: str | None = None
    project: NonBlankText | None = None
    monitoring: RunMonitoring | None = None

    @model_validator(mode="after")
    def validate_run_source(self) -> "StartBenchmarkRequest":
        if self.dataset_ref is None:
            validate_benchmark_name(self.benchmark_name)
            return self
        if not is_resource_name(self.benchmark_name):
            raise ValueError("Invalid benchmark name")
        if self.dataset or self.dataset_version or self.service_headers:
            raise ValueError("A dataset run can't set a benchmark service dataset, version or headers")
        self.ignore_custom_services = True
        return self

    @model_validator(mode="after")
    def validate_execution_environment(self) -> "StartBenchmarkRequest":
        if self.environment == "aws":
            if isinstance(self.properties, LocalResources):
                raise ValueError("AWS execution cannot include local resources")
            return self
        if isinstance(self.properties, AWSResources):
            raise ValueError("Local execution cannot include AWS configuration")
        if self.lambda_function:
            raise ValueError("Lambda callbacks require S3 storage")
        if (self.properties is None or reads_secrets_file(self.properties)) and (
            self.sandbox_provider_secret_name or self.service_auth_secret_name
        ):
            raise ValueError("Runs reading the secrets file cannot reference named secrets")
        return self

    def use_settings(self, settings: RunSettingsResponse, provider_secrets: dict[str, str]) -> None:
        if "sandbox_provider" not in self.model_fields_set and settings.default_provider:
            self.sandbox_provider = settings.default_provider
        self.sandbox_provider_secret_name = provider_secrets.get(self.sandbox_provider)
        self.service_auth_header_name, self.service_auth_secret_name = self.use_saved_service(settings) or (None, None)

    @property
    def benchmark_service(self) -> BenchmarkServiceClient:
        from tracker.config import create_benchmark_service_url
        from tracker.clients.benchmark_service import create_benchmark_service_client

        benchmark_service_url = self.custom_benchmark_service or create_benchmark_service_url(self.benchmark_name)
        return create_benchmark_service_client(
            url=benchmark_service_url,
            service_headers=self.service_headers,
            sandbox_provider=self.sandbox_provider,
        )


class RunExecutionRequest(StartBenchmarkRequest):
    resolved_dataset_version: DatasetVersion | None = None
    monitoring_rules: RunMonitoringRules | None = None

    @property
    def benchmark_service(self) -> BenchmarkServiceClient:
        from tracker.config import create_benchmark_service_url
        from tracker.clients.benchmark_service import create_benchmark_service_client

        benchmark_service_url = self.custom_benchmark_service or create_benchmark_service_url(self.benchmark_name)
        return create_benchmark_service_client(
            url=benchmark_service_url,
            service_headers=self.service_headers,
            dataset_version=self.resolved_dataset_version,
            sandbox_provider=self.sandbox_provider,
        )


class StartBenchmarkResponse(BaseModel):
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
    project: ProjectResponse | None = None


class FetchBenchmarkResponse(BaseModel):
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


class LogEventResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "One provider-neutral benchmark log event."})
    timestamp: datetime
    message: str
    task_id: str | None = None
    ingestion_time: datetime | None = None
    event_id: str | None = None


class LogPageResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "A bounded page of benchmark log events."})
    events: list[LogEventResponse]
    next_cursor: str | None = None
    available: bool = True


class MonitorTaskResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "Trusted identity and state for one current task attempt."}
    )
    task_id: str
    task_row_id: UUID
    attempt_id: str
    started_at: datetime
    status: TaskStatus

    _started_at_utc = field_serializer("started_at")(_serialize_utc)


class MonitoredRunResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "A run whose starter requested external monitoring."})
    benchmark_id: UUID
    benchmark_name: str
    agent_name: str
    policy: str | None = None
    watcher_policy_pack: str | None = None
    allowed_cidrs: list[str] = []
    allowed_hosts: list[str] = []


class MonitoredRunsResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "In-progress runs an external monitor should enroll, for one organization."}
    )
    runs: list[MonitoredRunResponse]


class MonitorTasksResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "Organization-scoped task identities for an external run observer."}
    )
    benchmark_id: UUID
    status: BenchmarkStatus
    tasks: list[MonitorTaskResponse]


class AverageTaskBreakdown(BaseModel):
    sandbox_build_duration: float | None
    agent_run_duration: float | None
    evaluation_run_duration: float | None
    sandbox_run_duration: float | None


class FinalViewResponse(BaseModel):
    benchmark_id: UUID
    benchmark_name: str
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    error_message: str | None
    benchmark_arguments: BenchmarkArguments
    tasks_stopped: int | None
    final_evaluation: FinalEvaluation | None
    average_task_breakdown: AverageTaskBreakdown | None
    evaluation_results: dict[str, dict[str, Any]] | None
    task_errors: dict[str, str] | None
    task_failure_categories: dict[str, FailureCategory] | None = None


class S3UploadResultsResponse(BaseModel):
    s3_url: str
    presigned_url: str
    console_url: str
    expires_in: int = 86400


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse


_FORBIDDEN_MANAGED_AWS_KEYS = {
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "aws_profile",
    "x_harness_aws_access_key_id",
    "x_harness_aws_secret_access_key",
    "x_harness_aws_session_token",
    "x_harness_aws_profile",
}


def _contains_forbidden_managed_aws_key(value: object) -> bool:
    if isinstance(value, dict):
        for key, nested_value in cast(dict[object, object], value).items():
            normalized_key = str(key).lower().replace("-", "_")
            if normalized_key in _FORBIDDEN_MANAGED_AWS_KEYS or _contains_forbidden_managed_aws_key(nested_value):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_managed_aws_key(item) for item in cast(list[object], value))
    return False


def validate_managed_execution_request(request: StartBenchmarkRequest) -> None:
    if _contains_forbidden_managed_aws_key(request.model_dump(mode="python")):
        raise ValueError("Managed execution cannot include AWS credentials")
    if not request.sandbox_provider or not request.sandbox_provider_secret_name:
        raise ValueError("Managed execution requires a sandbox provider and provider secret name")


class ManagedExecutionContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[2, 3]
    benchmark_id: UUID
    verified_task_ids: list[str]
    start_benchmark_request: RunExecutionRequest

    @model_validator(mode="after")
    def validate_credential_free_request(self) -> "ManagedExecutionContext":
        if self.start_benchmark_request.environment == "aws":
            validate_managed_execution_request(self.start_benchmark_request)

        return self


class AWSRuntimeResponse(BaseModel):
    mode: Literal["managed"]
    region: str | None = None
    s3_bucket: str | None = None


class StatusResponse(BaseModel):
    status: str


class StopBenchmarkResponse(StatusResponse):
    pass


class RetryOrResumeBenchmarkResponse(StatusResponse):
    pass


class RescoreBenchmarkResponse(BaseModel):
    benchmark_id: UUID
    final_score: float | None
    properties: dict[str, Any]


class RegradeBenchmarkRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "The dataset to regrade a finished run's saved task states against."}
    )
    dataset: str = Field(min_length=1, max_length=1024)
    dataset_version: str | None = Field(default=None, min_length=1, max_length=1024)
    concurrency: int | None = Field(default=None, ge=1, strict=True)
    service_headers: dict[str, str] = Field(default_factory=dict)


class RegradeBenchmarkResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "The new run and which source tasks it regrades or skips."}
    )
    benchmark_id: UUID
    regraded_from_run_id: UUID
    task_ids: list[str]
    skipped_task_ids: list[str]


class UpdateBenchmarkConcurrencyRequest(BaseModel):
    concurrency: int = Field(ge=1, strict=True)


class UpdateBenchmarkConcurrencyResponse(BaseModel):
    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


class ShedBenchmarkRequest(BaseModel):
    concurrency: int = Field(ge=1, strict=True)
    dry_run: bool = False


class ShedBenchmarkResponse(BaseModel):
    benchmark_id: UUID
    concurrency: int
    task_ids: list[str]


class Order(str, Enum):
    ASC = "asc"
    DESC = "desc"


class BenchmarkFilters(BaseModel):
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


class FetchBenchmarksRequest(BenchmarkFilters):
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class BenchmarkTableRow(BaseModel):
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
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)
    final_score: float | None = None
    error_message: str | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class FetchBenchmarksResponse(BaseModel):
    benchmarks: list[BenchmarkTableRow]
    total_count: int | None = None
    next_cursor: str | None = None


class FetchBenchmarkMetadataResponse(BaseModel):
    benchmark_id: UUID
    benchmark_name: str
    benchmark_arguments: BenchmarkArguments
    storage_bucket: str | None = None
    started_by_email: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_uri: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    regraded_from_run_id: UUID | None = None
    regrade_skipped_task_ids: list[str] | None = None
    project: ProjectResponse | None = None


class AnalyzeBenchmarkRequest(BaseModel):
    no_cache: bool = False
    lambda_function: str | None = None


class BenchmarkServiceEntry(BaseModel):
    name: str
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None

    @field_validator("url")
    @classmethod
    def remove_trailing_slashes(cls, value: str) -> str:
        return validate_service_url_syntax(value)


class BenchmarkServiceHealth(BaseModel):
    name: str
    url: str
    healthy: bool
    latency_ms: int | None
    error: str | None = None


class BenchmarkServicesResponse(BaseModel):
    services: list[BenchmarkServiceHealth]


class BenchmarkServiceCatalogResponse(BaseModel):
    services: list[BenchmarkServiceEntry]


class BenchmarkServicesRequest(BaseModel):
    services: list[BenchmarkServiceEntry] = Field(default_factory=list)


class BenchmarkStatusEntry(BaseModel):
    id: UUID
    status: BenchmarkStatus
    finished_at: datetime | None
    total_tasks: int
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)

    @field_serializer("finished_at")
    def _serialize_dt(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class BenchmarkStatusResponse(BaseModel):
    entries: list[BenchmarkStatusEntry]


class SingleBenchmarkResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "Single-run view: BenchmarkTableRow fields plus optional final_score."}
    )
    id: UUID
    name: str
    agent_name: str
    model: str | None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)
    started_by_email: str | None = None
    final_score: float | None = None
    error_message: str | None = None
    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None
    storage_bucket: str | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class TaskSummary(BaseModel):
    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None = None
    failure_category: FailureCategory | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class TasksResponse(BaseModel):
    tasks: list[TaskSummary]
    total_count: int


class TaskBreakdown(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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


class SingleTaskResponse(BaseModel):
    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None
    failure_category: FailureCategory | None = None
    evaluation_result: dict[str, Any] | None
    agent_caused_exit_reason: str | None
    task_breakdown: TaskBreakdown | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class AgentEntry(BaseModel):
    name: str
    last_modified: str | None = None


class AgentsResponse(BaseModel):
    agents: list[AgentEntry]


class AgentDownloadURLResponse(BaseModel):
    name: str
    download_url: str
    expires_in: int


class SecretEntry(BaseModel):
    name: str
    reference: str


class SecretsResponse(BaseModel):
    secrets: list[SecretEntry]


class SecretValueRequest(BaseModel):
    value: str


class ProviderSecretRequest(BaseModel):
    secret_name: SecretReference


class ProvidersResponse(BaseModel):
    providers: dict[str, str]


class DefaultProviderRequest(BaseModel):
    provider: ProviderName


class BenchmarkServiceRequest(BaseModel):
    url: ServiceUrl
    auth_secret_name: SecretReference | None = None
    auth_header_name: ServiceHeaderName | None = None

    @model_validator(mode="after")
    def default_header(self) -> Self:
        if self.auth_header_name and not self.auth_secret_name:
            raise ValueError("auth_header_name needs auth_secret_name")
        if self.auth_secret_name and not self.auth_header_name:
            self.auth_header_name = "Authorization"
        return self


class SavedBenchmarkService(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    url: str
    auth_header_name: str | None
    auth_secret_name: str | None


class RunSettingsResponse(BaseModel):
    default_provider: str | None
    benchmark_services: dict[str, SavedBenchmarkService]


class NativeOutputFile(BaseModel):
    path: str
    size_bytes: int


class NativeOutputFilesResponse(BaseModel):
    generation: str
    files: list[NativeOutputFile]


class TaskArtifactsResponse(BaseModel):
    cloudwatch_url: str | None
    agent_output_url: str | None
    agent_output_expires_in: int | None


class SchedulerSummaryResponse(BaseModel):
    waiting: int = 0
    building: int = 0
    in_progress: int = 0
    evaluating: int = 0


class SchedulerActiveStatus(str, Enum):
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"


class SchedulerResourceCapacityResponse(BaseModel):
    available: FiniteFloat = Field(ge=0)
    total: FiniteFloat = Field(ge=0)


class SchedulerCapacityResponse(BaseModel):
    cpu: SchedulerResourceCapacityResponse
    memory: SchedulerResourceCapacityResponse
    disk: SchedulerResourceCapacityResponse
    gpu: SchedulerResourceCapacityResponse | None = None
    allowed_gpu_types: list[str] | None = None


class SchedulerCapacityDomainResponse(BaseModel):
    target_id: str = Field(min_length=1)
    sandbox_class: str = Field(min_length=1)
    capacity: SchedulerCapacityResponse


class SchedulerPoolResponse(BaseModel):
    pool_id: str
    waiting: int
    provider: str | None = None
    capacity_domains: list[SchedulerCapacityDomainResponse] | None = None


class SchedulerWaitingEntryResponse(BaseModel):
    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    pool_id: str
    position: int
    priority: int
    enqueued_at: UTCDateTime


class SchedulerActiveEntryResponse(BaseModel):
    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    status: SchedulerActiveStatus
    started_at: UTCDateTime


class SchedulerOverviewResponse(BaseModel):
    observed_at: UTCDateTime
    summary: SchedulerSummaryResponse
    pools: list[SchedulerPoolResponse]
    waiting_entries: list[SchedulerWaitingEntryResponse]
    active_entries: list[SchedulerActiveEntryResponse]
    waiting_capped: bool
    active_capped: bool
    waiting_next_offset: int | None = Field(description="Next waiting offset, or null when this list is exhausted.")
    active_next_offset: int | None = Field(description="Next active offset, or null when this list is exhausted.")
