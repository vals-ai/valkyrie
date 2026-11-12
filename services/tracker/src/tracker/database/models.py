from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pydantic import field_serializer
from sqlalchemy import (
    BigInteger,
    Boolean,
    Connection,
    DateTime,
    Dialect,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    LargeBinary,
    event,
    text,
)
from sqlalchemy.orm import Mapped, Mapper
from sqlmodel import (
    JSON,
    CheckConstraint,
    Column,
    Field,
    Relationship,
    Session,
    SQLModel,
    TypeDecorator,
    UniqueConstraint,
    func,
)

from tracker.database.utils import has_field_changed
from tracker.domain import (
    AgentCausedExitReason,
    BenchmarkStatus,
    DocentReadingStatus,
    ExecutorDispatchKind,
    ExecutorReleaseStatus,
    FailureCategory,
    FailurePhase,
    RetryMode as RetryMode,
    SpendType,
    TaskStatus,
)
from executor_protocol import ExecutorDispatchStatus as ExecutorDispatchStatus
from tracker.types import ActiveNativeOutput, BenchmarkArguments, benchmark_arguments_adapter

if TYPE_CHECKING:
    from benchmark_service.client import BenchmarkServiceClient

    from tracker.types import (
        FetchBenchmarkMetadataResponse,
        RunExecutionRequest,
    )


DEFAULT_ORG_NAME = "default"
FINISHED_BENCHMARK_STATUSES = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)


class Org(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str = Field(unique=True, index=True)
    storage_key: str | None = Field(default=None, unique=True)
    requires_project: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default=text("false")),
    )


class FinalEvaluation(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    benchmark: UUID = Field(foreign_key="benchmark.id")
    final_score: float | None = None
    properties: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.services.results import fetch_evaluation_results

        return fetch_evaluation_results(self.benchmark, session, self.org_id)


class BenchmarkArgumentsType(TypeDecorator[BenchmarkArguments]):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: BenchmarkArguments | None, dialect: Dialect) -> dict[str, Any] | None:
        if value is None:
            return None
        excluded_fields = {"priority", "queue_pool_id", "selected_project_gateway_secret_ref"}
        if value.dataset_version is None:
            excluded_fields.add("dataset_version")
        serialized = value.model_dump(mode="json", exclude=excluded_fields)
        if value.priority is not None:
            serialized["priority"] = value.priority
        if value.dataset_ref is not None:
            serialized["dataset_ref"] = value.dataset_ref.model_dump(mode="json")
        if value.queue_pool_id is not None:
            serialized["queue_pool_id"] = value.queue_pool_id
        if value.selected_project_gateway_secret_ref is not None:
            serialized["selected_project_gateway_secret_ref"] = value.selected_project_gateway_secret_ref
        if value.regraded_from_run_id is not None:
            serialized["regraded_from_run_id"] = str(value.regraded_from_run_id)
            serialized["regrade_skipped_task_ids"] = value.regrade_skipped_task_ids

        return serialized

    def process_result_value(self, value: dict[str, Any] | None, dialect: Dialect) -> BenchmarkArguments | None:
        if value is None:
            return None
        return benchmark_arguments_adapter.validate_python({"environment": "aws", **value})


class ExecutorRelease(SQLModel, table=True):
    id: str = Field(primary_key=True)
    artifact_uri: str
    artifact_digest: str
    protocol_version: str
    task_definition_arn: str | None = None
    status: ExecutorReleaseStatus = Field(default=ExecutorReleaseStatus.CANDIDATE)
    created_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    activated_at: datetime | None = None
    draining_at: datetime | None = None
    retired_at: datetime | None = None
    artifact_retention_until: datetime | None = None


class ExecutorAdmission(SQLModel, table=True):
    id: int = Field(default=1, primary_key=True)
    release_id: str | None = Field(default=None, foreign_key="executorrelease.id")
    maintenance_target_sha: str | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))


class ExecutorDispatch(SQLModel, table=True):
    __table_args__ = (
        Index("ix_executordispatch_release_status", "executor_release_id", "status"),
        Index("ix_executordispatch_benchmark_kind", "benchmark_id", "kind"),
        Index("ix_executordispatch_status_lease_expires", "status", "lease_expires_at"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id")
    kind: ExecutorDispatchKind
    status: ExecutorDispatchStatus = Field(default=ExecutorDispatchStatus.QUEUED)
    executor_release_id: str = Field(foreign_key="executorrelease.id")
    executor_artifact_uri: str
    executor_artifact_digest: str
    executor_protocol_version: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    assigned_task_ids: list[str] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    claim_deadline_at: datetime | None = None
    lease_expires_at: datetime | None = None
    invocation_key_digest: bytes | None = Field(default=None, sa_column=Column(LargeBinary, nullable=True))
    claim_receipt_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    failure_reason: str | None = None
    ecs_task_arn: str | None = None
    lease_expires_at: datetime | None = None
    relaunch_count: int = Field(default=0, sa_column=Column(Integer, nullable=False, server_default=text("0")))
    last_relaunch_reason: str | None = None


class OrganizationInvite(SQLModel, table=True):
    __tablename__ = "organization_invite"

    organization: str = Field(primary_key=True)
    email: str = Field(primary_key=True, index=True)
    invited_by: str
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )


class RunAccessKey(SQLModel, table=True):
    __tablename__ = "run_access_key"

    key_id: str = Field(primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    last_attempt_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class EgressProxyPolicy(SQLModel, table=True):
    __tablename__ = "egress_proxy_policy"

    key_id: str = Field(primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id", index=True)
    token_sha256: str
    methods: list[str] = Field(sa_column=Column(JSON, nullable=False))
    domains: list[str] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    expires_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    inspect_domains: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, server_default=text("'[]'")),
    )
    gateway_url: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False, server_default=func.now()),
    )


class SandboxRecord(SQLModel, table=True):
    __tablename__ = "sandbox"
    __table_args__ = (Index("ix_sandbox_benchmark_id", "benchmark_id"),)

    provider: str = Field(primary_key=True)
    sandbox_id: str = Field(primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id")
    last_seen_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class RunCost(SQLModel, table=True):
    __tablename__ = "run_cost"
    __table_args__ = (
        Index("ix_run_cost_benchmark_id", "benchmark_id"),
        Index(
            "uq_run_cost_sandbox_provider_source_id",
            "provider",
            "source_id",
            unique=True,
            postgresql_where=text("cost_type = 'sandbox'"),
            sqlite_where=text("cost_type = 'sandbox'"),
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id")
    cost_type: str
    source_id: str | None = None
    provider: str
    amount_usd: float
    usage_end: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class RunCostUsageEndBackfill(SQLModel, table=True):
    __tablename__ = "run_cost_usage_end_backfill"

    run_cost_id: UUID = Field(foreign_key="run_cost.id", primary_key=True)
    backfilled_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))


class SandboxCostCursor(SQLModel, table=True):
    __tablename__ = "sandbox_cost_cursor"

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    secret_name: str = Field(primary_key=True)
    next_hour: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))


class SandboxUnmatchedUsage(SQLModel, table=True):
    __tablename__ = "sandbox_unmatched_usage"

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    secret_name: str = Field(primary_key=True)
    hour: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    amount_usd: float
    sandboxes: int
    unpriced_sandboxes: int


class OrgProviderSecret(SQLModel, table=True):
    __tablename__ = "org_provider_secret"

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    provider: str = Field(primary_key=True)
    secret_name: str


class UserDefaultProvider(SQLModel, table=True):
    __tablename__ = "user_default_provider"

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    user_id: str = Field(primary_key=True)
    provider: str


class UserBenchmarkService(SQLModel, table=True):
    __tablename__ = "user_benchmark_service"

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    user_id: str = Field(primary_key=True)
    benchmark: str = Field(primary_key=True)
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None


class ExecutorDispatchPayload(SQLModel, table=True):
    __tablename__ = "executor_dispatch_payload"

    dispatch_id: UUID = Field(primary_key=True, foreign_key="executordispatch.id")
    ciphertext: bytes
    encrypted_data_key: bytes
    nonce: bytes
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )


class SandboxBuildReservation(SQLModel, table=True):
    __table_args__ = (
        CheckConstraint(
            "requested_vcpu >= 0 AND requested_memory >= 0 AND requested_disk >= 0 AND requested_gpu >= 0",
            name="sandbox_build_reservation_resources_nonnegative",
        ),
        Index("ix_sandboxbuildreservation_pool", "pool_id"),
        Index("ix_sandboxbuildreservation_pool_released_at", "pool_id", "released_at"),
    )

    task_row_id: UUID = Field(primary_key=True, foreign_key="task.id")
    attempt_started_at: datetime = Field(primary_key=True)
    reserved_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    released_at: datetime | None = None
    phase_dispatch_id: UUID | None = Field(default=None, foreign_key="executordispatch.id")
    phase_token: UUID | None = None
    executor_dispatch_id: UUID | None = Field(default=None, foreign_key="executordispatch.id")
    accepted_at: datetime | None = None
    pool_id: str
    target_id: str
    sandbox_class: str
    requested_vcpu: float
    requested_memory: float
    requested_disk: float
    requested_gpu: float


class BenchmarkServiceWake(SQLModel, table=True):
    name: str = Field(primary_key=True)
    requested_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))


class SandboxAdmissionCandidate(SQLModel, table=True):
    __table_args__ = (Index("ix_sandboxadmissioncandidate_pool", "pool_id"),)

    task_row_id: UUID = Field(primary_key=True, foreign_key="task.id")
    attempt_started_at: datetime = Field(primary_key=True)
    executor_dispatch_id: UUID = Field(foreign_key="executordispatch.id")
    admission_ready: bool
    readiness_checked_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    readiness_requested_at: datetime | None = None
    pool_id: str
    target_id: str
    sandbox_class: str
    requested_vcpu: float
    requested_memory: float
    requested_disk: float
    requested_gpu: float


class Benchmark(SQLModel, table=True):
    __table_args__ = (
        CheckConstraint(
            "(status != 'FINISHED' AND status != 'ERROR') OR (finished_at IS NOT NULL)",
            name="benchmark_finished_requires_timestamp",
        ),
        CheckConstraint(
            "(executor_release_id IS NULL AND executor_artifact_uri IS NULL AND executor_artifact_digest IS NULL "
            "AND executor_protocol_version IS NULL) OR "
            "(executor_release_id IS NOT NULL AND executor_artifact_uri IS NOT NULL "
            "AND executor_artifact_digest IS NOT NULL AND executor_protocol_version IS NOT NULL)",
            name="benchmark_executor_ownership_complete",
        ),
        ForeignKeyConstraint(
            ["publication_phase_dispatch_id"],
            ["executordispatch.id"],
            name="fk_benchmark_publication_dispatch",
            use_alter=True,
        ),
        Index(
            "ix_benchmark_org_started_at_id",
            "org_id",
            text("started_at DESC"),
            "id",
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    status: BenchmarkStatus = Field(default=BenchmarkStatus.IN_PROGRESS)
    label: str | None = Field(default=None, index=True)
    executor_release_id: str | None = Field(default=None, foreign_key="executorrelease.id", index=True)
    current_execution_release_id: str | None = Field(default=None, foreign_key="executorrelease.id", index=True)
    executor_artifact_uri: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    publication_phase_dispatch_id: UUID | None = Field(default=None)
    publication_phase_token: UUID | None = None
    publication_phase_fingerprint: str | None = None
    publication_status: str | None = None

    error_message: str | None = Field(default=None)
    custom_benchmark_service: str | None = Field(default=None)
    arguments: BenchmarkArguments = Field(
        sa_column=Column(BenchmarkArgumentsType),
    )
    started_by_id: str | None = Field(default=None)
    started_by_email: str | None = Field(default=None, index=True)
    docent_reading_status: DocentReadingStatus = Field(default=DocentReadingStatus.IDLE)
    docent_reading_url: str | None = Field(default=None)
    final_evaluation: Mapped[FinalEvaluation | None] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[FinalEvaluation.benchmark]"}
    )

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.services.results import fetch_evaluation_results

        return fetch_evaluation_results(self.id, session, self.org_id)

    def local_start_benchmark_request(self, service_headers: dict[str, str]) -> "RunExecutionRequest":
        from tracker.types import RunExecutionRequest

        return RunExecutionRequest(
            **self.arguments.model_dump(exclude={"dataset_version"}),
            resolved_dataset_version=self.arguments.dataset_version,
            dataset_ref=self.arguments.dataset_ref,
            benchmark_name=self.name,
            label=self.label,
            custom_benchmark_service=self.custom_benchmark_service,
            service_headers=service_headers,
        )

    def managed_start_benchmark_request(self, service_headers: dict[str, str] | None = None) -> "RunExecutionRequest":
        from tracker.types import RunExecutionRequest

        if self.arguments.environment == "aws" and not self.arguments.sandbox_provider_secret_name:
            raise ValueError("Managed runs require a sandbox provider secret name")

        return RunExecutionRequest(
            environment=self.arguments.environment,
            properties=self.arguments.properties,
            contract=self.arguments.contract,
            benchmark_name=self.name,
            concurrency=self.arguments.concurrency,
            label=self.label,
            task_ids=self.arguments.task_ids,
            slice_str=self.arguments.slice_str,
            lambda_function=self.arguments.lambda_function,
            dataset=self.arguments.dataset,
            resolved_dataset_version=self.arguments.dataset_version,
            dataset_ref=self.arguments.dataset_ref,
            sandbox_provider=self.arguments.sandbox_provider,
            sandbox_provider_secret_name=self.arguments.sandbox_provider_secret_name,
            custom_benchmark_service=self.custom_benchmark_service,
            service_headers=service_headers or {},
        )

    def benchmark_service(
        self,
        service_headers: dict[str, str] | None = None,
        *,
        benchmark_url: str | None = None,
    ) -> "BenchmarkServiceClient":
        from tracker.config import create_benchmark_service_url
        from tracker.clients.benchmark_service import create_benchmark_service_client

        url = (
            benchmark_url
            if benchmark_url is not None
            else self.custom_benchmark_service or create_benchmark_service_url(self.name)
        )
        return create_benchmark_service_client(
            url=url,
            service_headers=service_headers,
            dataset_version=self.arguments.dataset_version,
            sandbox_provider=self.arguments.sandbox_provider,
        )

    @property
    def benchmark_metadata(self) -> "FetchBenchmarkMetadataResponse":
        from tracker.types import FetchBenchmarkMetadataResponse

        return FetchBenchmarkMetadataResponse(
            benchmark_id=self.id,
            benchmark_name=self.name,
            benchmark_arguments=self.arguments.with_hosted_dataset(),
            storage_bucket=None,
            started_by_email=self.started_by_email,
            executor_release_id=self.executor_release_id,
            current_execution_release_id=self.current_execution_release_id,
            executor_artifact_uri=self.executor_artifact_uri,
            executor_artifact_digest=self.executor_artifact_digest,
            executor_protocol_version=self.executor_protocol_version,
            regraded_from_run_id=self.arguments.regraded_from_run_id,
            regrade_skipped_task_ids=self.arguments.regrade_skipped_task_ids,
        )


class Project(SQLModel, table=True):
    __table_args__ = (
        UniqueConstraint("org_id", "handle", name="unique_project_handle_per_org"),
        Index("uq_project_org_name", "org_id", text("lower(btrim(name))"), unique=True),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    gateway_request_id: UUID | None = None
    gateway_key_id: UUID | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    created_by_email: str | None = None
    archived_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class RunProject(SQLModel, table=True):
    __tablename__ = "run_project"

    id: int | None = Field(default=None, sa_column=Column(Integer, Identity(), primary_key=True))
    benchmark_id: UUID = Field(foreign_key="benchmark.id", index=True)
    project_id: UUID = Field(foreign_key="project.id")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    created_by_email: str | None = None


class BenchmarkDefinition(SQLModel, table=True):
    __tablename__ = "benchmark_definition"
    __table_args__ = (Index("uq_benchmark_definition_org_name", "org_id", "name", unique=True),)

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    name: str
    description: str = ""
    created_by_email: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    archived_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class Dataset(SQLModel, table=True):
    __table_args__ = (Index("uq_dataset_benchmark_name", "benchmark_definition_id", "name", unique=True),)

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    benchmark_definition_id: UUID = Field(foreign_key="benchmark_definition.id")
    name: str
    description: str = ""
    created_by_email: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    archived_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))


class DatasetVersion(SQLModel, table=True):
    __tablename__ = "dataset_version"
    __table_args__ = (
        UniqueConstraint("dataset_id", "revision", name="uq_dataset_version_revision"),
        UniqueConstraint("dataset_id", "digest", name="uq_dataset_version_digest"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    dataset_id: UUID = Field(foreign_key="dataset.id")
    revision: int
    digest: str
    task_count: int
    metric_digest: str | None = None
    manifest: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    created_by_email: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )


class DatasetVersionTask(SQLModel, table=True):
    __tablename__ = "dataset_version_task"

    version_id: UUID = Field(foreign_key="dataset_version.id", primary_key=True)
    task_id: str = Field(primary_key=True)
    digest: str
    size_bytes: int = Field(sa_column=Column(BigInteger, nullable=False))
    source_name: str | None = None


class DatasetTag(SQLModel, table=True):
    __tablename__ = "dataset_tag"

    dataset_id: UUID = Field(foreign_key="dataset.id", primary_key=True)
    tag: str = Field(primary_key=True)
    version_id: UUID = Field(foreign_key="dataset_version.id", index=True)


@event.listens_for(Benchmark, "before_insert")
@event.listens_for(Benchmark, "before_update")
def set_finished_at_when_benchmark_finished(_mapper: Mapper[Benchmark], _connection: Connection, target: Benchmark):
    finished_states = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR]

    status_changed = has_field_changed(target, "status")

    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class ActiveNativeOutputType(TypeDecorator[ActiveNativeOutput]):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: ActiveNativeOutput | None, dialect: Dialect) -> dict[str, Any] | None:
        return value.model_dump(mode="json") if value is not None else None

    def process_result_value(self, value: dict[str, Any] | None, dialect: Dialect) -> ActiveNativeOutput | None:
        return ActiveNativeOutput.model_validate(value) if value is not None else None


class Task(SQLModel, table=True):
    __table_args__: tuple[CheckConstraint, UniqueConstraint, Index] = (
        CheckConstraint(
            "(status != 'FINISHED' AND status != 'ERROR') OR (finished_at IS NOT NULL)",
            name="task_finished_requires_timestamp",
        ),
        UniqueConstraint("benchmark", "task_id", name="unique_task_per_benchmark"),
        Index(
            "ix_task_benchmark_org_started_at",
            "benchmark",
            "org_id",
            text("started_at DESC"),
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    task_id: str
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    build_phase_dispatch_id: UUID | None = Field(default=None, foreign_key="executordispatch.id")
    build_phase_token: UUID | None = None
    evaluation_phase_dispatch_id: UUID | None = Field(default=None, foreign_key="executordispatch.id")
    evaluation_phase_token: UUID | None = None
    eval_resume_state: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    active_native_output: ActiveNativeOutput | None = Field(
        default=None, exclude=True, sa_column=Column(ActiveNativeOutputType(), nullable=True)
    )
    credited_wall_deadline_at: datetime | None = Field(
        default=None, sa_column=Column(DateTime(timezone=True), nullable=True)
    )
    benchmark: UUID = Field(foreign_key="benchmark.id")
    task_breakdown: UUID | None = Field(default=None, foreign_key="taskbreakdown.id")


@event.listens_for(Task, "before_insert")
@event.listens_for(Task, "before_update")
def set_finished_at_when_task_finished(_mapper: Mapper[Task], _connection: Connection, target: Task):
    finished_states = [TaskStatus.FINISHED, TaskStatus.ERROR]

    status_changed = has_field_changed(target, "status")

    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class TaskBreakdown(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True, exclude=True)
    sandbox_build_duration: float | None = Field(default=None)
    agent_run_duration: float | None = Field(default=None)
    evaluation_run_duration: float | None = Field(default=None)
    sandbox_run_duration: float | None = Field(default=None)
    accounting_id: str | None = Field(default=None)
    base_generation_allowance_seconds: float | None = Field(default=None)
    cumulative_time_credit_cap_seconds: float | None = Field(default=None)
    external_service_overhead_seconds: float | None = Field(default=None)
    external_service_credit_applied_seconds: float | None = Field(default=None)
    external_service_credit_revision: int | None = Field(default=None)
    generation_elapsed_seconds: float | None = Field(default=None)


class ResultBase(SQLModel):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    task: UUID = Field(foreign_key="task.id")
    created_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))


class EvaluationResult(ResultBase, table=True):
    __table_args__ = (
        Index(
            "ix_evaluationresult_org_task_created_at_id",
            "org_id",
            "task",
            text("created_at DESC"),
            text("id DESC"),
        ),
    )

    instance_id: str | None = Field(default=None, unique=True)
    agent_caused_exit_reason: AgentCausedExitReason | None = Field(default=None)
    result: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))


class ErrorResult(ResultBase, table=True):
    __table_args__ = (Index("ix_errorresult_org_task_created_at", "org_id", "task", "created_at"),)

    error_message: str = Field(nullable=False)
    producer: str | None = Field(default=None)
    operation: str | None = Field(default=None)
    error_type: str | None = Field(default=None)
    cause_code: str | None = Field(default=None)
    category: FailureCategory | None = Field(default=None)
    phase: FailurePhase | None = Field(default=None)
    transient: bool | None = Field(default=None)
    retry_scheduled: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default=text("false")),
    )
    failed_attempt_number: int | None = Field(default=None)
