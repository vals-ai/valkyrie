from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

from tracker.domain import BenchmarkStatus, FailureCategory, TaskStatus
from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import (
    AttemptPhaseSnapshot,
    AttemptRef,
    DispatchRef,
    PhaseRef,
    PublicationPhaseSnapshot,
)
from tracker.runtime.artifacts import benchmark_prefix
from tracker.runtime.storage import ObjectStore
from tracker.types import FinalViewResponse


class RunTaskState(BaseModel):
    ref: AttemptRef
    status: TaskStatus
    eval_resume_state: dict[str, Any] | None


class RunSnapshot(BaseModel):
    authority_current: bool
    status: BenchmarkStatus
    concurrency: int
    queue_pool_id: str | None
    tasks: list[RunTaskState]
    reserved_task_ids: list[UUID]
    org_id: UUID
    org_name: str
    org_storage_key: str | None
    benchmark_name: str
    custom_benchmark_service: str | None
    benchmark_service_woken: bool
    arguments: dict[str, Any]


class ReadRunStateRequest(BaseModel):
    ref: DispatchRef
    tracked_task_ids: list[str]
    create_missing_tasks: bool


class FinalScoreInputs(BaseModel):
    phase: PhaseRef
    fingerprint: str
    evaluation_results: dict[str, dict[str, Any] | None]


class FinalizationDecision(BaseModel):
    kind: str
    inputs: FinalScoreInputs | None
    task_errors: dict[str, str] | None
    fingerprint: str | None


class PrepareFinalizationRequest(BaseModel):
    ref: DispatchRef


class ReadDatasetVersionRequest(BaseModel):
    ref: DispatchRef


class DatasetVersionSnapshot(BaseModel):
    task_digests: dict[str, str]
    metric_digest: str | None


class CommitFinalScoreRequest(BaseModel):
    ref: DispatchRef
    phase: PhaseRef
    fingerprint: str
    final_score: float | None
    metadata: dict[str, Any]


class PublicationClaim(BaseModel):
    committed: bool
    phase: PhaseRef | None
    final_view: FinalViewResponse | None
    status: BenchmarkStatus | None


class PublishFinalViewRequest(BaseModel):
    ref: DispatchRef
    phase: PhaseRef
    effect: Literal["upload_before", "upload_after", "callback_before", "callback_after"]


class PublicationDecision(BaseModel):
    allowed: bool


class RunFailure(BaseModel):
    error_message: str
    producer: str
    operation: str
    error_type: str
    category: FailureCategory
    cause_code: str | None
    task_ids: list[str]
    fingerprint: str | None


class CommitRunErrorRequest(BaseModel):
    ref: DispatchRef
    failure: RunFailure


class WriteDecision(BaseModel):
    committed: bool


class CommitRunErrorDecision(BaseModel):
    committed: bool
    status: BenchmarkStatus


class RecordCancellationRequest(BaseModel):
    ref: DispatchRef
    owned_attempts: dict[UUID, datetime]
    verified_task_ids: list[str]


class SweepExitRequest(BaseModel):
    ref: DispatchRef
    task_ids: list[str]


class RunControl:
    def __init__(self, client: ControlClient, ref: DispatchRef):
        self.client = client
        self.ref = ref
        self._publication_phase: PhaseRef | None = None
        self._publication_status = "SCORING"

    def active_snapshot(self) -> list[AttemptPhaseSnapshot]:
        if self._publication_phase is None:
            return []
        return [
            PublicationPhaseSnapshot(
                scope="publication", status=self._publication_status, phase=self._publication_phase
            )
        ]

    async def read_run_state(self, tracked_task_ids: list[str], *, create_missing_tasks: bool = False) -> RunSnapshot:
        return await self.client.call(
            "read_run_state",
            ReadRunStateRequest(
                ref=self.ref, tracked_task_ids=tracked_task_ids, create_missing_tasks=create_missing_tasks
            ),
            RunSnapshot,
        )

    async def read_dataset_version(self) -> DatasetVersionSnapshot:
        return await self.client.call(
            "read_dataset_version", ReadDatasetVersionRequest(ref=self.ref), DatasetVersionSnapshot
        )

    async def prepare_finalization(self) -> FinalizationDecision:
        result = await self.client.call(
            "prepare_finalization", PrepareFinalizationRequest(ref=self.ref), FinalizationDecision
        )
        if result.inputs is not None:
            self._publication_phase = result.inputs.phase
            self._publication_status = "SCORING"
        return result

    async def commit_final_score(
        self, phase: PhaseRef, fingerprint: str, final_score: float | None, metadata: dict[str, Any]
    ) -> PublicationClaim:
        result = await self.client.call(
            "commit_final_score",
            CommitFinalScoreRequest(
                ref=self.ref, phase=phase, fingerprint=fingerprint, final_score=final_score, metadata=metadata
            ),
            PublicationClaim,
        )
        if result.committed:
            self._publication_status = "FINAL_STATUS_COMMITTED"
        return result

    async def publish_final_view(
        self, phase: PhaseRef, effect: Literal["upload_before", "upload_after", "callback_before", "callback_after"]
    ) -> PublicationDecision:
        result = await self.client.call(
            "publish_final_view",
            PublishFinalViewRequest(ref=self.ref, phase=phase, effect=effect),
            PublicationDecision,
        )
        if result.allowed:
            self._publication_status = {
                "upload_before": "UPLOAD_ATTEMPTED",
                "upload_after": "UPLOAD_COMPLETED",
                "callback_before": "CALLBACK_ATTEMPTED",
                "callback_after": "CALLBACK_COMPLETED",
            }[effect]
        return result

    async def commit_run_error(self, failure: RunFailure) -> CommitRunErrorDecision:
        return await self.client.call(
            "commit_run_error", CommitRunErrorRequest(ref=self.ref, failure=failure), CommitRunErrorDecision
        )

    async def record_cancellation(
        self, owned_attempts: dict[UUID, datetime], verified_task_ids: list[str]
    ) -> CommitRunErrorDecision:
        return await self.client.call(
            "record_run_cancellation",
            RecordCancellationRequest(ref=self.ref, owned_attempts=owned_attempts, verified_task_ids=verified_task_ids),
            CommitRunErrorDecision,
        )

    async def sweep_exit(self, task_ids: list[str]) -> bool:
        response = await self.client.call(
            "sweep_exit", SweepExitRequest(ref=self.ref, task_ids=task_ids), WriteDecision
        )
        return response.committed


async def upload_final_view(final_view: FinalViewResponse, object_store: ObjectStore) -> str:
    key = f"{benchmark_prefix(str(final_view.benchmark_id))}{final_view.benchmark_name}.json"
    await object_store.put_bytes(key, final_view.model_dump_json(indent=4, exclude_none=True).encode())
    return key
