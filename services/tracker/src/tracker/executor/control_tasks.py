from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel

from tracker.domain import BenchmarkStatus, FailureCategory, FailurePhase, TaskStatus
from tracker.egress_proxy.resolve import AttemptStoppedError
from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import (
    AttemptPhaseSnapshot,
    AttemptRef,
    BuildPhaseSnapshot,
    DispatchRef,
    EvaluationPhaseSnapshot,
    PhaseRef,
    TaskRef,
)
from tracker.logging import get_logger
from tracker.search_cost import SearchCharge
from tracker.types import ActiveNativeOutput

if TYPE_CHECKING:
    from tracker.egress_proxy.resolve import RunEgressProxy
    from tracker.runtime.run_egress import RunAttempt


logger = get_logger(__name__)


@dataclass(frozen=True)
class SandboxLink:
    benchmark_id: UUID
    provider: str


class TaskState(BaseModel):
    id: UUID
    org_id: UUID
    task_id: str
    benchmark: UUID
    started_at: datetime
    status: TaskStatus
    eval_resume_state: dict[str, Any] | None
    credited_wall_deadline_at: datetime | None
    build_phase: PhaseRef | None
    evaluation_phase: PhaseRef | None

    @property
    def ref(self) -> AttemptRef:
        return AttemptRef(org_id=self.org_id, task_row_id=self.id, task_id=self.task_id, started_at=self.started_at)


class BenchmarkState(BaseModel):
    name: str
    started_by_email: str | None
    selected_project_gateway_secret_ref: str | None
    status: BenchmarkStatus
    concurrency: int
    contract_name: str


class TaskReadRequest(BaseModel):
    ref: DispatchRef
    task: TaskRef


class TaskReadResponse(BaseModel):
    task: TaskState
    benchmark: BenchmarkState


class TaskMonitorRequest(BaseModel):
    ref: DispatchRef
    task_ids: list[str]


class TaskMonitorResponse(BaseModel):
    benchmark_status: BenchmarkStatus
    concurrency: int
    tasks: dict[str, TaskState]


class TaskMutationRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    to_status: TaskStatus
    expected_status: TaskStatus | list[TaskStatus] | None


class DirectAdmissionResponse(BaseModel):
    admitted: bool | None
    phase: PhaseRef | None


class TaskDecision(BaseModel):
    accepted: bool


class TaskDeadlineResponse(BaseModel):
    deadline: datetime


class TaskResumeRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    phase: PhaseRef
    state: dict[str, Any]


class TaskEvaluationRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef


class TaskEvaluationResponse(BaseModel):
    phase: PhaseRef | None
    resume_state: dict[str, Any] | None


class TaskResultRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    phase: PhaseRef
    result: dict[str, Any]
    instance_id: str | None
    agent_caused_exit_reason: str | None
    evaluation_run_duration: float
    sandbox_run_duration: float | None
    resumed: bool


class TaskFailure(BaseModel):
    message: str
    producer: str
    operation: str
    error_type: str
    category: FailureCategory
    cause_code: str | None
    phase: FailurePhase | None
    transient: bool | None


class TaskFailureRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    failure: TaskFailure
    expected_status: TaskStatus | None
    retry_scheduled: bool
    failed_attempt_number: int | None


class TaskAccountingRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    summary: dict[str, Any]


class TaskSearchCostsRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    charges: list[SearchCharge]
    step_name: str | None = None


class TaskBreakdownRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    sandbox_build_duration: float
    agent_run_duration: float


class NativeOutputRequest(BaseModel):
    ref: DispatchRef
    attempt: AttemptRef
    descriptor: ActiveNativeOutput


class SandboxLinkRequest(BaseModel):
    ref: DispatchRef
    benchmark_id: UUID
    provider: str
    sandbox_id: str
    seen_at: datetime | None = None


class SandboxCostRequest(BaseModel):
    ref: DispatchRef
    benchmark_id: UUID
    provider: str
    sandbox_id: str
    amount_usd: float
    usage_end: datetime


class EgressGrantRequest(BaseModel):
    ref: DispatchRef
    task_row_id: UUID
    key_id: str
    token_sha256: str
    methods: list[str]
    domains: list[str] | None
    expires_at: datetime
    inspect_domains: list[str]
    gateway_url: str | None


class EgressRevokeRequest(BaseModel):
    ref: DispatchRef
    task_row_id: UUID
    key_id: str


class EgressGrantResponse(BaseModel):
    accepted: bool


class TaskControl:
    def __init__(self, client: ControlClient, ref: DispatchRef):
        self.client = client
        self.ref = ref
        self._active: dict[UUID, AttemptPhaseSnapshot] = {}
        self._accounting: dict[UUID, TaskAccountingRequest] = {}
        self._search_costs: dict[tuple[UUID, str | None], TaskSearchCostsRequest] = {}
        self._checkpoints: dict[UUID, TaskResumeRequest] = {}
        self._checkpoint_locks: dict[UUID, asyncio.Lock] = {}
        self._invalidated: set[UUID] = set()

    def invalidate(self, task_id: UUID) -> None:
        self._invalidated.add(task_id)

    def invalidated(self, task_id: UUID) -> bool:
        return task_id in self._invalidated

    def active_snapshot(self) -> list[AttemptPhaseSnapshot]:
        return list(self._active.values())

    def track(self, task: TaskState) -> None:
        attempt = task.ref
        accounting = self._accounting.get(task.id)
        if accounting is not None and accounting.attempt != attempt:
            del self._accounting[task.id]
        for key, search_costs in list(self._search_costs.items()):
            if key[0] == task.id and search_costs.attempt != attempt:
                del self._search_costs[key]
        checkpoint = self._checkpoints.get(task.id)
        if checkpoint is not None and checkpoint.attempt != attempt:
            del self._checkpoints[task.id]
        if task.status == TaskStatus.EVALUATING:
            snapshot = EvaluationPhaseSnapshot(
                scope="evaluation",
                task=TaskRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id),
                started_at=task.started_at,
                status=task.status.value,
                phase=task.evaluation_phase,
            )
        else:
            snapshot = BuildPhaseSnapshot(
                scope="build",
                task=TaskRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id),
                started_at=task.started_at,
                status=task.status.value,
                phase=task.build_phase,
            )
        self._active[task.id] = snapshot

    def untrack(self, task_id: UUID) -> None:
        self._active.pop(task_id, None)

    async def read(self, task: TaskRef) -> TaskReadResponse:
        return await self.client.call("read_task", TaskReadRequest(ref=self.ref, task=task), TaskReadResponse)

    async def monitor(self, task_ids: list[str]) -> TaskMonitorResponse:
        return await self.client.call(
            "monitor_tasks", TaskMonitorRequest(ref=self.ref, task_ids=task_ids), TaskMonitorResponse
        )

    async def admit(self, attempt: AttemptRef) -> DirectAdmissionResponse:
        return await self.client.call(
            "admit_direct_task", TaskEvaluationRequest(ref=self.ref, attempt=attempt), DirectAdmissionResponse
        )

    async def transition(self, request: TaskMutationRequest) -> bool:
        return (await self.client.call("transition_task", request, TaskDecision)).accepted

    async def deadline(self, attempt: AttemptRef) -> datetime:
        response = await self.client.call(
            "ensure_task_deadline", TaskEvaluationRequest(ref=self.ref, attempt=attempt), TaskDeadlineResponse
        )
        return response.deadline

    async def evaluation(self, attempt: AttemptRef) -> TaskEvaluationResponse:
        return await self.client.call(
            "claim_task_evaluation", TaskEvaluationRequest(ref=self.ref, attempt=attempt), TaskEvaluationResponse
        )

    async def finish(self, request: TaskResultRequest) -> bool:
        return (await self.client.call("finish_task_evaluation", request, TaskDecision)).accepted

    async def fail(self, request: TaskFailureRequest) -> bool:
        if not request.retry_scheduled:
            await self.flush_attempt(request.attempt)
        return (await self.client.call("fail_task", request, TaskDecision)).accepted

    async def breakdown(self, request: TaskBreakdownRequest) -> bool:
        return (await self.client.call("write_task_breakdown", request, TaskDecision)).accepted

    async def publish_native_output(self, request: NativeOutputRequest) -> bool:
        return (await self.client.call("publish_native_output", request, TaskDecision)).accepted

    async def clear_native_output(self, request: NativeOutputRequest) -> None:
        await self.client.call("clear_native_output", request, TaskDecision)

    async def record_sandbox(self, link: SandboxLink, sandbox_id: str, seen_at: datetime | None = None) -> None:
        try:
            await self.client.call(
                "record_sandbox_link",
                SandboxLinkRequest(
                    ref=self.ref,
                    benchmark_id=link.benchmark_id,
                    provider=link.provider,
                    sandbox_id=sandbox_id,
                    seen_at=seen_at or datetime.now(UTC),
                ),
                TaskDecision,
            )
        except Exception:
            logger.exception(
                "Failed to save run sandbox", extra={"sandbox_id": sandbox_id, "benchmark_id": str(link.benchmark_id)}
            )

    async def record_sandbox_cost(
        self, link: SandboxLink, sandbox_id: str, amount_usd: float, usage_end: datetime
    ) -> None:
        try:
            await self.client.call(
                "record_sandbox_cost",
                SandboxCostRequest(
                    ref=self.ref,
                    benchmark_id=link.benchmark_id,
                    provider=link.provider,
                    sandbox_id=sandbox_id,
                    amount_usd=amount_usd,
                    usage_end=usage_end,
                ),
                TaskDecision,
            )
        except Exception:
            logger.exception(
                "Failed to save sandbox cost", extra={"sandbox_id": sandbox_id, "benchmark_id": str(link.benchmark_id)}
            )

    async def grant_egress_policy(self, proxy: RunEgressProxy, attempt: RunAttempt) -> None:
        response = await self.client.call(
            "grant_egress_policy",
            EgressGrantRequest(
                ref=self.ref,
                task_row_id=attempt.task_row_id,
                key_id=proxy.key_id,
                token_sha256=proxy.token_sha256,
                methods=sorted(proxy.methods),
                domains=sorted(proxy.domains) if proxy.domains is not None else None,
                expires_at=attempt.expires_at,
                inspect_domains=sorted(proxy.inspect_domains),
                gateway_url=proxy.gateway_url,
            ),
            EgressGrantResponse,
        )
        if not response.accepted:
            raise AttemptStoppedError(proxy.key_id)

    async def revoke_egress_policy(self, key_id: str, attempt: RunAttempt) -> None:
        await self.client.call(
            "revoke_egress_policy",
            EgressRevokeRequest(ref=self.ref, task_row_id=attempt.task_row_id, key_id=key_id),
            EgressGrantResponse,
        )

    async def checkpoint(self, request: TaskResumeRequest) -> None:
        task_id = request.attempt.task_row_id
        self._checkpoints[task_id] = request
        async with self._checkpoint_locks.setdefault(task_id, asyncio.Lock()):
            pending = self._checkpoints.get(task_id)
            if pending is not None:
                await self.client.send("save_task_resume", pending, TaskDecision)
                if self._checkpoints.get(task_id) is pending:
                    del self._checkpoints[task_id]

    async def accounting(self, request: TaskAccountingRequest) -> None:
        task_id = request.attempt.task_row_id
        self._accounting[task_id] = request
        await self.client.send("write_task_accounting", request, TaskDecision)
        if self._accounting.get(task_id) is request:
            del self._accounting[task_id]

    async def search_costs(self, request: TaskSearchCostsRequest) -> None:
        key = (request.attempt.task_row_id, request.step_name)
        self._search_costs[key] = request
        await self.client.send("write_task_search_costs", request, TaskDecision)
        if self._search_costs.get(key) is request:
            del self._search_costs[key]

    async def flush_attempt(self, attempt: AttemptRef) -> None:
        accounting = self._accounting.get(attempt.task_row_id)
        if accounting is not None and accounting.attempt == attempt:
            await self.client.call("write_task_accounting", accounting, TaskDecision)
            if self._accounting.get(attempt.task_row_id) is accounting:
                del self._accounting[attempt.task_row_id]
        for key, search_costs in list(self._search_costs.items()):
            if key[0] != attempt.task_row_id or search_costs.attempt != attempt:
                continue
            await self.client.call("write_task_search_costs", search_costs, TaskDecision)
            if self._search_costs.get(key) is search_costs:
                del self._search_costs[key]
        async with self._checkpoint_locks.setdefault(attempt.task_row_id, asyncio.Lock()):
            checkpoint = self._checkpoints.get(attempt.task_row_id)
            if checkpoint is not None and checkpoint.attempt == attempt:
                await self.client.call("save_task_resume", checkpoint, TaskDecision)
                if self._checkpoints.get(attempt.task_row_id) is checkpoint:
                    del self._checkpoints[attempt.task_row_id]
