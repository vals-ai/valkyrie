import asyncio
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from pydantic import BaseModel

from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import AttemptRef, BuildPhaseSnapshot, DispatchRef, PhaseRef, TaskRef


class QueueRequest(BaseModel):
    ref: DispatchRef
    pool_id: str


class QueueAttemptRequest(QueueRequest):
    attempt: AttemptRef


class QueueWaiter(BaseModel):
    attempt: AttemptRef
    ready: bool


class QueueRoundRequest(QueueRequest):
    waiters: list[QueueWaiter]
    retry_rejected: bool


class QueueRoundResponse(BaseModel):
    waiting: list[UUID]
    selected: list[UUID]
    retries: list[UUID]
    budget: int | None


class QueueEligibilityResponse(BaseModel):
    eligible: bool | None


class QueueTimeResponse(BaseModel):
    read_at: datetime


class QueueDemand(BaseModel):
    target_id: str
    sandbox_class: str
    vcpu: float
    memory: float
    disk: float
    gpu: float


class QueuePrepareRequest(QueueAttemptRequest):
    demand: QueueDemand
    admission_ready: bool


class QueueStateResponse(BaseModel):
    status: str | None
    readiness_requested_at: datetime | None


class QueueStatesRequest(QueueRequest):
    attempts: list[AttemptRef]


class QueueStatesResponse(BaseModel):
    states: list[QueueStateResponse]


class QueueReadinessRequest(QueueAttemptRequest):
    requested_at: datetime


class QueueReadinessResponse(BaseModel):
    started: bool


class QueueClaimResponse(BaseModel):
    phase: PhaseRef | None
    waiting: bool


class QueueBuildRequest(QueueAttemptRequest):
    phase: PhaseRef


class QueueBuildResponse(BaseModel):
    started: bool


class QueueOrphan(BaseModel):
    attempt: AttemptRef
    phase: PhaseRef
    status: str
    sandbox_name: str
    reserved_at: datetime


def queue_pool_lock_id(pool_id: str) -> str:
    return pool_id.removesuffix(".reserved")


def reserved_queue_pool_id(pool_id: str) -> str:
    return f"{queue_pool_lock_id(pool_id)}.reserved"


def is_reserved_queue_pool_id(pool_id: str) -> bool:
    return pool_id.endswith(".reserved")


def queue_pool_id(provider_pool_id: str) -> str:
    return f"pool_{sha256(provider_pool_id.encode()).hexdigest()[:24]}"


def queued_sandbox_name(task_row_id: UUID, started_at: datetime) -> str:
    attempt = started_at.replace(tzinfo=UTC) if started_at.tzinfo is None else started_at.astimezone(UTC)
    return f"queued-{task_row_id.hex}-{int(attempt.timestamp() * 1_000_000):x}"


class QueueOrphansResponse(BaseModel):
    orphans: list[QueueOrphan]
    server_now: datetime


class QueueOrphanResultRequest(QueueBuildRequest):
    cleaned: bool


_STATE_BATCH_WINDOW_SECONDS = 1.0


class QueueControl:
    def __init__(self, control: ControlClient, ref: DispatchRef) -> None:
        self.control = control
        self.ref = ref
        self._phases: dict[UUID, BuildPhaseSnapshot] = {}
        self._state_waiters: dict[str, list[tuple[AttemptRef, asyncio.Future[QueueStateResponse]]]] = {}
        self._state_batches: set[asyncio.Task[None]] = set()

    def active_snapshot(self) -> list[BuildPhaseSnapshot]:
        return list(self._phases.values())

    def request(self, pool_id: str) -> QueueRequest:
        return QueueRequest(ref=self.ref, pool_id=pool_id)

    async def clock(self, pool_id: str) -> datetime:
        return (await self.control.call("queue-clock", self.request(pool_id), QueueTimeResponse)).read_at

    async def eligibility(self, pool_id: str, attempt: AttemptRef) -> bool | None:
        request = QueueAttemptRequest(**self.request(pool_id).model_dump(), attempt=attempt)
        return (await self.control.call("queue-eligibility", request, QueueEligibilityResponse)).eligible

    async def round(self, pool_id: str, waiters: list[QueueWaiter], retry_rejected: bool) -> QueueRoundResponse:
        request = QueueRoundRequest(
            **self.request(pool_id).model_dump(), waiters=waiters, retry_rejected=retry_rejected
        )
        return await self.control.call("queue-round", request, QueueRoundResponse)

    async def prepare(self, pool_id: str, attempt: AttemptRef, demand: QueueDemand, ready: bool) -> None:
        request = QueuePrepareRequest(
            **self.request(pool_id).model_dump(), attempt=attempt, demand=demand, admission_ready=ready
        )
        await self.control.call("queue-prepare", request, QueueBuildResponse)

    async def state(self, pool_id: str, attempt: AttemptRef) -> QueueStateResponse:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[QueueStateResponse] = loop.create_future()
        waiters = self._state_waiters.get(pool_id)
        if waiters is None:
            waiters = self._state_waiters[pool_id] = []
            batch = loop.create_task(self._send_states(pool_id, waiters))
            self._state_batches.add(batch)
            batch.add_done_callback(self._state_batches.discard)
        waiters.append((attempt, future))
        return await future

    async def _send_states(
        self, pool_id: str, waiters: list[tuple[AttemptRef, asyncio.Future[QueueStateResponse]]]
    ) -> None:
        try:
            await asyncio.sleep(_STATE_BATCH_WINDOW_SECONDS)
            del self._state_waiters[pool_id]
            request = QueueStatesRequest(
                **self.request(pool_id).model_dump(), attempts=[attempt for attempt, _ in waiters]
            )
            try:
                response = await self.control.call("queue-states", request, QueueStatesResponse)
            except Exception as error:
                for _, future in waiters:
                    if not future.done():
                        future.set_exception(error)
                return
            for (_, future), state in zip(waiters, response.states, strict=True):
                if not future.done():
                    future.set_result(state)
        finally:
            if self._state_waiters.get(pool_id) is waiters:
                del self._state_waiters[pool_id]
            for _, future in waiters:
                future.cancel()

    async def begin_readiness(self, pool_id: str, attempt: AttemptRef, requested_at: datetime) -> bool:
        request = QueueReadinessRequest(
            **self.request(pool_id).model_dump(), attempt=attempt, requested_at=requested_at
        )
        return (await self.control.call("queue-begin-readiness", request, QueueReadinessResponse)).started

    async def accept(self, pool_id: str, attempt: AttemptRef) -> PhaseRef | None:
        request = QueueAttemptRequest(**self.request(pool_id).model_dump(), attempt=attempt)
        response = await self.control.call("queue-accept", request, QueueClaimResponse)
        if response.phase is not None:
            self._phases[attempt.task_row_id] = BuildPhaseSnapshot(
                scope="build",
                task=TaskRef(org_id=attempt.org_id, task_row_id=attempt.task_row_id, task_id=attempt.task_id),
                started_at=attempt.started_at,
                status="BUILDING",
                phase=response.phase,
            )
        return response.phase

    async def claim(self, pool_id: str, attempt: AttemptRef) -> QueueClaimResponse:
        request = QueueAttemptRequest(**self.request(pool_id).model_dump(), attempt=attempt)
        return await self.control.call("queue-claim", request, QueueClaimResponse)

    async def build(self, operation: str, pool_id: str, attempt: AttemptRef, phase: PhaseRef) -> QueueBuildResponse:
        request = QueueBuildRequest(**self.request(pool_id).model_dump(), attempt=attempt, phase=phase)
        response = await self.control.call(operation, request, QueueBuildResponse)
        if operation in ("queue-abort", "queue-abandon") or (operation == "queue-finish" and response.started):
            snapshot = self._phases.get(attempt.task_row_id)
            if snapshot is not None and snapshot.started_at == attempt.started_at and snapshot.phase == phase:
                self._phases.pop(attempt.task_row_id)
        return response

    async def abandon(self, pool_id: str, attempt: AttemptRef, phase: PhaseRef) -> None:
        await self.build("queue-abandon", pool_id, attempt, phase)

    async def clear(self, pool_id: str, attempt: AttemptRef) -> None:
        request = QueueAttemptRequest(**self.request(pool_id).model_dump(), attempt=attempt)
        await self.control.call("queue-clear", request, QueueBuildResponse)

    async def orphans(self, pool_id: str) -> QueueOrphansResponse:
        response = await self.control.call("queue-orphans", self.request(pool_id), QueueOrphansResponse)
        for orphan in response.orphans:
            attempt = orphan.attempt
            snapshot = self._phases.get(attempt.task_row_id)
            if snapshot is not None and (snapshot.started_at != attempt.started_at or snapshot.phase != orphan.phase):
                continue
            self._phases[attempt.task_row_id] = BuildPhaseSnapshot(
                scope="build",
                task=TaskRef(org_id=attempt.org_id, task_row_id=attempt.task_row_id, task_id=attempt.task_id),
                started_at=attempt.started_at,
                status=orphan.status,
                phase=orphan.phase,
            )
        return response

    async def reconcile(self, pool_id: str, attempt: AttemptRef, phase: PhaseRef, cleaned: bool) -> None:
        request = QueueOrphanResultRequest(
            **self.request(pool_id).model_dump(), attempt=attempt, phase=phase, cleaned=cleaned
        )
        await self.control.call("queue-reconcile", request, QueueBuildResponse)
        snapshot = self._phases.get(attempt.task_row_id)
        if snapshot is not None and snapshot.started_at == attempt.started_at and snapshot.phase == phase:
            self._phases.pop(attempt.task_row_id)
