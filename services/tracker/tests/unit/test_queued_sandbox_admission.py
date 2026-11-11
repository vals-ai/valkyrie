from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from benchmark_service import ResourceCapacity, Sandbox, SandboxCapacity, SandboxReservationDemand

from tracker.executor.control_protocol import AttemptRef, PhaseRef
from tracker.executor.control_queue import (
    QueueBuildResponse,
    QueueClaimResponse,
    QueueStateResponse,
    queue_pool_id,
    reserved_queue_pool_id,
)
from tracker.sandbox import SandboxCreationState
from tracker.scheduler import admission, store


@pytest.mark.parametrize(("queue_kind", "claims"), [("reserved", [True]), ("legacy", [False, True])])
@pytest.mark.description(
    "Control acceptance or legacy queue claim surrounds sandbox creation and publishes its build phase once."
)
async def test_claim_phase_surrounds_sandbox_creation(queue_kind: str, claims: list[bool]) -> None:
    phase = PhaseRef(token=uuid4())
    attempt = AttemptRef(
        org_id=uuid4(), task_row_id=uuid4(), task_id="queued", started_at=datetime(2026, 7, 27, tzinfo=UTC)
    )
    events: list[object] = []
    provider = Mock()
    provider.check_admission = AsyncMock(return_value=True)
    provider.reservable_demand = AsyncMock(
        return_value=SandboxReservationDemand(
            target_id="region-a",
            sandbox_class="sysbox",
            vcpu=1,
            memory=2,
            disk=3,
            gpu=0,
        )
    )
    remaining_claims = iter(claims)

    async def claim(*_args: object) -> QueueClaimResponse:
        events.append("claim")
        granted = next(remaining_claims)
        return QueueClaimResponse(phase=phase if granted else None, waiting=True)

    async def accept(*_args: object) -> PhaseRef | None:
        events.append("claim")
        assert next(remaining_claims)
        return phase

    async def build(*_args: object) -> QueueBuildResponse:
        events.append("finish")
        return QueueBuildResponse(started=True)

    reserved = queue_kind == "reserved"
    control = Mock(
        eligibility=AsyncMock(return_value=True),
        state=AsyncMock(
            side_effect=[
                QueueStateResponse(status="PENDING", readiness_requested_at=None),
                QueueStateResponse(status="BUILDING", readiness_requested_at=None),
            ]
        ),
        prepare=AsyncMock(),
        claim=AsyncMock(side_effect=claim),
        accept=AsyncMock(side_effect=accept),
        build=AsyncMock(side_effect=build),
        clear=AsyncMock(),
    )
    context = admission.SandboxQueueContext(
        provider=provider,
        pool_id=reserved_queue_pool_id(queue_pool_id("pool")) if reserved else queue_pool_id("pool"),
        control=control,
        poll_interval_seconds=0,
    )
    sandbox = Mock(spec=Sandbox)

    @asynccontextmanager
    async def create() -> AsyncGenerator[Sandbox, None]:
        events.append("create")
        try:
            yield sandbox
        finally:
            events.append("cleanup")

    async with AsyncExitStack() as stack, AsyncExitStack() as preparation_stack:
        await preparation_stack.enter_async_context(context.preparation_slots)
        result = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            attempt=attempt,
            source=Mock(),
            resources=Mock(),
            creation_state=SandboxCreationState(cleanup_pending=False),
            create=create,
            preparation_stack=preparation_stack,
            on_phase_claimed=lambda claimed: events.append(claimed),
            on_phase_released=lambda started: events.append(started),
            on_phase_abandon=lambda _phase: None,
        )
        assert result is sandbox

    assert events == ["claim"] * len(claims) + [phase, "create", "finish", True, "cleanup"]
    if reserved:
        control.prepare.assert_awaited_once()
        control.accept.assert_awaited_once_with(context.pool_id, attempt)
        control.clear.assert_awaited_once_with(context.pool_id, attempt)
    else:
        control.prepare.assert_not_awaited()
        control.accept.assert_not_awaited()
    control.build.assert_awaited_once_with("queue-finish", context.pool_id, attempt, phase)


@pytest.mark.description("Zero-charge resource dimensions do not block otherwise fitting CPU or GPU work.")
@pytest.mark.parametrize("gpu_work", [False, True], ids=["cpu", "gpu"])
def test_unrelated_quota_exhaustion_does_not_block_zero_charge_dimensions(gpu_work: bool) -> None:
    demand = SandboxReservationDemand(
        target_id="region-a",
        sandbox_class="sysbox",
        vcpu=0 if gpu_work else 1,
        memory=0 if gpu_work else 2,
        disk=0 if gpu_work else 3,
        gpu=1 if gpu_work else 0,
    )
    capacity = SandboxCapacity(
        cpu=ResourceCapacity(total=8, used=8 if gpu_work else 0),
        memory=ResourceCapacity(total=16, used=16 if gpu_work else 0),
        disk=ResourceCapacity(total=24, used=24 if gpu_work else 0),
        gpu=ResourceCapacity(total=1, used=0 if gpu_work else 1),
    )
    held = store.ActiveReservations(
        count=1,
        vcpu=8 if gpu_work else 0,
        memory=16 if gpu_work else 0,
        disk=24 if gpu_work else 0,
        gpu=0 if gpu_work else 1,
    )

    assert store.fits_in_capacity(demand, capacity, held)
