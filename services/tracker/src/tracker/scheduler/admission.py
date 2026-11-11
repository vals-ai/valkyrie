from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, timedelta

from benchmark_service import (
    Resources,
    Sandbox,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxSource,
)
from tracker.executor.control_protocol import AttemptRef, PhaseRef
from tracker.executor.control_queue import (
    QueueControl,
    QueueDemand,
    is_reserved_queue_pool_id,
    queue_pool_id,
    queue_pool_lock_id,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.logging import get_logger
from tracker.runtime.lifecycle import finish_cleanup
from tracker.sandbox import SandboxCreationState, delete_sandbox


logger = get_logger(__name__)
SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]
_ORPHAN_CLEANUP_GRACE = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    provider: SandboxProvider = field(repr=False)
    pool_id: str
    control: QueueControl = field(repr=False)
    preparation_slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(10), repr=False)
    poll_interval_seconds: float = 1.0

    def serves_queue(self, pool_id: str) -> bool:
        return queue_pool_lock_id(pool_id) == queue_pool_lock_id(self.pool_id)

    def for_queue(self, pool_id: str) -> SandboxQueueContext:
        return replace(self, pool_id=pool_id)


def create_queue_context(
    *, control: QueueControl, provider: SandboxProvider, poll_interval_seconds: float = 1.0
) -> SandboxQueueContext:
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")
    pool_id = queue_pool_id(provider_pool_id)
    return SandboxQueueContext(
        provider=provider,
        pool_id=pool_id,
        control=control,
        poll_interval_seconds=poll_interval_seconds,
    )


async def _cleanup_orphan_sandbox(context: SandboxQueueContext, name: str) -> bool:
    try:
        sandbox = await context.provider.get_sandbox(name)
        if sandbox.name != name:
            logger.error("sandbox.admission.recovery_identity_mismatch", extra={"sandbox_name": name})
            return False
        return await delete_sandbox(sandbox, context.provider, initiated_by="orphan_cleanup")
    except SandboxNotFoundError:
        return True
    except SandboxError:
        logger.warning("sandbox.admission.recovery_failed", extra={"sandbox_name": name}, exc_info=True)
        return False


async def recover_queued_pool(context: SandboxQueueContext) -> None:
    response = await context.control.orphans(context.pool_id)
    for orphan in response.orphans:
        cleaned = await _cleanup_orphan_sandbox(context, orphan.sandbox_name)
        if (
            not cleaned
            and orphan.reserved_at.replace(tzinfo=UTC)
            <= response.server_now.replace(tzinfo=UTC) - _ORPHAN_CLEANUP_GRACE
        ):
            logger.error(
                "sandbox.admission.reservation_leaked",
                extra={"sandbox_name": orphan.sandbox_name, "reserved_at": orphan.reserved_at.isoformat()},
            )
        await context.control.reconcile(context.pool_id, orphan.attempt, orphan.phase, cleaned)


async def _close_stack_before_cancellation(stack: AsyncExitStack) -> None:
    await finish_cleanup(asyncio.create_task(stack.aclose()))


async def _poll_sleep(context: SandboxQueueContext) -> None:
    await asyncio.sleep(context.poll_interval_seconds * (0.5 + random.random()))


async def _release_failed_build(
    context: SandboxQueueContext,
    attempt: AttemptRef,
    phase: PhaseRef,
    creation_state: SandboxCreationState,
    on_phase_released: Callable[[bool], None],
    on_phase_abandon: Callable[[PhaseRef], None],
) -> None:
    if creation_state.cleanup_pending:
        if is_reserved_queue_pool_id(context.pool_id):
            on_phase_abandon(phase)
        return
    await finish_cleanup(asyncio.create_task(context.control.build("queue-abort", context.pool_id, attempt, phase)))
    on_phase_released(False)


async def _start_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    attempt: AttemptRef,
    phase: PhaseRef,
    creation_state: SandboxCreationState,
    create: SandboxFactory,
    on_phase_released: Callable[[bool], None],
    on_phase_abandon: Callable[[PhaseRef], None],
) -> Sandbox | None:

    try:
        sandbox = await stack.enter_async_context(create())
    except BaseException:
        await _release_failed_build(context, attempt, phase, creation_state, on_phase_released, on_phase_abandon)
        raise
    try:
        started = (await context.control.build("queue-finish", context.pool_id, attempt, phase)).started
    except BaseException:
        try:
            await _close_stack_before_cancellation(stack)
        finally:
            await _release_failed_build(context, attempt, phase, creation_state, on_phase_released, on_phase_abandon)
        raise
    if not started:
        try:
            await _close_stack_before_cancellation(stack)
        finally:
            await _release_failed_build(context, attempt, phase, creation_state, on_phase_released, on_phase_abandon)
        return None
    on_phase_released(True)
    return sandbox


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    attempt: AttemptRef,
    source: SandboxSource,
    resources: Resources,
    creation_state: SandboxCreationState,
    create: SandboxFactory,
    preparation_stack: AsyncExitStack,
    on_phase_claimed: Callable[[PhaseRef], None],
    on_phase_released: Callable[[bool], None],
    on_phase_abandon: Callable[[PhaseRef], None],
) -> Sandbox | None:
    if is_reserved_queue_pool_id(context.pool_id):
        try:
            state = await context.control.state(context.pool_id, attempt)
        except ExecutionAuthorityRevoked:
            return None
        if state.status != "PENDING":
            return None
        provider_demand = await context.provider.reservable_demand(source, resources)
        demand = QueueDemand(
            target_id=provider_demand.target_id,
            sandbox_class=provider_demand.sandbox_class,
            vcpu=provider_demand.vcpu,
            memory=provider_demand.memory,
            disk=provider_demand.disk,
            gpu=provider_demand.gpu,
        )
        prepared = False
        try:
            admission_ready = await context.provider.check_admission(source, resources)
            await context.control.prepare(context.pool_id, attempt, demand, admission_ready)
            prepared = True
            await preparation_stack.aclose()
            last_request = None
            while True:
                state = await context.control.state(context.pool_id, attempt)
                if state.status == "BUILDING":
                    phase = await context.control.accept(context.pool_id, attempt)
                    if phase is None:
                        return None
                    on_phase_claimed(phase)
                    return await _start_sandbox(
                        stack=stack,
                        context=context,
                        attempt=attempt,
                        phase=phase,
                        creation_state=creation_state,
                        create=create,
                        on_phase_released=on_phase_released,
                        on_phase_abandon=on_phase_abandon,
                    )
                if state.status != "PENDING":
                    return None
                request = state.readiness_requested_at
                if not admission_ready and request is not None and request != last_request:
                    last_request = request
                    async with context.preparation_slots:
                        if await context.control.begin_readiness(context.pool_id, attempt, request):
                            admission_ready = await context.provider.check_admission(source, resources)
                            await context.control.prepare(context.pool_id, attempt, demand, admission_ready)
                await _poll_sleep(context)
        except ExecutionAuthorityRevoked:
            return None
        finally:
            if prepared:
                await context.control.clear(context.pool_id, attempt)

    while True:
        eligible = await context.control.eligibility(context.pool_id, attempt)
        if eligible is None:
            return None
        if not eligible:
            await _poll_sleep(context)
            continue
        if not await context.provider.check_admission(source, resources):
            await _poll_sleep(context)
            continue
        response = await context.control.claim(context.pool_id, attempt)
        if response.phase is not None:
            on_phase_claimed(response.phase)
            return await _start_sandbox(
                stack=stack,
                context=context,
                attempt=attempt,
                phase=response.phase,
                creation_state=creation_state,
                create=create,
                on_phase_released=on_phase_released,
                on_phase_abandon=on_phase_abandon,
            )
        if not response.waiting:
            return None
        await _poll_sleep(context)
