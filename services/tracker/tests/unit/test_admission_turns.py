import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from tracker.database.models import Task, TaskStatus
from tracker.scheduler import admission

_ATTEMPT = datetime(2026, 10, 16)


@pytest.fixture
def queued_task() -> Task:
    return Task(
        id=UUID(int=1),
        org_id=UUID(int=2),
        benchmark=UUID(int=3),
        task_id="queued",
        started_at=_ATTEMPT,
        status=TaskStatus.PENDING,
    )


class TestAdmissionSignals:
    @pytest.mark.description("An admission grant that arrives before the wait is not lost.")
    async def test_early_grant_is_not_lost(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await asyncio.wait_for(signals.wait(queued_task.id, _ATTEMPT, ready=True), timeout=5)

    @pytest.mark.description("A waiter returns `True` once its task is published as `BUILDING`.")
    async def test_pending_waiter_wakes_when_granted(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        waiter = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        signals.publish([queued_task], {})
        assert not waiter.done()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await asyncio.wait_for(waiter, timeout=5)

    @pytest.mark.description("A waiter returns `False` when its task is stopped or removed from the published list.")
    @pytest.mark.parametrize("outcome", ["removed", "restarted", "stopped"])
    async def test_invalid_attempt_finishes_without_grant(self, queued_task: Task, outcome: str) -> None:
        signals = admission.AdmissionSignals()
        waiter = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        if outcome == "restarted":
            queued_task.started_at += timedelta(seconds=1)
        elif outcome == "stopped":
            queued_task.status = TaskStatus.STOPPED
        signals.publish([] if outcome == "removed" else [queued_task], {})
        assert not await asyncio.wait_for(waiter, timeout=5)

    @pytest.mark.description(
        "A waiter for a retried attempt ignores the earlier `BUILDING` signal and wakes on the new one."
    )
    async def test_retry_forgets_previous_building_signal(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        signals.forget(queued_task.id)
        waiter = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        queued_task.status = TaskStatus.PENDING
        signals.publish([queued_task], {})
        assert not waiter.done()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await asyncio.wait_for(waiter, timeout=5)

    @pytest.mark.description("After a waiter is cancelled, its replacement waiter still receives the next grant.")
    async def test_cancelled_waiter_does_not_consume_later_grant(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        waiter = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        replacement = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await asyncio.wait_for(replacement, timeout=5)


class TestReadinessSignals:
    @pytest.mark.description("A rejected waiter needs a new pool probe.")
    async def test_rejected_waiter_requires_a_new_pool_probe(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        first = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=False))
        await asyncio.sleep(0)
        signals.publish([queued_task], {})
        assert not first.done()
        requested_at = _ATTEMPT + timedelta(seconds=1)
        signals.publish([queued_task], {queued_task.id: requested_at})
        assert await first == admission.ReadinessRequest(requested_at.replace(tzinfo=UTC))

        second = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=False))
        await asyncio.sleep(0)
        signals.publish([queued_task], {queued_task.id: requested_at})
        assert not second.done()
        next_request = requested_at + timedelta(seconds=1)
        signals.publish([queued_task], {queued_task.id: next_request})
        assert await second == admission.ReadinessRequest(next_request.replace(tzinfo=UTC))

    @pytest.mark.description("A waiter that is already ready skips readiness requests and returns `True` on the grant.")
    async def test_ready_waiter_ignores_readiness_probe(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        waiter = asyncio.create_task(signals.wait(queued_task.id, _ATTEMPT, ready=True))
        await asyncio.sleep(0)
        signals.publish([queued_task], {queued_task.id: _ATTEMPT})
        assert not waiter.done()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await waiter is True

    @pytest.mark.description(
        "A waiter that reported not ready returns `False` for a grant published before it was rejected."
    )
    async def test_rejected_waiter_does_not_accept_a_stale_grant(self, queued_task: Task) -> None:
        signals = admission.AdmissionSignals()
        queued_task.status = TaskStatus.BUILDING
        signals.publish([queued_task], {})
        assert await signals.wait(queued_task.id, _ATTEMPT, ready=False) is False
