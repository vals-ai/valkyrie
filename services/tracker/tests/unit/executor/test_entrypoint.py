import asyncio
import sys
import time
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from tracker.config import ENVIRONMENT
from tracker.executor import entrypoint
from tracker.executor.control_protocol import ClaimDispatchResponse, DispatchRef


@pytest.mark.description("The executor entrypoint runs the dispatch and flushes telemetry.")
def test_main_runs_one_dispatch_and_flushes(monkeypatch: pytest.MonkeyPatch) -> None:
    dispatch_id = uuid4()
    run_dispatch = AsyncMock()
    configure = Mock()
    flush = Mock()
    channel = Mock()
    monkeypatch.setattr(entrypoint, "_run_dispatch", run_dispatch)
    monkeypatch.setattr(entrypoint, "DeadlineChannel", lambda _fd: channel)
    monkeypatch.setattr(entrypoint, "configure_logging", Mock())
    monkeypatch.setattr(entrypoint, "configure_observability", configure)
    monkeypatch.setattr(entrypoint, "_flush_observability", flush)
    monkeypatch.setenv("TRACKER_WATCHDOG_FD", "123")
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", "--dispatch-id", str(dispatch_id)])

    entrypoint.main()

    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    shutdown_started = run_dispatch.await_args.args[2]
    assert isinstance(shutdown_started, asyncio.Event)
    run_dispatch.assert_awaited_once_with(dispatch_id, channel, shutdown_started)
    channel.close.assert_called_once()
    flush.assert_called_once()


@pytest.mark.description("A dispatch that cannot be claimed runs nothing.")
@pytest.mark.asyncio
async def test_unclaimable_dispatch_does_not_start_business_work(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock()
    client.send = AsyncMock(return_value=ClaimDispatchResponse(claimed=False, payload=None))
    client.close = AsyncMock()
    benchmark = AsyncMock()
    monkeypatch.setattr(entrypoint, "ControlClient", lambda **_kwargs: client)
    monkeypatch.setattr(entrypoint, "process_benchmark", benchmark)
    monkeypatch.setenv("EXECUTOR_CONTROL_URL", "http://control.test")
    monkeypatch.setenv("EXECUTOR_BOOTSTRAP_KEY", "test-bootstrap")

    await entrypoint._run_dispatch(uuid4(), Mock(), asyncio.Event())

    benchmark.assert_not_awaited()
    client.close.assert_awaited_once()


@pytest.mark.description("Work is cancelled when lease renewal is blocked past the lease, without finishing.")
@pytest.mark.asyncio
async def test_local_lease_cancels_work_when_renewal_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    renewal_started = asyncio.Event()
    release_renewal = asyncio.Event()
    work_cancelled = asyncio.Event()
    ref = DispatchRef(benchmark_id=uuid4(), dispatch_id=uuid4())

    async def blocked_renewal(*_args: object) -> object:
        renewal_started.set()
        await release_renewal.wait()
        return Mock(renewed=True)

    client = Mock(call=blocked_renewal)
    monkeypatch.setattr(entrypoint, "DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS", 0.12)
    monkeypatch.setattr(entrypoint, "DEFAULT_EXECUTOR_DISPATCH_LEASE_TICK_SECONDS", 0.02)
    lease = entrypoint.LocalLease(client, ref, time.monotonic())

    async def benchmark() -> None:
        try:
            await asyncio.Future()
        finally:
            work_cancelled.set()

    work = asyncio.create_task(benchmark())
    renewal = asyncio.create_task(lease.renew())
    try:
        await asyncio.wait_for(renewal_started.wait(), timeout=1)
        await asyncio.wait_for(lease.lost.wait(), timeout=1)
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        assert work_cancelled.is_set()
    finally:
        release_renewal.set()
        renewal.cancel()
        await asyncio.gather(renewal, return_exceptions=True)
        lease.close()
