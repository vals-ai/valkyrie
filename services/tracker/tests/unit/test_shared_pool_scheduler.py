import asyncio
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import ResourceCapacity, SandboxCapacity, SandboxCapacityDomain, SandboxError

from tests.factories import make_benchmark
from tracker.database.models import Org
from tracker.database.session import database_call
from tracker.executor.control_queue import queue_pool_id, reserved_queue_pool_id
from tracker.local.resources import LocalResources
from tracker.scheduler import coordinator
from tracker.types import LocalBenchmarkArguments, OrgContext


@pytest.mark.description("The pool provider uses the run's saved runtime.")
@pytest.mark.parametrize("environment", ["local", "aws"])
async def test_pool_provider_uses_the_runs_persisted_runtime(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, environment: str
) -> None:
    org = Org(name="scheduler")
    benchmark = make_benchmark(org_id=org.id)
    if environment == "local":
        benchmark.arguments = LocalBenchmarkArguments(
            contract=benchmark.arguments.contract,
            concurrency=1,
            properties=LocalResources(data_root=tmp_path),
            sandbox_provider="daytona",
            sandbox_provider_secret_name="test-provider-secret",
        )
    provider = Mock(admission_pool_id="unit-provider", close=AsyncMock())
    config = Mock(create_provider=Mock(return_value=provider))
    runtime = Mock(get_sandbox_provider_config=AsyncMock(return_value=config))
    local_factory = Mock(return_value=runtime)
    cloud_factory = Mock(return_value=runtime)
    resolved = Mock()
    resolve = Mock(return_value=resolved)
    monkeypatch.setattr(coordinator.LocalRuntimeFactory, "create_runtime", local_factory)
    monkeypatch.setattr(coordinator.CloudRuntimeFactory, "create_runtime", cloud_factory)
    monkeypatch.setattr(coordinator, "resolve_run_aws_runtime", resolve)
    reference = coordinator.PoolReference(reserved_queue_pool_id(queue_pool_id("unit-provider")), benchmark, org)

    assert await coordinator.open_pool_provider(reference) is provider
    if environment == "local":
        local_factory.assert_called_once_with(
            benchmark.arguments.properties,
            org.id,
            sandbox_provider="daytona",
            sandbox_provider_secret_name="test-provider-secret",
        )
        cloud_factory.assert_not_called()
        resolve.assert_not_called()
    else:
        resolve.assert_called_once_with(
            properties=benchmark.arguments.properties,
            org=OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
        )
        cloud_factory.assert_called_once_with(
            resolved, sandbox_provider="daytona", sandbox_provider_secret_name="test-provider-secret"
        )
        local_factory.assert_not_called()
    provider.close.assert_not_awaited()


@pytest.mark.description("A provider that does not match its pool reference is closed before any capacity read.")
@pytest.mark.parametrize("provider_pool", [None, "wrong-pool"])
async def test_wrong_pool_provider_is_closed_without_reading_capacity(
    monkeypatch: pytest.MonkeyPatch, provider_pool: str | None
) -> None:
    org = Org(name="scheduler")
    benchmark = make_benchmark(org_id=org.id)
    provider = Mock(admission_pool_id=provider_pool, close=AsyncMock(), get_capacity_domains=AsyncMock())
    config = Mock(create_provider=Mock(return_value=provider))
    runtime = Mock(get_sandbox_provider_config=AsyncMock(return_value=config))
    monkeypatch.setattr(coordinator.CloudRuntimeFactory, "create_runtime", Mock(return_value=runtime))
    monkeypatch.setattr(coordinator, "resolve_run_aws_runtime", Mock())
    reference = coordinator.PoolReference(reserved_queue_pool_id(queue_pool_id("unit-provider")), benchmark, org)
    with pytest.raises(ValueError, match="persisted admission pool"):
        await coordinator.open_pool_provider(reference)
    provider.close.assert_awaited_once()
    provider.get_capacity_domains.assert_not_awaited()


@pytest.fixture
def capacity_domain() -> SandboxCapacityDomain:
    return SandboxCapacityDomain(
        target_id="region-a",
        sandbox_class="sysbox",
        capacity=SandboxCapacity(
            cpu=ResourceCapacity(total=8, used=0),
            memory=ResourceCapacity(total=16, used=0),
            disk=ResourceCapacity(total=24, used=0),
        ),
    )


@pytest.mark.description(
    "A second read within the freshness window returns the cached capacity without asking the provider."
)
async def test_pool_reuses_a_fresh_capacity_snapshot(
    monkeypatch: pytest.MonkeyPatch, capacity_domain: SandboxCapacityDomain
) -> None:
    provider = Mock(get_capacity_domains=AsyncMock(return_value=[capacity_domain]))
    cache = coordinator.CapacityCache(provider, Mock(), ttl_seconds=60)
    read_at = datetime(2026, 10, 21, tzinfo=UTC)
    monkeypatch.setattr(cache, "_database_now", Mock(return_value=read_at))
    first = await cache.get()
    assert first is not None
    assert await cache.get() is first
    assert first.read_at == read_at
    assert first.capacities == {("region-a", "sysbox"): capacity_domain.capacity}
    provider.get_capacity_domains.assert_awaited_once()


@pytest.mark.description(
    "When a refresh fails after the snapshot expires, the cache returns nothing instead of the old snapshot."
)
@pytest.mark.parametrize("unavailable", [None, SandboxError("unavailable")])
async def test_unavailable_capacity_does_not_reuse_an_expired_snapshot(
    monkeypatch: pytest.MonkeyPatch, capacity_domain: SandboxCapacityDomain, unavailable: SandboxError | None
) -> None:
    provider = Mock(get_capacity_domains=AsyncMock(side_effect=[[capacity_domain], unavailable]))
    cache = coordinator.CapacityCache(provider, Mock(), ttl_seconds=0)
    monkeypatch.setattr(cache, "_database_now", Mock(return_value=datetime(2026, 10, 21, tzinfo=UTC)))
    assert await cache.get() is not None
    assert await cache.get() is None
    assert provider.get_capacity_domains.await_count == 2


@pytest.mark.description("Repeated capacity errors end the provider's lifetime.")
async def test_persistent_capacity_errors_end_the_provider_lifetime(monkeypatch: pytest.MonkeyPatch) -> None:
    failure_limit = 2
    monkeypatch.setattr(coordinator, "_CAPACITY_FAILURE_LIMIT", failure_limit)
    error = SandboxError("expired credentials")
    provider = Mock(get_capacity_domains=AsyncMock(side_effect=error))
    cache = coordinator.CapacityCache(provider, Mock())
    monkeypatch.setattr(cache, "_database_now", Mock(return_value=datetime(2026, 10, 21, tzinfo=UTC)))

    for _ in range(failure_limit - 1):
        assert await cache.get() is None
    with pytest.raises(SandboxError) as caught:
        await cache.get()
    assert caught.value is error


@pytest.mark.description("A successful capacity read clears the count of failed reads.")
async def test_successful_capacity_refresh_resets_the_failure_budget(
    monkeypatch: pytest.MonkeyPatch, capacity_domain: SandboxCapacityDomain
) -> None:
    failure_limit = 2
    monkeypatch.setattr(coordinator, "_CAPACITY_FAILURE_LIMIT", failure_limit)
    cycle = [SandboxError("temporary outage")] * (failure_limit - 1) + [[capacity_domain]]
    provider = Mock(get_capacity_domains=AsyncMock(side_effect=cycle * 2))
    cache = coordinator.CapacityCache(provider, Mock(), ttl_seconds=0)
    monkeypatch.setattr(cache, "_database_now", Mock(return_value=datetime(2026, 10, 21, tzinfo=UTC)))

    for _ in range(2):
        for _ in range(failure_limit - 1):
            assert await cache.get() is None
        assert await cache.get() is not None


@pytest.mark.description("Cancelling the pool owner also ends its capacity read before the cancel finishes.")
async def test_cancelled_capacity_read_does_not_outlive_its_pool_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    entered = asyncio.Event()
    finished = asyncio.Event()

    async def read_capacity() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    provider = Mock(get_capacity_domains=AsyncMock(side_effect=read_capacity))
    cache = coordinator.CapacityCache(provider, Mock())
    monkeypatch.setattr(cache, "_database_now", Mock(return_value=datetime(2026, 10, 21, tzinfo=UTC)))
    task = asyncio.create_task(cache.get())
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()


@pytest.mark.description("A cancelled database call keeps its connection until the running database work ends.")
@pytest.mark.parametrize("cancellations", [1, 2])
async def test_cancellation_waits_for_database_work_before_releasing_its_connection(cancellations: int) -> None:
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def transaction() -> int:
        entered.set()
        assert release.wait(timeout=5)
        finished.set()
        return 1

    task = asyncio.create_task(database_call(transaction))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        for _ in range(cancellations):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.description(
    "An error raised in a coordinator database call reaches the caller as the same exception object."
)
async def test_database_errors_propagate_unchanged() -> None:
    error = RuntimeError("database unavailable")

    def transaction() -> None:
        raise error

    with pytest.raises(RuntimeError) as caught:
        await database_call(transaction)
    assert caught.value is error
