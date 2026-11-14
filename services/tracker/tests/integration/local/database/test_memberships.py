import asyncio
from collections import Counter
from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from threading import Event, Lock
from time import monotonic

import anyio.to_thread
from descope.exceptions import AuthException, RateLimitException
import httpx
import pytest
from requests.exceptions import ReadTimeout
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlmodel import Session, create_engine

import main
import tracker.auth as auth
import tracker.database.session as session_module
from tracker.exceptions import MembershipUnavailableError
from tracker.repositories.membership import MembershipRepository
from tracker.services.memberships import MembershipCache


@pytest.fixture
def membership_engines(postgres_engine: Engine) -> Generator[list[Engine], None, None]:
    engines = [create_engine(postgres_engine.url, pool_size=4, max_overflow=4) for _ in range(8)]
    yield engines
    for engine in engines:
        engine.dispose()


@pytest.mark.description(
    "Cold membership refreshes keep health responsive with four pinned PostgreSQL connections and return them after failure."
)
async def test_cold_refresh_pool_isolation(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = postgres_engine.pool
    assert isinstance(pool, QueuePool)
    loop = asyncio.get_running_loop()
    release = Event()
    counter_lock = Lock()
    lookups_started = asyncio.Event()
    all_waiting = asyncio.Event()
    calls = 0
    attempts = 0

    def load(_user_id: str) -> dict[str, tuple[str, ...]]:
        nonlocal calls
        with counter_lock:
            calls += 1
            if calls == 4:
                loop.call_soon_threadsafe(lookups_started.set)
        assert release.wait(timeout=10)
        raise RateLimitException(429)

    async def load_roles(user_id: str) -> dict[str, tuple[str, ...]]:
        return await auth._descope_call(load, user_id)

    cache = MembershipCache(postgres_engine, load_roles)
    acquire = cache._refresh_slots.acquire

    async def acquire_slot() -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 60:
            all_waiting.set()

        return await acquire()

    checkouts: list[int] = []

    def checkout(_connection: object, _record: object, _proxy: object) -> None:
        checkouts.append(pool.checkedout())

    monkeypatch.setattr(cache._refresh_slots, "acquire", acquire_slot)
    monkeypatch.setattr(session_module, "engine", postgres_engine)
    session_module.configure_server_threads()
    with Session(postgres_engine) as session:
        assert MembershipRepository(session).get("project", "user-0") is None

    requests = [asyncio.create_task(cache.get("project", f"user-{index}")) for index in range(60)]
    try:
        await asyncio.wait_for(all_waiting.wait(), timeout=5)
        await asyncio.wait_for(lookups_started.wait(), timeout=5)
        assert calls == 4
        assert pool.checkedout() == 4
        assert all(not request.done() for request in requests)

        event.listen(postgres_engine, "checkout", checkout)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://local") as http:
                started = monotonic()
                health = await asyncio.wait_for(http.get("/health"), timeout=1)
                latency = monotonic() - started
        finally:
            event.remove(postgres_engine, "checkout", checkout)

        assert health.status_code == 200
        assert latency < 1
        assert checkouts == [5]
        assert pool.checkedout() == 4
        assert anyio.to_thread.current_default_thread_limiter().borrowed_tokens == 0
        assert calls == 4
        assert all(not request.done() for request in requests)
    finally:
        release.set()
        results = await asyncio.gather(*requests, return_exceptions=True)

    assert all(isinstance(result, MembershipUnavailableError) for result in results)
    assert calls == 60
    assert pool.checkedout() == 0


@pytest.mark.description(
    "Three hundred users share one lookup each across eight processes and refresh revoked membership after sixty seconds."
)
async def test_fleet_membership_expiry(membership_engines: list[Engine]) -> None:
    clock = [datetime.now(UTC)]
    calls: Counter[str] = Counter()
    revoked = False

    async def load(user_id: str) -> dict[str, tuple[str, ...]]:
        calls[user_id] += 1
        await asyncio.sleep(0)
        return {} if revoked else {"org": ("viewer",)}

    caches = [MembershipCache(engine, load, now=lambda: clock[0]) for engine in membership_engines]

    async def wave() -> list[dict[str, tuple[str, ...]]]:
        return await asyncio.gather(
            *(cache.get("project", f"user-{index}") for index in range(300) for cache in caches)
        )

    assert await wave() == [{"org": ("viewer",)}] * 2400
    assert calls == Counter({f"user-{index}": 1 for index in range(300)})

    revoked = True
    clock[0] += timedelta(seconds=59.9)
    assert await wave() == [{"org": ("viewer",)}] * 2400
    assert sum(calls.values()) == 300

    clock[0] += timedelta(seconds=0.1)
    assert await wave() == [{}] * 2400
    assert calls == Counter({f"user-{index}": 2 for index in range(300)})
    for engine in membership_engines:
        assert isinstance(engine.pool, QueuePool)
        assert engine.pool.checkedout() == 0


@pytest.mark.description(
    "Provider failures deny stale membership and suppress retries across eight processes for five seconds."
)
@pytest.mark.parametrize("error", [RateLimitException(429), AuthException(503), ReadTimeout()])
async def test_fleet_failure_cooldown(membership_engines: list[Engine], error: Exception) -> None:
    clock = [datetime.now(UTC)]
    calls = 0
    failing = False

    async def load(_user_id: str) -> dict[str, tuple[str, ...]]:
        nonlocal calls
        calls += 1
        if failing:
            raise error
        return {"org": ()}

    caches = [MembershipCache(engine, load, now=lambda: clock[0]) for engine in membership_engines]
    assert await caches[0].get("project", "user") == {"org": ()}
    fetched_at = clock[0]
    clock[0] += timedelta(seconds=60)
    failing = True

    async def denied() -> None:
        results = await asyncio.gather(*(cache.get("project", "user") for cache in caches), return_exceptions=True)
        assert all(isinstance(result, MembershipUnavailableError) for result in results)

    await denied()
    assert calls == 2
    with Session(membership_engines[0]) as session:
        snapshot = MembershipRepository(session).get("project", "user")
        assert snapshot is not None
        assert snapshot.fetched_at == fetched_at
        assert snapshot.tenant_roles == {"org": ()}
        assert snapshot.cooldown_until == clock[0] + timedelta(seconds=5)

    clock[0] += timedelta(seconds=4.999)
    await denied()
    assert calls == 2

    clock[0] += timedelta(seconds=0.001)
    failing = False
    assert await asyncio.gather(*(cache.get("project", "user") for cache in caches)) == [{"org": ()}] * 8
    assert calls == 3


@pytest.mark.description(
    "Empty memberships are cached, isolated by project and fail closed when the database is unavailable."
)
async def test_empty_membership_and_database_failure(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def load(_user_id: str) -> dict[str, tuple[str, ...]]:
        nonlocal calls
        calls += 1
        return {}

    cache = MembershipCache(postgres_engine, load)
    assert await cache.get("first", "user") == {}
    assert await cache.get("first", "user") == {}
    assert await cache.get("second", "user") == {}
    assert calls == 2

    def unavailable(_project_id: str, _user_id: str, _connection: object = None) -> None:
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr(cache, "_read", unavailable)
    with pytest.raises(MembershipUnavailableError):
        await cache.get("first", "unknown-user")
    assert calls == 2


@pytest.mark.description(
    "Invalidation removes revoked membership from warmed workers and fences an older in-flight refresh."
)
@pytest.mark.parametrize("refreshing", [False, True])
async def test_fleet_invalidation(
    membership_engines: list[Engine], wait_for_database_lock: Callable[[], None], refreshing: bool
) -> None:
    calls = 0
    revoked = False
    started = asyncio.Event()
    release = asyncio.Event()

    async def load(_user_id: str) -> dict[str, tuple[str, ...]]:
        nonlocal calls
        calls += 1
        roles = {} if revoked else {"org": ()}
        if refreshing and calls == 1:
            started.set()
            await release.wait()
        return roles

    caches = [MembershipCache(engine, load) for engine in membership_engines]
    if refreshing:
        reader = asyncio.create_task(caches[0].get("project", "user"))
        await started.wait()
        revoked = True
        invalidation = asyncio.create_task(asyncio.to_thread(caches[1].invalidate, "project", "user"))
        try:
            await asyncio.to_thread(wait_for_database_lock)
        finally:
            release.set()
        await invalidation
        await reader
    else:
        assert await asyncio.gather(*(cache.get("project", "user") for cache in caches)) == [{"org": ()}] * 8
        revoked = True
        await asyncio.to_thread(caches[0].invalidate, "project", "user")

    assert await asyncio.gather(*(cache.get("project", "user") for cache in caches)) == [{}] * 8
    assert calls == 2
