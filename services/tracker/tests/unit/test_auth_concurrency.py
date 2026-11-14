import asyncio
from collections.abc import Generator
from pathlib import Path
import time
from threading import Event, Lock
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import anyio.to_thread
from descope.exceptions import RateLimitException
from fastapi import Depends, FastAPI
import httpx
import pytest
from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine, select

import main
import tracker.auth as auth
from tests.utils import add_sqlite_btrim
from tracker.database.models import Org
from tracker.database.session import SERVER_THREADS, configure_server_threads, get_session


@pytest.mark.asyncio
@pytest.mark.parametrize("credential_kind", ["same-user", "distinct-users", "bound-key"])
@pytest.mark.description(
    "Rate-limited starts leave health checks responsive without occupying worker threads or DB connections."
)
async def test_rate_limited_starts_leave_resources_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, credential_kind: str
) -> None:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'auth.sqlite'}",
        pool_size=25,
        max_overflow=25,
        connect_args={"check_same_thread": False},
    )
    event.listen(engine, "connect", add_sqlite_btrim)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Org(name="probe", storage_key="probe-bucket"))
        session.commit()

    def sessions() -> Generator[Session, None, None]:
        with Session(engine, expire_on_commit=False) as session:
            yield session

    def healthy() -> bool:
        with Session(engine) as session:
            return session.exec(select(1)).one() == 1

    client = MagicMock()
    lookup = client.mgmt.user.load_by_user_id
    lookup.side_effect = RateLimitException(429, rate_limit_parameters={"Retry-After": 20})

    def rate_limited(_user_id: str) -> None:
        raise RateLimitException(429)

    owner_requests = 0
    owners_waiting = asyncio.Event()
    membership = credential_kind == "bound-key"
    cached_owner = auth._live_user if membership else auth._cached_run_owner_user
    key_token = f"local-key-{uuid4()}"
    client.exchange_access_key.return_value = {
        "tenants": {"probe": {}},
        "keyId": "K-local",
        "sessionToken": {
            "exp": time.time() + 60,
            "customClaims": {"user_id": "U-local", "valkyrie_user_key": True},
        },
    }

    async def owner(user_id: str) -> dict[str, Any] | None:
        nonlocal owner_requests
        owner_requests += 1
        if owner_requests == SERVER_THREADS:
            owners_waiting.set()
        return await cached_owner(user_id)

    monkeypatch.setattr(auth, "AUTH_REQUIRED", True)
    monkeypatch.setattr(auth, "PASSKEY_ORGS", frozenset())
    monkeypatch.setattr(auth, "DESCOPE_MANAGEMENT_KEY", "local-fake")
    monkeypatch.setattr(auth, "_descope_client", client)
    monkeypatch.setattr(auth, "_descope_user_client", client)
    monkeypatch.setattr(auth, "_live_user" if membership else "_cached_run_owner_user", owner)
    monkeypatch.setattr(auth, "_validate_bearer", lambda token: {"sub": token, "tenants": {"probe": {}}})
    monkeypatch.setattr(auth.random, "uniform", lambda _low, _high: 0.5)
    monkeypatch.setattr(main, "check_database_connection", healthy)
    monkeypatch.setattr(main.app, "middleware_stack", None)
    monkeypatch.setitem(main.app.dependency_overrides, get_session, sessions)
    monkeypatch.delitem(main.app.dependency_overrides, auth.get_current_runner, raising=False)
    configure_server_threads()

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://local") as http:
            await http.get("/health")
            warmup = await http.post(
                f"/retry-or-resume-benchmark/{uuid4()}",
                headers={"x-api-key": key_token} if membership else {"Authorization": "Bearer warmup"},
                json={},
            )
            assert warmup.status_code == 503
            lookup.side_effect = rate_limited
            lookup.reset_mock()
            owner_requests = 0
            owners_waiting.clear()
            tasks = [
                asyncio.create_task(
                    http.post(
                        f"/retry-or-resume-benchmark/{uuid4()}",
                        headers={"x-api-key": key_token}
                        if membership
                        else {"Authorization": f"Bearer U-{index if credential_kind == 'distinct-users' else 0}"},
                        json={},
                    )
                )
                for index in range(60)
            ]
            try:
                await asyncio.sleep(0.25)
                started = time.monotonic()
                health = await http.get("/health")
                latency = time.monotonic() - started
                await asyncio.wait_for(owners_waiting.wait(), timeout=5)
                tokens = anyio.to_thread.current_default_thread_limiter().borrowed_tokens
                connections = engine.pool.checkedout()
                credential_available = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(auth._credential_executor, lambda: True), timeout=0.5
                )
                pending = any(not task.done() for task in tasks)
                responses = await asyncio.gather(*tasks)
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)

        assert pending
        assert lookup.call_count > 0
        assert tokens == 0
        assert connections == 0
        assert credential_available
        assert health.status_code == 200
        assert latency < 1.0
        assert [response.status_code for response in responses] == [503] * 60
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.description("Cancelling one owner lookup leaves the shared lookup available to another caller.")
async def test_cancelled_owner_waiter_keeps_shared_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    started, release = Event(), Event()

    def load(_user_id: str) -> dict[str, object]:
        started.set()
        assert release.wait(5)
        return {"user": {"name": "Alice"}}

    client = MagicMock()
    client.mgmt.user.load_by_user_id.side_effect = load
    monkeypatch.setattr(auth, "_descope_user_client", client)
    first = asyncio.create_task(auth._cached_run_owner_user("U-alice"))
    second = asyncio.create_task(auth._cached_run_owner_user("U-alice"))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
    finally:
        release.set()

    assert await second == {"name": "Alice"}
    client.mgmt.user.load_by_user_id.assert_called_once_with("U-alice")


@pytest.mark.asyncio
@pytest.mark.description("Concurrent user lookups never exceed the dedicated executor limit.")
async def test_user_lookup_concurrency_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    saturated, release = Event(), Event()
    lock = Lock()
    active = 0
    peak = 0

    def load(_user_id: str) -> dict[str, object]:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == auth._DESCOPE_CONCURRENCY:
                saturated.set()
        try:
            assert release.wait(5)
            return {"user": {}}
        finally:
            with lock:
                active -= 1

    client = MagicMock()
    client.mgmt.user.load_by_user_id.side_effect = load
    monkeypatch.setattr(auth, "_descope_user_client", client)
    tasks = [asyncio.create_task(auth._live_user(f"U-{index}")) for index in range(16)]
    try:
        assert await asyncio.to_thread(saturated.wait, 5)
        assert client.mgmt.user.load_by_user_id.call_count == auth._DESCOPE_CONCURRENCY
    finally:
        release.set()
        results = await asyncio.gather(*tasks)

    assert peak == auth._DESCOPE_CONCURRENCY
    assert results == [{}] * 16


@pytest.mark.asyncio
@pytest.mark.description(
    "Runner and organization authorization reuse credentials without another lookup while a DB connection is held."
)
async def test_runner_and_identity_share_credentials(
    database_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = FastAPI()

    @app.get("/identity", description="Return the authorized run and organization identity.")
    async def identity(
        runner: auth.RequestIdentity = Depends(auth.get_current_runner),
        caller: auth.RequestIdentity = Depends(auth.get_current_identity),
    ) -> dict[str, bool]:
        return {"same_org": runner.org.id == caller.org.id}

    def sessions() -> Generator[Session, None, None]:
        with Session(database_session.get_bind(), expire_on_commit=False) as session:
            yield session

    client = MagicMock()
    client.mgmt.user.load_by_user_id.return_value = {"user": {}}
    validate = MagicMock(return_value={"sub": "U-alice", "tenants": {"default": {}}})
    monkeypatch.setattr(auth, "AUTH_REQUIRED", True)
    monkeypatch.setattr(auth, "PASSKEY_ORGS", frozenset())
    monkeypatch.setattr(auth, "DESCOPE_MANAGEMENT_KEY", "local-fake")
    monkeypatch.setattr(auth, "_descope_client", client)
    monkeypatch.setattr(auth, "_descope_user_client", client)
    monkeypatch.setattr(auth, "_validate_bearer", validate)
    app.dependency_overrides[get_session] = sessions

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as http:
        response = await http.get("/identity", headers={"Authorization": "Bearer local-fake"})

    assert response.status_code == 200, response.text
    assert response.json() == {"same_org": True}
    validate.assert_called_once_with("local-fake")
    client.mgmt.user.load_by_user_id.assert_called_once_with("U-alice")
