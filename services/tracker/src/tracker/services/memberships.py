import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.database.session import database_call
from tracker.exceptions import MembershipUnavailableError
from tracker.logging import get_logger
from tracker.repositories.advisory_lock import PostgresAdvisoryLock
from tracker.repositories.membership import MembershipRepository, MembershipSnapshot, membership_lock_resource_id

logger = get_logger(__name__)
MEMBERSHIP_TTL_SECONDS = 60.0
_COOLDOWN_SECONDS = 5.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MembershipCache:
    def __init__(
        self,
        engine: Engine,
        load_roles: Callable[[str], Awaitable[dict[str, tuple[str, ...]]]],
        *,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._engine = engine
        self._load_roles = load_roles
        self._now = now
        self._pending: dict[tuple[str, str], asyncio.Task[MembershipSnapshot]] = {}
        self._refresh_slots = asyncio.Semaphore(4)

    def invalidate(self, project_id: str, user_id: str) -> None:
        with Session(self._engine) as session:
            MembershipRepository(session).invalidate(project_id, user_id)

    def _roles(self, snapshot: MembershipSnapshot | None) -> dict[str, tuple[str, ...]] | None:
        if snapshot is None:
            return None
        now = self._now()
        if (
            snapshot.tenant_roles is not None
            and snapshot.fetched_at is not None
            and snapshot.fetched_at + timedelta(seconds=MEMBERSHIP_TTL_SECONDS) > now
        ):
            return snapshot.tenant_roles
        if snapshot.cooldown_until is not None and snapshot.cooldown_until > now:
            raise MembershipUnavailableError("Auth service unavailable")
        return None

    async def get(self, project_id: str, user_id: str) -> dict[str, tuple[str, ...]]:
        key = (project_id, user_id)
        try:
            while True:
                snapshot = await database_call(lambda: self._read(project_id, user_id))
                if self._ready(snapshot):
                    assert snapshot is not None
                    roles = self._roles(snapshot)
                    assert roles is not None
                    return roles
                task = self._pending.get(key)
                if task is None:
                    task = asyncio.create_task(self._load_shared(project_id, user_id))
                    self._pending[key] = task
                    task.add_done_callback(lambda completed: self._finished(key, completed))
                await asyncio.shield(task)
        except Exception as error:
            raise MembershipUnavailableError("Auth service unavailable") from error

    def _ready(self, snapshot: MembershipSnapshot | None) -> bool:
        try:
            return self._roles(snapshot) is not None
        except MembershipUnavailableError:
            return True

    def _finished(self, key: tuple[str, str], task: asyncio.Task[MembershipSnapshot]) -> None:
        if self._pending.get(key) is task:
            self._pending.pop(key)
        if not task.cancelled():
            task.exception()

    def _read(self, project_id: str, user_id: str, connection: Connection | None = None) -> MembershipSnapshot | None:
        with Session(connection if connection is not None else self._engine) as session:
            return MembershipRepository(session).get(project_id, user_id)

    def _save(self, project_id: str, user_id: str, snapshot: MembershipSnapshot, connection: Connection) -> None:
        with Session(connection) as session:
            MembershipRepository(session).save(project_id, user_id, snapshot)

    async def _load_shared(self, project_id: str, user_id: str) -> MembershipSnapshot:
        while True:
            snapshot = await database_call(lambda: self._read(project_id, user_id))
            if self._ready(snapshot):
                assert snapshot is not None
                return snapshot
            async with self._refresh_slots:
                lock = PostgresAdvisoryLock(self._engine, resource_id=membership_lock_resource_id(project_id, user_id))
                async with lock as acquired:
                    if acquired:
                        snapshot = await database_call(lambda: self._read(project_id, user_id, lock.connection))
                        if self._ready(snapshot):
                            assert snapshot is not None
                            return snapshot
                        try:
                            async with asyncio.timeout(15):
                                roles = await self._load_roles(user_id)
                        except Exception as error:
                            logger.warning("Descope membership lookup failed: %s", type(error).__name__)
                            snapshot = replace(
                                snapshot or MembershipSnapshot(None, None),
                                cooldown_until=self._now() + timedelta(seconds=_COOLDOWN_SECONDS),
                            )
                        else:
                            snapshot = MembershipSnapshot(roles, self._now())
                        await database_call(lambda: self._save(project_id, user_id, snapshot, lock.connection))
                        return snapshot
            await asyncio.sleep(0.05)
