import asyncio
from hashlib import sha256
from types import TracebackType
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

_TASK_EVALUATION_LOCK_SCOPE = "task-evaluation"
_TASK_BUILD_LOCK_SCOPE = "task-build"
_SANDBOX_COST_SWEEP_LOCK_RESOURCE_ID = "sandbox-cost-sweep"


def advisory_lock_key(resource_id: str) -> int:
    return int.from_bytes(
        sha256(resource_id.encode()).digest()[:8],
        byteorder="big",
        signed=True,
    )


def task_evaluation_lock_resource_id(task_row_id: UUID) -> str:
    return f"{_TASK_EVALUATION_LOCK_SCOPE}:{task_row_id}"


def task_build_lock_resource_id(task_row_id: UUID) -> str:
    return f"{_TASK_BUILD_LOCK_SCOPE}:{task_row_id}"


class PostgresAdvisoryLock:
    def __init__(self, engine: Engine, *, resource_id: str) -> None:
        self._engine = engine
        self._lock_key = advisory_lock_key(resource_id)
        self._connection: Connection | None = None

    @property
    def held(self) -> bool:
        return self._connection is not None

    @property
    def connection(self) -> Connection:
        if self._connection is None:
            raise RuntimeError("PostgreSQL advisory lock is not held")
        return self._connection

    async def __aenter__(self) -> bool:
        return await self.acquire()

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.release()

    async def acquire(self) -> bool:
        acquire_task = asyncio.create_task(asyncio.to_thread(self._try_acquire))
        try:
            return await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            acquired = await acquire_task
            if acquired:
                await asyncio.to_thread(self._release)
            raise

    async def still_held(self) -> bool:
        if self._connection is None:
            return False
        return await asyncio.to_thread(self._check_connection)

    async def release(self) -> None:
        if self._connection is None:
            return

        release_task = asyncio.create_task(asyncio.to_thread(self._release))
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await release_task
            raise

    def _try_acquire(self) -> bool:
        connection = self._engine.connect()
        try:
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            connection.commit()
            if acquired:
                self._connection = connection

                return True
        except BaseException:
            connection.close()
            raise

        connection.close()

        return False

    def _check_connection(self) -> bool:
        connection = self.connection
        try:
            connection.execute(text("SELECT 1"))
            connection.commit()
        except Exception:
            self._connection = None
            connection.invalidate()
            connection.close()
            return False
        return True

    def _release(self) -> None:
        connection = self.connection
        self._connection = None
        try:
            unlocked = bool(
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": self._lock_key},
                ).scalar_one()
            )
            if not unlocked:
                raise RuntimeError("PostgreSQL advisory lock ownership was lost before release")
            connection.commit()
        except BaseException:
            connection.invalidate()
            raise
        finally:
            connection.close()


def task_evaluation_lock(engine: Engine, task_row_id: UUID) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=task_evaluation_lock_resource_id(task_row_id))


def sandbox_cost_sweep_lock(engine: Engine) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=_SANDBOX_COST_SWEEP_LOCK_RESOURCE_ID)


def task_build_lock(engine: Engine, task_row_id: UUID) -> PostgresAdvisoryLock:
    return PostgresAdvisoryLock(engine, resource_id=task_build_lock_resource_id(task_row_id))
