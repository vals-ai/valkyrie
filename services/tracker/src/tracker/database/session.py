import asyncio
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import anyio.to_thread
from sqlalchemy.pool import NullPool
from sqlmodel import Session, SQLModel, create_engine, select

from tracker.config import build_database_url
from tracker.database.models import Benchmark, EvaluationResult, Org, Task
from tracker.logging import get_logger

logger = get_logger(__name__)

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Org, Task]

engine = create_engine(build_database_url(), poolclass=NullPool)
SERVER_POOL_SIZE = 25
SERVER_POOL_OVERFLOW = 25
SERVER_THREADS = SERVER_POOL_SIZE + SERVER_POOL_OVERFLOW
SERVER_POOL_RECYCLE_SECONDS = 1800
server_engine = create_engine(
    build_database_url(),
    pool_size=SERVER_POOL_SIZE,
    max_overflow=SERVER_POOL_OVERFLOW,
    pool_pre_ping=True,
    pool_recycle=SERVER_POOL_RECYCLE_SECONDS,
)
_Result = TypeVar("_Result")


def configure_server_threads() -> None:
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=SERVER_THREADS, thread_name_prefix="tracker-database")
    )
    anyio.to_thread.current_default_thread_limiter().total_tokens = SERVER_THREADS


def get_session() -> Generator[Session, Any, None]:
    with Session(server_engine, expire_on_commit=False) as session:
        yield session


async def database_call(work: Callable[[], _Result]) -> _Result:
    task = asyncio.create_task(asyncio.to_thread(work))
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
        except Exception:
            break
    result = task.result()
    if cancellation is not None:
        raise cancellation
    return result


def check_database_connection() -> bool:
    try:
        with Session(engine) as session:
            session.exec(select(1))
        return True
    except Exception:
        logger.warning("Database health check failed", exc_info=True)
        return False


if __name__ == "__main__":
    SQLModel.metadata.create_all(bind=engine)
