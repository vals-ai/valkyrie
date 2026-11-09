import asyncio
from collections.abc import Callable, Generator
from typing import Any, TypeVar

from sqlalchemy.pool import NullPool
from sqlmodel import Session, SQLModel, create_engine, select

from tracker.config import DATABASE_URL
from tracker.database.models import Benchmark, EvaluationResult, Org, Task
from tracker.logging import get_logger

logger = get_logger(__name__)

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Org, Task]

engine = create_engine(DATABASE_URL, poolclass=NullPool)
_Result = TypeVar("_Result")


def get_session() -> Generator[Session, Any, None]:
    with Session(engine, expire_on_commit=False) as session:
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
