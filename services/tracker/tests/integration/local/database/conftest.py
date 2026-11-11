from collections.abc import Callable, Generator
from time import monotonic

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from tracker.database.models import ExecutorAdmission


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def postgres_engine(postgres_container: PostgresContainer) -> Generator[Engine, None, None]:
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(ExecutorAdmission())
        session.commit()

    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def postgres_session(postgres_engine: Engine) -> Generator[Session, None, None]:
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture
def wait_for_database_lock(postgres_engine: Engine) -> Callable[[], None]:
    def wait() -> None:
        deadline = monotonic() + 5
        with Session(postgres_engine) as observer:
            while monotonic() < deadline:
                blocked = (
                    observer.connection()
                    .execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                            "AND pid <> pg_backend_pid())"
                        )
                    )
                    .scalar_one()
                )
                if blocked:
                    return
                observer.rollback()
        raise AssertionError("the second transaction never waited on a PostgreSQL lock")

    return wait
