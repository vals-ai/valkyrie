import asyncio
import threading
from pathlib import Path

import anyio.to_thread
import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import create_engine

import tracker.database.session as session_module


@pytest.mark.description("An unreachable database fails the health check and logs the connection error.")
def test_check_database_connection_logs_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    unreachable = create_engine(f"sqlite:///{tmp_path / 'missing' / 'tracker.db'}", poolclass=NullPool)
    monkeypatch.setattr(session_module, "engine", unreachable)
    monkeypatch.setattr(session_module.logger, "handlers", [caplog.handler])

    connected = session_module.check_database_connection()

    assert connected is False

    record = next(record for record in caplog.records if record.getMessage() == "Database health check failed")
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], OperationalError)


@pytest.mark.description(
    "A Tracker server runs as many database calls and sync dependencies at once as its connection pool can serve."
)
async def test_configure_server_threads_matches_the_connection_pool() -> None:
    session_module.configure_server_threads()
    database_calls = threading.Barrier(session_module.SERVER_THREADS, timeout=5)
    dependencies = threading.Barrier(session_module.SERVER_THREADS, timeout=5)

    await asyncio.gather(
        *(session_module.database_call(database_calls.wait) for _ in range(session_module.SERVER_THREADS))
    )
    await asyncio.gather(*(anyio.to_thread.run_sync(dependencies.wait) for _ in range(session_module.SERVER_THREADS)))
