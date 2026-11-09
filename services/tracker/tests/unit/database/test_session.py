from pathlib import Path

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
