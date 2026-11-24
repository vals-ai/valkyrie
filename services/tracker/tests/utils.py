from collections.abc import AsyncIterator, Callable, Generator, Iterable
from contextlib import contextmanager
from typing import Any, TypeVar
from uuid import UUID, uuid4

import pytest
from descope.exceptions import AuthException
from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url
from sqlmodel import SQLModel, create_engine

_Item = TypeVar("_Item")


TEST_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_SCHEMA_TEMPLATE = "tracker_schema"


def create_schema_template(url: str) -> None:
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {_SCHEMA_TEMPLATE}"))
    admin.dispose()
    template = create_engine(make_url(url).set(database=_SCHEMA_TEMPLATE))
    SQLModel.metadata.create_all(template)
    template.dispose()


@contextmanager
def database_from_schema_template(url: str) -> Generator[Engine, None, None]:
    name = f"test_{uuid4().hex}"
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {name} TEMPLATE {_SCHEMA_TEMPLATE} STRATEGY FILE_COPY"))
    engine = create_engine(make_url(url).set(database=name))
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f"DROP DATABASE {name} WITH (FORCE)"))
        admin.dispose()


def inline_database_calls_in(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run_inline(work: Callable[[], Any]) -> Any:
        return work()

    for module in (
        "main",
        "tracker.api.single_task",
        "tracker.modal_cost",
        "tracker.sandbox_cost",
        "tracker.scheduler.coordinator",
        "tracker.services.results",
        "tracker.services.runs.stop",
        "tracker.services.sandboxes",
    ):
        monkeypatch.setattr(f"{module}.database_call", run_inline)


def random_task_id() -> str:
    return f"test-task-{uuid4().hex[:5]}"


async def unexpected_sandbox_record(_link: object, _sandbox_id: str) -> None:
    raise AssertionError("Sandbox without a link must not request persistence")


async def async_iterator(items: Iterable[_Item]) -> AsyncIterator[_Item]:
    for item in items:
        yield item


class FakeAccessKeys:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.missing: set[str] = set()
        self.failing: set[str] = set()
        self.fail_create = False

    def create(self, name: str, expire_time: int = 0, **kwargs: Any) -> dict[str, Any]:
        if self.fail_create:
            raise RuntimeError("Descope is unavailable")
        key_id = f"K{len(self.created) + 1}"
        self.created.append({"id": key_id, "name": name, "expire_time": expire_time, **kwargs})
        return {"key": {"id": key_id}, "cleartext": f"run-secret-{key_id}"}

    def delete(self, id: str) -> None:
        if id in self.failing:
            raise RuntimeError("Descope is unavailable")
        if id in self.missing:
            raise AuthException(404, "E", "not found")
        self.deleted.append(id)


def _btrim(value: str | None) -> str | None:
    return None if value is None else value.strip(" ")


def add_sqlite_btrim(dbapi_connection: Any, _connection_record: Any) -> None:
    dbapi_connection.create_function("btrim", 1, _btrim, deterministic=True)
