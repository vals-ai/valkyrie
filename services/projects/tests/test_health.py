from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine

from projects.database import engine
from projects.main import app


@pytest.fixture
def client_for() -> Iterator[Callable[[Engine], TestClient]]:
    def build(bound: Engine) -> TestClient:
        app.dependency_overrides[engine] = lambda: bound
        return TestClient(app)

    yield build
    app.dependency_overrides.clear()


class TestHealth:
    @pytest.mark.description("The health route reports `ok` when the database answers a query.")
    def test_reports_ok_when_database_answers(self, client_for: Callable[[Engine], TestClient]) -> None:
        response = client_for(create_engine("sqlite://")).get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @pytest.mark.description("The health route returns 503 when the database refuses connections.")
    def test_returns_503_when_database_is_unreachable(self, client_for: Callable[[Engine], TestClient]) -> None:
        unreachable = create_engine("postgresql://projects:projects@127.0.0.1:1/projects?connect_timeout=1")

        response = client_for(unreachable).get("/health")

        assert response.status_code == 503
        assert response.json() == {"detail": "Database is not accessible"}
