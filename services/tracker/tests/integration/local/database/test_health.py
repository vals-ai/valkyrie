import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from main import app

_client = TestClient(app)


@pytest.mark.description("Health endpoint behavior against available and unavailable databases.")
class TestHealthCheckIntegration:
    @pytest.mark.description(
        (
            "The health endpoint succeeds when the integration database is reachable.\n"
            "\n"
            "Test cases:\n"
            "- The endpoint returns 200 when check_database_connection uses the test Postgres engine.\n"
            "- The response body matches the public health-check success payload."
        )
    )
    def test_health_check_with_database(
        self,
        postgres_engine: Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import tracker.database.session as session_module

        monkeypatch.setattr(session_module, "engine", postgres_engine)

        response = _client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @pytest.mark.description(
        (
            "The health endpoint reports database connectivity failures.\n"
            "\n"
            "Test cases:\n"
            "- The endpoint returns 503 when check_database_connection reports failure.\n"
            "- The response body includes the public database-unavailable detail."
        )
    )
    def test_health_check_database_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("main.check_database_connection", lambda: False)

        response = _client.get("/health")

        assert response.status_code == 503
        assert response.json() == {"detail": "Database is not accessible"}
