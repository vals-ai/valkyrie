import socketserver
import threading
from collections.abc import Iterator
from uuid import uuid4

import httpcore
import httpx
import pytest
from fastapi.testclient import TestClient

from main import app
from tracker.clients.projects import (
    PROJECT_SERVICE_TIMEOUT,
    ProjectServiceClient,
    ProjectServiceUnavailableError,
    UnavailableAwareTransport,
)
from tests.utils import TEST_ORG_ID
from tracker.repositories import project as project_repository

client = TestClient(app)


def _client(handler: httpx.MockTransport) -> ProjectServiceClient:
    return ProjectServiceClient(httpx.Client(base_url="http://projects", transport=UnavailableAwareTransport(handler)))


def _refused(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


@pytest.mark.description(
    "A refused connection or a 502, 503 or 504 from the project service raises its unavailable error."
)
@pytest.mark.parametrize("status", [None, 502, 503, 504])
def test_an_unreachable_project_service_raises_unavailable(status: int | None) -> None:
    transport = httpx.MockTransport(_refused if status is None else lambda _request: httpx.Response(status))

    with pytest.raises(ProjectServiceUnavailableError):
        _ = _client(transport).get(TEST_ORG_ID, TEST_ORG_ID)


class _DroppedBody(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        raise httpx.ReadError("connection dropped")


@pytest.mark.description("A project service that drops the connection mid-body raises its unavailable error.")
def test_a_dropped_response_body_raises_unavailable() -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, stream=_DroppedBody()))

    with pytest.raises(ProjectServiceUnavailableError):
        _ = _client(transport).get(TEST_ORG_ID, TEST_ORG_ID)


@pytest.mark.description("A 404 from the project service still means the project is missing.")
def test_a_missing_project_is_not_an_outage() -> None:
    client = _client(httpx.MockTransport(lambda _request: httpx.Response(404)))

    assert client.get(TEST_ORG_ID, TEST_ORG_ID) is None


@pytest.mark.description("Tracker answers 503 with a try-again message when the project service is unreachable.")
def test_tracker_returns_503_try_again_when_the_project_service_is_down(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(project_repository, "project_service", lambda: _client(httpx.MockTransport(_refused)))

    response = client.get("/projects")

    assert response.status_code == 503
    assert response.json() == {"detail": "Project service unavailable, try again."}


@pytest.mark.description("Project reads allow thirty seconds while connections time out after two seconds.")
def test_the_environment_client_allows_slow_reads() -> None:
    assert PROJECT_SERVICE_TIMEOUT == httpx.Timeout(30, connect=2)


def _environment_client(monkeypatch: pytest.MonkeyPatch, url: str) -> ProjectServiceClient:
    monkeypatch.setenv("PROJECT_SERVICE_URL", url)
    monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "token")
    return ProjectServiceClient.from_environment()


@pytest.mark.description("A failed connect to the project service is retried once before the unavailable error.")
def test_a_failed_connect_is_retried_once(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[str] = []

    def refuse(_backend: object, host: str, *_args: object, **_kwargs: object) -> None:
        attempts.append(host)
        raise httpcore.ConnectError("connection refused")

    monkeypatch.setattr("httpcore._backends.sync.SyncBackend.connect_tcp", refuse)

    with pytest.raises(ProjectServiceUnavailableError):
        _ = _environment_client(monkeypatch, "http://projects").get(TEST_ORG_ID, TEST_ORG_ID)

    assert attempts == ["projects", "projects"]


@pytest.mark.description("A write that reaches the project service and then fails is sent once, never retried.")
def test_a_write_that_reaches_the_service_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[bytes] = []

    class DropAfterRequest(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            received.append(self.request.recv(65536))

    with socketserver.TCPServer(("127.0.0.1", 0), DropAfterRequest) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        service = _environment_client(monkeypatch, f"http://127.0.0.1:{server.server_address[1]}")

        with pytest.raises(ProjectServiceUnavailableError):
            _ = service.set_gateway_key(TEST_ORG_ID, uuid4(), uuid4())
        server.shutdown()

    assert len(received) == 1
    assert received[0].startswith(b"POST ")
