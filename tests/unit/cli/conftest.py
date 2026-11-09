import copy
from collections.abc import Callable

import httpx
import pytest
from click.testing import CliRunner

from tests.unit.cli.fake_tracker import FakeTracker


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_tracker(monkeypatch: pytest.MonkeyPatch) -> FakeTracker:
    tracker = FakeTracker()

    async def handle(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        await request.aread()
        return tracker.handle(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)
    monkeypatch.setenv("VALKYRIE_URL", "https://tracker.test")
    monkeypatch.setenv("VALKYRIE_API_KEY", "test-key")
    return tracker


Reply = httpx.Response | BaseException | Callable[[httpx.Request], httpx.Response]


class MockTracker:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._replies: dict[tuple[str, str], list[Reply]] = {}

    def reply(self, method: str, path: str, *replies: Reply) -> None:
        self._replies[(method, path)] = list(replies)

    def requested(self, method: str, path: str) -> list[httpx.Request]:
        return [request for request in self.requests if (request.method, request.url.path) == (method, path)]

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queued = self._replies.get((request.method, request.url.path))
        if not queued:
            return httpx.Response(404, json={"detail": f"No mock for {request.method} {request.url.path}"})

        reply = queued.pop(0) if len(queued) > 1 else queued[0]
        if isinstance(reply, BaseException):
            raise copy.copy(reply)
        if callable(reply):
            return reply(request)

        return reply


@pytest.fixture
def tracker(monkeypatch: pytest.MonkeyPatch) -> MockTracker:
    monkeypatch.setenv("VALKYRIE_URL", "https://tracker.test")
    monkeypatch.setenv("VALKYRIE_API_KEY", "test-key")
    mock = MockTracker()

    async def handle(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        await request.aread()
        return mock.handle(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)

    return mock
