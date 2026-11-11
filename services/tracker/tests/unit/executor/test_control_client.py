from uuid import uuid4

import httpx
import pytest
from pydantic import BaseModel

from tracker.executor import control_client
from tracker.executor.control_client import ControlClient


class _Body(BaseModel):
    ok: bool


def _client(responses: list[Exception | httpx.Response], attempts: list[str]) -> ControlClient:
    def handle(request: httpx.Request) -> httpx.Response:
        attempts.append(request.url.path)
        outcome = responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return ControlClient(
        http=httpx.AsyncClient(base_url="http://control", transport=httpx.MockTransport(handle)),
        dispatch_id=uuid4(),
        invocation_key="key",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read_run_state", "queue-states"])
@pytest.mark.description("A retry-safe operation survives read timeouts and gateway errors within its retry budget.")
async def test_retry_safe_operation_survives_transient_control_failures(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    monkeypatch.setattr(control_client.random, "uniform", lambda _low, _high: 0)
    attempts: list[str] = []
    client = _client(
        [httpx.ReadTimeout("stalled"), httpx.Response(503), httpx.Response(200, json={"ok": True})], attempts
    )

    result = await client.call(operation, _Body(ok=True), _Body)

    assert result.ok
    assert attempts == [f"/executor-control/v1/{operation}"] * 3


@pytest.mark.asyncio
@pytest.mark.description("A compare-and-set operation is not replayed after a read timeout.")
async def test_compare_and_set_operation_raises_on_first_read_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control_client.random, "uniform", lambda _low, _high: 0)
    attempts: list[str] = []
    client = _client([httpx.ReadTimeout("stalled"), httpx.Response(200, json={"ok": True})], attempts)

    with pytest.raises(httpx.ReadTimeout):
        await client.call("transition_task", _Body(ok=True), _Body)

    assert len(attempts) == 1


@pytest.mark.asyncio
@pytest.mark.description("A retry-safe operation stops retrying once the retry budget is spent.")
async def test_retry_safe_operation_raises_after_retry_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control_client.random, "uniform", lambda _low, _high: 0)
    monkeypatch.setattr(control_client, "RETRY_BUDGET_SECONDS", 0)
    attempts: list[str] = []
    client = _client([httpx.ReadTimeout("stalled"), httpx.Response(200, json={"ok": True})], attempts)

    with pytest.raises(httpx.ReadTimeout):
        await client.call("monitor_tasks", _Body(ok=True), _Body)

    assert len(attempts) == 1
