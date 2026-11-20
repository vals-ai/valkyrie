"""Focused proof for benchmark-service Sentry telemetry and trace propagation."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock, patch

import pytest
import sentry_sdk
from fastapi.testclient import TestClient
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.stdlib import StdlibIntegration
from sentry_sdk.transport import Transport
from starlette.websockets import WebSocket, WebSocketDisconnect

import benchmark_service
from benchmark_service import __version__ as framework_version
from benchmark_service import app as app_module
from benchmark_service.observability import (
    RUN_ID_HEADER,
    TASK_ID_HEADER,
)
from benchmark_service.schemas import EvaluateResponseRequest
from vals_benchmark_service.app import ValsBenchmarkServiceApp
from conftest import ValsStubBenchmark


class _CaptureTransport(Transport):
    def __init__(self, options: dict[str, Any] | None = None) -> None:
        super().__init__(options)
        self.events: list[dict[str, Any]] = []
        self.transactions: list[dict[str, Any]] = []
        self.init_calls = 0

    def capture_envelope(self, envelope: Envelope) -> None:
        for item in envelope.items:
            event = item.get_event()
            if event is not None:
                self.events.append(dict(event))
            transaction = item.get_transaction_event()
            if transaction is not None:
                self.transactions.append(dict(transaction))


class _FailedStatusError(Exception):
    status_code = 500


class _FailingWebSocketBenchmark(ValsStubBenchmark):
    def get_service_version(self) -> str:
        return "websocket-service-4.5.6"

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        raise RuntimeError("websocket evaluation failed")


class _VersionedBenchmark(ValsStubBenchmark):
    def get_service_version(self) -> str:
        return "service-hook-1.2.3"


class _OtherVersionedBenchmark(ValsStubBenchmark):
    def get_service_version(self) -> str:
        return "other-service-4.5.6"


_INCOMING_TRACE_ID = "0123456789abcdef0123456789abcdef"
_INCOMING_CLIENT_SPAN_ID = "0123456789abcdef"
_INCOMING_SENTRY_TRACE = f"{_INCOMING_TRACE_ID}-{_INCOMING_CLIENT_SPAN_ID}-1"


@pytest.fixture
def configured_sentry(monkeypatch: pytest.MonkeyPatch) -> Iterator[_CaptureTransport]:
    transport = _CaptureTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(
        sentry_sdk,
        "is_initialized",
        lambda: transport.init_calls > 0,
    )

    def init_with_transport(*args: Any, **kwargs: Any) -> Any:
        transport.init_calls += 1
        kwargs["transport"] = transport
        kwargs["disabled_integrations"] = [StdlibIntegration]
        return real_init(*args, **kwargs)

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    monkeypatch.setenv("SERVICE_NAME", "swebench")
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "dev")
    monkeypatch.setenv("SENTRY_RELEASE", "abc123")
    monkeypatch.setattr(sentry_sdk, "init", init_with_transport)
    yield transport
    sentry_sdk.flush()
    sentry_sdk.get_client().close()
    real_init(dsn=None, disabled_integrations=[StdlibIntegration])


def test_vals_app_without_sentry_preserves_health(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start and serve health from the Vals app without importing the optional Sentry SDK."""
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    init = Mock()
    monkeypatch.setattr(sentry_sdk, "init", init)

    # Block both cached import paths because other tests import the integration.
    monkeypatch.delattr(benchmark_service, "sentry", raising=False)
    with patch.dict(sys.modules, {"benchmark_service.sentry": None, "sentry_sdk": None}):
        with TestClient(ValsBenchmarkServiceApp(ValsStubBenchmark)) as client:
            response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    init.assert_not_called()


def test_lifespan_failure_before_service_creation_uses_package_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )
    monkeypatch.setattr(
        ValsBenchmarkServiceApp,
        "service_lifespan",
        Mock(side_effect=RuntimeError("startup configuration failed")),
    )
    app = ValsBenchmarkServiceApp(_VersionedBenchmark)

    with sentry_sdk.isolation_scope():
        sentry_sdk.set_tag("service.version", "caller-service-8.8.8")
        with pytest.raises(RuntimeError, match="startup configuration failed"):
            with TestClient(app):
                pass
        sentry_sdk.flush()

        assert len(configured_sentry.events) == 1
        tags = configured_sentry.events[0]["tags"]
        assert tags["service.name"] == "swebench"
        assert tags["framework.version"] == framework_version
        assert tags["service.version"] == "package-1.2.3"

        sentry_sdk.capture_message("caller scope after lifespan failure")
        sentry_sdk.flush()
        assert configured_sentry.events[1]["tags"]["service.version"] == "caller-service-8.8.8"


def test_lifespan_failure_after_service_creation_uses_runtime_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )

    class FailingStartupApp(ValsBenchmarkServiceApp):
        @asynccontextmanager
        async def service_lifespan(self) -> AsyncIterator[None]:
            async with super().service_lifespan():
                raise RuntimeError("post-create startup failed")
                yield

    app = FailingStartupApp(_VersionedBenchmark)

    with pytest.raises(RuntimeError, match="post-create startup failed"):
        with TestClient(app):
            pass
    sentry_sdk.flush()

    assert len(configured_sentry.events) == 1
    tags = configured_sentry.events[0]["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "service-hook-1.2.3"


def test_versionless_app_does_not_inherit_other_app_service_version(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", None)),
    )
    monkeypatch.setenv("SERVICE_NAME", "versioned-service")
    versioned_app = ValsBenchmarkServiceApp(_VersionedBenchmark)
    monkeypatch.setenv("SERVICE_NAME", "versionless-service")
    versionless_app = ValsBenchmarkServiceApp(ValsStubBenchmark)

    async def fail() -> None:
        raise RuntimeError("app failed")

    versioned_app.add_api_route("/fail", fail, methods=["GET"])
    versionless_app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(versioned_app, raise_server_exceptions=False) as client:
        versioned_response = client.get("/fail")
    with TestClient(versionless_app, raise_server_exceptions=False) as client:
        versionless_response = client.get("/fail")
    sentry_sdk.flush()

    assert (versioned_response.status_code, versionless_response.status_code) == (
        500,
        500,
    )
    assert len(configured_sentry.events) == 2
    versioned_tags = configured_sentry.events[0]["tags"]
    versionless_tags = configured_sentry.events[1]["tags"]
    assert versioned_tags["service.version"] == "service-hook-1.2.3"
    assert versionless_tags["service.name"] == "versionless-service"
    assert versionless_tags["framework.version"] == framework_version
    assert "service.version" not in versionless_tags


@pytest.mark.parametrize("error_type", [RuntimeError, _FailedStatusError])
def test_http_exceptions_are_captured_once_with_request_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
    error_type: type[Exception],
) -> None:
    app = ValsBenchmarkServiceApp(ValsStubBenchmark)

    async def fail() -> None:
        raise error_type("request failed")

    app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(
            "/fail",
            headers={
                RUN_ID_HEADER: "run-1",
                TASK_ID_HEADER: "task-1",
                "sentry-trace": _INCOMING_SENTRY_TRACE,
            },
        )
    sentry_sdk.flush()

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    assert len(configured_sentry.events) == 1
    event = configured_sentry.events[0]
    assert event["exception"]["values"][-1]["type"] == error_type.__name__
    assert event["tags"]["run_id"] == "run-1"
    assert event["tags"]["task_id"] == "task-1"
    assert event["tags"]["service.name"] == "swebench"
    assert event["environment"] == "dev"
    assert event["release"] == "abc123"
    transaction = next(
        item
        for item in configured_sentry.transactions
        if item["contexts"]["trace"].get("parent_span_id") == _INCOMING_CLIENT_SPAN_ID
    )
    assert transaction["contexts"]["trace"]["trace_id"] == _INCOMING_TRACE_ID


def test_request_identity_uses_runtime_service_version_override(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )
    app = ValsBenchmarkServiceApp(_VersionedBenchmark)

    async def fail() -> None:
        raise RuntimeError("versioned request failed")

    app.add_api_route("/fail-versioned", fail, methods=["GET"])
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/fail-versioned")
    sentry_sdk.flush()

    assert response.status_code == 500
    assert len(configured_sentry.events) == 1
    event = configured_sentry.events[0]
    tags = event["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "service-hook-1.2.3"
    assert event["release"] == "abc123"


def test_multiple_apps_keep_request_identity_and_single_client_initialization(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setenv("SERVICE_NAME", "first-service")
    first_app = ValsBenchmarkServiceApp(_VersionedBenchmark)
    monkeypatch.setenv("SERVICE_NAME", "second-service")
    second_app = ValsBenchmarkServiceApp(_OtherVersionedBenchmark)

    async def fail() -> None:
        raise RuntimeError("app failed")

    first_app.add_api_route("/fail", fail, methods=["GET"])
    second_app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(first_app, raise_server_exceptions=False) as client:
        first_response = client.get("/fail")
    with TestClient(second_app, raise_server_exceptions=False) as client:
        second_response = client.get("/fail")
    sentry_sdk.flush()

    assert (first_response.status_code, second_response.status_code) == (500, 500)
    assert len(configured_sentry.events) == 2
    assert [
        (
            event["tags"]["service.name"],
            event["tags"]["framework.version"],
            event["tags"]["service.version"],
        )
        for event in configured_sentry.events
    ] == [
        ("first-service", framework_version, "service-hook-1.2.3"),
        ("second-service", framework_version, "other-service-4.5.6"),
    ]
    assert configured_sentry.init_calls == 1


def test_websocket_exception_is_captured_once_with_request_identity(
    configured_sentry: _CaptureTransport,
) -> None:
    app = ValsBenchmarkServiceApp(_FailingWebSocketBenchmark)

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/evaluate-response",
            headers={RUN_ID_HEADER: "run-2", "sentry-trace": _INCOMING_SENTRY_TRACE},
        ) as websocket:
            websocket.send_json({"task_id": "task-2", "response": "answer", "dataset": "default"})
            response = websocket.receive_json()
    sentry_sdk.flush()

    assert response["type"] == "error"
    assert "websocket evaluation failed" in response["data"]
    assert len(configured_sentry.events) == 1
    tags = configured_sentry.events[0]["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "websocket-service-4.5.6"
    assert tags["run_id"] == "run-2"
    assert tags["task_id"] == "task-2"
    assert tags["dataset"] == "default"
    transaction = next(
        item
        for item in configured_sentry.transactions
        if item["contexts"]["trace"].get("parent_span_id") == _INCOMING_CLIENT_SPAN_ID
    )
    assert transaction["contexts"]["trace"]["trace_id"] == _INCOMING_TRACE_ID


@pytest.mark.parametrize("path", ["evaluate-instance", "evaluate-response"])
@pytest.mark.parametrize("transport_failure", ["none", "close_disconnect", "close_oserror", "send_oserror"])
def test_malformed_grading_token_frame_is_not_reported(
    path: str,
    transport_failure: str,
    configured_sentry: _CaptureTransport,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = ValsBenchmarkServiceApp(_FailingWebSocketBenchmark)
    token = "project-grader-token"

    async def disconnected_close(_websocket: WebSocket) -> None:
        raise WebSocketDisconnect(code=1006)

    async def failed_close(_websocket: WebSocket) -> None:
        raise OSError("transport closed")

    async def failed_send(_websocket: WebSocket, _payload: Any) -> None:
        raise OSError("transport closed")

    with TestClient(app) as client:
        with client.websocket_connect("/ws/evaluate-response") as websocket:
            websocket.send_json({"task_id": "task-2", "response": "answer", "grading_token": None})
            response = websocket.receive_json()
        sentry_sdk.flush()
        assert response["type"] == "error"
        assert "websocket evaluation failed" in response["data"]
        assert len(configured_sentry.events) == 1

        with monkeypatch.context() as patcher:
            if transport_failure == "close_disconnect":
                patcher.setattr(WebSocket, "close", disconnected_close)
            elif transport_failure == "close_oserror":
                patcher.setattr(WebSocket, "close", failed_close)
            elif transport_failure == "send_oserror":
                patcher.setattr(WebSocket, "send_json", failed_send)
            with client.websocket_connect(f"/ws/{path}") as websocket:
                websocket.send_text(f'{{"task_id":"task-2","grading_token":"{token}",')
                if transport_failure == "send_oserror":
                    with pytest.raises(WebSocketDisconnect):
                        websocket.receive_json()
                else:
                    assert websocket.receive_json() == {"type": "error", "data": "Evaluation failed"}
    sentry_sdk.flush()

    assert len(configured_sentry.events) == 1
    assert token not in str(configured_sentry.events)
    assert token not in caplog.text
