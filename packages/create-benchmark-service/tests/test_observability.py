"""Focused proof for benchmark-service Sentry telemetry and trace propagation."""

from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock, patch

import opentelemetry.propagate as propagate
import pytest
import sentry_sdk
import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.opentelemetry import SentryPropagator, SentrySpanProcessor
from sentry_sdk.integrations.stdlib import StdlibIntegration
from sentry_sdk.tracing import Transaction
from sentry_sdk.transport import Transport

import benchmark_service
from benchmark_service import observability
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.client import BenchmarkServiceClient, SandboxRecoveryAttempt
from benchmark_service.observability import (
    ATTEMPT_ID_HEADER,
    EMAIL_HEADER,
    RUN_ID_HEADER,
    TASK_ID_HEADER,
    correlation_scope,
    request_headers,
)
from benchmark_service.sentry import init_sentry
from tests.conftest import StubBenchmark


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


_INCOMING_TRACE_ID = "0123456789abcdef0123456789abcdef"
_INCOMING_CLIENT_SPAN_ID = "0123456789abcdef"
_INCOMING_SENTRY_TRACE = f"{_INCOMING_TRACE_ID}-{_INCOMING_CLIENT_SPAN_ID}-1"


@asynccontextmanager
async def _serve_loopback(app: FastAPI) -> AsyncIterator[str]:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 0))
    server_socket.listen()
    server_socket.setblocking(False)
    port = server_socket.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="critical", access_log=False))
    server_task = asyncio.create_task(server.serve(sockets=[server_socket]))

    try:
        deadline = asyncio.get_running_loop().time() + 5
        while not server.started:
            if server_task.done():
                server_task.result()
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("Uvicorn did not start within 5 seconds")
            await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5)
        finally:
            server_socket.close()


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


def _composite_propagator() -> CompositePropagator:
    return CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator(), SentryPropagator()])


@pytest.fixture
def otel_tracer(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[trace.Tracer, InMemorySpanExporter, TracerProvider]]:
    real_init = sentry_sdk.init
    real_init(
        dsn="https://public@example.com/1",
        transport=_CaptureTransport,
        instrumenter=INSTRUMENTER.OTEL,
        traces_sample_rate=1.0,
        disabled_integrations=[StdlibIntegration],
    )
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SentrySpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("benchmark-service-test")
    monkeypatch.setattr(observability, "_tracer", tracer)
    previous_propagator = propagate.get_global_textmap()
    propagate.set_global_textmap(_composite_propagator())
    try:
        yield tracer, exporter, provider
    finally:
        propagate.set_global_textmap(previous_propagator)
        try:
            provider.shutdown()
            sentry_sdk.flush()
            sentry_sdk.get_client().close()
        finally:
            real_init(dsn=None, disabled_integrations=[StdlibIntegration])


@pytest.mark.parametrize("dsn", [None, "https://public@example.com/1"])
def test_app_without_sentry_preserves_health(monkeypatch: pytest.MonkeyPatch, dsn: str | None) -> None:
    """Start and serve health without importing the optional Sentry SDK.

    Test cases:
    - The app ignores an inherited DSN when Sentry is unavailable.
    - The app leaves Sentry disabled when no DSN is configured.
    - Application startup, health requests, and shutdown still succeed.
    """
    if dsn is None:
        monkeypatch.delenv("SENTRY_DSN", raising=False)
    else:
        monkeypatch.setenv("SENTRY_DSN", dsn)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    init = Mock()
    monkeypatch.setattr(sentry_sdk, "init", init)

    # Block both cached import paths because other tests import the integration.
    monkeypatch.delattr(benchmark_service, "sentry", raising=False)
    with patch.dict(sys.modules, {"benchmark_service.sentry": None, "sentry_sdk": None}):
        with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
            response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    init.assert_not_called()


def test_init_sentry_uses_process_configuration_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "dev")
    monkeypatch.setenv("SENTRY_RELEASE", "abc123")
    is_initialized = Mock(side_effect=[False, True])
    init = Mock()
    monkeypatch.setattr(sentry_sdk, "is_initialized", is_initialized)
    monkeypatch.setattr(sentry_sdk, "init", init)

    assert init_sentry() is True
    assert init_sentry() is True

    assert init.call_count == 1
    options = init.call_args.kwargs
    assert options["dsn"] == "https://public@example.com/1"
    assert options["environment"] == "dev"
    assert options["release"] == "abc123"
    assert options["traces_sample_rate"] == 0.1


@pytest.mark.parametrize(
    ("parent_sampled", "sample_rand", "expected_sampled"),
    [(None, 0.05, True), (None, 0.15, False), (True, 0.95, True), (False, 0.05, False)],
)
def test_process_sampling_keeps_parent_decisions_and_error_events(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
    parent_sampled: bool | None,
    sample_rand: float,
    expected_sampled: bool,
) -> None:
    monkeypatch.setattr("sentry_sdk.tracing._generate_sample_rand", Mock(return_value=sample_rand))
    assert init_sentry() is True

    with sentry_sdk.start_transaction(Transaction(name="sampling-test", parent_sampled=parent_sampled)) as transaction:
        assert transaction.sampled is expected_sampled
        sentry_sdk.capture_exception(RuntimeError("sampling-test failure"))
    sentry_sdk.flush()

    assert len(configured_sentry.transactions) == int(expected_sampled)
    assert len(configured_sentry.events) == 1
    assert configured_sentry.events[0]["contexts"]["trace"]["trace_id"] == transaction.trace_id


async def test_concurrent_correlation_scopes_restore_their_caller() -> None:
    barrier = asyncio.Barrier(2)

    async def headers_for(run_id: str, task_id: str) -> dict[str, str]:
        with correlation_scope(run_id=run_id, task_id=task_id):
            await barrier.wait()
            inner_headers = request_headers({})
            assert (inner_headers[RUN_ID_HEADER], inner_headers[TASK_ID_HEADER]) == (run_id, task_id)
        restored = request_headers({})
        assert (restored[RUN_ID_HEADER], restored[TASK_ID_HEADER]) == ("outer-run", "outer-task")
        return inner_headers

    with correlation_scope(run_id="outer-run", task_id="outer-task"):
        first, second = await asyncio.gather(
            headers_for("run-a", "task-a"),
            headers_for("run-b", "task-b"),
        )
        restored = request_headers({})

    uncorrelated = request_headers({})
    assert (first[RUN_ID_HEADER], first[TASK_ID_HEADER]) == ("run-a", "task-a")
    assert (second[RUN_ID_HEADER], second[TASK_ID_HEADER]) == ("run-b", "task-b")
    assert (restored[RUN_ID_HEADER], restored[TASK_ID_HEADER]) == ("outer-run", "outer-task")
    assert RUN_ID_HEADER not in uncorrelated
    assert TASK_ID_HEADER not in uncorrelated


async def test_http_client_transport_span_injects_trace_and_dynamic_identity(
    otel_tracer: tuple[trace.Tracer, InMemorySpanExporter, TracerProvider],
) -> None:
    tracer, exporter, provider = otel_tracer
    app = FastAPI()
    observed_headers: dict[str, str] = {}

    async def health(request: Request) -> dict[str, str]:
        observed_headers.update(request.headers)
        return {"status": "ok"}

    app.add_api_route("/health", health, methods=["GET"])

    instrumentor = HTTPXClientInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    try:
        async with _serve_loopback(app) as url:
            async with BenchmarkServiceClient(url, {"Authorization": "Bearer test"}) as client:

                async def operation(_attempt: SandboxRecoveryAttempt) -> Any:
                    return await client.health_check()

                with tracer.start_as_current_span("caller") as caller_span:
                    caller_context = caller_span.get_span_context()
                    result = await client.run_with_sandbox_recovery("task-3", "run-3", operation)
    finally:
        instrumentor.uninstrument()

    client_spans = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.CLIENT]
    assert len(client_spans) == 1
    client_span = client_spans[0]
    assert client_span.name == "GET"
    assert result.status == "ok"
    assert client_span.context is not None
    assert client_span.context.trace_id == caller_context.trace_id
    assert client_span.context.span_id != caller_context.span_id
    assert observed_headers[RUN_ID_HEADER.lower()] == "run-3"
    assert observed_headers[TASK_ID_HEADER.lower()] == "task-3"
    assert observed_headers["traceparent"].split("-")[:3] == [
        "00",
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]
    assert observed_headers["sentry-trace"].split("-")[:2] == [
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]


async def test_websocket_handshake_uses_its_client_span_and_dynamic_identity(
    otel_tracer: tuple[trace.Tracer, InMemorySpanExporter, TracerProvider],
) -> None:
    tracer, exporter, _provider = otel_tracer
    app = FastAPI()
    observed_headers: dict[str, str] = {}

    async def evaluate_response(websocket: WebSocket) -> None:
        observed_headers.update(websocket.headers)
        await websocket.accept()
        request = await websocket.receive_json()
        await websocket.send_json(
            {
                "type": "result",
                "data": {
                    "task_id": request["task_id"],
                    "state": request["eval_resume_state"],
                },
            }
        )
        await websocket.close()

    app.add_api_websocket_route("/ws/evaluate-response", evaluate_response)

    async with _serve_loopback(app) as url:
        async with BenchmarkServiceClient(url, {"Authorization": "Bearer test"}) as client:

            async def operation(_attempt: SandboxRecoveryAttempt) -> Any:
                return await client.resume_evaluation(
                    "task-1",
                    {"artifact_prefix": "s3://bucket/run"},
                )

            with tracer.start_as_current_span("caller") as caller_span:
                caller_context = caller_span.get_span_context()
                result = await client.run_with_sandbox_recovery("task-1", "run-4", operation)

    client_spans = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.CLIENT]
    assert len(client_spans) == 1
    client_span = client_spans[0]
    assert client_span.name == "WEBSOCKET /ws/evaluate-response"
    assert result == {"task_id": "task-1", "state": {"artifact_prefix": "s3://bucket/run"}}
    assert client_span.context is not None
    assert client_span.context.trace_id == caller_context.trace_id
    assert client_span.context.span_id != caller_context.span_id
    assert observed_headers[RUN_ID_HEADER.lower()] == "run-4"
    assert observed_headers[TASK_ID_HEADER.lower()] == "task-1"
    assert observed_headers["traceparent"].split("-")[:3] == [
        "00",
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]
    assert observed_headers["sentry-trace"].split("-")[:2] == [
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]


def test_nested_run_scope_does_not_inherit_the_outer_attempt() -> None:
    with correlation_scope(run_id="outer-run", task_id="outer-task", attempt_id="outer-attempt"):
        with correlation_scope(run_id="inner-run", task_id="inner-task"):
            assert ATTEMPT_ID_HEADER not in request_headers({})
        assert request_headers({})[ATTEMPT_ID_HEADER] == "outer-attempt"


def test_nested_run_scope_does_not_inherit_the_outer_email() -> None:
    with correlation_scope(run_id="outer-run", email="outer@example.com"):
        with correlation_scope(run_id="inner-run"):
            assert EMAIL_HEADER not in request_headers({})
        assert request_headers({})[EMAIL_HEADER] == "outer@example.com"
