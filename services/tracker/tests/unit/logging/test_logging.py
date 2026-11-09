import json
from collections.abc import Iterator
import logging
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from main import app
from tracker.logging import (
    ContextFilter,
    DevFormatter,
    benchmark_id_var,
    configure_logging,
    request_id_var,
    task_id_var,
)
from tracker.logging.events import LogEvent
from tracker.logging.context import sandbox_id_var, sandbox_provider_scope, sandbox_provider_var
from tracker.observability.sentry import clear_sandbox_context, set_sandbox_context
from tracker.api.dependencies import TrackedBenchmarkId, bind_benchmark_id
from tracker.logging.filters import AccessLogNoiseFilter, EngineioNoiseFilter
from tracker.types import AgentContractRequest, StartBenchmarkRequest


class ContextLogRecord(logging.LogRecord):
    request_id: str
    benchmark_id: str
    task_id: str
    trace_id: str
    span_id: str
    sandbox_id: str
    sandbox_provider: str


TRACE_ID = "019e04c6fbf0397e32a8d9601f98e45c"
SPAN_ID = "a1f0f4fc15b83e82"


def _unsampled_span() -> NonRecordingSpan:
    return NonRecordingSpan(
        SpanContext(
            trace_id=int(TRACE_ID, 16),
            span_id=int(SPAN_ID, 16),
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.DEFAULT),
        )
    )


def _log_record(
    *,
    name: str = "test",
    level: int = logging.INFO,
    message: str = "hello",
) -> ContextLogRecord:
    return ContextLogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


def _configure_test_logging(monkeypatch: pytest.MonkeyPatch, environment: str) -> None:
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    configure_logging()


@pytest.fixture
def _restore_test_logging(capfd: pytest.CaptureFixture[str]) -> Iterator[None]:
    loggers = [logging.getLogger(name) for name in ("", "tracker", "uvicorn", "uvicorn.access", "uvicorn.error")]
    original = [(logger, logger.handlers[:], logger.level, logger.propagate, logger.disabled) for logger in loggers]
    yield
    for logger, handlers, level, propagate, disabled in original:
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate
        logger.disabled = disabled


@pytest.mark.description("Service headers are left out of the logged representation.")
def test_service_headers_are_excluded_from_log_repr() -> None:
    request = StartBenchmarkRequest(
        contract=AgentContractRequest(name="agent", run_cmd="run"),
        benchmark_name="test",
        service_headers={"Authorization": "secret-repr-sentinel"},
    )
    formatter = logging.Formatter("%(message)s")

    record = logging.LogRecord(
        name="tracker.test_headers",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="Service request failed: %r",
        args=(request,),
        exc_info=None,
    )
    output = formatter.format(record)

    assert "secret-repr-sentinel" not in output


@pytest.mark.description("Log record enrichment from request context.")
class TestContextFilter:
    @pytest.mark.description(
        (
            "ContextFilter copies empty and populated ContextVar values onto log records.\n"
            "\n"
            "Test cases:\n"
            "- Unset context variables produce empty record fields.\n"
            "- Set context variables are copied onto the record."
        )
    )
    def test_context_filter_injects_current_values(self) -> None:
        context_filter = ContextFilter()
        record = _log_record()

        context_filter.filter(record)

        assert record.request_id == ""
        assert record.benchmark_id == ""
        assert record.task_id == ""

        request_token = request_id_var.set("req-123")
        benchmark_token = benchmark_id_var.set("bench-456")
        task_token = task_id_var.set("task-789")
        try:
            context_filter.filter(record)

            assert record.request_id == "req-123"
            assert record.benchmark_id == "bench-456"
            assert record.task_id == "task-789"
        finally:
            request_id_var.reset(request_token)
            benchmark_id_var.reset(benchmark_token)
            task_id_var.reset(task_token)

    @pytest.mark.description(
        (
            "Sandbox identity follows the task's sandbox lifecycle onto every log record.\n"
            "\n"
            "Test cases:\n"
            "- The provider scope stamps `sandbox_provider` and restores the outer value on exit.\n"
            "- Binding a sandbox stamps `sandbox_id`, and clearing it empties the field for the next attempt."
        )
    )
    def test_context_filter_injects_sandbox_fields(self) -> None:
        context_filter = ContextFilter()

        with sandbox_provider_scope("docker"):
            set_sandbox_context(SimpleNamespace(id="sandbox-123", name="bench-task-1"))
            bound = _log_record()
            context_filter.filter(bound)
            clear_sandbox_context()
            cleared = _log_record()
            context_filter.filter(cleared)

        assert (bound.sandbox_id, bound.sandbox_provider) == ("sandbox-123", "docker")
        assert (cleared.sandbox_id, cleared.sandbox_provider) == ("", "docker")
        assert sandbox_provider_var.get() == ""
        assert sandbox_id_var.get() == ""

    @pytest.mark.description(
        "Fields passed via `extra` survive ambient context, so audit records keep their own identity."
    )
    def test_context_filter_preserves_explicit_record_fields(self) -> None:
        context_filter = ContextFilter()
        record = _log_record()
        record.benchmark_id = "bench-from-extra"
        record.task_id = "task-from-extra"

        request_token = request_id_var.set("req-123")
        benchmark_token = benchmark_id_var.set("bench-ambient")
        task_token = task_id_var.set("task-ambient")
        try:
            context_filter.filter(record)

            assert record.request_id == "req-123"
            assert record.benchmark_id == "bench-from-extra"
            assert record.task_id == "task-from-extra"
        finally:
            request_id_var.reset(request_token)
            benchmark_id_var.reset(benchmark_token)
            task_id_var.reset(task_token)

    @pytest.mark.description(
        (
            "ContextFilter copies the current OpenTelemetry trace and span IDs onto log records, sampled or not.\n"
            "\n"
            "Test cases:\n"
            "- Outside a span the trace fields are empty.\n"
            "- Inside an unsampled span the record carries its trace and span IDs."
        )
    )
    def test_context_filter_injects_current_trace_ids(self) -> None:
        context_filter = ContextFilter()
        outside_record = _log_record()

        context_filter.filter(outside_record)

        assert outside_record.trace_id == ""
        assert outside_record.span_id == ""

        inside_record = _log_record()
        with trace.use_span(_unsampled_span()):
            context_filter.filter(inside_record)

        assert inside_record.trace_id == TRACE_ID
        assert inside_record.span_id == SPAN_ID


@pytest.mark.description("Binding a run ID updates the logging context and the request span.")
async def test_bind_benchmark_id_updates_logging_context_and_request_span(monkeypatch: pytest.MonkeyPatch) -> None:
    span = Mock()
    monkeypatch.setattr("tracker.api.dependencies.trace.get_current_span", lambda: span)

    benchmark_id = UUID("12345678-1234-5678-9234-567812345678")
    result = await bind_benchmark_id(benchmark_id)

    assert result == benchmark_id
    assert benchmark_id_var.get() == str(benchmark_id)
    span.set_attribute.assert_called_once_with("benchmark_id", str(benchmark_id))


@pytest.mark.description("A bound run ID survives FastAPI dependency resolution.")
async def test_bind_benchmark_id_survives_fastapi_dependency_resolution() -> None:
    test_app = FastAPI()
    observed_benchmark_ids: list[str] = []

    async def endpoint(benchmark_id: TrackedBenchmarkId) -> dict[str, str]:
        observed_benchmark_ids.append(benchmark_id_var.get())
        return {"benchmark_id": str(benchmark_id)}

    test_app.add_api_route("/runs/{benchmark_id}", endpoint)
    benchmark_id = UUID("12345678-1234-5678-9234-567812345678")

    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as client:
        response = await client.get(f"/runs/{benchmark_id}")

    assert response.status_code == 200
    assert observed_benchmark_ids == [str(benchmark_id)]


@pytest.mark.description("Logging configuration across deployment environments.")
class TestConfigureLogging:
    @pytest.mark.description("Each supported environment selects its expected human or JSON format.")
    @pytest.mark.usefixtures("_restore_test_logging")
    @pytest.mark.parametrize(
        ("environment", "logger_name", "message", "structured", "expects_request_id"),
        [
            ("development", "tracker.test_dev", "hello dev", False, False),
            ("release-test", "tracker.test_release_test", "hello release-test", False, False),
            ("production", "tracker.test_prod", "hello prod", True, True),
            ("prod", "tracker.test_public_prod", "hello public prod", True, True),
            ("dev", "tracker.test_deployed_dev", "hello deployed dev", True, False),
        ],
    )
    def test_configure_logging_formats_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
        environment: str,
        logger_name: str,
        message: str,
        structured: bool,
        expects_request_id: bool,
    ) -> None:
        _configure_test_logging(monkeypatch, environment)

        logging.getLogger(logger_name).info(message)
        output = capfd.readouterr().out

        if not structured:
            assert message in output
            assert logger_name in output
            return

        log_payload = json.loads(output.strip())
        assert log_payload["message"] == message
        assert "timestamp" in log_payload
        assert "level" in log_payload
        if expects_request_id:
            assert "request_id" in log_payload

    @pytest.mark.description("Context variables appear in structured JSON output.")
    @pytest.mark.usefixtures("_restore_test_logging")
    def test_configure_logging_includes_context_vars(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
    ) -> None:
        _configure_test_logging(monkeypatch, "production")

        token = request_id_var.set("req-abc")
        try:
            logging.getLogger("tracker.test_ctx").info("with context")

            log_payload = json.loads(capfd.readouterr().out.strip())
            assert log_payload["request_id"] == "req-abc"
        finally:
            request_id_var.reset(token)

    @pytest.mark.description("Structured JSON log lines carry the trace and span IDs of the span they were logged in.")
    @pytest.mark.usefixtures("_restore_test_logging")
    def test_configure_logging_includes_trace_ids(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
    ) -> None:
        _configure_test_logging(monkeypatch, "production")

        with trace.use_span(_unsampled_span()):
            logging.getLogger("tracker.test_trace").info("inside span")

        log_payload = json.loads(capfd.readouterr().out.strip())

        assert log_payload["trace_id"] == TRACE_ID
        assert log_payload["span_id"] == SPAN_ID

    @pytest.mark.description("A stable event name passed in `extra` appears as the `event` field of the JSON log line.")
    @pytest.mark.usefixtures("_restore_test_logging")
    def test_configure_logging_includes_event_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
    ) -> None:
        _configure_test_logging(monkeypatch, "production")

        logging.getLogger("tracker.test_event").error("Task execution failed", extra={"event": LogEvent.TASK_FAILED})

        log_payload = json.loads(capfd.readouterr().out.strip())

        assert log_payload["event"] == "task.failed"
        assert log_payload["message"] == "Task execution failed"

    @pytest.mark.description("Unknown ENVIRONMENT values raise ValueError.")
    def test_configure_logging_rejects_unknown_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "staging")

        with pytest.raises(ValueError, match="staging"):
            configure_logging()


@pytest.mark.description("Development log formatting with optional context.")
class TestDevelopmentFormatter:
    @pytest.mark.description(
        (
            "DevFormatter includes populated context and omits empty context.\n"
            "\n"
            "Test cases:\n"
            "- Populated event, request, task, and sandbox fields appear without an empty benchmark ID.\n"
            "- A record without context omits the context bracket."
        )
    )
    def test_dev_formatter_handles_optional_context(self) -> None:
        formatter = DevFormatter("%(message)s")
        context_record = _log_record(name="tracker.test", message="test msg")
        context_record.event = LogEvent.SANDBOX_CREATE
        context_record.request_id = "req-1"
        context_record.benchmark_id = ""
        context_record.task_id = "task-2"
        context_record.sandbox_id = "sandbox-3"
        context_record.sandbox_provider = "docker"
        context_record.failure_category = "infrastructure"

        context_output = formatter.format(context_record)

        assert "event=sandbox.create request_id=req-1" in context_output
        assert "task_id=task-2" in context_output
        assert "sandbox_id=sandbox-3 sandbox_provider=docker" in context_output
        assert "failure_category=infrastructure" in context_output
        assert "benchmark_id" not in context_output

        plain_record = _log_record(name="tracker.test", level=logging.WARNING, message="no ctx")
        plain_record.request_id = ""
        plain_record.benchmark_id = ""
        plain_record.task_id = ""

        plain_output = formatter.format(plain_record)

        assert "no ctx" in plain_output
        assert " [" not in plain_output

    @pytest.mark.description("DevFormatter colors cannot be mutated through one instance and leak into another.")
    def test_dev_formatter_colors_are_read_only(self) -> None:
        first = DevFormatter("%(message)s")
        second = DevFormatter("%(message)s")

        with pytest.raises(TypeError):
            first.COLORS["ERROR"] = "\033[35m"  # pyright: ignore[reportIndexIssue]

        assert second.format(_log_record(name="tracker.test", level=logging.ERROR, message="boom")).startswith(
            "\033[31mERROR"
        )


@pytest.mark.description("Middleware generates request_id and returns it in response header.")
async def test_request_context_middleware_sets_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("main.check_database_connection", lambda: True)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    request_id = response.headers.get("x-request-id")
    assert request_id is not None

    assert len(request_id) == 12


def _access_record(path: str, status: int) -> logging.LogRecord:
    return logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("10.0.0.1:5000", "GET", path, "1.1", status),
        exc_info=None,
    )


@pytest.mark.description("Access logs drop health checks and successful run-monitor polls, and keep everything else.")
@pytest.mark.parametrize(
    ("path", "status", "kept"),
    [
        ("/health", 200, False),
        ("/health", 503, False),
        ("/benchmarks/0b5e/monitor/tasks", 200, False),
        ("/benchmarks/0b5e/monitor/tasks?cursor=10", 200, False),
        ("/benchmarks/0b5e/monitor/tasks", 404, True),
        ("/benchmarks/0b5e/monitor/tasks", 500, True),
        ("/benchmarks/0b5e/monitor/tasks/extra", 200, True),
        ("/benchmarks/0b5e/tasks", 200, True),
        ("/benchmarks/run", 200, True),
    ],
)
def test_access_log_noise_filter(path: str, status: int, kept: bool) -> None:
    assert AccessLogNoiseFilter().filter(_access_record(path, status)) is kept


@pytest.mark.description("Only the routine engineio close message is dropped.")
def test_engineio_noise_filter() -> None:
    noise_filter = EngineioNoiseFilter()

    assert not noise_filter.filter(_log_record(name="engineio.client", message="packet queue is empty, aborting"))
    assert noise_filter.filter(_log_record(name="engineio.client", message="Connection refused by the server"))


@pytest.mark.description(
    "Logging config quiets per-request httpx lines and wires the noise filters onto their loggers."
)
@pytest.mark.usefixtures("_restore_test_logging")
def test_configure_logging_quiets_noisy_loggers(monkeypatch: pytest.MonkeyPatch) -> None:
    names = ("httpx", "httpcore", "engineio.client")
    original = [
        (logging.getLogger(name), logging.getLogger(name).level, logging.getLogger(name).filters[:]) for name in names
    ]
    access_filters = logging.getLogger("uvicorn.access").filters[:]
    try:
        _configure_test_logging(monkeypatch, "production")

        assert not logging.getLogger("httpx").isEnabledFor(logging.INFO)
        assert logging.getLogger("httpx").isEnabledFor(logging.WARNING)
        assert not logging.getLogger("httpcore").isEnabledFor(logging.INFO)
        assert any(isinstance(f, EngineioNoiseFilter) for f in logging.getLogger("engineio.client").filters)
        assert any(isinstance(f, AccessLogNoiseFilter) for f in logging.getLogger("uvicorn.access").filters)
    finally:
        for logger, level, filters in original:
            logger.setLevel(level)
            logger.filters[:] = filters
        logging.getLogger("uvicorn.access").filters[:] = access_filters


@pytest.mark.description(
    "Log event names are a stable contract for log queries and alerts, so renaming one must update this list."
)
def test_log_event_names_are_stable() -> None:
    assert {event.name: event.value for event in LogEvent} == {
        "TASK_ATTEMPT_STARTED": "task.attempt.started",
        "TASK_RETRY": "task.retry",
        "TASK_FAILED": "task.failed",
        "TASK_CANCELLED": "task.cancelled",
        "TASK_UNHANDLED_ERROR": "task.unhandled_error",
        "AGENT_RUN_COMPLETE": "agent.run.complete",
        "TASK_EVALUATION_START": "task.evaluation.start",
        "SANDBOX_CREATE": "sandbox.create",
        "SANDBOX_CREATE_FAILED": "sandbox.create.failed",
        "SANDBOX_DELETE": "sandbox.delete",
        "RUN_FAILED": "run.failed",
    }
