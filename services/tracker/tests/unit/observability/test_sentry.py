import asyncio
import json
from collections.abc import Callable, Iterator
from contextlib import nullcontext
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.trace import TracerProvider
import pytest
import sentry_sdk
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import sentry_sdk.scope as sentry_scope
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.transport import Transport
from sentry_sdk.types import Event, Hint, Log

import tracker.observability.sentry as sentry_module
import tracker.observability.tracing as tracing_module
import tracker.services.runs.start as run_orchestration
import tracker.services.tasks.execute as task_execution
import tracker.services.tasks.execute as task_monitor_module
from tracker.domain import FailureCategory, TaskStatus
from tracker.executor.control_tasks import TaskState
from tracker.executor.control_protocol import DispatchRef
from tracker.exceptions import SandboxError, SandboxSetupError, SSLConnectionError
from tracker.logging.context import (
    attempt_started_at_var,
    benchmark_id_var,
    executor_dispatch_id_var,
    request_id_var,
    task_id_var,
)

BeforeSend = Callable[[Event, Hint], Event | None]
BeforeSendLog = Callable[[Log, Hint], Log | None]


def _before_send() -> BeforeSend:
    return cast(BeforeSend, getattr(sentry_module, "_before_send"))


def _before_send_log() -> BeforeSendLog:
    return cast(BeforeSendLog, getattr(sentry_module, "_before_send_log"))


@pytest.fixture(autouse=True)
def isolated_sentry_scopes() -> Iterator[None]:
    with sentry_scope.use_isolation_scope(sentry_sdk.Scope()), sentry_scope.use_scope(sentry_sdk.Scope()):
        yield


@pytest.mark.description("Sentry event grouping and context filtering.")
class TestBeforeSend:
    @pytest.mark.description("SSL connection errors are fingerprinted into one Sentry issue.")
    def test_before_send_fingerprints_ssl_connection_errors(self) -> None:
        exc = SSLConnectionError("curl failed with exit code 35")
        event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

        assert event is not None
        assert event.get("fingerprint") == ["{{ default }}", "SSLConnectionError"]

    @pytest.mark.description("Other errors keep their grouping, and empty context tags are dropped.")
    def test_before_send_preserves_non_grouped_errors_and_filters_empty_context_tags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        exc = SandboxError("ordinary sandbox setup failure")

        def fake_context_tags() -> dict[str, str]:
            return {
                "benchmark_id": "benchmark-123",
                "task_id": "",
            }

        monkeypatch.setattr(sentry_module, "get_context_tags", fake_context_tags)

        event = _before_send()({}, {"exc_info": (type(exc), exc, None)})

        assert event is not None
        assert "fingerprint" not in event
        assert event.get("tags") == {"benchmark_id": "benchmark-123"}

    @pytest.mark.description("Events without exception info pass through `before_send`.")
    def test_before_send_handles_events_without_exception_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_context_tags() -> dict[str, str]:
            return {}

        monkeypatch.setattr(sentry_module, "get_context_tags", fake_context_tags)

        event = _before_send()({"message": "log event"}, {})

        assert event == {"message": "log event", "tags": {}}


@pytest.mark.description("Only captured OTel root identities are promoted.")
class TestBeforeSendTransaction:
    @pytest.mark.description("Only captured owned attributes become tags, and other tags are kept.")
    def test_promotes_only_captured_owned_attributes_and_preserves_other_tags(self) -> None:
        event: Event = {
            "type": "transaction",
            "contexts": {"otel": {"attributes": {"request_id": "captured-request", "http.method": "POST"}}},
            "tags": {"request_id": "stale-request", "component": "tracker"},
        }

        with sentry_module.task_scope("late-task", attempt_started_at="2026-04-01T13:00:00+00:00"):
            result = sentry_module._before_send_transaction(event, {})

        assert result is event
        assert event["tags"] == {"request_id": "captured-request", "component": "tracker"}
        assert event["contexts"]["otel"]["attributes"] == {
            "request_id": "captured-request",
            "http.method": "POST",
        }

    @pytest.mark.description("Native transactions without OpenTelemetry context are unchanged.")
    @pytest.mark.parametrize(
        "event",
        [
            {"type": "transaction"},
            {
                "type": "transaction",
                "contexts": {
                    "trace": {"trace_id": "019e04c6fbf0397e32a8d9601f98e45c00", "span_id": "a1f0f4fc15b83e82"},
                },
                "tags": {"component": "tracker"},
            },
        ],
        ids=["without-contexts", "native-trace-context"],
    )
    def test_native_transactions_without_otel_context_are_unchanged(self, event: Event) -> None:
        expected = deepcopy(event)

        with sentry_module.task_scope("late-task", attempt_started_at="2026-04-01T13:00:00+00:00"):
            result = sentry_module._before_send_transaction(event, {})

        assert result is event
        assert event == expected


@pytest.mark.description(
    "Captured HTTP errors and transactions exclude the remote worker key while retaining request context."
)
@pytest.mark.parametrize(
    ("instrumenter", "expected_kinds"),
    [(INSTRUMENTER.SENTRY, {"event", "transaction"}), (INSTRUMENTER.OTEL, {"event", "transaction"})],
    ids=["native-transaction", "otel-transaction"],
)
def test_remote_worker_key_is_absent_from_captured_requests(instrumenter: str, expected_kinds: set[str]) -> None:
    captured: list[tuple[str, dict[str, Any]]] = []

    class CaptureTransport(Transport):
        def capture_envelope(self, envelope: Envelope) -> None:
            for item in envelope.items:
                if item.type in {"event", "transaction"}:
                    captured.append((item.type, json.loads(item.get_bytes())))

    app = FastAPI()

    @app.get("/ok")
    async def ok() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/fail")
    async def fail() -> None:
        raise RuntimeError("request failed")

    provider = TracerProvider()
    if instrumenter == INSTRUMENTER.OTEL:
        provider.add_span_processor(tracing_module._FilteredSentrySpanProcessor())
        FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)

    try:
        with sentry_sdk.init(
            dsn="https://public@example.com/1",
            transport=CaptureTransport,
            default_integrations=False,
            traces_sample_rate=1.0,
            instrumenter=instrumenter,
            send_default_pii=False,
            max_request_body_size="never",
            include_local_variables=False,
            before_send=sentry_module._before_send,
            before_send_transaction=sentry_module._before_send_transaction,
        ):
            with TestClient(SentryAsgiMiddleware(app), raise_server_exceptions=False) as api:
                headers = {"X-Remote-Grader-Worker-Key": "private-worker-key", "X-Test-Context": "retained"}
                assert api.get("/ok", headers=headers).status_code == 200
                assert api.get("/fail", headers=headers).status_code == 500
    finally:
        provider.shutdown()

    assert {kind for kind, _ in captured} == expected_kinds
    for _, event in captured:
        assert "private-worker-key" not in json.dumps(event)
        assert event["request"]["headers"]["x-test-context"] == "retained"
        assert event["request"]["headers"]["x-remote-grader-worker-key"] == "[Filtered]"


@pytest.mark.description("Sentry initialization and log context behavior.")
class TestSentrySetup:
    @pytest.mark.description(
        "`init_sentry` adds the OTLP integration with its trace exporter and propagator turned off."
    )
    def test_init_sentry_registers_otlp_integration_without_exporter_or_propagator(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        init_mock = Mock()
        monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
        monkeypatch.setenv("SENTRY_ENVIRONMENT", "bench")
        monkeypatch.setattr(sentry_sdk, "init", init_mock)
        monkeypatch.setattr(sentry_sdk, "set_tag", Mock())

        sentry_module.init_sentry("valkyrie-worker", environment="test")

        assert init_mock.call_args.kwargs["environment"] == "bench"
        assert init_mock.call_args.kwargs["include_local_variables"] is False
        assert init_mock.call_args.kwargs["max_request_body_size"] == "never"
        assert init_mock.call_args.kwargs["before_send_transaction"] is sentry_module._before_send_transaction
        integrations = init_mock.call_args.kwargs["integrations"]
        otlp_integrations = [i for i in integrations if isinstance(i, OTLPIntegration)]
        assert len(otlp_integrations) == 1, "expected exactly one OTLPIntegration in integrations="
        otlp = otlp_integrations[0]
        assert otlp.setup_otlp_traces_exporter is False
        assert otlp.setup_propagator is False

    @pytest.mark.description(
        "A raised 500 `HTTPException` reaches Sentry, and 502 and 503 responses for upstream failures do not."
    )
    def test_init_sentry_reports_only_internal_error_http_exceptions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        init_mock = Mock()
        monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
        monkeypatch.setattr(sentry_sdk, "init", init_mock)
        sentry_module.init_sentry("valkyrie-tracker", environment="test")
        options = init_mock.call_args.kwargs
        monkeypatch.undo()

        app = FastAPI()

        @app.get("/status/{status_code}")
        async def fail(status_code: int) -> None:
            raise HTTPException(status_code=status_code, detail="failed")

        events: list[Event] = []
        with sentry_sdk.init(
            dsn=options["dsn"],
            integrations=options["integrations"],
            transport=events.append,
        ):
            client = TestClient(app)
            statuses = [client.get(f"/status/{code}").status_code for code in (500, 502, 503)]
            sentry_sdk.flush()

        assert statuses == [500, 502, 503]
        assert [event["request"]["url"] for event in events if event.get("exception")] == [
            "http://testserver/status/500"
        ]

    @pytest.mark.description("Sentry logs carry the sandbox fields and prefer the current OpenTelemetry trace context.")
    def test_before_send_log_prefers_current_otel_trace_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        trace_id = "019e04c6fbf0397e32a8d9601f98e45c"
        span_id = "a1f0f4fc15b83e82"
        span_context = SimpleNamespace(
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            is_valid=True,
        )
        span = SimpleNamespace(get_span_context=lambda: span_context)
        log = cast(
            Log,
            {
                "body": "created sandbox",
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "span_id": None,
            },
        )
        monkeypatch.setattr("tracker.logging.context.trace.get_current_span", lambda: span)
        monkeypatch.setattr(
            sentry_module,
            "get_context_tags",
            lambda: {"request_id": "request-123", "benchmark_id": "benchmark-123", "task_id": ""},
        )
        monkeypatch.setattr(
            sentry_module,
            "get_sandbox_fields",
            lambda: {"sandbox_id": "sandbox-123", "sandbox_provider": "docker"},
        )

        result = _before_send_log()(log, {})

        assert result is log
        assert log["trace_id"] == trace_id
        assert log["span_id"] == span_id
        assert log["attributes"] == {
            "request_id": "request-123",
            "benchmark_id": "benchmark-123",
            "sandbox_id": "sandbox-123",
            "sandbox_provider": "docker",
        }

    @pytest.mark.description("A failed Sentry setup logs a warning.")
    def test_init_sentry_logs_warning_when_initialization_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        warnings: list[tuple[str, tuple[object, ...]]] = []

        def fake_warning(message: str, *args: object) -> None:
            warnings.append((message, args))

        monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
        monkeypatch.setattr(sentry_sdk, "init", Mock(side_effect=RuntimeError("bad dsn")))
        monkeypatch.setattr(sentry_module.logger, "warning", fake_warning)

        sentry_module.init_sentry("valkyrie-worker", environment="production")

        assert len(warnings) == 1
        assert warnings[0][0] == "Failed to initialize Sentry: %s: %s"
        assert warnings[0][1][:1] == ("RuntimeError",)
        assert str(warnings[0][1][1]) == "bad dsn"

    @pytest.mark.description("Sentry setup is skipped when the production DSN is missing.")
    def test_init_sentry_skips_missing_production_dsn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        init_mock = Mock()
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        monkeypatch.setattr(sentry_sdk, "init", init_mock)

        sentry_module.init_sentry("valkyrie-worker", environment="production")

        init_mock.assert_not_called()


@pytest.mark.description("Characterize SDK serialization, not Logfire registration or backend indexing.")
@pytest.mark.asyncio
@pytest.mark.parametrize("client_is_root", [True, False], ids=["client-root", "client-child"])
async def test_sentry_export_captures_task_identities_on_roots_and_children(
    monkeypatch: pytest.MonkeyPatch,
    client_is_root: bool,
) -> None:
    transactions: list[dict[str, Any]] = []

    class CaptureTransport(Transport):
        def capture_envelope(self, envelope: Envelope) -> None:
            for item in envelope.items:
                if item.type == "transaction":
                    transactions.append(json.loads(item.get_bytes()))

    monkeypatch.setattr(sentry_scope, "global_event_processors", list(sentry_scope.global_event_processors))
    provider = TracerProvider()
    provider.add_span_processor(tracing_module._ContextVarSpanProcessor())
    provider.add_span_processor(tracing_module._FilteredSentrySpanProcessor())
    tracer = provider.get_tracer(__name__)
    client = sentry_sdk.Client(
        dsn="https://public@example.com/1",
        transport=CaptureTransport,
        default_integrations=False,
        traces_sample_rate=1.0,
        instrumenter=INSTRUMENTER.OTEL,
        before_send=sentry_module._before_send,
        before_send_transaction=sentry_module._before_send_transaction,
    )
    identities = {
        "task-a": {
            "request_id": "request-a",
            "benchmark_id": "benchmark-a",
            "task_id": "task-a",
            "executor_dispatch_id": "dispatch-a",
            "attempt_started_at": "2026-04-01T12:00:00+00:00",
        },
        "task-b": {
            "request_id": "request-b",
            "benchmark_id": "benchmark-b",
            "task_id": "task-b",
            "executor_dispatch_id": "dispatch-b",
            "attempt_started_at": "2026-04-01T12:01:00+00:00",
        },
    }
    client_ids: dict[str, str] = {}
    response_ids: dict[str, str] = {}
    ready = 0
    release = asyncio.Event()

    async def request(task_id: str) -> None:
        nonlocal ready
        identity = identities[task_id]
        request_token = request_id_var.set(identity["request_id"])
        benchmark_token = benchmark_id_var.set(identity["benchmark_id"])
        dispatch_token = executor_dispatch_id_var.set(identity["executor_dispatch_id"])
        try:
            with sentry_module.task_scope(task_id, attempt_started_at=identity["attempt_started_at"]):
                with tracer.start_as_current_span(
                    "POST /execute",
                    context=Context() if client_is_root else None,
                    kind=trace.SpanKind.CLIENT,
                    attributes={"http.method": "POST", "http.url": "https://benchmark.example/execute"},
                ) as span:
                    client_ids[task_id] = f"{span.get_span_context().span_id:016x}"
                    ready += 1
                    if ready == 2:
                        release.set()
                    await release.wait()
                    with tracer.start_as_current_span("response.process") as response:
                        response_ids[task_id] = f"{response.get_span_context().span_id:016x}"
                    request_id_var.set("finishing-request")
                    benchmark_id_var.set("finishing-benchmark")
                    executor_dispatch_id_var.set("finishing-dispatch")
                    task_id_var.set("finishing-scope-task")
                    attempt_started_at_var.set("2026-04-01T13:00:00+00:00")
                    sentry_sdk.set_tag("task_id", "finishing-scope-task")
        finally:
            executor_dispatch_id_var.reset(dispatch_token)
            benchmark_id_var.reset(benchmark_token)
            request_id_var.reset(request_token)

    try:
        with sentry_sdk.new_scope() as scope:
            scope.set_client(client)
            scope.set_tag("component", "tracker")
            parent = nullcontext() if client_is_root else tracer.start_as_current_span("dispatch", context=Context())
            with parent:
                await asyncio.gather(request("task-a"), request("task-b"))
                scope.set_tag("task_id", "finishing-scope-task")
    finally:
        provider.shutdown()
        client.close()

    assert len(transactions) == (2 if client_is_root else 1)
    children = {span["span_id"]: span for event in transactions for span in event["spans"]}
    assert len(children) == (2 if client_is_root else 4)
    for task_id, identity in identities.items():
        if client_is_root:
            event = next(
                event for event in transactions if event["contexts"]["trace"]["span_id"] == client_ids[task_id]
            )
            captured_client = event["contexts"]["otel"]["attributes"]
            assert event["contexts"]["trace"]["op"] == "http.client"
            assert {key: event["tags"][key] for key in identity} == identity
            assert "http.url" not in event["tags"]
            trace_id = event["contexts"]["trace"]["trace_id"]
        else:
            event = transactions[0]
            client_span = children[client_ids[task_id]]
            captured_client = client_span["data"]
            assert client_span["parent_span_id"] == event["contexts"]["trace"]["span_id"]
            assert client_span["op"] == "http.client"
            trace_id = client_span["trace_id"]
            assert trace_id == event["contexts"]["trace"]["trace_id"]

        response = children[response_ids[task_id]]
        assert response["parent_span_id"] == client_ids[task_id]
        assert response["trace_id"] == trace_id
        for key, value in identity.items():
            assert captured_client[key] == value
            assert response["data"][key] == value
        assert event["tags"]["component"] == "tracker"

    if not client_is_root:
        assert transactions[0]["tags"]["task_id"] == "finishing-scope-task"


@pytest.mark.description("Each task scope keeps its sandbox events apart from other tasks and the outer scope.")
@pytest.mark.asyncio
async def test_task_scope_isolates_concurrent_sandbox_events_and_outer_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[Event] = []
    rows: dict[str, TaskState] = {}

    attempt_starts = {
        "task-a": datetime(2026, 4, 1, 12, tzinfo=UTC),
        "task-b": datetime(2026, 4, 1, 13, tzinfo=UTC),
    }

    async def run_tasks() -> None:
        ready = 0
        release = asyncio.Event()

        async def body(task_id: str, sandbox_id: str, *, fail: bool) -> dict[str, dict[str, object] | None]:
            nonlocal ready
            sentry_module.set_sandbox_context(SimpleNamespace(id=sandbox_id, name=f"{sandbox_id}-name"))
            sentry_sdk.capture_message("sandbox event")
            ready += 1
            if ready == 2:
                release.set()
            await release.wait()
            if fail:
                raise RuntimeError("outer failure")
            return {task_id: {"ok": True}}

        benchmark_id = uuid4()
        for task_id in ("task-a", "task-b"):
            rows[task_id] = TaskState(
                id=uuid4(),
                org_id=uuid4(),
                task_id=task_id,
                benchmark=benchmark_id,
                started_at=attempt_starts[task_id],
                status=TaskStatus.IN_PROGRESS,
                eval_resume_state=None,
                credited_wall_deadline_at=None,
                task_breakdown=None,
                build_phase=None,
                evaluation_phase=None,
            )

        control = Mock()
        control.ref = DispatchRef(benchmark_id=benchmark_id, dispatch_id=uuid4())
        control.fail = AsyncMock()
        task_a = task_execution.TrackedTask(body("task-a", "sandbox-a", fail=False), control, attempt_starts["task-a"])
        task_b = task_execution.TrackedTask(body("task-b", "sandbox-b", fail=True), control, attempt_starts["task-b"])
        await asyncio.gather(
            task_a.run(None, rows["task-a"]),
            task_b.run(None, rows["task-b"]),
        )

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=sentry_module._before_send,
    ):
        await run_tasks()
        sentry_sdk.capture_message("after tasks")

    task_events = [event for event in events if event.get("tags", {}).get("task_id")]
    assert len(task_events) == 3
    task_tags = [cast(dict[str, str], event.get("tags", {})) for event in task_events]
    assert {(tags["task_id"], tags["sandbox_id"], tags["attempt_started_at"]) for tags in task_tags} == {
        ("task-a", "sandbox-a", "2026-04-01T12:00:00"),
        ("task-b", "sandbox-b", "2026-04-01T13:00:00"),
    }
    exception_events = [event for event in task_events if "exception" in event]
    assert len(exception_events) == 1
    assert cast(dict[str, str], exception_events[0].get("tags")) == {
        "task_id": "task-b",
        "attempt_started_at": "2026-04-01T13:00:00",
        "sandbox_id": "sandbox-b",
        "sandbox_name": "sandbox-b-name",
        "failure_category": "unknown",
    }
    assert events[-1].get("tags") == {}


@pytest.mark.description(
    "Events from a retry carry the new attempt tags and no sandbox context from the earlier attempt."
)
@pytest.mark.asyncio
async def test_retry_attempt_clears_previous_sandbox_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[Event] = []

    class FakeBenchmarkService:
        async def run_with_sandbox_recovery(
            self,
            *,
            operation: Callable[[Any], Any],
            **_kwargs: object,
        ) -> dict[str, dict[str, object] | None]:
            try:
                await operation(SimpleNamespace(number=1))
            except SandboxSetupError:
                pass
            return await operation(SimpleNamespace(number=2))

    async def process_attempt(**kwargs: Any) -> dict[str, dict[str, object] | None]:
        if kwargs["recovery_attempt"].number == 1:
            sentry_module.set_sandbox_context(SimpleNamespace(id="sandbox-first", name="sandbox-first-name"))
            raise SandboxSetupError("retry after first sandbox")

        sentry_sdk.capture_exception(RuntimeError("second attempt failed before sandbox assignment"))
        return {"task-0": {"ok": True}}

    monkeypatch.setattr(task_execution, "_process_task_attempt", process_attempt)
    row = TaskState(
        id=uuid4(),
        org_id=uuid4(),
        task_id="task-0",
        benchmark=uuid4(),
        started_at=datetime(2026, 4, 1, 12, tzinfo=UTC),
        status=TaskStatus.IN_PROGRESS,
        eval_resume_state=None,
        credited_wall_deadline_at=None,
        task_breakdown=None,
        build_phase=None,
        evaluation_phase=None,
    )
    control = Mock()
    control.ref = DispatchRef(benchmark_id=row.benchmark, dispatch_id=uuid4())
    control.fail = AsyncMock()

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=_before_send(),
    ):
        with sentry_module.task_scope("task-0", attempt_started_at="2026-04-01T12:00:00+00:00"):
            result = await task_execution.process_task(
                task_row=row,
                start_benchmark_request=cast(
                    Any,
                    SimpleNamespace(
                        benchmark_name="retry-sandbox-context",
                        contract=SimpleNamespace(name="test-agent"),
                        dataset=None,
                    ),
                ),
                benchmark_service=cast(Any, FakeBenchmarkService()),
                benchmark_id=row.benchmark,
                task_id="task-0",
                runtime=cast(Any, SimpleNamespace(logs=None)),
                org=cast(Any, object()),
                sandbox_provider_config=cast(Any, object()),
                creation_semaphore=cast(Any, object()),
                control=control,
                started_by_email=None,
            )

    assert result == {"task-0": {"ok": True}}
    retry_event = next(event for event in events if "exception" in event)
    retry_tags = cast(dict[str, str], retry_event.get("tags", {}))
    assert retry_tags == {"task_id": "task-0", "attempt_started_at": "2026-04-01T12:00:00+00:00"}
    assert "sandbox" not in retry_event.get("contexts", {})


@pytest.mark.description(
    "An unhandled task error tags the Sentry event and the log record with the stored `failure_category`."
)
@pytest.mark.asyncio
async def test_unhandled_task_error_tags_sentry_and_log_with_stored_category(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    events: list[Event] = []
    committed = AsyncMock()
    row = TaskState(
        id=uuid4(),
        org_id=uuid4(),
        task_id="task-0",
        benchmark=uuid4(),
        started_at=datetime(2026, 4, 1, 12, tzinfo=UTC),
        status=TaskStatus.IN_PROGRESS,
        eval_resume_state=None,
        credited_wall_deadline_at=None,
        task_breakdown=None,
        build_phase=None,
        evaluation_phase=None,
    )
    control = Mock()
    control.ref = DispatchRef(benchmark_id=row.benchmark, dispatch_id=uuid4())
    control.fail = committed

    async def body() -> dict[str, dict[str, object] | None]:
        raise SandboxSetupError("provider rejected the sandbox request")

    monitor_logger = task_monitor_module.logger
    monkeypatch.setattr(monitor_logger, "handlers", [*monitor_logger.handlers, caplog.handler])
    monkeypatch.setattr(monitor_logger, "propagate", False)

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=_before_send(),
    ):
        task_monitor_module.logger.addHandler(caplog.handler)
        try:
            tracked = task_execution.TrackedTask(body(), control, row.started_at)
            await tracked.run(None, row)
        finally:
            task_monitor_module.logger.removeHandler(caplog.handler)

    stored_category = committed.await_args.args[0].failure.category
    assert stored_category == FailureCategory.INFRASTRUCTURE
    (exception_event,) = [event for event in events if "exception" in event]
    assert cast(dict[str, str], exception_event.get("tags", {}))["failure_category"] == stored_category.value
    (record,) = [r for r in caplog.records if "Task error was not handled" in r.getMessage()]
    assert getattr(record, "failure_category") == stored_category.value


@pytest.mark.description("The `failure_category` tag set for a run error is absent from the next Sentry event.")
def test_run_error_failure_category_tag_does_not_leak_to_later_events() -> None:
    events: list[Event] = []

    with sentry_sdk.init(
        dsn="https://public@example.com/1",
        transport=events.append,
        default_integrations=False,
        before_send=_before_send(),
    ):
        run_orchestration._capture_run_error(  # pyright: ignore[reportPrivateUsage]
            RuntimeError("runtime resolution failed"),
            UUID("00000000-0000-0000-0000-000000000811"),
            producer="tracker",
            operation="process_benchmark",
            category=FailureCategory.INFRASTRUCTURE,
        )
        sentry_sdk.capture_exception(RuntimeError("unrelated later failure in the same worker"))

    run_event, later_event = events
    assert cast(dict[str, str], run_event.get("tags", {}))["failure_category"] == "infrastructure"
    assert "failure_category" not in later_event.get("tags", {})
