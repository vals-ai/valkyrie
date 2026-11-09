import asyncio
import time
from unittest.mock import Mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from tracker.logging.context import sandbox_id_var, task_id_var
from tracker.aws.cloudwatch_logs import _EVENT_OVERHEAD_BYTES, _MAX_BATCH_BYTES, _log_event_batches
from tracker.runtime.logs import TaskLogLine
from tracker.runtime.task_logs import _MAX_LINE_CHARS, TaskLogBuffer


@pytest.mark.description(
    "Output split across writes becomes one line per newline, stamped when its first chunk arrived, with trace fields and blank lines kept."
)
async def test_lines_carry_receive_time_and_trace_context(monkeypatch: pytest.MonkeyPatch) -> None:
    receive_times = iter([100.0, 105.0, 103.0])
    monkeypatch.setattr("tracker.runtime.task_logs.time.time", lambda: next(receive_times))
    sink = Mock()
    tracer = TracerProvider().get_tracer(__name__)
    task_token = task_id_var.set("task-1")
    sandbox_token = sandbox_id_var.set("sandbox-1")

    try:
        with tracer.start_as_current_span("task") as span:
            async with TaskLogBuffer(sink, "run:stream") as logs:
                logs.write("first ")
                logs.write("line\nsecond line\n\nthird")
                logs.write(" line")
    finally:
        task_id_var.reset(task_token)
        sandbox_id_var.reset(sandbox_token)

    lines: list[TaskLogLine] = [line for call in sink.write.call_args_list for line in call.args[1]]
    assert [(line.message, line.received_at) for line in lines] == [
        ("first line", 100.0),
        ("second line", 105.0),
        ("", 105.0),
        ("third line", 105.0),
    ]

    trace_id = trace.format_trace_id(span.get_span_context().trace_id)
    assert {line.fields["trace_id"] for line in lines} == {trace_id}
    assert all("task_id" not in line.fields for line in lines)
    assert {line.fields["sandbox_id"] for line in lines} == {"sandbox-1"}


@pytest.mark.description("Output with no newline is cut into bounded lines instead of growing until the next newline.")
async def test_unterminated_output_is_cut_into_bounded_lines() -> None:
    sink = Mock()
    chunk = "\r" + "#" * 999

    async with TaskLogBuffer(sink, "run:stream") as logs:
        for _ in range(450):
            logs.write(chunk)
        logs.write("\n")

    lines: list[TaskLogLine] = [line for call in sink.write.call_args_list for line in call.args[1]]
    assert all(len(line.message) <= _MAX_LINE_CHARS for line in lines)
    assert "".join(line.message for line in lines) == chunk * 450


@pytest.mark.description(
    "Complete lines are flushed within seconds even while writes keep arriving, such as a setup heartbeat."
)
async def test_steady_output_flushes_complete_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.runtime.task_logs._MAX_BUFFERED_SECONDS", 1)
    sink = Mock()

    async with TaskLogBuffer(sink, "run:stream") as logs:
        for _ in range(12):
            logs.write_line("Waiting for setup")
            await asyncio.sleep(0.25)
        flushed: list[TaskLogLine] = [line for call in sink.write.call_args_list for line in call.args[1]]

    assert flushed
    assert {line.message for line in flushed} == {"Waiting for setup"}


@pytest.mark.description("A line arriving in chunks under steady output is never split across log events.")
async def test_steady_output_keeps_chunked_lines_whole(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.runtime.task_logs._MAX_BUFFERED_SECONDS", 1)
    sink = Mock()
    expected = [f"Collecting package-{index} (from -r requirements.txt)" for index in range(10)]

    async with TaskLogBuffer(sink, "run:stream") as logs:
        for line in expected:
            logs.write(line[:12])
            await asyncio.sleep(0.3)
            logs.write(line[12:] + "\n")
            await asyncio.sleep(0.01)

    lines: list[TaskLogLine] = [line for call in sink.write.call_args_list for line in call.args[1]]
    assert [line.message for line in lines] == expected


@pytest.mark.description("Each message written as a line ends any unterminated output and becomes its own line.")
async def test_message_lines_stay_separate() -> None:
    sink = Mock()

    async with TaskLogBuffer(sink, "run:stream") as logs:
        logs.write("partial output")
        logs.write_line("Uploaded problem statement")
        logs.write_line("Verified Docker daemon\n")

    lines: list[TaskLogLine] = [line for call in sink.write.call_args_list for line in call.args[1]]
    assert [line.message for line in lines] == [
        "partial output",
        "Uploaded problem statement",
        "Verified Docker daemon",
    ]


@pytest.mark.description(
    "A line of characters JSON escapes to six bytes still serializes under CloudWatch's 1 MiB event limit."
)
async def test_escaped_line_fits_cloudwatch_event_limit() -> None:
    sink = Mock()
    fields = {
        key: "f" * 64 for key in ("executor_dispatch_id", "sandbox_id", "sandbox_provider", "trace_id", "span_id")
    }

    async with TaskLogBuffer(sink, "run:stream") as logs:
        logs.write("\x00" * 250_000 + "\n")

    lines = [
        TaskLogLine(received_at=line.received_at, message=line.message, fields=fields)
        for call in sink.write.call_args_list
        for line in call.args[1]
    ]
    events = [event for batch in _log_event_batches(lines) for event in batch]
    assert "".join(line.message for line in lines) == "\x00" * 250_000
    assert all(
        len(event["message"].encode()) + _EVENT_OVERHEAD_BYTES <= min(1_048_576, _MAX_BATCH_BYTES) for event in events
    )


@pytest.mark.description(
    "`flush` returns only after buffered lines reach the sink, so a terminal status never precedes them."
)
async def test_flush_waits_for_the_sink_write() -> None:
    written: list[str] = []

    def slow_write(_stream_key: str, lines: list[TaskLogLine]) -> None:
        time.sleep(0.2)
        written.extend(line.message for line in lines)

    sink = Mock()
    sink.write.side_effect = slow_write

    async with TaskLogBuffer(sink, "run:stream") as logs:
        logs.write_line("[ERROR] boom")
        await logs.flush()
        assert written == ["[ERROR] boom"]
