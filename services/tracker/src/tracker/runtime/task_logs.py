import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from types import TracebackType

from tracker.logging import get_logger
from tracker.logging.context import get_context_tags, get_sandbox_fields, get_trace_fields
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.logs import TASK_LOG_LINE_FIELDS, BenchmarkLogSink, TaskLogLine

logger = get_logger(__name__)
logging_executor = ThreadPoolExecutor(thread_name_prefix="tracker-task-logs")

_FLUSH_LINE_COUNT = 20
_MAX_LINE_CHARS = 100_000
_MAX_BUFFERED_SECONDS = 10


class TaskLogBuffer:
    def __init__(self, sink: BenchmarkLogSink, stream_key: str) -> None:
        self._sink = sink
        self._stream_key = stream_key
        self._lines: list[TaskLogLine] = []
        self._partial: TaskLogLine | None = None
        self._last_received_at = 0.0
        self._lines_since: float | None = None
        self._pending_writes: set[asyncio.Future[None]] = set()
        self._flush_task = asyncio.create_task(self._auto_flush())
        self._close_task: asyncio.Task[None] | None = None
        self.last_log_time = time.monotonic()

    async def __aenter__(self) -> "TaskLogBuffer":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def write(self, data: str) -> None:
        self.last_log_time = time.monotonic()
        self._last_received_at = max(self._last_received_at, time.time())
        context = get_context_tags() | get_sandbox_fields() | get_trace_fields()
        fields = {key: context[key] for key in TASK_LOG_LINE_FIELDS if context.get(key)}
        *complete, remainder = data.split("\n")
        for text in complete:
            self._append(self._continue_line(text, fields))
            self._partial = None
        if remainder:
            self._partial = self._continue_line(remainder, fields)
            if len(self._partial.message) >= _MAX_LINE_CHARS:
                self._append(self._partial)
                self._partial = None
        self.buffer_logs()

    def write_line(self, text: str) -> None:
        self.write(("\n" if self._partial is not None else "") + text.removesuffix("\n") + "\n")

    def _append(self, line: TaskLogLine) -> None:
        if self._lines_since is None:
            self._lines_since = time.monotonic()
        for start in range(0, max(len(line.message), 1), _MAX_LINE_CHARS):
            self._lines.append(
                TaskLogLine(
                    received_at=line.received_at,
                    message=line.message[start : start + _MAX_LINE_CHARS],
                    fields=line.fields,
                )
            )

    def _continue_line(self, text: str, fields: dict[str, str]) -> TaskLogLine:
        if self._partial is None:
            return TaskLogLine(received_at=self._last_received_at, message=text, fields=fields)
        return TaskLogLine(
            received_at=self._partial.received_at,
            message=self._partial.message + text,
            fields=self._partial.fields,
        )

    def buffer_logs(self, *, force_flush: bool = False) -> None:
        if force_flush and self._partial is not None:
            self._append(self._partial)
            self._partial = None
        if len(self._lines) >= _FLUSH_LINE_COUNT or force_flush:
            self._flush_lines()

    def _flush_lines(self) -> None:
        lines = self._lines
        self._lines = []
        self._lines_since = None
        if not lines:
            return

        future = asyncio.get_running_loop().run_in_executor(logging_executor, self._sink.write, self._stream_key, lines)
        self._pending_writes.add(future)
        future.add_done_callback(self._write_finished)

    def _write_finished(self, completed: asyncio.Future[None]) -> None:
        self._pending_writes.discard(completed)
        if not completed.cancelled() and (error := completed.exception()) is not None:
            logger.error("Task log write failed", exc_info=(type(error), error, error.__traceback__))

    async def _auto_flush(self) -> None:
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            if now - self.last_log_time >= 10:
                self.buffer_logs(force_flush=True)
            elif self._lines_since is not None and now - self._lines_since >= _MAX_BUFFERED_SECONDS:
                self._flush_lines()

    async def flush(self) -> None:
        self.buffer_logs(force_flush=True)
        if self._pending_writes:
            await asyncio.gather(*self._pending_writes, return_exceptions=True)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await finish_cleanup(self._close_task)

    async def _close(self) -> None:
        self._flush_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._flush_task

        await self.flush()
