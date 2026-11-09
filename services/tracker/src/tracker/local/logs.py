import asyncio
import fcntl
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ValidationError

from tracker.local.storage import local_path
from tracker.runtime.logs import (
    LogEvent,
    LogPage,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogLine,
    TaskLogReference,
    sanitize_log_stream_name,
    task_log_stream_name,
)


class _LogRecord(BaseModel):
    stream: str
    timestamp: float
    message: str
    fields: dict[str, str] = {}


class FilesystemLogs:
    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("Local log root must be absolute")
        self.root = root.resolve()

    def _path(self, benchmark_id: str) -> Path:
        return local_path(self.root, f"{benchmark_id}/logs.jsonl")

    async def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        path = self._path(benchmark_id)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(path.touch, exist_ok=True)

    def write(self, stream_key: str, lines: Sequence[TaskLogLine]) -> None:
        benchmark_id, stream = stream_key.split(":", 1)
        records = b"".join(
            _LogRecord(stream=stream, timestamp=line.received_at, message=line.message, fields=line.fields)
            .model_dump_json()
            .encode()
            + b"\n"
            for line in lines
        )
        with self._path(benchmark_id).open("ab") as output:
            fcntl.flock(output, fcntl.LOCK_EX)
            output.write(records)

    def benchmark_location(self, benchmark_id: str) -> str:
        return str(self._path(benchmark_id))

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        return self.benchmark_location(benchmark_id)

    async def fetch(
        self,
        reference: RunLogReference | TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        try:
            offset = int(cursor) if cursor is not None else 0
            if offset < 0:
                raise ValueError
        except ValueError:
            raise LogProviderError("Invalid local log cursor") from None

        start = start_time.timestamp() if start_time is not None else None
        end = end_time.timestamp() if end_time is not None else None
        tasks = (reference,) if isinstance(reference, TaskLogReference) else reference.tasks
        task_names = {sanitize_log_stream_name(task.task_id): task.task_id for task in tasks}

        def read() -> LogPage:
            path = self._path(str(reference.run_id))
            if not path.exists():
                return LogPage(events=[], available=False)
            events: list[LogEvent] = []
            with path.open("rb") as source:
                source.seek(offset)
                while line := source.readline():
                    if not line.endswith(b"\n"):
                        break
                    record = _LogRecord.model_validate_json(line)
                    if (
                        isinstance(reference, TaskLogReference)
                        and reference.strict_attempt
                        and record.stream != task_log_stream_name(reference.task_id, reference.started_at)
                    ):
                        continue
                    task = record.stream.rsplit("_", 1)[0]
                    if isinstance(reference, TaskLogReference) and task not in task_names:
                        continue
                    if query and query not in record.message:
                        continue
                    if start is not None and record.timestamp < start:
                        continue
                    if end is not None and record.timestamp > end:
                        continue
                    if len(events) == limit:
                        return LogPage(events=events, next_cursor=events[-1].event_id)
                    events.append(
                        LogEvent(
                            event_id=str(source.tell()),
                            task_id=task_names.get(task),
                            timestamp=datetime.fromtimestamp(record.timestamp, UTC),
                            message=record.message,
                        )
                    )
            return LogPage(events=events)

        try:
            return await asyncio.to_thread(read)
        except (OSError, ValidationError) as error:
            raise LogProviderError("Failed to read local task logs") from error

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncGenerator[LogEvent]:
        cursor: str | None = None
        run = RunLogReference(reference.run_id, (RunTaskLogReference(reference.task_id, reference.started_at),))
        start = start_time.timestamp() if start_time is not None else None
        end = end_time.timestamp() if end_time is not None else None
        while True:
            page = await self.fetch(reference if reference.strict_attempt else run, cursor=cursor)
            if reference.strict_attempt and not page.available:
                raise LogProviderError("Task log source is unavailable")
            for event in page.events:
                cursor = event.event_id
                if event.task_id != reference.task_id or (query and query not in event.message):
                    continue
                if start is not None and event.timestamp.timestamp() < start:
                    continue
                if end is not None and event.timestamp.timestamp() > end:
                    continue
                yield event
            if page.next_cursor is not None:
                continue
            if end_time is not None and datetime.now(UTC) >= end_time:
                return
            await asyncio.sleep(poll_interval)
