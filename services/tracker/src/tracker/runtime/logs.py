from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
import hashlib
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID


TASK_LOG_LINE_FIELDS = ("executor_dispatch_id", "sandbox_id", "sandbox_provider", "trace_id", "span_id")


@dataclass(frozen=True)
class TaskLogLine:
    received_at: float
    message: str
    fields: dict[str, str]


class BenchmarkLogSink(Protocol):
    async def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        raise NotImplementedError

    def write(self, stream_key: str, lines: Sequence[TaskLogLine]) -> None:
        raise NotImplementedError


class BenchmarkLogLocations(Protocol):
    def benchmark_location(self, benchmark_id: str) -> str:
        raise NotImplementedError

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        raise NotImplementedError


class LogProviderError(Exception):
    pass


@dataclass(frozen=True)
class LogEvent:
    timestamp: datetime
    message: str
    task_id: str | None = None
    ingestion_time: datetime | None = None
    event_id: str | None = None


@dataclass(frozen=True)
class LogPage:
    events: list[LogEvent]
    next_cursor: str | None = None
    available: bool = True


@dataclass(frozen=True)
class RunTaskLogReference:
    task_id: str
    started_at: datetime


@dataclass(frozen=True)
class TaskLogReference:
    run_id: UUID
    task_id: str
    started_at: datetime
    siblings: tuple[RunTaskLogReference, ...] | None = None
    strict_attempt: bool = False


@dataclass(frozen=True)
class RunLogReference:
    run_id: UUID
    tasks: tuple[RunTaskLogReference, ...] = ()


class LogProvider(Protocol):
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
        raise NotImplementedError

    def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        raise NotImplementedError


def sanitize_log_stream_name(task_id: str) -> str:
    escaped = task_id.replace("%", "%25").replace(":", "%3A").replace("*", "%2A")
    if escaped == task_id:
        return task_id
    digest = hashlib.sha256(task_id.encode()).hexdigest()[:16]
    return f"{escaped}-{digest}"


def task_log_stream_name(task_id: str, started_at: datetime) -> str:
    if started_at.utcoffset() is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    suffix = f"{int(started_at.timestamp() * 1_000_000):x}"
    return f"{sanitize_log_stream_name(task_id)}_{suffix}"
