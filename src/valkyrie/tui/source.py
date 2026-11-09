from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID

from valkyrie.sdk import LoginSession, ValkyrieClient, ValkyrieConfig, ValkyrieSDKError
from valkyrie.sdk.models import (
    BenchmarkStatus,
    BenchmarkStatusEntry,
    BenchmarkTableRow,
    DatasetResponse,
    DatasetTaskResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchTasksRequest,
    LogEvent,
    SingleTaskResponse,
    TasksResponse,
    TaskStatus,
)

TASK_PAGE_LIMIT = 500
URGENT_TASKS = [TaskStatus.ERROR, TaskStatus.EVALUATING, TaskStatus.IN_PROGRESS, TaskStatus.BUILDING]
ACTIVE_TASKS = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
OTHER_TASKS = [TaskStatus.PENDING, TaskStatus.STOPPED, TaskStatus.FINISHED]
TRIAGE_ORDER = URGENT_TASKS + OTHER_TASKS
FINAL_STATUSES = frozenset({BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED})


@dataclass(frozen=True)
class Environment:
    name: str
    url: str

    @classmethod
    def of(cls, url: str) -> Environment:
        return cls(urlparse(url).netloc or url, url)


class RunSource(Protocol):
    environment: Environment

    async def runs(self, limit: int) -> list[BenchmarkTableRow]: ...

    async def statuses(self, run_ids: Sequence[UUID]) -> list[BenchmarkStatusEntry]: ...

    async def datasets(self) -> list[DatasetResponse]: ...

    async def dataset_tasks(self, benchmark: str, dataset: str) -> list[DatasetTaskResponse]: ...

    def watch_run(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]: ...

    async def tasks(self, run_id: UUID) -> TasksResponse: ...

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse: ...

    async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]: ...

    def stream_logs(self, run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]: ...

    async def close(self) -> None: ...


class TrackerSource:
    def __init__(self, client: ValkyrieClient, environment: Environment) -> None:
        self._client = client
        self.environment = environment

    @classmethod
    def from_cli_config(cls) -> TrackerSource:
        return cls.from_config(ValkyrieConfig())

    @classmethod
    def from_config(cls, config: ValkyrieConfig) -> TrackerSource:
        return cls(ValkyrieClient(config), Environment.of(config.url))

    async def runs(self, limit: int) -> list[BenchmarkTableRow]:
        page = await self._client.runs.list(FetchBenchmarksRequest(limit=limit))
        return page.benchmarks

    async def statuses(self, run_ids: Sequence[UUID]) -> list[BenchmarkStatusEntry]:
        response = await self._client.benchmarks.statuses(run_ids)
        return response.entries

    async def datasets(self) -> list[DatasetResponse]:
        benchmarks = (await self._client.benchmark_definitions.list()).benchmarks
        listings = await asyncio.gather(*(self._client.datasets.list(benchmark.name) for benchmark in benchmarks))
        return [dataset for listing in listings for dataset in listing.datasets]

    async def dataset_tasks(self, benchmark: str, dataset: str) -> list[DatasetTaskResponse]:
        return (await self._client.datasets.tasks.list_all(benchmark, dataset)).tasks

    def watch_run(self, run_id: UUID) -> AsyncIterator[FetchBenchmarkResponse]:
        return self._client.runs.stream(run_id)

    async def tasks(self, run_id: UUID) -> TasksResponse:
        urgent, rest = await asyncio.gather(
            self._client.benchmarks.tasks(
                run_id, FetchTasksRequest(status=URGENT_TASKS, sort="status", limit=TASK_PAGE_LIMIT)
            ),
            self._client.benchmarks.tasks(run_id, FetchTasksRequest(status=OTHER_TASKS, limit=TASK_PAGE_LIMIT)),
        )
        room = max(TASK_PAGE_LIMIT - len(urgent.tasks), 0)
        return TasksResponse(tasks=urgent.tasks + rest.tasks[:room], total_count=urgent.total_count + rest.total_count)

    async def task(self, run_id: UUID, task_id: str) -> SingleTaskResponse:
        try:
            return await self._client.benchmarks.task(run_id, task_id)
        except ValueError as error:
            raise ValkyrieSDKError(f"No detail for this task: {error}") from error

    async def fetch_logs(self, run_id: UUID, task_id: str) -> list[LogEvent]:
        return await self._client.logs.fetch_task(run_id, task_id)

    def stream_logs(self, run_id: UUID, task_id: str) -> AsyncIterator[LogEvent]:
        return self._client.logs.stream_task(run_id, task_id)

    async def close(self) -> None:
        await self._client.close()


class Switcher(Protocol):
    def choices(self) -> list[Environment]: ...

    def open(self, url: str) -> RunSource: ...


class TrackerSwitcher:
    def choices(self) -> list[Environment]:
        return [Environment.of(login.tracker_url) for login in LoginSession.saved()]

    def open(self, url: str) -> RunSource:
        return TrackerSource.from_config(ValkyrieConfig(url=url))
