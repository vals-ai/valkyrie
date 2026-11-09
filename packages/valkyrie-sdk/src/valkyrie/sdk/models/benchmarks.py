"""Models for hosted benchmark and task inspection."""

from typing import Literal

from pydantic import BaseModel, Field

from valkyrie.sdk.models._generated.benchmarks import (
    BenchmarkStatusEntry,
    BenchmarkStatusResponse,
    SingleBenchmarkResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskBreakdown,
    TaskSummary,
    TasksResponse,
)
from valkyrie.sdk.models._generated.runs import Order, TaskStatus

__all__ = [
    "BenchmarkStatusEntry",
    "BenchmarkStatusResponse",
    "FetchTasksRequest",
    "SingleBenchmarkResponse",
    "SingleTaskResponse",
    "TaskArtifactsResponse",
    "TaskBreakdown",
    "TaskSummary",
    "TasksResponse",
]


class FetchTasksRequest(BaseModel):
    """Filters, sorting, and pagination for a benchmark's tasks."""

    status: list[TaskStatus] | None = None
    task_id_search: str | None = None
    sort: Literal["task_id", "started_at", "duration", "status"] = "started_at"
    sort_dir: Order = Order.DESC
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
