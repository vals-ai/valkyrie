from __future__ import annotations
from valkyrie.sdk.models._validation import BenchmarkServiceEntryBase
from valkyrie.sdk.models._base import ResponseModel
from pydantic import BaseModel, Field
from typing import Annotated


class BenchmarkServiceEntry(BenchmarkServiceEntryBase):
    """One benchmark service available through the hosted catalog."""

    name: str
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None


class BenchmarkServiceHealth(ResponseModel):
    """Health-check result for one benchmark service."""

    name: str
    url: str
    healthy: bool
    latency_ms: int | None
    error: str | None = None


class BenchmarkServicesRequest(BaseModel):
    """Benchmark services to health-check through Tracker."""

    services: Annotated[list[BenchmarkServiceEntry], Field(validate_default=True)] = []


class BenchmarkServicesResponse(ResponseModel):
    """Health-check results for benchmark services."""

    services: list[BenchmarkServiceHealth]


class FetchBenchmarkTasksRequest(BaseModel):
    """Wire payload for discovering task IDs from a benchmark service."""

    benchmark_name: str
    dataset: str | None = None
    ignore_custom_services: bool = False
    service_headers: Annotated[dict[str, str], Field(repr=False)] = {}


class TaskIDsResponse(ResponseModel):
    """Task IDs returned by a benchmark service through Tracker."""

    task_ids: Annotated[
        list[str],
        Field(description="List of verified task IDs that exist in the benchmark"),
    ]


class BenchmarkServiceCatalogResponse(ResponseModel):
    """Benchmark services visible to the configured tenant."""

    services: list[BenchmarkServiceEntry]
