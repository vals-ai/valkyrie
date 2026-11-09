"""Models for benchmark-service discovery and task selection."""

from valkyrie.sdk.models._generated.services import (
    BenchmarkServiceCatalogResponse,
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
    FetchBenchmarkTasksRequest,
    TaskIDsResponse,
)

__all__ = [
    "BenchmarkServiceCatalogResponse",
    "BenchmarkServiceEntry",
    "BenchmarkServiceHealth",
    "BenchmarkServicesRequest",
    "BenchmarkServicesResponse",
    "FetchBenchmarkTasksRequest",
    "TaskIDsResponse",
]
