from typing import TypeAlias

from benchmark_service.client import BenchmarkServiceClient

from tracker.clients.dataset_tasks import DatasetTaskService

TaskService: TypeAlias = BenchmarkServiceClient | DatasetTaskService
