from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from opentelemetry import trace

from tracker.auth import get_current_org
from tracker.aws.resolver import (
    resolve_org_aws_runtime,
    resolve_run_aws_runtime,
)
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org, Task
from tracker.repositories.benchmark import BenchmarkRepository, BenchmarkRepositoryDependency
from tracker.database.scoping import assert_org
from tracker.logging import benchmark_id_var
from tracker.local import config as local_config
from tracker.runtime.services import RuntimeServices


async def bind_benchmark_id(benchmark_id: UUID) -> UUID:
    value = str(benchmark_id)
    benchmark_id_var.set(value)
    trace.get_current_span().set_attribute("benchmark_id", value)
    return benchmark_id


TrackedBenchmarkId = Annotated[UUID, Depends(bind_benchmark_id)]


def get_run_benchmark(
    benchmark_id: TrackedBenchmarkId,
    repository: BenchmarkRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> Benchmark:
    return assert_org(repository.get(org.id, benchmark_id), org)


RunBenchmarkDependency = Annotated[Benchmark, Depends(get_run_benchmark)]


async def get_run_runtime(
    benchmark: RunBenchmarkDependency,
    org: Org = Depends(get_current_org),
) -> RuntimeServices:
    arguments = benchmark.arguments
    if arguments.environment == "local":
        from tracker.local.runtime import LocalRuntimeFactory

        return LocalRuntimeFactory.create_runtime(
            arguments.properties,
            org.id,
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )

    aws_runtime = resolve_run_aws_runtime(
        properties=arguments.properties,
        org=org,
    )
    return CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider=arguments.sandbox_provider,
        sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
    )


RunRuntimeDependency = Annotated[RuntimeServices, Depends(get_run_runtime)]


def get_agent_library_runtime(
    org: Org = Depends(get_current_org),
) -> RuntimeServices:
    if local_config.resources is not None:
        from tracker.local.runtime import LocalRuntimeFactory

        return LocalRuntimeFactory.create_runtime(local_config.resources, org.id)
    aws_runtime = resolve_org_aws_runtime(org)

    return CloudRuntimeFactory.create_runtime(aws_runtime)


AgentLibraryRuntimeDependency = Annotated[RuntimeServices, Depends(get_agent_library_runtime)]


def load_task_for_benchmark_or_404(
    benchmark: Benchmark, task_id: str, org: Org, repository: BenchmarkRepository
) -> Task:
    task = repository.get_task(org.id, benchmark.id, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
