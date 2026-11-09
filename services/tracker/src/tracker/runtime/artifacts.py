import re
from datetime import datetime

from tracker.agent.schemas import validate_agent_name
from tracker.runtime.storage import ObjectStore, StoredObjectCopy

AGENTS_PREFIX = "agents"
BENCHMARKS_PREFIX = "benchmarks"
DATASETS_PREFIX = "datasets"
TASK_ARCHIVE_SUFFIX = ".zip"
TASK_ID_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"


def agent_bundle_key(agent_name: str) -> str:
    validate_agent_name(agent_name)
    return f"{AGENTS_PREFIX}/{agent_name}.zip"


def benchmark_prefix(benchmark_id: str) -> str:
    return f"{BENCHMARKS_PREFIX}/{benchmark_id}/"


def dataset_tasks_prefix(dataset_id: str) -> str:
    return f"{DATASETS_PREFIX}/{dataset_id}/tasks/"


def dataset_task_key(dataset_id: str, task_id: str) -> str:
    return f"{dataset_tasks_prefix(dataset_id)}{task_id}{TASK_ARCHIVE_SUFFIX}"


def dataset_task_id(dataset_id: str, key: str) -> str | None:
    task_id = key.removeprefix(dataset_tasks_prefix(dataset_id)).removesuffix(TASK_ARCHIVE_SUFFIX)
    if key.endswith(TASK_ARCHIVE_SUFFIX) and re.fullmatch(TASK_ID_PATTERN, task_id):
        return task_id
    return None


def benchmark_agent_bundle_key(benchmark_id: str, agent_name: str) -> str:
    validate_agent_name(agent_name)
    return f"{benchmark_prefix(benchmark_id)}{agent_name}.zip"


def task_artifact_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    return f"{benchmark_prefix(benchmark_id)}{task_id}/{output_name}"


async def copy_agent_to_benchmark(
    store: ObjectStore,
    benchmark_id: str,
    agent_name: str,
) -> StoredObjectCopy | None:
    source_key = agent_bundle_key(agent_name)
    destination_key = benchmark_agent_bundle_key(benchmark_id, agent_name)
    if await store.exists(destination_key):
        return None

    return await store.copy(source_key, destination_key)


async def list_agents(store: ObjectStore) -> list[tuple[str, datetime | None]]:
    agents: list[tuple[str, datetime | None]] = []
    prefix = f"{AGENTS_PREFIX}/"
    async for stored_object in store.list_objects(prefix):
        tail = stored_object.key.removeprefix(prefix)
        if tail.endswith(".zip"):
            agents.append((tail.removesuffix(".zip"), stored_object.last_modified))
    return agents
