import re
from datetime import UTC, datetime

from tracker.runtime.storage import ObjectStore, StoredObjectCopy

AGENTS_PREFIX = "agents"
BENCHMARKS_PREFIX = "benchmarks"
DATASETS_PREFIX = "datasets"
TASK_ARCHIVE_SUFFIX = ".zip"
TASK_ID_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"


def agent_bundle_key(agent_name: str) -> str:
    from tracker.agent.schemas import validate_agent_name

    validate_agent_name(agent_name)
    return f"{AGENTS_PREFIX}/{agent_name}.zip"


def benchmark_prefix(benchmark_id: str) -> str:
    return f"{BENCHMARKS_PREFIX}/{benchmark_id}/"


def dataset_blob_key(dataset_id: str, digest: str) -> str:
    return f"{DATASETS_PREFIX}/{dataset_id}/blobs/sha256/{digest.removeprefix('sha256:')}{TASK_ARCHIVE_SUFFIX}"


def dataset_file_key(dataset_id: str, digest: str) -> str:
    return f"{DATASETS_PREFIX}/{dataset_id}/files/sha256/{digest.removeprefix('sha256:')}"


def benchmark_agent_bundle_key(benchmark_id: str, agent_name: str) -> str:
    from tracker.agent.schemas import validate_agent_name

    validate_agent_name(agent_name)
    return f"{benchmark_prefix(benchmark_id)}{agent_name}.zip"


def task_artifact_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    return f"{benchmark_prefix(benchmark_id)}{task_id}/{output_name}"


STEPS_FOLDER = "steps"
STEP_OUTPUT = re.compile(rf"{STEPS_FOLDER}/[^/]+/agent_output\.tar\.gz")
EPOCH = datetime.min.replace(tzinfo=UTC)


def step_folder(step_name: str) -> str:
    return f"{STEPS_FOLDER}/{step_name}"


def step_output_name(step_name: str) -> str:
    return f"{step_folder(step_name)}/agent_output.tar.gz"


async def latest_step_output(store: ObjectStore, benchmark_id: str, task_id: str) -> str | None:
    prefix = task_artifact_key(benchmark_id, task_id, "")
    outputs = [
        stored
        async for stored in store.list_objects(f"{prefix}{STEPS_FOLDER}/")
        if STEP_OUTPUT.fullmatch(stored.key.removeprefix(prefix))
    ]
    if not outputs:
        return None
    return max(outputs, key=lambda stored: (stored.last_modified or EPOCH, stored.key)).key.removeprefix(prefix)


ATTEMPT_RECORD = "attempt.json"


def run_attempts_prefix(benchmark_id: str) -> str:
    return f"{BENCHMARKS_PREFIX}/attempts/{benchmark_id}/"


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
