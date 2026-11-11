import asyncio

from sqlalchemy import Connection, Engine
from sqlmodel import Session

from tracker.services.datasets.ref import HostedDataset
from tracker.repositories.dataset_version import DatasetVersionRepository


class UnresolvedDatasetError(ValueError):
    pass


def _tasks(bind: Engine | Connection, ref: HostedDataset) -> dict[str, str]:
    if ref.dataset_id is None or ref.revision is None:
        raise UnresolvedDatasetError(f"The dataset {ref.benchmark}/{ref.dataset} has not been resolved to a version")
    with Session(bind=bind) as session:
        tasks = DatasetVersionRepository(session).task_digests(ref.dataset_id, ref.revision)
    if tasks is None:
        raise UnresolvedDatasetError(f"The dataset {ref.benchmark}/{ref.dataset} has no revision {ref.revision}")
    return tasks


async def dataset_tasks(bind: Engine | Connection, ref: HostedDataset) -> dict[str, str]:
    return await asyncio.to_thread(_tasks, bind, ref)
