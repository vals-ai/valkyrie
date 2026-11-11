from sqlmodel import Session

from tracker.database.models import Task
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.types import ActiveNativeOutput


def publish_native_output(session: Session, task: Task, descriptor: ActiveNativeOutput) -> None:
    task.active_native_output = descriptor
    BenchmarkRepository(session).save(task)


def clear_native_output(session: Session, task: Task, descriptor: ActiveNativeOutput) -> None:
    BenchmarkRepository(session).clear_native_output(
        task.id, descriptor.sandbox_id, descriptor.model_dump(mode="json")["attempt_started_at"]
    )
