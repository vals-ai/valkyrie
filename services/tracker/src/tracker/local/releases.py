import hashlib
from pathlib import Path
from uuid import uuid4

from sqlmodel import Session

from executor_protocol import SUPPORTED_PROTOCOL_VERSION, source_executor_artifact_uri
from tracker.database.models import ExecutorRelease
from tracker.executor.release_control import (
    register_release,
    promote_release,
    lock_executor_admission,
    select_active_release,
)


def register_source_release(session: Session, source_root: Path) -> ExecutorRelease:
    artifact_uri = source_executor_artifact_uri(source_root.resolve())
    artifact_digest = hashlib.sha256(artifact_uri.encode()).hexdigest()

    admission = lock_executor_admission(session)
    if admission.release_id is not None:
        active = select_active_release(session)
        if (
            active.artifact_uri == artifact_uri
            and active.artifact_digest == artifact_digest
            and active.protocol_version == SUPPORTED_PROTOCOL_VERSION
        ):
            return active

    candidate = ExecutorRelease(
        id=f"source-{uuid4()}",
        artifact_uri=artifact_uri,
        artifact_digest=artifact_digest,
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
    )
    register_release(session, candidate)
    return promote_release(session, candidate.id)


def register_checkout_release() -> None:
    from tracker.database.session import engine

    source_root = Path(__file__).resolve().parents[2]
    with Session(engine) as session:
        release = register_source_release(session, source_root)
        session.commit()
        print(f"Executor source release ready: {release.id} ({source_root})")


if __name__ == "__main__":
    register_checkout_release()
