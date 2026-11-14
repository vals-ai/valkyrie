from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import update
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, Org, RemoteGraderGrant, RemoteGraderRegistration, Task, TaskStatus


class RemoteGraderRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def lock_task(self, task_row_id: UUID) -> Task:
        return self._session.exec(select(Task).where(col(Task.id) == task_row_id).with_for_update()).one()

    def get_task(self, task_row_id: UUID) -> Task | None:
        return self._session.get(Task, task_row_id)

    def get_benchmark(self, benchmark_id: UUID) -> Benchmark | None:
        return self._session.get(Benchmark, benchmark_id)

    def get_org(self, org_id: UUID) -> Org | None:
        return self._session.get(Org, org_id)

    def get_grant(self, grant_id: UUID) -> RemoteGraderGrant | None:
        return self._session.get(RemoteGraderGrant, grant_id)

    def get_grant_for_attempt(
        self, task_row_id: UUID, attempt_id: str, worker_service: str
    ) -> RemoteGraderGrant | None:
        return self._session.exec(
            select(RemoteGraderGrant).where(
                col(RemoteGraderGrant.task_row_id) == task_row_id,
                col(RemoteGraderGrant.attempt_id) == attempt_id,
                col(RemoteGraderGrant.worker_service) == worker_service,
            )
        ).first()

    def lock_grant_by_digest(self, token_digest: str) -> RemoteGraderGrant | None:
        return self._session.exec(
            select(RemoteGraderGrant).where(col(RemoteGraderGrant.token_digest) == token_digest).with_for_update()
        ).first()

    def add_grant(self, grant: RemoteGraderGrant) -> None:
        self._session.add(grant)

    def find_registration(
        self, service: str, job_id: str, task_id: str, attempt_id: str
    ) -> RemoteGraderRegistration | None:
        return self._session.exec(
            select(RemoteGraderRegistration).where(
                col(RemoteGraderRegistration.worker_service) == service,
                col(RemoteGraderRegistration.job_id) == job_id,
                col(RemoteGraderRegistration.task_id) == task_id,
                col(RemoteGraderRegistration.attempt_id) == attempt_id,
            )
        ).first()

    def lock_registration(
        self, registration_id: UUID, service: str, job_id: str, task_id: str, attempt_id: str
    ) -> RemoteGraderRegistration | None:
        return self._session.exec(
            select(RemoteGraderRegistration)
            .where(
                col(RemoteGraderRegistration.id) == registration_id,
                col(RemoteGraderRegistration.worker_service) == service,
                col(RemoteGraderRegistration.job_id) == job_id,
                col(RemoteGraderRegistration.task_id) == task_id,
                col(RemoteGraderRegistration.attempt_id) == attempt_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).first()

    def add_registration(self, registration: RemoteGraderRegistration) -> None:
        self._session.add(registration)

    def get_registration_lease_id(self, registration_id: UUID) -> str | None:
        return self._session.exec(
            select(RemoteGraderRegistration.gateway_lease_id).where(col(RemoteGraderRegistration.id) == registration_id)
        ).one_or_none()

    def close_attempt(self, task_row_id: UUID, attempt_id: str) -> None:
        self._session.exec(
            update(RemoteGraderGrant)
            .where(
                col(RemoteGraderGrant.task_row_id) == task_row_id,
                col(RemoteGraderGrant.attempt_id) == attempt_id,
                col(RemoteGraderGrant.closed_at).is_(None),
            )
            .values(closed_at=datetime.now(UTC), token_ciphertext=None)
        )

    def close_stopped_tasks(self, benchmark_id: UUID) -> None:
        stopped_task_ids = select(Task.id).where(
            col(Task.benchmark) == benchmark_id, col(Task.status) == TaskStatus.STOPPED
        )
        self._session.exec(
            update(RemoteGraderGrant)
            .where(
                col(RemoteGraderGrant.task_row_id).in_(stopped_task_ids),
                col(RemoteGraderGrant.closed_at).is_(None),
            )
            .values(closed_at=datetime.now(UTC), token_ciphertext=None)
        )

    def clear_expired_child_tokens(self) -> None:
        self._session.exec(
            update(RemoteGraderRegistration)
            .where(
                col(RemoteGraderRegistration.token_ciphertext).is_not(None),
                col(RemoteGraderRegistration.expires_at) <= datetime.now(UTC).timestamp(),
            )
            .values(token_ciphertext=None)
        )
