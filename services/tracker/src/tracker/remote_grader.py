from hashlib import sha256
from secrets import token_urlsafe
from uuid import UUID

from fastapi import HTTPException
from sqlmodel import Session

from tracker.database.models import BenchmarkStatus, RemoteGraderGrant, Task, TaskStatus
from tracker.executor.control_queue import queued_sandbox_name
from tracker.remote_grader_services import WORKER_SERVICE_FOR_BENCHMARK
from tracker.remote_grader_crypt import decrypt_grader_token, encrypt_grader_token
from tracker.repositories.remote_grader import RemoteGraderRepository


def attempt_id_for(task: Task) -> str:
    return queued_sandbox_name(task.id, task.started_at).removeprefix("queued-")


def issue_remote_grader_grant(
    session: Session,
    *,
    benchmark_id: UUID,
    task_row_id: UUID,
    attempt_id: str,
    worker_service: str,
    gateway_secret_ref: str,
) -> str:
    repository = RemoteGraderRepository(session)
    task = repository.lock_task(task_row_id)
    benchmark = repository.get_benchmark(benchmark_id)
    if (
        benchmark is None
        or WORKER_SERVICE_FOR_BENCHMARK.get(benchmark.name) != worker_service
        or task.benchmark != benchmark.id
        or task.org_id != benchmark.org_id
        or attempt_id_for(task) != attempt_id
        or task.status not in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
        or benchmark.status != BenchmarkStatus.IN_PROGRESS
    ):
        raise HTTPException(status_code=409, detail="Tracker attempt cannot authorize remote grading")
    grant = repository.get_grant_for_attempt(task_row_id, attempt_id, worker_service)
    if grant is not None:
        if grant.closed_at is not None or grant.gateway_secret_ref != gateway_secret_ref:
            raise HTTPException(status_code=409, detail="Remote grading permission is closed")
        assert grant.token_ciphertext is not None
        return decrypt_grader_token(grant.token_ciphertext, record_id=grant.id, kind="grant")
    token = token_urlsafe(32)
    identity = {"benchmark_name": benchmark.name, "agent_name": "grader"}
    if benchmark.started_by_email:
        identity["email"] = benchmark.started_by_email
    grant = RemoteGraderGrant(
        token_digest=sha256(token.encode()).hexdigest(),
        org_id=benchmark.org_id,
        task_row_id=task.id,
        attempt_id=attempt_id,
        worker_service=worker_service,
        gateway_secret_ref=gateway_secret_ref,
        identity=identity,
    )
    grant.token_ciphertext = encrypt_grader_token(token, record_id=grant.id, kind="grant")
    repository.add_grant(grant)
    session.commit()
    return token
