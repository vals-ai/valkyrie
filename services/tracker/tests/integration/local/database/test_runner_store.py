import asyncio
import base64
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from executor_protocol import MANAGED_EXECUTION_PROTOCOL_VERSION
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorRelease,
    Org,
    Task,
)
from executor_protocol import ExecutorDispatchStatus
from tracker.types import AgentContractRequest
from tracker.database.session import get_session
from tracker.domain import BenchmarkStatus, ExecutorDispatchKind, FailureCategory, TaskStatus
from tracker.executor.control_app import app as control_app
from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import (
    AckDispatchRequest,
    AckDispatchResponse,
    ClaimDispatchRequest,
    ClaimDispatchResponse,
    DispatchRef,
    FailedFinishDispatchRequest,
    FinishDispatchRequest,
    FinishDispatchResponse,
    RenewDispatchRequest,
    RenewDispatchResponse,
)
from tracker.executor import dispatch_recovery
from tracker.executor.dispatch_control import admit_recovery_dispatch, admit_start_dispatch
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.release_control import (
    create_executor_dispatch,
    pin_benchmark_to_release,
    promote_release,
    register_release,
)
from tracker.services.runs.stop import apply_stop_benchmark


@pytest.fixture
async def control_clients(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[Callable[[UUID, str], ControlClient], None]:
    monkeypatch.setenv("EXECUTOR_BOOTSTRAP_KEY", "claim-test-bootstrap")

    def server_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(control_app.dependency_overrides, get_session, server_session)
    clients: list[ControlClient] = []

    def create(dispatch_id: UUID, key: str) -> ControlClient:
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=control_app), base_url="http://control.test")
        client = ControlClient(
            http=http, dispatch_id=dispatch_id, invocation_key=key, bootstrap_key="claim-test-bootstrap"
        )
        clients.append(client)
        return client

    yield create
    for client in clients:
        await client.close()


async def _claim(client: ControlClient, dispatch: ExecutorDispatch) -> ClaimDispatchResponse:
    return await client.call("claim_dispatch", ClaimDispatchRequest(dispatch_id=dispatch.id), ClaimDispatchResponse)


def _ref(dispatch: ExecutorDispatch) -> DispatchRef:
    return DispatchRef(benchmark_id=dispatch.benchmark_id, dispatch_id=dispatch.id)


def _sealed_case(session: Session, monkeypatch: pytest.MonkeyPatch) -> ExecutorDispatch:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    org = Org(id=uuid4(), name=f"runner-claim-{uuid4()}")
    benchmark = make_benchmark(
        name="runner-claim",
        org_id=org.id,
        contract=AgentContractRequest(name="claim-agent", install_cmd="true", run_cmd="true"),
        status=BenchmarkStatus.IN_PROGRESS,
    )
    release = ExecutorRelease(
        id=f"runner-claim-{uuid4()}",
        artifact_uri="source:///executor-test/src",
        artifact_digest="a" * 64,
        protocol_version=MANAGED_EXECUTION_PROTOCOL_VERSION,
        created_at=datetime.now(UTC),
    )
    session.add(org)
    session.flush()
    register_release(session, release)
    pin_benchmark_to_release(benchmark, release)
    session.add(benchmark)
    session.flush()
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.assigned_task_ids = ["task-0"]
    session.add(dispatch)
    sealed = seal_payload(
        dispatch.id,
        {
            "start_benchmark_request_json": {
                "request_id": "admitted-request",
                "service_headers": {"authorization": "unique-persisted-dispatch-secret-marker"},
            },
            "benchmark_id_str": str(benchmark.id),
            "verified_task_ids": ["task-0"],
            "telemetry_context_json": {"request_id": "admitted-request", "trace_headers": {}},
        },
        generate_payload_key(dispatch.id),
    )
    session.add(
        ExecutorDispatchPayload(
            dispatch_id=dispatch.id,
            ciphertext=sealed.ciphertext,
            encrypted_data_key=sealed.encrypted_data_key,
            nonce=sealed.nonce,
            created_at=datetime.now(UTC),
        )
    )
    session.commit()
    return dispatch


@pytest.mark.asyncio
@pytest.mark.description(
    "Claim, acknowledgement, and finish operations enforce dispatch ownership while sibling work remains isolated."
)
async def test_postgres_control_fences_claim_ack_finish_and_sibling(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    first = _sealed_case(postgres_session, monkeypatch)
    benchmark = postgres_session.get(Benchmark, first.benchmark_id)
    assert benchmark is not None
    release = postgres_session.get(ExecutorRelease, first.executor_release_id)
    assert release is not None
    sibling = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.RETRY, dispatch_id=uuid4())
    sibling.assigned_task_ids = ["task-0", "newer-task"]
    postgres_session.add(sibling)
    task = make_task(
        benchmark, "task-0", status=TaskStatus.IN_PROGRESS, started_at=sibling.created_at - timedelta(seconds=1)
    )
    newer_task = make_task(
        benchmark, "newer-task", status=TaskStatus.IN_PROGRESS, started_at=sibling.created_at - timedelta(seconds=1)
    )
    postgres_session.add_all([task, newer_task])
    postgres_session.commit()
    sealed = seal_payload(
        sibling.id,
        {
            "start_benchmark_request_json": {"request_id": "sibling-request"},
            "benchmark_id_str": str(benchmark.id),
            "verified_task_ids": sibling.assigned_task_ids,
        },
        generate_payload_key(sibling.id),
    )
    postgres_session.add(
        ExecutorDispatchPayload(
            dispatch_id=sibling.id,
            ciphertext=sealed.ciphertext,
            encrypted_data_key=sealed.encrypted_data_key,
            nonce=sealed.nonce,
            created_at=datetime.now(UTC),
        )
    )
    postgres_session.commit()
    persisted = postgres_session.execute(
        text("SELECT ciphertext, encrypted_data_key, nonce FROM executor_dispatch_payload WHERE dispatch_id = :id"),
        {"id": first.id},
    ).one()
    assert all(b"unique-persisted-dispatch-secret-marker" not in bytes(field) for field in persisted)
    owner = control_clients(first.id, "first-owner")
    other = control_clients(sibling.id, "sibling-owner")
    claim = await _claim(owner, first)
    assert claim.claimed
    assert claim.payload["start_benchmark_request_json"]["service_headers"]["authorization"] == (
        "unique-persisted-dispatch-secret-marker"
    )
    assert postgres_session.get(ExecutorDispatchPayload, first.id) is not None
    assert (await _claim(owner, first)).claimed
    assert (await _claim(control_clients(first.id, "wrong-owner"), first)).claimed is False
    assert (await _claim(other, sibling)).claimed
    ack = await owner.call("ack_dispatch", AckDispatchRequest(ref=_ref(first)), AckDispatchResponse)
    assert ack.acknowledged
    assert (await owner.call("ack_dispatch", AckDispatchRequest(ref=_ref(first)), AckDispatchResponse)).acknowledged
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, first.id) is None
    assert postgres_session.get(ExecutorDispatchPayload, sibling.id) is not None
    finish = await owner.call(
        "finish_dispatch", FinishDispatchRequest(ref=_ref(first), succeeded=True), FinishDispatchResponse
    )
    assert finish.finished
    failed = await other.call(
        "finish_dispatch",
        FailedFinishDispatchRequest(
            ref=_ref(sibling),
            succeeded=False,
            error_message="Executor dispatch failed",
            error_type="ExecutorDispatchFailed",
            category=FailureCategory.INFRASTRUCTURE,
        ),
        FinishDispatchResponse,
    )
    assert failed.finished
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, first.id).status == ExecutorDispatchStatus.FINISHED
    assert postgres_session.get(ExecutorDispatch, sibling.id).status == ExecutorDispatchStatus.FAILED
    assert postgres_session.get(Task, task.id).status == TaskStatus.ERROR
    assert postgres_session.get(Task, newer_task.id).status == TaskStatus.ERROR
    assert postgres_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.ERROR


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,stopped,expired,locked,mismatch,expected",
    [
        (ExecutorDispatchStatus.RUNNING, False, False, False, False, (True, True, False)),
        (ExecutorDispatchStatus.RUNNING, True, False, False, False, (True, True, True)),
        (ExecutorDispatchStatus.RUNNING, False, True, False, False, (False, False, False)),
        (ExecutorDispatchStatus.FINISHED, False, False, False, False, (False, False, False)),
        (ExecutorDispatchStatus.RUNNING, False, False, True, False, (False, True, False)),
        (ExecutorDispatchStatus.RUNNING, False, False, False, True, None),
    ],
)
@pytest.mark.description(
    "Renewal classifies one dispatch across status, expiry, stop, lock, and reference mismatch states."
)
async def test_renew_classifies_one_dispatch_without_batching(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
    status: ExecutorDispatchStatus,
    stopped: bool,
    expired: bool,
    locked: bool,
    mismatch: bool,
    expected: tuple[bool, bool, bool] | None,
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    owner = control_clients(dispatch.id, "owner")
    assert (await _claim(owner, dispatch)).claimed
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    dispatch.status = status
    past = datetime.now(UTC) - timedelta(minutes=1)
    future = datetime.now(UTC) + timedelta(seconds=10)
    dispatch.lease_expires_at = past if expired else future
    benchmark.status = BenchmarkStatus.STOPPED if stopped else BenchmarkStatus.IN_PROGRESS
    postgres_session.add_all([dispatch, benchmark])
    postgres_session.commit()
    blocker = postgres_engine.raw_connection() if locked else None
    try:
        if blocker is not None:
            with blocker.cursor() as cursor:
                cursor.execute("SELECT id FROM executordispatch WHERE id = %s FOR UPDATE", (str(dispatch.id),))
        ref = DispatchRef(benchmark_id=benchmark.org_id if mismatch else benchmark.id, dispatch_id=dispatch.id)
        if mismatch:
            with pytest.raises(httpx.HTTPStatusError) as caught:
                await owner.call("renew_dispatch", RenewDispatchRequest(ref=ref), RenewDispatchResponse)
            assert caught.value.response.status_code == 403
        else:
            result = await asyncio.wait_for(
                owner.call(
                    "renew_dispatch",
                    RenewDispatchRequest(ref=ref),
                    RenewDispatchResponse,
                ),
                timeout=3,
            )
            assert (result.renewed, result.live, result.stopped) == expected
    finally:
        if blocker is not None:
            blocker.rollback()
            blocker.close()
    postgres_session.expire_all()
    persisted = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert persisted is not None
    assert (persisted.lease_expires_at > future.replace(tzinfo=None)) is (expected is not None and expected[0])


@pytest.mark.asyncio
@pytest.mark.description(
    "A dispatch lease renews while the benchmark table is locked, then classifies the benchmark after unlock."
)
async def test_renew_commits_dispatch_lease_while_benchmark_table_is_locked(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    owner = control_clients(dispatch.id, "owner")
    assert (await _claim(owner, dispatch)).claimed
    postgres_session.refresh(dispatch)
    blocker = postgres_engine.raw_connection()
    try:
        with blocker.cursor() as cursor:
            cursor.execute("LOCK TABLE benchmark IN ACCESS EXCLUSIVE MODE")
        before = postgres_session.get(ExecutorDispatch, dispatch.id).lease_expires_at
        response = await asyncio.wait_for(
            owner.call(
                "renew_dispatch",
                RenewDispatchRequest(ref=_ref(dispatch)),
                RenewDispatchResponse,
            ),
            timeout=4,
        )
        assert response.renewed and response.live is None and response.stopped is None
        postgres_session.expire_all()
        assert postgres_session.get(ExecutorDispatch, dispatch.id).lease_expires_at > before
    finally:
        blocker.rollback()
        blocker.close()
    classified = await owner.call("renew_dispatch", RenewDispatchRequest(ref=_ref(dispatch)), RenewDispatchResponse)
    assert classified.renewed and classified.live and classified.stopped is False


@pytest.mark.description(
    "Expired queued and running dispatches leave no assigned task active after recovery, alone or as siblings."
)
@pytest.mark.parametrize("dispatch_count", [1, 2])
@pytest.mark.parametrize("running", [False, True])
def test_expiry_cleans_all_admitted_assignments(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, dispatch_count: int, running: bool
) -> None:
    org = Org(id=uuid4(), name=f"review-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"review-{uuid4()}",
        artifact_uri="source:///executor-test/review/src",
        artifact_digest="a" * 64,
        protocol_version=MANAGED_EXECUTION_PROTOCOL_VERSION,
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    promote_release(postgres_session, release.id)
    postgres_session.commit()
    tasks: list[Task] = []
    dispatches: list[ExecutorDispatch] = []
    for index in range(dispatch_count):
        task = Task(org_id=org.id, benchmark=benchmark.id, task_id=f"task-{index}")
        postgres_session.add(task)
        if index == 0:
            dispatch = admit_start_dispatch(
                postgres_session, benchmark=benchmark, dispatch_id=uuid4(), task_ids=[task.task_id]
            )
        else:
            dispatch = admit_recovery_dispatch(
                postgres_session,
                benchmark=benchmark,
                pre_action_status=benchmark.status,
                dispatch_id=uuid4(),
                kind=ExecutorDispatchKind.RESUME,
                task_ids=[task.task_id],
            )
        assert task.started_at is not None
        assert task.started_at <= dispatch.created_at
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(minutes=1)
        if running:
            dispatch.status = ExecutorDispatchStatus.RUNNING
            dispatch.started_at = datetime.now(UTC) - timedelta(minutes=6)
            dispatch.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
            task.status = TaskStatus.IN_PROGRESS
        tasks.append(task)
        dispatches.append(dispatch)
    postgres_session.commit()

    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setattr(dispatch_recovery, "engine", postgres_session.get_bind())
    assert dispatch_recovery.reconcile_dispatches_once() == dispatch_count
    for row in [benchmark, *tasks, *dispatches]:
        postgres_session.refresh(row)

    assert benchmark.status == BenchmarkStatus.ERROR
    assert all(row.status == ExecutorDispatchStatus.FAILED for row in dispatches)
    assert [row.status for row in tasks] == [TaskStatus.ERROR] * dispatch_count
    errors = postgres_session.exec(
        select(ErrorResult).where(col(ErrorResult.task).in_([row.id for row in tasks]))
    ).all()
    assert len(errors) == dispatch_count


@pytest.mark.asyncio
@pytest.mark.description(
    "Concurrent claimants produce one payload owner and acknowledgement removes only the claimed payload."
)
async def test_concurrent_claimants_have_one_payload_and_owner(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    dispatch.ecs_task_arn = "arn:aws:ecs:us-east-1:123456789012:task/runner"
    postgres_session.add(dispatch)
    postgres_session.commit()
    first = control_clients(dispatch.id, "first-owner")
    rival = control_clients(dispatch.id, "rival-owner")
    claims = await asyncio.gather(_claim(first, dispatch), _claim(rival, dispatch))
    winners = [claim for claim in claims if claim.claimed]
    assert len(winners) == 1
    assert winners[0].payload["start_benchmark_request_json"]["request_id"] == "admitted-request"
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None
    claimed = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert claimed.status == ExecutorDispatchStatus.RUNNING
    assert claimed.ecs_task_arn == "arn:aws:ecs:us-east-1:123456789012:task/runner"
    assert claimed.lease_expires_at is None
    owner = first if claims[0].claimed else rival
    assert (await owner.call("ack_dispatch", AckDispatchRequest(ref=_ref(dispatch)), AckDispatchResponse)).acknowledged
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None


@pytest.mark.asyncio
@pytest.mark.description("A corrupt local payload fails as infrastructure before a claim is recorded.")
async def test_corrupt_local_payload_records_infrastructure_failure_without_claim(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    task = make_task(benchmark, "task-0", status=TaskStatus.PENDING)
    task.started_at = dispatch.created_at - timedelta(seconds=1)
    postgres_session.add(task)
    postgres_session.commit()
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"z" * 32).decode())

    with pytest.raises(httpx.HTTPStatusError) as caught:
        await _claim(control_clients(dispatch.id, "claim-owner"), dispatch)
    assert caught.value.response.status_code == 409
    postgres_session.expire_all()
    failed = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert failed.status == ExecutorDispatchStatus.FAILED
    assert failed.failure_reason == "PAYLOAD_DECRYPT_FAILED"
    assert postgres_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.ERROR
    error = postgres_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
    assert error.error_message == "Executor payload decrypt failed: InvalidTag"
    assert error.error_type == "InvalidTag"
    assert error.cause_code == "PAYLOAD_DECRYPT_FAILED"
    assert error.category == FailureCategory.INFRASTRUCTURE


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["expired", "superseded", "nonqueued"])
@pytest.mark.description(
    "Expired, superseded, and nonqueued dispatches retain payloads without reading or deleting them."
)
async def test_nonclaimable_dispatch_never_reads_or_deletes_payload(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
    state: str,
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    if state == "expired":
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    elif state == "superseded":
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.failure_reason = "SUPERSEDED"
    else:
        dispatch.status = ExecutorDispatchStatus.FINISHED
    postgres_session.add(dispatch)
    postgres_session.commit()
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"z" * 32).decode())
    assert not (await _claim(control_clients(dispatch.id, "claim-owner"), dispatch)).claimed
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None


@pytest.mark.asyncio
@pytest.mark.description("A queued dispatch with no payload returns an error without committing a claim.")
async def test_missing_queued_payload_is_error_without_claim_commit(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    row = postgres_session.get(ExecutorDispatchPayload, dispatch.id)
    assert row is not None
    postgres_session.delete(row)
    postgres_session.commit()
    with pytest.raises(httpx.HTTPStatusError) as caught:
        await _claim(control_clients(dispatch.id, "claim-owner"), dispatch)
    assert caught.value.response.status_code == 409
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.QUEUED


@pytest.mark.asyncio
@pytest.mark.description("A KMS decryption failure records a diagnostic without exposing the secret.")
async def test_kms_decrypt_failure_records_diagnostic_without_leaking_secret(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    from botocore.exceptions import ClientError
    from tracker.executor import control_dispatch

    dispatch = _sealed_case(postgres_session, monkeypatch)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    task = make_task(benchmark, "task-0", status=TaskStatus.PENDING)
    task.started_at = dispatch.created_at - timedelta(seconds=1)
    postgres_session.add(task)
    postgres_session.commit()

    def kms_failure(*_args: object) -> None:
        raise ClientError({"Error": {"Code": "InvalidCiphertextException", "Message": "secret-marker"}}, "Decrypt")

    monkeypatch.setattr(control_dispatch, "open_payload", kms_failure)
    with pytest.raises(httpx.HTTPStatusError) as caught:
        await _claim(control_clients(dispatch.id, "claim-owner"), dispatch)
    assert caught.value.response.status_code == 409
    assert caught.value.response.json()["detail"] == "Executor dispatch payload decryption failed"
    assert "secret-marker" not in str(caught.value)
    postgres_session.expire_all()
    failed = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert failed.status == ExecutorDispatchStatus.FAILED
    assert failed.failure_reason == "PAYLOAD_DECRYPT_FAILED"
    assert postgres_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.ERROR
    error = postgres_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
    assert error.error_type == "InvalidCiphertextException"
    assert error.cause_code == "PAYLOAD_DECRYPT_FAILED"
    assert error.category == FailureCategory.INFRASTRUCTURE
    assert error.error_message == "Executor payload decrypt failed: InvalidCiphertextException"


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_first", [False, True])
@pytest.mark.description(
    "Stop and claim leave the dispatch with one owner whether the claim happens before or after stop."
)
async def test_stop_and_claim_have_one_owner(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
    claim_first: bool,
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    org = postgres_session.get(Org, benchmark.org_id)
    assert org is not None
    postgres_session.add(make_task(benchmark, "task-0", status=TaskStatus.PENDING))
    postgres_session.commit()
    owner = control_clients(dispatch.id, "claim-owner")
    if claim_first:
        assert (await _claim(owner, dispatch)).claimed
        assert (
            await owner.call("ack_dispatch", AckDispatchRequest(ref=_ref(dispatch)), AckDispatchResponse)
        ).acknowledged
    task_arns = apply_stop_benchmark(benchmark, postgres_session, force=False, org=org)
    postgres_session.commit()
    assert task_arns == []
    if not claim_first:
        assert not (await _claim(owner, dispatch)).claimed
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == (
        ExecutorDispatchStatus.RUNNING if claim_first else ExecutorDispatchStatus.FAILED
    )
    assert postgres_session.get(Benchmark, benchmark.id).status == (
        BenchmarkStatus.STOPPING if claim_first else BenchmarkStatus.STOPPED
    )
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None


@pytest.mark.asyncio
@pytest.mark.description("A concurrent stop prevents a revoked dispatch from being resurrected by claim.")
async def test_concurrent_stop_and_claim_cannot_resurrect_revoked_dispatch(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
) -> None:
    dispatch = _sealed_case(postgres_session, monkeypatch)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    postgres_session.add(make_task(benchmark, "task-0", status=TaskStatus.PENDING))
    postgres_session.commit()

    def stop() -> None:
        with Session(postgres_engine) as session:
            row = session.get(Benchmark, benchmark.id)
            org = session.get(Org, benchmark.org_id)
            assert row is not None and org is not None
            apply_stop_benchmark(row, session, force=False, org=org)
            session.commit()

    owner = control_clients(dispatch.id, "claim-owner")
    claimed, _ = await asyncio.gather(_claim(owner, dispatch), asyncio.to_thread(stop))
    if claimed.claimed:
        assert (
            await owner.call("ack_dispatch", AckDispatchRequest(ref=_ref(dispatch)), AckDispatchResponse)
        ).acknowledged
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == (
        ExecutorDispatchStatus.RUNNING if claimed.claimed else ExecutorDispatchStatus.FAILED
    )
    assert postgres_session.get(Benchmark, benchmark.id).status == (
        BenchmarkStatus.STOPPING if claimed.claimed else BenchmarkStatus.STOPPED
    )
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None
