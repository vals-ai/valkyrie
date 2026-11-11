import asyncio
import base64
import os
import signal
import socket
import subprocess
import sys
import threading
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
import uvicorn
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, Session, col, select

from executor_protocol import DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS
from tests.factories import make_benchmark, make_task
from tests.integration.local.database import test_runner_store
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_control import (
    stop_unclaimed_dispatches,
)
from tracker.database.session import get_session
from tracker.executor import control_dispatch, dispatch_recovery, launcher
from tracker.executor.control_app import app as control_app
from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import (
    AckDispatchRequest,
    AckDispatchResponse,
    ClaimDispatchRequest,
    ClaimDispatchResponse,
    DispatchRef,
)
from tracker.executor.control_store import invocation_digest
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.types import AgentContractRequest, ManagedExecutionContext, RunExecutionRequest

control_clients = test_runner_store.control_clients


@pytest.fixture
def control_server(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    monkeypatch.setenv("EXECUTOR_BOOTSTRAP_KEY", "claim-test-bootstrap")

    def server_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(control_app.dependency_overrides, get_session, server_session)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        monkeypatch.setenv("EXECUTOR_CONTROL_URL", url)
        server = uvicorn.Server(uvicorn.Config(control_app, log_level="error", lifespan="off"))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
        thread.start()
        try:
            deadline = monotonic() + 10
            while not server.started and thread.is_alive() and monotonic() < deadline:
                sleep(0.01)
            assert server.started
            yield
        finally:
            server.should_exit = True
            thread.join(10)
            assert not thread.is_alive()


def _runner_environment(tmp_path: Path) -> dict[str, str]:
    environment = os.environ.copy()
    for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USERNAME", "DB_PASSWORD", "DATABASE_URL"):
        environment.pop(key, None)
    environment["SENTRY_DSN"] = ""
    environment["PYTHONPATH"] = str(tmp_path) + os.pathsep + environment.get("PYTHONPATH", "")

    return environment


def _managed_payload(benchmark: Benchmark, task_ids: list[str]) -> dict[str, Any]:
    request = RunExecutionRequest(
        benchmark_name=benchmark.name,
        contract=benchmark.arguments.contract,
        sandbox_provider=benchmark.arguments.sandbox_provider,
        sandbox_provider_secret_name=benchmark.arguments.sandbox_provider_secret_name,
    )
    return {
        "execution_context_json": ManagedExecutionContext(
            version=3,
            benchmark_id=benchmark.id,
            verified_task_ids=task_ids,
            start_benchmark_request=request,
        ).model_dump(mode="json"),
    }


@pytest.mark.description(
    "One concurrent Control claim wins and replays after reconciliation, which retains active payloads and deletes terminal payloads before receipt."
)
@pytest.mark.parametrize("task_arn", [None, "arn:aws:ecs:us-east-1:123456789012:task/test/task-1"])
async def test_concurrent_claims_admit_exactly_one_executor(
    postgres_session: Session,
    postgres_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    control_clients: Callable[[UUID, str], ControlClient],
    task_arn: str | None,
) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    org = Org(id=uuid4(), name=f"dispatch-claim-{uuid4()}")
    benchmark = make_benchmark(
        name="dispatch-claim",
        org_id=org.id,
        contract=AgentContractRequest(name="claim-agent", install_cmd="true", run_cmd="true"),
    )
    release = ExecutorRelease(
        id="dispatch-claim-release",
        artifact_uri="source:///artifacts/dispatch-claim",
        artifact_digest="a" * 64,
        protocol_version="1",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    dispatch.ecs_task_arn = task_arn
    payload = _managed_payload(benchmark, ["task-1"])
    sealed = seal_payload(dispatch.id, payload, generate_payload_key(dispatch.id))
    postgres_session.add_all(
        [
            benchmark,
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()

    first_holds_row_lock = threading.Event()
    first_may_commit = threading.Event()
    second_entered_claim = threading.Event()
    first_digest = invocation_digest("first-owner")
    second_digest = invocation_digest("second-owner")
    original_save = ExecutorDispatchRepository.save
    original_eligible = control_dispatch._eligible_claim

    def hold_first_commit(repository: ExecutorDispatchRepository, *rows: SQLModel) -> None:
        original_save(repository, *rows)
        if any(
            isinstance(row, ExecutorDispatch)
            and row.id == dispatch.id
            and row.status == ExecutorDispatchStatus.RUNNING
            and row.invocation_key_digest == first_digest
            for row in rows
        ):
            first_holds_row_lock.set()
            assert first_may_commit.wait(30)

    def observe_second_claim(
        session: Session, request: ClaimDispatchRequest, digest: bytes, must_still_replay: bool
    ) -> Any:
        if digest == second_digest:
            second_entered_claim.set()
        return original_eligible(session, request, digest, must_still_replay)

    monkeypatch.setattr(ExecutorDispatchRepository, "save", hold_first_commit)
    monkeypatch.setattr(control_dispatch, "_eligible_claim", observe_second_claim)
    first = control_clients(dispatch.id, "first-owner")
    second = control_clients(dispatch.id, "second-owner")
    first_claim = asyncio.create_task(
        first.call("claim_dispatch", ClaimDispatchRequest(dispatch_id=dispatch.id), ClaimDispatchResponse)
    )
    try:
        assert await asyncio.to_thread(first_holds_row_lock.wait, 10)
        second_claim = asyncio.create_task(
            second.call("claim_dispatch", ClaimDispatchRequest(dispatch_id=dispatch.id), ClaimDispatchResponse)
        )
        assert await asyncio.to_thread(second_entered_claim.wait, 10)
        await asyncio.sleep(0.05)
        assert not second_claim.done(), "second claim must block until the first Control claim commits"
    finally:
        first_may_commit.set()
    claims = await asyncio.gather(first_claim, second_claim)
    assert claims[0].claimed
    assert not claims[1].claimed
    owner = first
    claimed = claims[0]
    assert claimed.payload is not None
    assert claimed.payload["execution_context_json"] == payload["execution_context_json"]
    postgres_session.expire_all()
    stored = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert stored is not None
    assert stored.status == ExecutorDispatchStatus.RUNNING
    assert stored.ecs_task_arn == task_arn
    assert (stored.lease_expires_at is not None) == (task_arn is None)
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None

    cleanup_dispatches: dict[ExecutorDispatchStatus, UUID] = {}
    for status in (
        ExecutorDispatchStatus.QUEUED,
        ExecutorDispatchStatus.FAILED,
        ExecutorDispatchStatus.FINISHED,
        ExecutorDispatchStatus.SUPERSEDED,
    ):
        cleanup_dispatch = create_executor_dispatch(
            benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
        )
        cleanup_dispatch.status = status
        sealed_cleanup = seal_payload(cleanup_dispatch.id, payload, generate_payload_key(cleanup_dispatch.id))
        postgres_session.add_all(
            [
                cleanup_dispatch,
                ExecutorDispatchPayload(
                    dispatch_id=cleanup_dispatch.id,
                    ciphertext=sealed_cleanup.ciphertext,
                    encrypted_data_key=sealed_cleanup.encrypted_data_key,
                    nonce=sealed_cleanup.nonce,
                ),
            ]
        )
        cleanup_dispatches[status] = cleanup_dispatch.id
    postgres_session.commit()

    monkeypatch.setattr(dispatch_recovery, "engine", postgres_engine)
    assert dispatch_recovery.reconcile_dispatches_once() == 0

    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None
    assert postgres_session.get(ExecutorDispatchPayload, cleanup_dispatches[ExecutorDispatchStatus.QUEUED]) is not None
    for status in (
        ExecutorDispatchStatus.FAILED,
        ExecutorDispatchStatus.FINISHED,
        ExecutorDispatchStatus.SUPERSEDED,
    ):
        assert postgres_session.get(ExecutorDispatchPayload, cleanup_dispatches[status]) is None

    replay = await owner.call("claim_dispatch", ClaimDispatchRequest(dispatch_id=dispatch.id), ClaimDispatchResponse)
    assert replay.claimed
    assert replay.payload == claimed.payload

    receipt = await owner.call(
        "ack_dispatch",
        AckDispatchRequest(ref=DispatchRef(benchmark_id=benchmark.id, dispatch_id=dispatch.id)),
        AckDispatchResponse,
    )
    assert receipt.acknowledged
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None
    assert postgres_session.get(ExecutorDispatchPayload, cleanup_dispatches[ExecutorDispatchStatus.QUEUED]) is not None


@pytest.mark.description("Worker failure and corrupt ciphertext fail their dispatches. Completed work finishes.")
@pytest.mark.usefixtures("control_server")
@pytest.mark.parametrize("outcome", ["worker_failure", "corrupt", "complete"])
def test_executor_process_records_outcome(postgres_session: Session, tmp_path: Path, outcome: str) -> None:
    org = Org(id=uuid4(), name=f"executor-process-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"executor-process-{uuid4()}",
        artifact_uri="source:///local",
        artifact_digest="a" * 64,
        protocol_version="5",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    postgres_session.add(benchmark)
    if outcome == "complete":
        task = make_task(benchmark, "task-0", status=TaskStatus.FINISHED, finished_at=datetime.now(UTC))
        postgres_session.add(task)
        postgres_session.flush()
        postgres_session.add(EvaluationResult(org_id=org.id, task=task.id, result={"score": 1.0}))
    sealed = seal_payload(
        dispatch.id,
        _managed_payload(benchmark, []),
        generate_payload_key(dispatch.id),
    )
    postgres_session.add_all(
        [
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext + (b"corrupt" if outcome == "corrupt" else b""),
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()

    if outcome == "complete":
        (tmp_path / "sitecustomize.py").write_text(
            "import tracker.services.runs.start as orchestration\n"
            "async def complete(*, run_control, **kwargs):\n"
            "    decision = await run_control.prepare_finalization()\n"
            "    assert decision.kind == 'score' and decision.inputs is not None\n"
            "    claim = await run_control.commit_final_score(\n"
            "        decision.inputs.phase, decision.inputs.fingerprint, 1.0, {}\n"
            "    )\n"
            "    assert claim.committed\n"
            "orchestration.process_benchmark = complete\n"
        )
    elif outcome == "worker_failure":
        (tmp_path / "sitecustomize.py").write_text(
            "import tracker.services.runs.start as orchestration\n"
            "async def fail_after_claim(**kwargs):\n"
            "    raise RuntimeError('executor worker failed')\n"
            "orchestration.process_benchmark = fail_after_claim\n"
        )
    environment = _runner_environment(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "tracker.executor.watchdog", "--dispatch-id", str(dispatch.id)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert (result.returncode == 0) == (outcome == "complete"), result.stderr
    postgres_session.expire_all()
    stored_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    stored_benchmark = postgres_session.get(Benchmark, benchmark.id)
    assert stored_dispatch is not None and stored_benchmark is not None
    if outcome == "complete":
        assert stored_dispatch.status == ExecutorDispatchStatus.FINISHED
        assert stored_benchmark.status == BenchmarkStatus.FINISHED
        assert stored_dispatch.finished_at is not None
        assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None
    elif outcome == "corrupt":
        assert stored_dispatch.status == ExecutorDispatchStatus.FAILED
        assert stored_benchmark.status == BenchmarkStatus.ERROR
        assert stored_dispatch.failure_reason == "PAYLOAD_DECRYPT_FAILED"
        assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None
    else:
        assert stored_dispatch.status == ExecutorDispatchStatus.FAILED
        assert stored_benchmark.status == BenchmarkStatus.ERROR
        assert stored_dispatch.failure_reason == "EXECUTOR_FAILED"
        assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None


@pytest.mark.description(
    "A local executor that is revoked or crashes leaves a `FAILED` dispatch and a `STOPPED` or `ERROR` run."
)
@pytest.mark.usefixtures("control_server")
@pytest.mark.parametrize("termination", ["revoke", "kill"])
def test_local_process_recovers_after_stop_or_crash(
    postgres_engine: Engine,
    postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    termination: str,
) -> None:
    org = Org(id=uuid4(), name=f"local-process-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"local-process-{uuid4()}",
        artifact_uri="source:///local",
        artifact_digest="a" * 64,
        protocol_version="5",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    sealed = seal_payload(
        dispatch.id,
        _managed_payload(benchmark, []),
        generate_payload_key(dispatch.id),
    )
    postgres_session.add_all(
        [
            benchmark,
            dispatch,
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
            ),
        ]
    )
    postgres_session.commit()
    work_started = tmp_path / "work-started"
    (tmp_path / "sitecustomize.py").write_text(
        "import asyncio\nfrom pathlib import Path\nimport tracker.services.runs.start as orchestration\n"
        "async def wait_for_stop(**kwargs):\n"
        f"    Path({str(work_started)!r}).write_text('entered')\n"
        "    await asyncio.Future()\n"
        "orchestration.process_benchmark = wait_for_stop\n"
    )
    environment = _runner_environment(tmp_path)
    with subprocess.Popen(
        [sys.executable, "-m", "tracker.executor.watchdog", "--dispatch-id", str(dispatch.id)],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            deadline = monotonic() + DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS
            while monotonic() < deadline:
                postgres_session.refresh(dispatch)
                if dispatch.status == ExecutorDispatchStatus.RUNNING:
                    break
                assert process.poll() is None, process.communicate()[1]
                sleep(0.05)
            else:
                pytest.fail("Executor did not claim the dispatch")
            assert dispatch.lease_expires_at is not None
            while not work_started.exists():
                assert monotonic() < deadline
                assert process.poll() is None, process.communicate()[1]
                sleep(0.05)
            assert process.poll() is None
            children = Path(f"/proc/{process.pid}/task/{process.pid}/children").read_text().split()
            assert len(children) == 1
            child_pid = int(children[0])
            os.kill(child_pid, 0)
            if termination == "revoke":
                dispatch.status = ExecutorDispatchStatus.FAILED
                benchmark.status = BenchmarkStatus.STOPPED
                postgres_session.add_all([dispatch, benchmark])
                postgres_session.commit()
                assert process.wait(timeout=20) != 0
            else:
                process.kill()
                os.kill(child_pid, signal.SIGKILL)
                process.wait(timeout=10)
                dispatch.lease_expires_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=1)
                postgres_session.add(dispatch)
                postgres_session.commit()
                monkeypatch.setattr(dispatch_recovery, "engine", postgres_engine)
                assert dispatch_recovery.reconcile_dispatches_once() == 1
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    postgres_session.expire_all()
    stored_dispatch = postgres_session.get(ExecutorDispatch, dispatch.id)
    stored_benchmark = postgres_session.get(Benchmark, benchmark.id)
    assert stored_dispatch is not None and stored_benchmark is not None
    assert stored_dispatch.status == ExecutorDispatchStatus.FAILED
    assert stored_benchmark.status == (BenchmarkStatus.STOPPED if termination == "revoke" else BenchmarkStatus.ERROR)


@pytest.mark.description("Relaunch cannot revive a dispatch that stop revoked.")
@pytest.mark.usefixtures("ecs_runner_env")
def test_relaunch_cannot_revive_a_dispatch_that_stop_revoked(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    wait_for_database_lock: Callable[[], None],
) -> None:
    org = Org(id=uuid4(), name=f"dispatch-relaunch-{uuid4()}")
    benchmark = make_benchmark(
        name="dispatch-relaunch",
        org_id=org.id,
        contract=AgentContractRequest(name="relaunch-agent", install_cmd="true", run_cmd="true"),
    )
    release = ExecutorRelease(
        id="dispatch-relaunch-release",
        artifact_uri="source:///artifacts/dispatch-relaunch",
        artifact_digest="a" * 64,
        protocol_version="1",
        task_definition_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/runner:7",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    stopped_task_arn = "arn:aws:ecs:us-east-1:123456789012:task/cluster/unstarted"
    dispatch.ecs_task_arn = stopped_task_arn
    postgres_session.add_all([benchmark, dispatch])
    postgres_session.commit()
    monkeypatch.setattr(dispatch_recovery, "engine", postgres_engine)
    monkeypatch.setattr(launcher, "engine", postgres_engine)
    ecs = Mock()
    ecs.describe_tasks.return_value = {
        "tasks": [{"taskArn": stopped_task_arn, "lastStatus": "STOPPED", "stopCode": "TaskFailedToStart"}],
        "failures": [],
    }
    ecs.run_task.return_value = {"tasks": [{"taskArn": "arn:task:replacement"}], "failures": []}
    monkeypatch.setattr(dispatch_recovery.boto3, "client", lambda *_args, **_kwargs: ecs)

    with Session(postgres_engine) as stop_session, ThreadPoolExecutor(max_workers=1) as pool:
        locked_benchmark = stop_session.exec(
            select(Benchmark).where(col(Benchmark.id) == benchmark.id).with_for_update()
        ).one()
        stop_unclaimed_dispatches(stop_session, locked_benchmark)
        recovery = pool.submit(dispatch_recovery.reconcile_dispatches_once)
        try:
            wait_for_database_lock()
        finally:
            stop_session.commit()
        assert recovery.result(timeout=10) == 0

    ecs.run_task.assert_called_once()

    postgres_session.expire_all()
    stored = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert stored is not None
    assert (stored.status, stored.failure_reason, stored.ecs_task_arn) == (
        ExecutorDispatchStatus.FAILED,
        "STOPPED_BEFORE_CLAIM",
        stopped_task_arn,
    )


@pytest.mark.description("A database error during a relaunch fails the pass instead of dropping its work.")
def test_a_database_error_during_a_relaunch_fails_the_pass_instead_of_dropping_its_work(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    org = Org(id=uuid4(), name=f"dispatch-relaunch-error-{uuid4()}")
    release = ExecutorRelease(
        id="dispatch-relaunch-error-release",
        artifact_uri="source:///artifacts/dispatch-relaunch-error",
        artifact_digest="b" * 64,
        protocol_version="1",
        task_definition_arn="arn:aws:ecs:us-east-1:123456789012:task-definition/runner:7",
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    dispatch_ids = {}
    task_arns = {}
    for name, status in (("stopped", ExecutorDispatchStatus.RUNNING), ("unstarted", ExecutorDispatchStatus.QUEUED)):
        benchmark = make_benchmark(
            name=f"dispatch-relaunch-{name}",
            org_id=org.id,
            contract=AgentContractRequest(name="relaunch-agent", install_cmd="true", run_cmd="true"),
        )
        pin_benchmark_to_release(benchmark, release)
        dispatch = create_executor_dispatch(
            benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
        )
        dispatch.ecs_task_arn = f"arn:aws:ecs:us-east-1:123456789012:task/cluster/{name}"
        dispatch.status = status
        if status == ExecutorDispatchStatus.RUNNING:
            dispatch.started_at = datetime.now(UTC)
        task = Task(
            org_id=org.id,
            benchmark=benchmark.id,
            task_id="task-1",
            status=TaskStatus.PENDING,
            started_at=dispatch.created_at - timedelta(seconds=1),
        )
        postgres_session.add_all([benchmark, dispatch, task])
        dispatch_ids[name] = dispatch.id
        task_arns[name] = dispatch.ecs_task_arn
    postgres_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    monkeypatch.setattr(dispatch_recovery, "engine", postgres_engine)
    ecs = Mock()
    ecs.describe_tasks.return_value = {
        "tasks": [
            {
                "taskArn": task_arns["stopped"],
                "lastStatus": "STOPPED",
                "stopCode": "EssentialContainerExited",
                "stoppedReason": "Essential container in task exited",
            },
            {"taskArn": task_arns["unstarted"], "lastStatus": "STOPPED", "stopCode": "TaskFailedToStart"},
        ],
        "failures": [],
    }
    monkeypatch.setattr(dispatch_recovery.boto3, "client", lambda *_args, **_kwargs: ecs)

    def relaunch_into_a_database_error(dispatch: ExecutorDispatch) -> str:
        session = Session.object_session(dispatch)
        assert session is not None
        session.execute(text("SELECT 1 / 0"))
        return "arn:task:never-started"

    def stored() -> tuple[ExecutorDispatch, ExecutorDispatch]:
        postgres_session.expire_all()
        dispatches = [postgres_session.get(ExecutorDispatch, dispatch_ids[name]) for name in ("stopped", "unstarted")]
        assert dispatches[0] is not None and dispatches[1] is not None
        return dispatches[0], dispatches[1]

    monkeypatch.setattr(dispatch_recovery, "relaunch_dispatch", relaunch_into_a_database_error)
    with pytest.raises(DBAPIError):
        dispatch_recovery.reconcile_dispatches_once()

    stopped, unstarted = stored()
    assert stopped.status == ExecutorDispatchStatus.RUNNING
    assert (unstarted.status, unstarted.ecs_task_arn) == (ExecutorDispatchStatus.QUEUED, task_arns["unstarted"])

    monkeypatch.setattr(dispatch_recovery, "relaunch_dispatch", lambda *_args: "arn:task:replacement")
    assert dispatch_recovery.reconcile_dispatches_once() == 2

    stopped, unstarted = stored()
    assert stopped.status == ExecutorDispatchStatus.FAILED
    assert (unstarted.status, unstarted.ecs_task_arn) == (ExecutorDispatchStatus.QUEUED, "arn:task:replacement")
