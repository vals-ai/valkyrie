import asyncio
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session, select

import tracker.api.remote_grader as remote_api
import tracker.remote_grader as grants
import tracker.runtime.model_gateway as gateway_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import Org, RemoteGraderGrant, RemoteGraderRegistration, TaskStatus
from tracker.domain import FailureCategory
from tracker.executor.control_protocol import AttemptRef
from tracker.executor.control_runs import RunControl
from tracker.executor.control_tasks import TaskControl, TaskFailure, TaskFailureRequest, TaskMutationRequest
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.secret_access import tenant_secret_prefix
from tracker.types import AgentContractRequest


class ProjectSecret:
    async def get(self, name: str) -> dict[str, str]:
        return {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "project-root"}


@pytest.mark.description(
    "Remote attempts cannot change grants or identity while concurrent pickups share one child and stop wins after racing pickup"
)
@pytest.mark.usefixtures("kms_grader_tokens")
async def test_remote_grader_identity_and_pickup_stop_contention(
    postgres_engine: Engine, remote_grader_app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote_api, "secret_store", lambda org: ProjectSecret())
    monkeypatch.setenv("VCB_REMOTE_GRADER_WORKER_API_KEY", "vcb-worker")
    monkeypatch.setenv("VCB100_REMOTE_GRADER_WORKER_API_KEY", "vcb100-worker")
    monkeypatch.setenv("CYBER_REMOTE_GRADER_WORKER_API_KEY", "cyber-worker")

    org = Org(id=uuid4(), name="vals.ai")
    reference = f"{tenant_secret_prefix(org.id)}model-gateway-{uuid4()}"
    contract = AgentContractRequest(
        name="test-agent", secrets={"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}
    )
    with Session(postgres_engine) as session:
        session.add(org)
        session.flush()
        benchmark = make_benchmark(name="vcb", org_id=org.id, contract=contract, started_by_email="runner@vals.ai")
        benchmark.arguments = benchmark.arguments.model_copy(update={"selected_project_gateway_secret_ref": reference})
        session.add(benchmark)
        session.flush()
        first_task = make_task(benchmark, "tracker-task-1", status=TaskStatus.EVALUATING)
        second_task = make_task(benchmark, "tracker-task-2", status=TaskStatus.EVALUATING)
        session.add(first_task)
        session.add(second_task)
        session.commit()
        run_id = benchmark.id
        first_task_id, second_task_id = first_task.id, second_task.id
        first_attempt, second_attempt = grants.attempt_id_for(first_task), grants.attempt_id_for(second_task)

    with Session(postgres_engine) as issuer_session:
        first_grant = grants.issue_remote_grader_grant(
            issuer_session,
            benchmark_id=run_id,
            task_row_id=first_task_id,
            attempt_id=first_attempt,
            worker_service="vcb",
            gateway_secret_ref=reference,
        )
        second_grant = grants.issue_remote_grader_grant(
            issuer_session,
            benchmark_id=run_id,
            task_row_id=second_task_id,
            attempt_id=second_attempt,
            worker_service="vcb",
            gateway_secret_ref=reference,
        )

    minted: list[str] = []
    revoked: list[str] = []
    mint_entered = asyncio.Event()
    release_mint = asyncio.Event()

    async def gateway(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/service-auth":
            lease_id = f"child-{len(minted) + 1}"
            minted.append(lease_id)
            mint_entered.set()
            await asyncio.wait_for(release_mint.wait(), 15)
            return httpx.Response(
                200, json={"token": f"token-{lease_id}", "lease_id": lease_id, "expires_at": 1_800_000_000.0}
            )
        assert request.url.path == "/service-auth/revoke"
        revoked.append(json.loads(request.content)["lease_id"])
        return httpx.Response(200)

    worker = {"X-Remote-Grader-Worker-Key": "vcb-worker"}
    other_worker = {"X-Remote-Grader-Worker-Key": "cyber-worker"}
    first_identity = {"job_id": "job-1", "task_id": "task-1", "attempt_id": "attempt-1"}
    second_identity = {"job_id": "job-2", "task_id": "task-2", "attempt_id": "attempt-2"}
    scope = {
        "allowed_models": ["openai/gpt-5"],
        "first_pickup_deadline": (datetime.now(UTC) + timedelta(hours=1)).timestamp(),
    }
    pickup = {"allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600}
    app = remote_grader_app

    lock_count = 0
    lock_connections: list[object] = []
    lock_guard = threading.Lock()
    second_lock_started = threading.Event()
    observing = False
    event_loop_thread = threading.current_thread()

    def observe_lock(
        connection: Connection, cursor: object, statement: str, parameters: object, context: object, many: bool
    ) -> None:
        nonlocal lock_count
        if (
            not observing
            or "remotegraderregistration" not in statement.lower()
            or "FOR UPDATE" not in statement.upper()
        ):
            return
        assert threading.current_thread() is not event_loop_thread, "PostgreSQL lock wait blocked the event loop"
        with lock_guard:
            lock_count += 1
            if lock_count <= 2:
                lock_connections.append(connection.connection.dbapi_connection)
            if lock_count == 2:
                second_lock_started.set()

    event.listen(postgres_engine, "before_cursor_execute", observe_lock)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(gateway)) as gateway_client:
            monkeypatch.setattr(gateway_module, "_client", gateway_client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://tracker"
            ) as client:
                first_registration = await client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={"authorization": first_grant, **first_identity, **scope},
                )
                assert first_registration.status_code == 200
                first_id = first_registration.json()["registration_id"]
                assert UUID(first_id)
                assert (
                    await client.post(
                        "/remote-grader/registrations",
                        headers=worker,
                        json={"authorization": second_grant, **first_identity, **scope},
                    )
                ).status_code == 409
                first_path = f"/remote-grader/registrations/{first_id}"
                assert (
                    await client.post(
                        f"{first_path}/lease",
                        headers=worker,
                        json={**first_identity, "task_id": "wrong", **pickup},
                    )
                ).status_code == 404
                assert (
                    await client.post(
                        f"{first_path}/stop",
                        headers=worker,
                        json={**first_identity, "attempt_id": "wrong"},
                    )
                ).status_code == 404
                assert (
                    await client.post(f"{first_path}/lease", headers=other_worker, json={**first_identity, **pickup})
                ).status_code == 404

                second_registration = await client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={"authorization": first_grant, **second_identity, **scope},
                )
                assert second_registration.status_code == 200
                second_path = f"/remote-grader/registrations/{second_registration.json()['registration_id']}"

                async def race(
                    first_path: str,
                    first_body: dict[str, object],
                    second_path: str,
                    second_body: dict[str, object],
                ) -> tuple[httpx.Response, httpx.Response]:
                    nonlocal lock_count, observing
                    lock_count = 0
                    lock_connections.clear()
                    second_lock_started.clear()
                    mint_entered.clear()
                    release_mint.clear()
                    observing = True
                    first = asyncio.create_task(client.post(first_path, headers=worker, json=first_body))
                    try:
                        await asyncio.wait_for(mint_entered.wait(), 10)
                        second = asyncio.create_task(client.post(second_path, headers=worker, json=second_body))
                        await asyncio.wait_for(asyncio.to_thread(second_lock_started.wait, 10), 11)
                        assert len(lock_connections) == 2
                        assert lock_connections[0] is not lock_connections[1]
                    finally:
                        release_mint.set()
                    responses = await asyncio.wait_for(asyncio.gather(first, second), 20)
                    observing = False
                    return responses[0], responses[1]

                first_pickup, redelivery = await race(
                    f"{first_path}/lease",
                    {**first_identity, **pickup},
                    f"{first_path}/lease",
                    {**first_identity, **pickup},
                )
                assert first_pickup.status_code == redelivery.status_code == 200
                assert first_pickup.json() == redelivery.json()
                assert first_pickup.json()["lease_id"] == "child-1"
                assert minted == ["child-1"]

                racing_pickup, racing_stop = await race(
                    f"{second_path}/lease", {**second_identity, **pickup}, f"{second_path}/stop", second_identity
                )
                assert racing_pickup.status_code == 200
                assert racing_pickup.json()["lease_id"] == "child-2"
                assert racing_stop.status_code == 200
                assert racing_stop.json() == {"stopped": True}
                assert minted == ["child-1", "child-2"]
                assert revoked == ["child-2"]
                assert (
                    await client.post(f"{second_path}/lease", headers=worker, json={**second_identity, **pickup})
                ).status_code == 409
                with Session(postgres_engine) as session:
                    registrations = session.exec(select(RemoteGraderRegistration)).all()
                    assert len(registrations) == 2
                    saved = next(row for row in registrations if str(row.id) == second_path.rsplit("/", 1)[1])
                    assert saved.token_ciphertext is None
                    assert saved.revoked_at is not None
    finally:
        event.remove(postgres_engine, "before_cursor_execute", observe_lock)


@pytest.mark.description(
    "Native terminal closure blocks racing new registrations while preserving pickup for registered jobs."
)
@pytest.mark.usefixtures("kms_grader_tokens")
@pytest.mark.parametrize("terminal_owner", ["transition", "failure", "cancellation"])
async def test_native_terminal_registration_cutoff(
    postgres_engine: Engine,
    postgres_session: Session,
    remote_grader_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    terminal_owner: str,
) -> None:
    monkeypatch.setattr(remote_api, "secret_store", lambda org: ProjectSecret())
    monkeypatch.setenv("VCB_REMOTE_GRADER_WORKER_API_KEY", "vcb-worker")
    org = Org(id=uuid4(), name="vals.ai")
    reference = f"{tenant_secret_prefix(org.id)}model-gateway-{uuid4()}"
    postgres_session.add(org)
    postgres_session.flush()
    benchmark = make_benchmark(
        name="vcb",
        org_id=org.id,
        contract=AgentContractRequest(
            name="test-agent", secrets={"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}
        ),
    )
    benchmark.arguments = benchmark.arguments.model_copy(update={"selected_project_gateway_secret_ref": reference})
    postgres_session.add(benchmark)
    postgres_session.flush()
    task = make_task(benchmark, "tracker-task", status=TaskStatus.EVALUATING)
    postgres_session.add(task)
    postgres_session.commit()
    controls = bound_controls(benchmark, session=postgres_session)
    postgres_session.refresh(task)
    task_control = cast(TaskControl, controls["task_control"])
    run_control = cast(RunControl, controls["run_control"])
    attempt = AttemptRef(org_id=org.id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    with Session(postgres_engine) as issuer_session:
        authorization = grants.issue_remote_grader_grant(
            issuer_session,
            benchmark_id=benchmark.id,
            task_row_id=task.id,
            attempt_id=grants.attempt_id_for(task),
            worker_service="vcb",
            gateway_secret_ref=reference,
        )
    app = remote_grader_app
    worker = {"X-Remote-Grader-Worker-Key": "vcb-worker"}
    old_job = {"job_id": "old-job", "task_id": "remote-task", "attempt_id": "remote-attempt"}
    new_job = {**old_job, "job_id": "new-job"}
    scope = {
        "allowed_models": ["openai/gpt-5"],
        "first_pickup_deadline": (datetime.now(UTC) + timedelta(hours=1)).timestamp(),
    }
    closure_entered = threading.Event()
    release_closure = threading.Event()
    registration_entered = threading.Event()
    original_close = RemoteGraderRepository.close_attempt
    original_lock = RemoteGraderRepository.lock_grant_by_digest

    def pause_closure(self: RemoteGraderRepository, task_row_id: UUID, attempt_id: str) -> None:
        original_close(self, task_row_id, attempt_id)
        closure_entered.set()
        assert release_closure.wait(10)

    def observe_registration(self: RemoteGraderRepository, token_digest: str) -> RemoteGraderGrant | None:
        registration_entered.set()
        return original_lock(self, token_digest)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://tracker"
    ) as client:
        old_registration = await client.post(
            "/remote-grader/registrations",
            headers=worker,
            json={"authorization": authorization, **old_job, **scope},
        )
        assert old_registration.status_code == 200
        if terminal_owner == "transition":
            assert await task_control.transition(
                TaskMutationRequest(
                    ref=task_control.ref,
                    attempt=attempt,
                    to_status=TaskStatus.PENDING,
                    expected_status=TaskStatus.EVALUATING,
                )
            )
            with Session(postgres_engine) as session:
                grant = session.exec(select(RemoteGraderGrant)).one()
                assert grant.closed_at is None
                assert grant.token_ciphertext is not None
            assert await task_control.transition(
                TaskMutationRequest(
                    ref=task_control.ref,
                    attempt=attempt,
                    to_status=TaskStatus.EVALUATING,
                    expected_status=TaskStatus.PENDING,
                )
            )
            with Session(postgres_engine) as issuer_session:
                assert (
                    grants.issue_remote_grader_grant(
                        issuer_session,
                        benchmark_id=benchmark.id,
                        task_row_id=task.id,
                        attempt_id=grants.attempt_id_for(task),
                        worker_service="vcb",
                        gateway_secret_ref=reference,
                    )
                    == authorization
                )
        monkeypatch.setattr(RemoteGraderRepository, "close_attempt", pause_closure)
        monkeypatch.setattr(RemoteGraderRepository, "lock_grant_by_digest", observe_registration)

        async def terminal() -> None:
            if terminal_owner == "transition":
                accepted = await task_control.transition(
                    TaskMutationRequest(
                        ref=task_control.ref,
                        attempt=attempt,
                        to_status=TaskStatus.STOPPED,
                        expected_status=TaskStatus.EVALUATING,
                    )
                )
            elif terminal_owner == "failure":
                accepted = await task_control.fail(
                    TaskFailureRequest(
                        ref=task_control.ref,
                        attempt=attempt,
                        failure=TaskFailure(
                            message="evaluation failed",
                            producer="tracker",
                            operation="evaluate",
                            error_type="RuntimeError",
                            category=FailureCategory.INFRASTRUCTURE,
                            cause_code=None,
                            phase=None,
                            transient=None,
                        ),
                        expected_status=TaskStatus.EVALUATING,
                        retry_scheduled=False,
                        failed_attempt_number=None,
                    )
                )
            else:
                accepted = (await run_control.record_cancellation({task.id: task.started_at}, [task.task_id])).committed
            assert accepted

        pending_terminal = asyncio.create_task(terminal())
        try:
            entered = await asyncio.to_thread(closure_entered.wait, 10)
            if not entered:
                await asyncio.wait_for(pending_terminal, 2)
            assert entered
            pending_registration = asyncio.create_task(
                client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={"authorization": authorization, **new_job, **scope},
                )
            )
            assert await asyncio.to_thread(registration_entered.wait, 10)
            assert not pending_registration.done()
        finally:
            release_closure.set()
        await asyncio.wait_for(pending_terminal, 15)
        rejected = await asyncio.wait_for(pending_registration, 15)
        assert rejected.status_code == 409
        assert (
            await client.post(
                "/remote-grader/registrations",
                headers=worker,
                json={"authorization": authorization, **old_job, **scope},
            )
        ).json() == old_registration.json()
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"token": "scoped-child", "lease_id": "child-1", "expires_at": 1_800_000_000.0}
                )
            )
        ) as gateway_client:
            monkeypatch.setattr(gateway_module, "_client", gateway_client)
            pickup = await client.post(
                f"/remote-grader/registrations/{old_registration.json()['registration_id']}/lease",
                headers=worker,
                json={**old_job, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
            )
            assert pickup.status_code == 200
            assert pickup.json()["token"] == "scoped-child"
    with Session(postgres_engine) as session:
        grant = session.exec(select(RemoteGraderGrant)).one()
        assert grant.closed_at is not None
        assert grant.token_ciphertext is None
        assert len(session.exec(select(RemoteGraderRegistration)).all()) == 1
