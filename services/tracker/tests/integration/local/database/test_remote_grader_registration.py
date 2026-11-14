import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

import tracker.api.remote_grader as remote_api
import tracker.remote_grader as grants
import tracker.runtime.model_gateway as gateway_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    Benchmark,
    Org,
    RemoteGraderGrant,
    RemoteGraderRegistration,
    TaskStatus,
)
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.secret_access import tenant_secret_prefix
from tracker.services.runs.stop import apply_stop_benchmark
from tracker.types import AgentContractRequest


class ProjectSecret:
    async def get(self, name: str) -> dict[str, str]:
        return {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "project-root"}


@pytest.mark.description("A failed pickup revokes only a known unsaved lease and preserves the original failure.")
@pytest.mark.usefixtures("kms_grader_tokens")
@pytest.mark.parametrize("failed_phase", ["encrypt", "commit", "commit_persisted", "commit_read_failed"])
@pytest.mark.parametrize("revoke_fails", [False, True])
async def test_pickup_revokes_lease_after_persistence_failure(
    postgres_engine: Engine,
    remote_grader_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    failed_phase: str,
    revoke_fails: bool,
) -> None:
    monkeypatch.setenv("VCB_REMOTE_GRADER_WORKER_API_KEY", "vcb-worker")
    with Session(postgres_engine) as session:
        org = Org(id=uuid4(), name="vals.ai")
        session.add(org)
        session.flush()
        benchmark = make_benchmark(name="vcb", org_id=org.id)
        session.add(benchmark)
        session.flush()
        task = make_task(benchmark, "tracker-task", status=TaskStatus.EVALUATING)
        session.add(task)
        session.commit()
        authorization = grants.issue_remote_grader_grant(
            session,
            benchmark_id=benchmark.id,
            task_row_id=task.id,
            attempt_id=grants.attempt_id_for(task),
            worker_service="vcb",
            gateway_secret_ref=f"{tenant_secret_prefix(org.id)}model-gateway-{uuid4()}",
        )

    identity = {"job_id": "remote-job", "task_id": "remote-task", "attempt_id": "remote-attempt"}
    worker = {"X-Remote-Grader-Worker-Key": "vcb-worker"}
    mint = AsyncMock(
        return_value=SimpleNamespace(token="child-token", lease_id="child-lease", expires_at=1_900_000_000)
    )
    revoke = AsyncMock(side_effect=RuntimeError("revoke unavailable") if revoke_fails else None)
    monkeypatch.setattr(remote_api, "mint_gateway_lease", mint)
    monkeypatch.setattr(remote_api, "revoke_gateway_lease", revoke)
    monkeypatch.setattr(remote_api, "_root", lambda *_args: ("https://gateway.test", "project-root"))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=remote_grader_app), base_url="http://tracker"
    ) as client:
        registration = await client.post(
            "/remote-grader/registrations",
            headers=worker,
            json={
                "authorization": authorization,
                **identity,
                "allowed_models": ["openai/gpt-5"],
                "first_pickup_deadline": (datetime.now(UTC) + timedelta(hours=1)).timestamp(),
            },
        )
        assert registration.status_code == 200
        registration_id = registration.json()["registration_id"]
        failure = RuntimeError(f"{failed_phase} failed")
        if failed_phase == "encrypt":
            monkeypatch.setattr(remote_api, "encrypt_grader_token", Mock(side_effect=failure))
        elif failed_phase == "commit_persisted":
            original_commit = Session.commit

            def commit_then_fail(session: Session) -> None:
                original_commit(session)
                raise failure

            monkeypatch.setattr(Session, "commit", commit_then_fail)
        else:
            monkeypatch.setattr(Session, "commit", Mock(side_effect=failure))
            if failed_phase == "commit_read_failed":
                monkeypatch.setattr(
                    RemoteGraderRepository,
                    "get_registration_lease_id",
                    Mock(side_effect=RuntimeError("database unavailable")),
                )

        with pytest.raises(RuntimeError) as error:
            await client.post(
                f"/remote-grader/registrations/{registration_id}/lease",
                headers=worker,
                json={**identity, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
            )

        assert error.value is failure
        mint.assert_awaited_once()
        if failed_phase in ("encrypt", "commit"):
            revoke.assert_awaited_once_with("https://gateway.test", "project-root", "child-lease")
        else:
            revoke.assert_not_awaited()
        with Session(postgres_engine) as fresh:
            saved = fresh.get(RemoteGraderRegistration, UUID(registration_id))
            assert saved is not None
            assert saved.gateway_lease_id == ("child-lease" if failed_phase == "commit_persisted" else None)
            assert (saved.token_ciphertext is not None) == (failed_phase == "commit_persisted")
        if failed_phase == "commit_persisted":
            replay = await client.post(
                f"/remote-grader/registrations/{registration_id}/lease",
                headers=worker,
                json={**identity, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
            )
            assert replay.status_code == 200
            assert replay.json()["token"] == "child-token"
            assert replay.json()["lease_id"] == "child-lease"
            mint.assert_awaited_once()
            revoke.assert_not_awaited()


@pytest.mark.description(
    "Registration binds model scope and first pickup deadline, persists sealed credentials, and replays an "
    "unexpired lease through closure before stop fences issuance and retries failed revocation."
)
@pytest.mark.usefixtures("kms_grader_tokens")
@pytest.mark.parametrize(
    ("benchmark_name", "worker_service"),
    [("vcb", "vcb"), ("vcb-1-100-package", "vcb-1-100")],
)
async def test_registered_remote_grading_attempt_survives_tracker_attempt_end_and_revokes_after_stop(
    postgres_engine: Engine,
    remote_grader_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    benchmark_name: str,
    worker_service: str,
) -> None:
    monkeypatch.setattr(remote_api, "secret_store", lambda org: ProjectSecret())
    monkeypatch.setenv("VCB_REMOTE_GRADER_WORKER_API_KEY", "vcb-worker")
    monkeypatch.setenv("VCB100_REMOTE_GRADER_WORKER_API_KEY", "vcb100-worker")
    monkeypatch.setenv("CYBER_REMOTE_GRADER_WORKER_API_KEY", "cyber-worker")

    org = Org(id=uuid4(), name="vals.ai")
    reference = f"{tenant_secret_prefix(org.id)}model-gateway-{uuid4()}"
    org_id = org.id
    contract = AgentContractRequest(
        name="test-agent", secrets={"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}
    )
    with Session(postgres_engine) as session:
        session.add(org)
        session.flush()
        benchmark = make_benchmark(
            name=benchmark_name, org_id=org.id, contract=contract, started_by_email="runner@vals.ai"
        )
        benchmark.arguments = benchmark.arguments.model_copy(update={"selected_project_gateway_secret_ref": reference})
        session.add(benchmark)
        session.flush()
        task = make_task(benchmark, "tracker-task", status=TaskStatus.EVALUATING)
        session.add(task)
        session.commit()
        run_id, task_row_id = benchmark.id, task.id
        tracker_attempt = grants.attempt_id_for(task)

    with Session(postgres_engine) as issuer_session:
        authorization = grants.issue_remote_grader_grant(
            issuer_session,
            benchmark_id=run_id,
            task_row_id=task_row_id,
            attempt_id=tracker_attempt,
            worker_service=worker_service,
            gateway_secret_ref=reference,
        )
    with Session(postgres_engine) as session:
        grant = session.exec(select(RemoteGraderGrant)).one()
        assert grant.token_ciphertext is not None
        assert authorization.encode() not in grant.token_ciphertext
        assert not hasattr(grant, "token")
    requests: list[tuple[str, dict[str, object], str]] = []
    revoke_status = 503

    def gateway(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, json.loads(request.content), request.headers["Authorization"]))
        if request.url.path == "/service-auth":
            child = sum(path == "/service-auth" for path, _, _ in requests)
            return httpx.Response(
                200,
                json={
                    "token": "scoped-child" if child == 1 else "scoped-child-2",
                    "lease_id": f"child-{child}",
                    "expires_at": 1_800_000_000.0,
                },
            )
        return httpx.Response(revoke_status)

    app = remote_grader_app
    worker = {"X-Remote-Grader-Worker-Key": "vcb-worker" if worker_service == "vcb" else "vcb100-worker"}
    remote_attempt = {"job_id": "remote-job-1", "task_id": "remote-task-1", "attempt_id": "remote-attempt-1"}
    deadline = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    registration_scope = {"allowed_models": ["openai/gpt-5"], "first_pickup_deadline": deadline}
    async with httpx.AsyncClient(transport=httpx.MockTransport(gateway)) as gateway_client:
        monkeypatch.setattr(gateway_module, "_client", gateway_client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://tracker"
        ) as client:
            registration_body = {"authorization": authorization, **remote_attempt, **registration_scope}
            assert (
                await client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={"authorization": authorization, **remote_attempt},
                )
            ).status_code == 422
            created = await client.post("/remote-grader/registrations", headers=worker, json=registration_body)
            assert created.status_code == 200
            registration_id = created.json()["registration_id"]
            assert (
                await client.post("/remote-grader/registrations", headers=worker, json=registration_body)
            ).json() == created.json()
            assert (
                await client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={**registration_body, "first_pickup_deadline": deadline + 3600},
                )
            ).json() == created.json()
            assert (
                await client.post(
                    "/remote-grader/registrations",
                    headers=worker,
                    json={**registration_body, "allowed_models": ["other/model"]},
                )
            ).status_code == 409
            with Session(postgres_engine) as session:
                registration = session.get(RemoteGraderRegistration, UUID(registration_id))
                assert registration.allowed_models == ["openai/gpt-5"]
                assert registration.first_pickup_deadline == deadline
            assert (
                await client.post(
                    "/remote-grader/registrations",
                    headers={"X-Remote-Grader-Worker-Key": "cyber-worker"},
                    json=registration_body,
                )
            ).status_code == 403

            pickup_body = {**remote_attempt, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 7200}
            pickup_path = f"/remote-grader/registrations/{registration_id}/lease"
            assert (
                await client.post(pickup_path, headers=worker, json={**pickup_body, "allowed_models": ["other/model"]})
            ).status_code == 409
            assert requests == []
            first = await client.post(pickup_path, headers=worker, json=pickup_body)
            assert first.status_code == 200
            assert first.json() == {
                "gateway_url": "https://gateway.test",
                "token": "scoped-child",
                "lease_id": "child-1",
                "expires_at": 1_800_000_000.0,
            }
            with Session(postgres_engine) as session:
                registration = session.get(RemoteGraderRegistration, UUID(registration_id))
                assert registration.token_ciphertext is not None
                assert first.json()["token"].encode() not in registration.token_ciphertext
                assert not hasattr(registration, "token")
                registration.first_pickup_deadline = deadline - 7200
                session.commit()
            assert (await client.post(pickup_path, headers=worker, json=pickup_body)).json() == first.json()
            assert (
                await client.post(pickup_path, headers=worker, json={**pickup_body, "allowed_models": ["other/model"]})
            ).status_code == 409
            second_attempt = {"job_id": "remote-job-2", "task_id": "remote-task-2", "attempt_id": "remote-attempt-2"}
            second_registration = await client.post(
                "/remote-grader/registrations",
                headers=worker,
                json={"authorization": authorization, **second_attempt, **registration_scope},
            )
            assert second_registration.status_code == 200
            deadline_attempt = {"job_id": "remote-job-3", "task_id": "remote-task-3", "attempt_id": "remote-attempt-3"}
            deadline_registration = await client.post(
                "/remote-grader/registrations",
                headers=worker,
                json={"authorization": authorization, **deadline_attempt, **registration_scope},
            )
            assert deadline_registration.status_code == 200
            assert requests == [
                (
                    "/service-auth",
                    {
                        "run_id": str(run_id),
                        "task_id": "tracker-task",
                        "attempt_id": tracker_attempt,
                        "allowed_models": ["openai/gpt-5"],
                        "identity": {
                            "benchmark_name": benchmark_name,
                            "agent_name": "grader",
                            "email": "runner@vals.ai",
                        },
                        "egress": "*",
                        "ttl_seconds": 7200,
                        "allowed_operations": ["query", "tokens_count"],
                    },
                    "Bearer project-root",
                )
            ]

            with Session(postgres_engine) as session:
                saved_org = session.get(Org, org_id)
                saved_run = session.get(Benchmark, run_id)
                apply_stop_benchmark(saved_run, session, force=False, org=saved_org, task_ids=["tracker-task"])
                session.commit()
                grant = session.exec(select(RemoteGraderGrant)).one()
                assert grant.token_ciphertext is None
                assert grant.closed_at is not None
                saved_run.arguments = saved_run.arguments.model_copy(
                    update={
                        "selected_project_gateway_secret_ref": f"{tenant_secret_prefix(org_id)}model-gateway-{uuid4()}"
                    }
                )
                saved_run.custom_benchmark_service = "https://custom.example"
                session.commit()
            assert (
                await client.post("/remote-grader/registrations", headers=worker, json=registration_body)
            ).json() == created.json()
            shorter_pickup = {**pickup_body, "ttl_seconds": 3600}
            assert (await client.post(pickup_path, headers=worker, json=shorter_pickup)).json() == first.json()
            deadline_id = UUID(deadline_registration.json()["registration_id"])
            with Session(postgres_engine) as session:
                deadline_saved = session.get(RemoteGraderRegistration, deadline_id)
                deadline_saved.first_pickup_deadline = deadline - 7200
                session.commit()
            assert (
                await client.post(
                    f"/remote-grader/registrations/{deadline_id}/lease",
                    headers=worker,
                    json={**deadline_attempt, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
                )
            ).status_code == 409
            with Session(postgres_engine) as session:
                deadline_saved = session.get(RemoteGraderRegistration, deadline_id)
                assert deadline_saved.gateway_lease_id is None
                assert deadline_saved.token_ciphertext is None
            second_registration_id = second_registration.json()["registration_id"]
            second_path = f"/remote-grader/registrations/{second_registration_id}/lease"
            assert (
                await client.post(
                    second_path,
                    headers=worker,
                    json={**second_attempt, "allowed_models": ["other/model"], "ttl_seconds": 3600},
                )
            ).status_code == 409
            second_pickup = await client.post(
                second_path,
                headers=worker,
                json={**second_attempt, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
            )
            assert second_pickup.status_code == 200
            assert second_pickup.json()["token"] == "scoped-child-2"
            with Session(postgres_engine) as session:
                first_saved = session.get(RemoteGraderRegistration, UUID(registration_id))
                second_saved = session.get(RemoteGraderRegistration, UUID(second_registration_id))
                assert first_saved.token_ciphertext is not None
                assert second_saved.token_ciphertext is not None
                second_saved.expires_at = (datetime.now(UTC) - timedelta(seconds=1)).timestamp()
                session.commit()
            assert (
                await client.post(
                    second_path,
                    headers=worker,
                    json={**second_attempt, "allowed_models": ["openai/gpt-5"], "ttl_seconds": 3600},
                )
            ).status_code == 409
            with Session(postgres_engine) as session:
                RemoteGraderRepository(session).clear_expired_child_tokens()
                session.commit()
                assert session.get(RemoteGraderRegistration, UUID(second_registration_id)).token_ciphertext is None
                assert session.get(RemoteGraderRegistration, UUID(registration_id)).token_ciphertext is not None
            new_job = {**registration_body, "job_id": "remote-job-2"}
            assert (await client.post("/remote-grader/registrations", headers=worker, json=new_job)).status_code == 409

            stop_path = f"/remote-grader/registrations/{registration_id}/stop"
            assert (await client.post(stop_path, headers=worker, json=remote_attempt)).status_code == 500
            with Session(postgres_engine) as session:
                saved = session.get(RemoteGraderRegistration, UUID(registration_id))
                assert saved.stopped_at is not None and saved.revoked_at is None
                assert saved.token_ciphertext is None
            assert (await client.post(pickup_path, headers=worker, json=pickup_body)).status_code == 409
            revoke_status = 200
            assert (await client.post(stop_path, headers=worker, json=remote_attempt)).json() == {"stopped": True}
            with Session(postgres_engine) as session:
                saved = session.get(RemoteGraderRegistration, UUID(registration_id))
                assert saved.revoked_at is not None
            assert (await client.post(stop_path, headers=worker, json=remote_attempt)).json() == {"stopped": True}
            second_stop = await client.post(
                f"/remote-grader/registrations/{second_registration_id}/stop",
                headers=worker,
                json=second_attempt,
            )
            assert second_stop.json() == {"stopped": True}
            assert [path for path, _, _ in requests] == [
                "/service-auth",
                "/service-auth",
                "/service-auth/revoke",
                "/service-auth/revoke",
                "/service-auth/revoke",
            ]
