import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from benchmark_service import BenchmarkServiceError
from botocore.exceptions import ClientError
from fastapi import HTTPException
from sqlalchemy.engine import Engine
from sqlmodel import Session

import main as main_module
from tests.factories import make_benchmark, make_task
from tracker.database.models import BenchmarkServiceWake, BenchmarkStatus, TaskStatus
from tracker.exceptions import TrackerServiceError
from tracker.repositories import service_demand
from tracker.scheduler import service_capacity
from tracker.scheduler.service_capacity import ScaledService, ServiceDemand

NOW = datetime(2026, 10, 29, 12, tzinfo=UTC)
TRACKER_TAG = {"key": "vals:demand-scaling", "value": "tracker"}


def ecs_service(name: str, desired: int = 0, *tags: dict[str, str], **fields: Any) -> dict[str, Any]:
    return {
        "serviceName": name,
        "desiredCount": desired,
        "runningCount": desired,
        "status": "ACTIVE",
        "tags": list(tags),
    } | fields


def ecs_cluster(*services: dict[str, Any]) -> Mock:
    ecs = Mock()
    ecs.get_paginator.return_value.paginate.return_value = [
        {"serviceArns": [service["serviceName"] for service in services]}
    ]
    ecs.describe_services.return_value = {"services": list(services)}
    return ecs


@pytest.mark.description(
    "Tagged ECS services are read with their `vals:service` name, floor and deploy state, and untagged ones are skipped."
)
def test_tagged_services_are_read_from_their_own_tags() -> None:
    ecs = ecs_cluster(
        ecs_service(
            "a2j-r2",
            0,
            TRACKER_TAG,
            {"key": "vals:min-tasks", "value": "2"},
            {"key": "vals:service", "value": "a2j"},
            deployments=[{"rolloutState": "IN_PROGRESS"}],
        ),
        ecs_service("valsmith", 1),
        ecs_service("emb", 0, TRACKER_TAG, {"key": "vals:min-tasks", "value": "x"}),
    )

    assert service_capacity.read_scaled_services(ecs, "cluster") == [
        ScaledService("a2j", "a2j-r2", min_tasks=2, desired=0, deploying=True),
        ScaledService("emb", "emb", min_tasks=1, desired=0),
    ]
    ecs.describe_services.assert_called_once_with(
        cluster="cluster", services=["a2j-r2", "valsmith", "emb"], include=["TAGS"]
    )


@pytest.mark.description(
    "A service keeps its count while it has demand or is deploying and drops to 0 after 30 idle minutes."
)
@pytest.mark.parametrize(
    ("desired", "demand", "deploying", "min_tasks", "target"),
    [
        (0, ServiceDemand(active_runs=1), False, 1, 1),
        (0, ServiceDemand(last_active=NOW), False, 1, 1),
        (0, ServiceDemand(active_tasks=9), False, 1, 1),
        (3, ServiceDemand(active_tasks=1), False, 1, 3),
        (0, ServiceDemand(active_runs=1), False, 2, 2),
        (0, ServiceDemand(active_runs=1), True, 1, 1),
        (2, ServiceDemand(last_active=NOW - timedelta(minutes=29)), False, 1, 2),
        (2, ServiceDemand(last_active=NOW - timedelta(minutes=30)), False, 1, 0),
        (2, ServiceDemand(), True, 1, 2),
        (2, ServiceDemand(), False, 1, 0),
    ],
)
def test_target_count(desired: int, demand: ServiceDemand, deploying: bool, min_tasks: int, target: int) -> None:
    service = ScaledService("a2j", "a2j", min_tasks=min_tasks, desired=desired, deploying=deploying)
    assert service.target(demand, NOW) == target


@pytest.mark.description("Reconcile only updates tagged services and keeps going after one update fails.")
def test_reconcile_skips_untagged_services_and_continues_after_update_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ecs = ecs_cluster(
        ecs_service("first", 0, TRACKER_TAG), ecs_service("second", 0, TRACKER_TAG), ecs_service("untagged")
    )
    ecs.describe_services.return_value["failures"] = [{"arn": "missing-service", "reason": "MISSING"}]
    ecs.update_service.side_effect = [RuntimeError("failed"), None]
    logger = Mock()
    monkeypatch.setattr(
        service_capacity,
        "read_demand",
        lambda *_args: {name: ServiceDemand(active_runs=1) for name in ("first", "second", "untagged")},
    )
    monkeypatch.setattr(service_capacity, "logger", logger)

    service_capacity.reconcile(ecs, cast(Engine, Mock()), "cluster", {})

    assert [call.kwargs["service"] for call in ecs.update_service.call_args_list] == ["first", "second"]
    assert logger.warning.call_args.args[0] == "Could not describe benchmark ECS service"
    assert logger.exception.call_args.args[0] == "Could not reconcile benchmark service"


@pytest.mark.description("A service with demand and no running task is reported to Sentry after 10 minutes.")
def test_reconcile_reports_a_needed_service_that_stays_down(monkeypatch: pytest.MonkeyPatch) -> None:
    ecs = ecs_cluster(ecs_service("a2j", 1, TRACKER_TAG, runningCount=0))
    capture = Mock()
    monkeypatch.setattr(service_capacity.sentry_sdk, "capture_message", capture)
    monkeypatch.setattr(service_capacity, "read_demand", lambda *_args: {"a2j": ServiceDemand(active_runs=1)})
    down_since: dict[str, datetime] = {}

    service_capacity.reconcile(ecs, cast(Engine, Mock()), "cluster", down_since)
    capture.assert_not_called()

    down_since["a2j"] -= timedelta(minutes=10)
    service_capacity.reconcile(ecs, cast(Engine, Mock()), "cluster", down_since)
    capture.assert_called_once()

    ecs.describe_services.return_value = {"services": [ecs_service("a2j", 1, TRACKER_TAG)]}
    service_capacity.reconcile(ecs, cast(Engine, Mock()), "cluster", down_since)
    assert down_since == {}


@pytest.mark.description(
    "Demand counts active hosted runs, unfinished tasks and wakes, and ignores finished runs and custom services."
)
def test_demand_counts_hosted_runs_tasks_and_wakes(database_session: Session) -> None:
    engine = cast(Engine, database_session.get_bind())
    hosted = make_benchmark("a2j", session=database_session)
    finished = make_benchmark("a2j", status=BenchmarkStatus.FINISHED, session=database_session)
    custom = make_benchmark("a2j", session=database_session)
    custom.custom_benchmark_service = "https://custom.example"
    database_session.add(custom)
    for benchmark, task_id, status in [
        (hosted, "t1", TaskStatus.PENDING),
        (hosted, "t2", TaskStatus.EVALUATING),
        (custom, "t3", TaskStatus.IN_PROGRESS),
    ]:
        database_session.add(make_task(benchmark, task_id, status=status))
    done = make_task(finished, "t4", status=TaskStatus.FINISHED)
    database_session.add(done)
    database_session.commit()
    done.finished_at = NOW - timedelta(minutes=10)
    database_session.add(done)
    database_session.commit()

    assert service_capacity.read_demand(engine, NOW) == {
        "a2j": ServiceDemand(active_runs=1, active_tasks=2, last_active=NOW - timedelta(minutes=10))
    }

    assert not service_demand.was_woken(engine, "a2j")
    service_demand.record_wake(engine, "a2j")
    service_demand.record_wake(engine, "a2j")
    assert service_demand.was_woken(engine, "a2j")
    last_active = service_capacity.read_demand(engine, datetime.now(UTC))["a2j"].last_active
    assert last_active is not None and last_active > NOW


@pytest.mark.description("The readiness wait retries cold-start errors and fails at once on client errors.")
async def test_wait_for_service_retries_cold_start_but_not_client_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    responses: list[Exception | None] = [
        httpx.ConnectError("refused"),
        BenchmarkServiceError("503", status_code=503),
        None,
        BenchmarkServiceError("401", status_code=401),
    ]

    async def health_check() -> None:
        if (response := responses.pop(0)) is not None:
            raise response

    client = Mock(health_check=health_check)
    await service_capacity.wait_for_benchmark_service(client, poll_seconds=0)
    assert len(responses) == 1
    with pytest.raises(BenchmarkServiceError):
        await service_capacity.wait_for_benchmark_service(client, poll_seconds=0)

    responses[:] = [httpx.ConnectError("refused")] * 3
    with pytest.raises(TrackerServiceError):
        await service_capacity.wait_for_benchmark_service(client, timeout_seconds=0, poll_seconds=0)


@pytest.mark.description("An API wake of a stopped tagged service records demand and raises its count to 1.")
async def test_hosted_api_wake_records_demand_and_bumps_service(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = cast(Engine, database_session.get_bind())
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    ecs = ecs_cluster(ecs_service("a2j-r2", 0, TRACKER_TAG, {"key": "vals:service", "value": "a2j"}))
    monkeypatch.setattr(service_capacity, "_ecs", lambda: ecs)
    record_wake = service_capacity.record_wake
    monkeypatch.setattr(service_capacity, "record_wake", lambda _engine, name: record_wake(engine, name))
    client = Mock(health_check=AsyncMock())

    await main_module._wake_hosted_service("a2j", None, client)

    ecs.update_service.assert_called_once_with(cluster="cluster", service="a2j-r2", desiredCount=1)
    client.health_check.assert_awaited_once_with()
    with Session(engine) as session:
        assert session.get(BenchmarkServiceWake, "a2j") is not None


@pytest.mark.description("An API call against a custom service does not wake a hosted service.")
async def test_custom_api_service_skips_wake(monkeypatch: pytest.MonkeyPatch) -> None:
    wake = AsyncMock()
    monkeypatch.setattr(main_module, "wake_benchmark_service", wake)
    client = Mock(health_check=AsyncMock())

    await main_module._wake_hosted_service("a2j", "https://custom.example", client)

    wake.assert_not_awaited()
    client.health_check.assert_not_awaited()


@pytest.mark.description("An API wake that times out returns 503 with `Retry-After`.")
async def test_api_wake_timeout_returns_retryable_503(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        main_module,
        "wake_benchmark_service",
        AsyncMock(side_effect=TrackerServiceError("not ready")),
    )

    with pytest.raises(HTTPException) as exc_info:
        await main_module._wake_hosted_service("a2j", None, Mock())

    assert exc_info.value.status_code == 503
    assert exc_info.value.headers == {"Retry-After": "60"}


@pytest.mark.description("An untagged service is not woken or waited on.")
async def test_untagged_service_is_not_woken_or_waited_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    ecs = ecs_cluster(ecs_service("a2j", 1))
    monkeypatch.setattr(service_capacity, "_ecs", lambda: ecs)
    record_wake = Mock()
    monkeypatch.setattr(service_capacity, "record_wake", record_wake)
    client = Mock(health_check=AsyncMock())

    await service_capacity.wake_benchmark_service("a2j", client, engine=cast(Engine, Mock()))

    ecs.update_service.assert_not_called()
    record_wake.assert_not_called()
    client.health_check.assert_not_awaited()


@pytest.mark.description(
    "An API wake that cannot read ECS records no wake and leaves the plain health check to the caller."
)
async def test_api_wake_falls_back_to_health_check_when_ecs_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    ecs = ecs_cluster()
    ecs.get_paginator.side_effect = ClientError({"Error": {"Code": "ThrottlingException"}}, "ListServices")
    monkeypatch.setattr(service_capacity, "_ecs", lambda: ecs)
    record_wake = Mock()
    monkeypatch.setattr(service_capacity, "record_wake", record_wake)
    client = Mock(health_check=AsyncMock())

    await main_module._wake_hosted_service("a2j", None, client)

    record_wake.assert_not_called()
    client.health_check.assert_not_awaited()


@pytest.mark.description("The readiness wait gives up on a health probe that hangs.")
async def test_wait_for_service_bounds_a_hung_health_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(health_check=AsyncMock(side_effect=asyncio.Event().wait))

    with pytest.raises(TrackerServiceError):
        await service_capacity.wait_for_benchmark_service(client, timeout_seconds=0.01)
