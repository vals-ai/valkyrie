import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import boto3
import httpx
import sentry_sdk
from benchmark_service import BenchmarkServiceClient, BenchmarkServiceError
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy.engine import Engine

from tracker.config import BenchmarkServiceDestination, classify_benchmark_service_destination
from tracker.database.models import BenchmarkStatus, TaskStatus
from tracker.database.session import engine as tracker_engine
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.repositories.service_demand import active_runs, recent_tasks, record_wake, wakes_since
from tracker.scheduler.store import PostgresAdvisoryLock

logger = get_logger(__name__)

DEMAND_SCALING_TAG = "vals:demand-scaling"
MIN_TASKS_TAG = "vals:min-tasks"
SERVICE_TAG = "vals:service"

_ACTIVE_RUN_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ACTIVE_TASK_STATUSES = (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_IDLE_MINUTES = 30
_DOWN_MINUTES = 10
_RECONCILE_SECONDS = 30.0
_AWS_CONFIG = Config(connect_timeout=3, read_timeout=8, retries={"max_attempts": 3})
_WAKE_TIMEOUT_SECONDS = 15 * 60.0
_WAKE_POLL_SECONDS = 15.0
API_WAKE_SECONDS = 40.0


@dataclass(frozen=True)
class ServiceDemand:
    active_runs: int = 0
    active_tasks: int = 0
    last_active: datetime | None = None

    def needed(self, now: datetime) -> bool:
        recent = self.last_active is not None and now - self.last_active < timedelta(minutes=_IDLE_MINUTES)
        return bool(self.active_runs or self.active_tasks or recent)


@dataclass(frozen=True)
class ScaledService:
    name: str
    ecs_name: str
    min_tasks: int
    desired: int
    running: int = 0
    deploying: bool = False

    def target(self, demand: ServiceDemand, now: datetime) -> int:
        if demand.needed(now):
            return max(self.min_tasks, 1, self.desired)
        return self.desired if self.deploying else 0


def _ecs() -> Any:
    return cast(Any, boto3.client("ecs", config=_AWS_CONFIG))  # pyright: ignore[reportUnknownMemberType]


def _utc(moment: datetime | None) -> datetime | None:
    return moment.replace(tzinfo=UTC) if moment is not None and moment.tzinfo is None else moment


def _latest(*moments: datetime | None) -> datetime | None:
    return max((moment for moment in moments if moment is not None), default=None)


def read_demand(engine: Engine, now: datetime) -> dict[str, ServiceDemand]:
    since = now - timedelta(minutes=_IDLE_MINUTES)
    runs = active_runs(engine, _ACTIVE_RUN_STATUSES)
    tasks = recent_tasks(engine, _ACTIVE_TASK_STATUSES, since)
    wakes = wakes_since(engine, since)

    demand: dict[str, ServiceDemand] = {}

    def add(name: str, custom: str | None, runs: int = 0, tasks: int = 0, active: datetime | None = None) -> None:
        try:
            if classify_benchmark_service_destination(name, custom).value != BenchmarkServiceDestination.HOSTED.value:
                return
        except ValueError:
            return
        current = demand.get(name, ServiceDemand())
        demand[name] = ServiceDemand(
            active_runs=current.active_runs + runs,
            active_tasks=current.active_tasks + tasks,
            last_active=_latest(current.last_active, _utc(active)),
        )

    for name, custom, count in runs:
        add(name, custom, runs=count)
    for name, custom, count, finished_at in tasks:
        add(name, custom, tasks=count, active=finished_at)
    for wake in wakes:
        add(wake.name, None, active=wake.requested_at)
    return demand


def _scaled_service(raw: dict[str, Any]) -> ScaledService | None:
    tags = {tag["key"]: tag["value"] for tag in raw.get("tags", [])}
    if tags.get(DEMAND_SCALING_TAG) != "tracker" or raw["status"] != "ACTIVE":
        return None
    try:
        min_tasks = int(tags.get(MIN_TASKS_TAG, "1"))
    except ValueError:
        logger.warning("Ignoring malformed min-tasks tag", extra={"ecs_service": raw["serviceName"]})
        min_tasks = 1
    deployments = raw.get("deployments", [])
    return ScaledService(
        name=tags.get(SERVICE_TAG, raw["serviceName"]),
        ecs_name=raw["serviceName"],
        min_tasks=min_tasks,
        desired=raw["desiredCount"],
        running=raw.get("runningCount", 0),
        deploying=len(deployments) > 1
        or any(deployment.get("rolloutState") == "IN_PROGRESS" for deployment in deployments),
    )


def describe_scaled_services(ecs: Any, cluster: str, ecs_names: list[str]) -> list[ScaledService]:
    services: list[ScaledService] = []
    for offset in range(0, len(ecs_names), 10):
        response = ecs.describe_services(cluster=cluster, services=ecs_names[offset : offset + 10], include=["TAGS"])
        for failure in response.get("failures", []):
            logger.warning("Could not describe benchmark ECS service", extra={"failure": failure})
        services.extend(service for raw in response["services"] if (service := _scaled_service(raw)))
    return services


def read_scaled_services(ecs: Any, cluster: str) -> list[ScaledService]:
    arns = [arn for page in ecs.get_paginator("list_services").paginate(cluster=cluster) for arn in page["serviceArns"]]
    return describe_scaled_services(ecs, cluster, arns)


def reconcile(ecs: Any, engine: Engine, cluster: str, down_since: dict[str, datetime]) -> None:
    now = datetime.now(UTC)
    services = read_scaled_services(ecs, cluster)
    demand = read_demand(engine, now)
    for service in services:
        needed = demand.get(service.name, ServiceDemand())
        _check_down(service, needed, now, down_since)
        target = service.target(needed, now)
        if target == service.desired:
            continue
        logger.info(
            "Scaling benchmark service",
            extra={
                "service": service.name,
                "desired_count": service.desired,
                "target_count": target,
                "active_runs": needed.active_runs,
                "active_tasks": needed.active_tasks,
            },
        )
        try:
            ecs.update_service(cluster=cluster, service=service.ecs_name, desiredCount=target)
        except Exception:
            logger.exception("Could not reconcile benchmark service", extra={"service": service.name})


def _check_down(service: ScaledService, demand: ServiceDemand, now: datetime, down_since: dict[str, datetime]) -> None:
    if service.running or not demand.needed(now):
        down_since.pop(service.ecs_name, None)
        return
    since = down_since.setdefault(service.ecs_name, now)
    if now - since >= timedelta(minutes=_DOWN_MINUTES):
        sentry_sdk.capture_message(
            f"Benchmark service {service.name} ({service.ecs_name}) has had demand but no running task "
            f"for {_DOWN_MINUTES} minutes",
            level="error",
        )
        down_since[service.ecs_name] = now


class ServiceCapacityScheduler:
    def __init__(self, engine: Engine = tracker_engine, interval_seconds: float = _RECONCILE_SECONDS) -> None:
        self._engine = engine
        self._interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        cluster = os.environ.get("EXECUTOR_RUNNER_CLUSTER")
        if not cluster:
            return
        self._task = asyncio.create_task(self._run(cluster), name="service-capacity")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self, cluster: str) -> None:
        ecs = _ecs()
        lock = PostgresAdvisoryLock(self._engine, resource_id="benchmark-service-capacity")
        down_since: dict[str, datetime] = {}
        while True:
            try:
                if lock.held or await lock.acquire():
                    if await lock.still_held():
                        await asyncio.to_thread(reconcile, ecs, self._engine, cluster, down_since)
            except asyncio.CancelledError:
                await lock.release()
                raise
            except Exception:
                logger.exception("Benchmark service capacity reconcile failed")
            await asyncio.sleep(self._interval_seconds)


def wake_scaled_service(ecs: Any, cluster: str, name: str) -> ScaledService | None:
    service = next((service for service in read_scaled_services(ecs, cluster) if service.name == name), None)
    if service is not None and not service.deploying and service.desired < 1:
        ecs.update_service(cluster=cluster, service=service.ecs_name, desiredCount=1)
    return service


async def wake_benchmark_service(
    name: str,
    client: BenchmarkServiceClient,
    *,
    timeout_seconds: float = _WAKE_TIMEOUT_SECONDS,
    engine: Engine = tracker_engine,
) -> None:
    cluster = os.environ.get("EXECUTOR_RUNNER_CLUSTER")
    if not cluster:
        return
    try:
        service = await asyncio.to_thread(wake_scaled_service, _ecs(), cluster, name)
    except (BotoCoreError, ClientError):
        logger.warning("Could not wake benchmark service", exc_info=True, extra={"service": name})
        return
    if service is None:
        return
    await asyncio.to_thread(record_wake, engine, name)
    await wait_for_benchmark_service(client, timeout_seconds=timeout_seconds)


async def wait_for_benchmark_service(
    client: BenchmarkServiceClient,
    *,
    timeout_seconds: float = _WAKE_TIMEOUT_SECONDS,
    poll_seconds: float = _WAKE_POLL_SECONDS,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TrackerServiceError("Benchmark service did not become ready before the wake deadline")
        try:
            await asyncio.wait_for(client.health_check(), remaining)
            return
        except TimeoutError as error:
            raise TrackerServiceError("Benchmark service did not become ready before the wake deadline") from error
        except (httpx.TransportError, BenchmarkServiceError) as error:
            if isinstance(error, BenchmarkServiceError) and (error.status_code or 500) < 500:
                raise
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TrackerServiceError(f"Benchmark service is not ready: {error}") from error
            logger.info("Waiting for benchmark service to start", extra={"error": str(error)})
            await asyncio.sleep(min(poll_seconds, remaining))
