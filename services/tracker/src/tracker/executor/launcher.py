from __future__ import annotations

import asyncio
import logging
import os
import random
import subprocess
import sys
from datetime import UTC, datetime
from functools import partial
from threading import BoundedSemaphore
from time import monotonic
from typing import Any
from uuid import UUID

from botocore.exceptions import ClientError
from sqlmodel import Session

from tracker.aws.clients import ECS_LAUNCH_DEADLINE, ecs_client
from tracker.database.models import ExecutorDispatch
from tracker.database.session import engine
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.executor_release import ExecutorReleaseRepository

logger = logging.getLogger(__name__)
_ECS_RUNNER_PYTHON = "/app/.venv/bin/python"
_LAUNCH_TIMEOUT_SECONDS = 180
_CLAIM_ALLOWANCE_SECONDS = 90
_RUN_TASK_CONCURRENCY = 8
_run_task_semaphore = asyncio.Semaphore(_RUN_TASK_CONCURRENCY)
_run_task_slots = BoundedSemaphore(_RUN_TASK_CONCURRENCY)
_run_task_calls: set[asyncio.Task[dict[str, Any]]] = set()


class DefinitiveLaunchFailure(Exception):
    pass


def _task_definition(dispatch: ExecutorDispatch) -> str:
    with Session(engine) as session:
        task_definition_arn = ExecutorReleaseRepository(session).task_definition_arn(dispatch.executor_release_id)
    if task_definition_arn is None:
        raise DefinitiveLaunchFailure(f"Executor release {dispatch.executor_release_id} has no task definition")
    return task_definition_arn


def _record_task_arn(dispatch_id: UUID, task_arn: str) -> None:
    with Session(engine) as session:
        dispatch = ExecutorDispatchRepository(session).get(dispatch_id)
        if dispatch is None:
            raise RuntimeError(f"Executor dispatch {dispatch_id} disappeared after launch")
        dispatch.ecs_task_arn = task_arn
        session.commit()


async def _record_task_arn_after_launch(dispatch_id: UUID, task_arn: str) -> None:
    for attempt in range(3):
        try:
            await asyncio.to_thread(_record_task_arn, dispatch_id, task_arn)
            return
        except Exception:
            if attempt == 2:
                logger.exception("Failed to record ECS task %s for executor dispatch %s", task_arn, dispatch_id)
                return
            await asyncio.sleep(0.2 * (2**attempt))


def _run_task_params(dispatch: ExecutorDispatch, client_token: str) -> dict[str, Any]:
    dispatch_id = str(dispatch.id)
    return {
        "cluster": os.environ["EXECUTOR_RUNNER_CLUSTER"],
        "taskDefinition": _task_definition(dispatch),
        "launchType": "FARGATE",
        "count": 1,
        "clientToken": client_token,
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": os.environ["EXECUTOR_RUNNER_SUBNETS"].split(","),
                "securityGroups": [os.environ["EXECUTOR_RUNNER_SECURITY_GROUP"]],
                "assignPublicIp": "ENABLED",
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": os.environ["EXECUTOR_RUNNER_CONTAINER"],
                    "command": [_ECS_RUNNER_PYTHON, "-m", "tracker.executor.watchdog", "--dispatch-id", dispatch_id],
                }
            ]
        },
    }


def relaunch_dispatch(dispatch: ExecutorDispatch) -> str:
    response = _call_run_task(
        ecs_client(recovery=True), _run_task_params(dispatch, client_token=f"{dispatch.id}-relaunch")
    )
    if len(response["tasks"]) != 1:
        raise RuntimeError(
            f"ECS did not start a replacement for executor dispatch {dispatch.id}: {response['failures']}"
        )

    return response["tasks"][0]["taskArn"]


def _run_task_finished(task: asyncio.Task[dict[str, Any]], semaphore: asyncio.Semaphore) -> None:
    _run_task_calls.discard(task)
    semaphore.release()
    if not task.cancelled():
        task.exception()


def _call_run_task(ecs: Any, params: dict[str, Any]) -> dict[str, Any]:
    with _run_task_slots:
        return ecs.run_task(**params)


async def _run_task(ecs: Any, params: dict[str, Any]) -> dict[str, Any]:
    semaphore = _run_task_semaphore
    await semaphore.acquire()
    task = asyncio.create_task(asyncio.to_thread(_call_run_task, ecs, params))
    _run_task_calls.add(task)
    task.add_done_callback(partial(_run_task_finished, semaphore=semaphore))
    return await asyncio.shield(task)


def _launch_budget(dispatch: ExecutorDispatch) -> float:
    if dispatch.claim_deadline_at is None:
        return _LAUNCH_TIMEOUT_SECONDS
    deadline = dispatch.claim_deadline_at.replace(tzinfo=UTC)
    remaining = (deadline - datetime.now(UTC)).total_seconds() - _CLAIM_ALLOWANCE_SECONDS
    return max(0, min(_LAUNCH_TIMEOUT_SECONDS, remaining))


async def launch_dispatch(dispatch: ExecutorDispatch) -> None:
    launcher = os.environ["EXECUTOR_LAUNCHER"]
    dispatch_id = str(dispatch.id)
    command = [sys.executable, "-m", "tracker.executor.watchdog", "--dispatch-id", dispatch_id]
    if launcher == "local":
        runner_environment = os.environ.copy()
        for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USERNAME", "DB_PASSWORD", "DATABASE_URL"):
            runner_environment.pop(key, None)
        await asyncio.to_thread(subprocess.Popen, command, start_new_session=True, env=runner_environment)
        return
    if launcher != "ecs":
        raise ValueError("EXECUTOR_LAUNCHER must be ecs or local")

    budget = _launch_budget(dispatch)
    started = monotonic()
    last_throttle: ClientError | None = None
    deadline_token = ECS_LAUNCH_DEADLINE.set(started + budget)
    try:
        async with asyncio.timeout(budget):
            params = await asyncio.to_thread(_run_task_params, dispatch, dispatch_id)
            ecs = await asyncio.to_thread(ecs_client)
            attempt = 0
            while True:
                try:
                    response = await _run_task(ecs, params)
                    tasks = response["tasks"]
                    if response["failures"] and not tasks:
                        raise DefinitiveLaunchFailure(
                            f"ECS rejected executor dispatch {dispatch_id}: {response['failures']}"
                        )
                    if len(tasks) != 1:
                        raise RuntimeError(f"Expected one ECS task for executor dispatch {dispatch_id}")
                    task_arn = tasks[0]["taskArn"]
                    break
                except DefinitiveLaunchFailure:
                    raise
                except Exception as error:
                    throttled = isinstance(error, ClientError) and error.response.get("Error", {}).get("Code") in {
                        "ThrottlingException",
                        "Throttling",
                        "RequestLimitExceeded",
                        "TooManyRequestsException",
                    }
                    server_error = isinstance(error, ClientError) and (
                        error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) >= 500
                        or error.response.get("Error", {}).get("Code")
                        in {"ServerException", "ServiceUnavailableException", "InternalFailure"}
                    )
                    if isinstance(error, ClientError) and not (throttled or server_error):
                        raise
                    if isinstance(error, ClientError) and throttled:
                        last_throttle = error
                    if attempt >= (19 if throttled else 2):
                        if throttled:
                            raise RuntimeError(
                                f"ECS RunTask throttled for {monotonic() - started:.1f}s: {error}"
                            ) from error
                        raise
                    await asyncio.sleep(random.uniform(0, min(8, 2**attempt)))
                    attempt += 1
    except TimeoutError as error:
        cause = (
            f"ECS RunTask throttled for {budget:g}s: {last_throttle}"
            if last_throttle is not None
            else f"ECS RunTask launch exceeded its {budget:g}s budget"
        )
        raise TimeoutError(cause) from error
    finally:
        ECS_LAUNCH_DEADLINE.reset(deadline_token)

    logger.info("Launched executor dispatch %s as ECS task %s", dispatch_id, task_arn)
    await _record_task_arn_after_launch(dispatch.id, task_arn)
