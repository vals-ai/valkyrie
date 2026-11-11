from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from typing import Any
from uuid import UUID

import boto3
from botocore.config import Config
from sqlmodel import Session

from tracker.database.models import ExecutorDispatch
from tracker.database.session import engine
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.executor_release import ExecutorReleaseRepository

logger = logging.getLogger(__name__)
_ECS_RUNNER_PYTHON = "/app/.venv/bin/python"
ECS_CONFIG = Config(connect_timeout=3, read_timeout=8, retries={"max_attempts": 1})


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
    response = boto3.client("ecs", config=ECS_CONFIG).run_task(
        **_run_task_params(dispatch, client_token=f"{dispatch.id}-relaunch")
    )
    if len(response["tasks"]) != 1:
        raise RuntimeError(
            f"ECS did not start a replacement for executor dispatch {dispatch.id}: {response['failures']}"
        )

    return response["tasks"][0]["taskArn"]


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

    params = await asyncio.to_thread(_run_task_params, dispatch, dispatch_id)
    ecs = boto3.client("ecs", config=ECS_CONFIG)
    for attempt in range(3):
        try:
            response = await asyncio.to_thread(ecs.run_task, **params)
            tasks = response["tasks"]
            if response["failures"] and not tasks:
                raise DefinitiveLaunchFailure(f"ECS rejected executor dispatch {dispatch_id}")
            if len(tasks) != 1:
                raise RuntimeError(f"Expected one ECS task for executor dispatch {dispatch_id}")
            task_arn = tasks[0]["taskArn"]
            logger.info("Launched executor dispatch %s as ECS task %s", dispatch_id, task_arn)
            await _record_task_arn_after_launch(dispatch.id, task_arn)
            return
        except DefinitiveLaunchFailure:
            raise
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(0.2 * (2**attempt))
