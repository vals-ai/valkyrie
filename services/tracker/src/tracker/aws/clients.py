from __future__ import annotations

import asyncio
from collections.abc import Callable, Hashable
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from threading import Lock
from time import monotonic
from typing import Any, cast

import aioboto3
import boto3
from aiobotocore.credentials import AioDeferredRefreshableCredentials
from aiobotocore.session import AioSession
from botocore.config import Config

from tracker.aws.tags import ORG_ID_TAG, SECRET_SCOPE_TAG

_LOGS_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "adaptive", "total_max_attempts": 20})
_S3_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})
ECS_CONFIG = Config(
    connect_timeout=3,
    read_timeout=8,
    max_pool_connections=200,
    retries={"mode": "standard", "total_max_attempts": 3},
)
ECS_RUN_TASK_CONFIG = Config(
    connect_timeout=3,
    read_timeout=8,
    max_pool_connections=200,
    retries={"mode": "standard", "total_max_attempts": 1},
)
ECS_LAUNCH_DEADLINE: ContextVar[float | None] = ContextVar("ecs_launch_deadline", default=None)
_DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS = 3600
_TENANT_ROLE_MAXIMUM_PRESIGN_TTL_SECONDS = 600


_loop_sessions_lock = Lock()
_ecs_client_lock = Lock()
_loop_sessions: dict[asyncio.AbstractEventLoop, dict[Hashable, aioboto3.Session]] = {}


def _loop_session(key: Hashable, create: Callable[[], aioboto3.Session]) -> aioboto3.Session:
    loop = asyncio.get_running_loop()
    with _loop_sessions_lock:
        for closed in [other for other in _loop_sessions if other.is_closed()]:
            del _loop_sessions[closed]
        sessions = _loop_sessions.setdefault(loop, {})
        if key not in sessions:
            sessions[key] = create()
        return sessions[key]


@lru_cache(maxsize=32)
def _logs_client(region: str) -> Any:
    client_factory = cast(Any, boto3.client)  # pyright: ignore[reportUnknownMemberType]
    return client_factory("logs", config=_LOGS_CLIENT_CONFIG, region_name=region)


def _check_ecs_launch_deadline(**_kwargs: Any) -> None:
    deadline = ECS_LAUNCH_DEADLINE.get()
    if deadline is not None and monotonic() >= deadline:
        raise TimeoutError("ECS launch deadline expired")


@lru_cache(maxsize=2)
def _ecs_client(recovery: bool) -> Any:
    client_factory = cast(Any, boto3.client)  # pyright: ignore[reportUnknownMemberType]
    client = client_factory("ecs", config=ECS_CONFIG if recovery else ECS_RUN_TASK_CONFIG)
    client.meta.events.register_last("before-send", _check_ecs_launch_deadline)
    return client


def ecs_client(*, recovery: bool = False) -> Any:
    with _ecs_client_lock:
        return _ecs_client(recovery)


@dataclass(frozen=True)
class AWSClientProvider:
    region: str

    def _task_session(self) -> aioboto3.Session:
        return _loop_session(self.region, lambda: aioboto3.Session(region_name=self.region))

    def _data_session(self) -> aioboto3.Session:
        return self._task_session()

    def s3_client(self) -> Any:
        return self._data_session().client("s3", config=_S3_CLIENT_CONFIG)  # pyright: ignore[reportUnknownMemberType]

    def secretsmanager_async_client(self) -> Any:
        return self._data_session().client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]

    def cloudwatch_logs_client(self) -> Any:
        return _logs_client(self.region)

    def cloudwatch_logs_async_client(self) -> Any:
        return cast(Any, self._task_session().client("logs", config=_LOGS_CLIENT_CONFIG))  # pyright: ignore[reportUnknownMemberType]

    def lambda_client(self, config: Config | None = None) -> Any:
        return cast(Any, self._task_session().client("lambda", config=config))  # pyright: ignore[reportUnknownMemberType]

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return min(requested_seconds, _DEFAULT_CHAIN_MAXIMUM_PRESIGN_TTL_SECONDS)


@dataclass(frozen=True)
class TenantRoleAWSClientProvider(AWSClientProvider):
    role_arn: str
    org_id: str
    shared_secrets: bool

    def _data_session(self) -> aioboto3.Session:
        return _loop_session(self, self._tenant_session)

    def maximum_presign_ttl(self, requested_seconds: int) -> int:
        return min(requested_seconds, _TENANT_ROLE_MAXIMUM_PRESIGN_TTL_SECONDS)

    def _tenant_session(self) -> aioboto3.Session:
        botocore_session = AioSession()
        botocore_session._credentials = AioDeferredRefreshableCredentials(  # pyright: ignore[reportAttributeAccessIssue]
            refresh_using=self._assume_role, method="sts-assume-role"
        )
        return aioboto3.Session(botocore_session=botocore_session, region_name=self.region)

    async def _assume_role(self) -> dict[str, str]:
        async with self._task_session().client("sts") as sts:  # pyright: ignore[reportUnknownMemberType]
            response = await sts.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=f"valkyrie-{self.org_id}",
                Tags=[
                    {"Key": ORG_ID_TAG, "Value": self.org_id},
                    *([{"Key": SECRET_SCOPE_TAG, "Value": "shared-internal"}] if self.shared_secrets else []),
                ],
            )
        credentials = cast(dict[str, Any], response["Credentials"])
        return {
            "access_key": credentials["AccessKeyId"],
            "secret_key": credentials["SecretAccessKey"],
            "token": credentials["SessionToken"],
            "expiry_time": credentials["Expiration"].isoformat(),
        }
