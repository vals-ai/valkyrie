import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import boto3
import metric_process
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker import config
from tracker.services.datasets.rewards import RewardMap

INVOKE_CONFIG = Config(connect_timeout=5, read_timeout=900, retries={"max_attempts": 0})


@dataclass(frozen=True)
class MetricResult:
    metrics: dict[str, float]
    duration: float


class MetricRunnerError(Exception):
    pass


class MetricInvoker(Protocol):
    async def invoke(self, tenant_id: str, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult: ...


class LambdaMetricInvoker:
    def __init__(self, function_name: str) -> None:
        self._function_name = function_name
        self._client = boto3.client("lambda", region_name=config.AWS_DEPLOYMENT_REGION, config=INVOKE_CONFIG)

    async def invoke(self, tenant_id: str, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult:
        payload = json.dumps({"contract": metric_process.CONTRACT, "script": script, "rewards": list(rewards)}).encode()
        try:
            response = await asyncio.to_thread(
                self._client.invoke, FunctionName=self._function_name, TenantId=tenant_id, Payload=payload
            )
            body = cast(dict[str, Any], json.loads(response["Payload"].read()))
        except (BotoCoreError, ClientError, ValueError) as error:
            raise MetricRunnerError(f"The metric runner could not be invoked: {error}") from error
        if "FunctionError" in response or "error" in body:
            raise MetricRunnerError(str(body.get("error") or body.get("errorMessage") or "The metric runner failed"))
        return MetricResult(metrics=dict(body["metrics"]), duration=float(body["duration"]))


class LocalMetricInvoker:
    async def invoke(self, tenant_id: str, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult:
        event = {"contract": metric_process.CONTRACT, "script": script, "rewards": list(rewards)}
        try:
            result = await asyncio.to_thread(metric_process.run, event)
        except metric_process.MetricError as error:
            raise MetricRunnerError(str(error)) from error
        return MetricResult(metrics=cast(dict[str, float], result["metrics"]), duration=cast(float, result["duration"]))
