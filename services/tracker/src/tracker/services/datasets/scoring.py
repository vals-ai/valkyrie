import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple, Protocol
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError

from tracker import config
from tracker.services.datasets.metric_script import METRIC_FILE, file_digest
from tracker.services.datasets.ref import DatasetRef, HostedDataset, LocalDataset
from tracker.services.datasets.rewards import RewardMap, extract_rewards, final_score, primary_score, reward_stats
from tracker.clients.metric_runner import (
    LambdaMetricInvoker,
    LocalMetricInvoker,
    MetricInvoker,
    MetricResult,
    MetricRunnerError,
)
from tracker.runtime.artifacts import dataset_file_key
from tracker.runtime.storage import ObjectStore


class Score(NamedTuple):
    final_score: float | None
    properties: dict[str, Any]


class Scorer(Protocol):
    async def score(self, evaluation_results: Mapping[str, object]) -> Score: ...


class MeanScorer:
    async def score(self, evaluation_results: Mapping[str, object]) -> Score:
        return Score(*final_score(evaluation_results))


class ScriptScorer:
    def __init__(
        self,
        load_script: Callable[[], Awaitable[str]],
        script_digest: str,
        org_id: UUID,
        invoker: MetricInvoker | None,
    ) -> None:
        self._load_script = load_script
        self._script_digest = script_digest
        self._org_id = org_id
        self._invoker = invoker

    async def score(self, evaluation_results: Mapping[str, object]) -> Score:
        rewards_by_task: dict[str, RewardMap | None] = {
            task_id: extract_rewards(evaluation_results[task_id]) for task_id in sorted(evaluation_results)
        }
        rewards = list(rewards_by_task.values())
        base: dict[str, Any] = {
            "total": len(rewards),
            "n_trials": sum(reward is not None for reward in rewards),
            "n_errors": sum(reward is None for reward in rewards),
            "reward_stats": reward_stats(rewards_by_task),
        }
        scorer: dict[str, Any] = {"digest": self._script_digest}
        try:
            result = await self._run(await self._load_script(), rewards)
        except MetricRunnerError as error:
            scorer["error"] = str(error)
            return Score(None, {**base, "metrics": [], "score_key": None, "mean_reward": None, "scorer": scorer})
        metrics = result.metrics
        scorer["duration"] = result.duration
        headline = primary_score(metrics)
        mean_reward, score_key = (None, None) if headline is None else headline
        properties = {
            **base,
            "metrics": [metrics],
            "score_key": score_key,
            "mean_reward": mean_reward,
            "scorer": scorer,
        }
        return Score(None if mean_reward is None else mean_reward * 100, properties)

    async def _run(self, script: str, rewards: Sequence[RewardMap | None]) -> MetricResult:
        if self._invoker is None:
            raise MetricRunnerError("No metric runner is configured for this Tracker")
        result = await self._invoker.invoke(str(self._org_id), script, rewards)
        if not result.metrics or any(not math.isfinite(value) for value in result.metrics.values()):
            raise MetricRunnerError("The metric runner returned no finite metrics")
        return result


def _hosted_revision(ref: DatasetRef | None) -> HostedDataset | None:
    if isinstance(ref, HostedDataset) and ref.dataset_id is not None and ref.revision is not None:
        return ref
    return None


async def scorer_for(
    ref: DatasetRef | None,
    org_id: UUID,
    script_digest: str | None,
    objects: ObjectStore,
    invoker: MetricInvoker | None = None,
) -> Scorer:
    if isinstance(ref, LocalDataset):
        return await _folder_scorer(Path(ref.path) / METRIC_FILE, org_id)
    hosted = _hosted_revision(ref)
    if hosted is None or hosted.dataset_id is None or hosted.revision is None:
        return MeanScorer()
    dataset_id = hosted.dataset_id
    if script_digest is None:
        return MeanScorer()

    async def load_script() -> str:
        try:
            return (await objects.get_bytes(dataset_file_key(str(dataset_id), script_digest))).decode()
        except (OSError, BotoCoreError, ClientError, UnicodeDecodeError) as error:
            raise MetricRunnerError(f"The metric script could not be read: {error}") from error

    if invoker is None and config.METRIC_RUNNER_FUNCTION:
        invoker = LambdaMetricInvoker(config.METRIC_RUNNER_FUNCTION)
    return ScriptScorer(load_script, script_digest, org_id, invoker)


async def _folder_scorer(path: Path, org_id: UUID) -> Scorer:
    if not path.is_file():
        return MeanScorer()
    try:
        content = await asyncio.to_thread(path.read_bytes)
        script = content.decode()
    except (OSError, UnicodeDecodeError) as error:
        message = f"The metric script could not be read: {error}"

        async def unreadable() -> str:
            raise MetricRunnerError(message)

        return ScriptScorer(unreadable, "", org_id, LocalMetricInvoker())

    async def load_script() -> str:
        return script

    return ScriptScorer(load_script, file_digest(content), org_id, LocalMetricInvoker())
