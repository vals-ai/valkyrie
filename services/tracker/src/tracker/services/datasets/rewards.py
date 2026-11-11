import math
from collections.abc import Mapping, Sequence
from typing import Any

from tracker.services.datasets.config import is_table

RewardMap = dict[str, float]
SCORE_KEY = "reward"


def normalize_rewards(rewards: Mapping[str, object]) -> RewardMap:
    normalized: RewardMap = {}
    for key, value in rewards.items():
        if isinstance(value, bool) or not isinstance(value, int | float | str):
            raise TypeError(f"Expected a numeric reward for {key!r}, got {type(value).__name__}")
        reward = float(value)
        if not math.isfinite(reward):
            raise ValueError(f"The reward for {key!r} is not a finite number: {value!r}")
        normalized[key] = reward
    return normalized


def extract_rewards(result: object) -> RewardMap | None:
    rewards = result.get("rewards") if is_table(result) else None
    return normalize_rewards(rewards) if is_table(rewards) else None


def mean_rewards(rewards: Sequence[RewardMap | None]) -> RewardMap:
    keys = sorted({key for reward in rewards if reward is not None for key in reward})
    if len(keys) <= 1:
        values = [0.0 if reward is None else next(iter(reward.values()), 0.0) for reward in rewards]
        return {"mean": sum(values) / len(values) if values else 0.0}
    return {
        key: sum(0.0 if reward is None else reward.get(key, 0.0) for reward in rewards) / len(rewards) for key in keys
    }


def primary_score(means: RewardMap) -> tuple[float, str] | None:
    if SCORE_KEY in means:
        key = SCORE_KEY
    elif len(means) == 1:
        key = next(iter(means))
    else:
        return None
    return (means[key], key) if 0.0 <= means[key] <= 1.0 else None


def reward_stats(rewards_by_task: Mapping[str, RewardMap | None]) -> dict[str, dict[float, list[str]]]:
    stats: dict[str, dict[float, list[str]]] = {}
    for task_id, rewards in rewards_by_task.items():
        for key, value in (rewards or {}).items():
            stats.setdefault(key, {}).setdefault(value, []).append(task_id)
    return stats


def final_score(evaluation_results: Mapping[str, object]) -> tuple[float | None, dict[str, Any]]:
    rewards_by_task = {task_id: extract_rewards(result) for task_id, result in evaluation_results.items()}
    rewards = list(rewards_by_task.values())
    means = mean_rewards(rewards)
    headline = primary_score(means)
    mean_reward, score_key = (None, None) if headline is None else headline
    return None if mean_reward is None else mean_reward * 100, {
        "total": len(rewards),
        "n_trials": sum(reward is not None for reward in rewards),
        "n_errors": sum(reward is None for reward in rewards),
        "metrics": [means],
        "reward_stats": reward_stats(rewards_by_task),
        "score_key": score_key,
        "mean_reward": mean_reward,
    }
