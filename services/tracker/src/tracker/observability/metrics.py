import logging
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from sentry_sdk import metrics as _sentry_metrics

from tracker.logging.context import sandbox_provider_var

logger = logging.getLogger(__name__)

_BANNED_TAG_KEYS = {
    "benchmark_id",
    "command",
    "error_message",
    "org_id",
    "request_id",
    "run_id",
    "sandbox_id",
    "session_id",
    "task_id",
}

TASK_OUTCOME_METRIC = "valkyrie.task.outcome"


class TaskOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNHANDLED_ERROR = "unhandled_error"


def _normalize(tags: Mapping[str, Any] | None) -> dict[str, str]:
    if not tags:
        return {}

    attributes: dict[str, str] = {}
    for key, value in tags.items():
        if value is None:
            continue

        key_str = str(key)
        if key_str in _BANNED_TAG_KEYS:
            logger.debug("dropping high-cardinality metric tag %s", key_str)
            continue

        attributes[key_str] = str(value)

    return attributes


def incr(name: str, value: float = 1, tags: Mapping[str, Any] | None = None) -> None:
    try:
        _sentry_metrics.count(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric incr(%s) failed: %s: %s", name, type(e).__name__, e)


def distribution(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
    try:
        _sentry_metrics.distribution(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric distribution(%s) failed: %s: %s", name, type(e).__name__, e)


def gauge(name: str, value: float, tags: Mapping[str, Any] | None = None) -> None:
    try:
        _sentry_metrics.gauge(name, value, attributes=_normalize(tags))
    except Exception as e:
        logger.warning("metric gauge(%s) failed: %s: %s", name, type(e).__name__, e)


def record_task_outcome(outcome: TaskOutcome, tags: Mapping[str, Any] | None = None) -> None:
    incr(
        TASK_OUTCOME_METRIC,
        tags={"outcome": outcome, "sandbox_provider": sandbox_provider_var.get() or None, **(tags or {})},
    )
