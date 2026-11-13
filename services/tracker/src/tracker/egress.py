from typing import Literal
from contextvars import ContextVar

from tracker.logging import get_logger

EgressPolicy = Literal["*"] | list[str]
logger = get_logger(__name__)
policy_context: ContextVar[dict[str, str]] = ContextVar("egress_policy_context", default={})


def select_policy(
    policy: EgressPolicy | None,
    stage: str,
    step: str | None = None,
    phase: str | None = None,
    *,
    context: dict[str, str] | None = None,
) -> EgressPolicy:
    logger.info(
        "Task egress policy selected",
        extra={
            **(policy_context.get() if context is None else context),
            "event": "task.egress.policy",
            "stage": stage,
            "step": step,
            "phase": phase,
            "implicit_allow_all": policy is None,
        },
    )
    return "*" if policy is None else policy


def combine_run_egress_policies(
    benchmark_policy: EgressPolicy | None,
    legacy_agent_allowlist: list[str],
    *,
    default_allow_all: bool = False,
) -> EgressPolicy | None:
    if benchmark_policy is None:
        return None if default_allow_all else legacy_agent_allowlist or None
    if benchmark_policy == "*":
        return "*"
    return list(dict.fromkeys([*benchmark_policy, *legacy_agent_allowlist]))


def child_key_id(attempt_id: str, profile: str) -> str:
    return f"{attempt_id}-child-{profile}"
