from typing import Literal


EgressPolicy = Literal["*"] | list[str]


def combine_run_egress_policies(
    benchmark_policy: EgressPolicy | None,
    legacy_agent_allowlist: list[str],
) -> EgressPolicy:
    if benchmark_policy is None:
        return legacy_agent_allowlist or "*"
    if benchmark_policy == "*":
        return "*"
    return list(dict.fromkeys([*benchmark_policy, *legacy_agent_allowlist]))


def child_key_id(attempt_id: str, profile: str) -> str:
    return f"{attempt_id}-child-{profile}"
