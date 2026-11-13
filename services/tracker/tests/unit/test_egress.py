import pytest
import json
from pydantic import ValidationError

from tracker.types import AgentContractRequest
from tracker.egress import EgressPolicy, combine_run_egress_policies, select_policy


@pytest.mark.description("Effective run policies match dev while undeclared folder policies remain implicit.")
@pytest.mark.parametrize("task_folder", [False, True])
@pytest.mark.parametrize(
    ("benchmark_policy", "legacy_agent_allowlist", "expected"),
    [
        (None, [], "*"),
        (None, ["agent.example.com"], ["agent.example.com"]),
        ("*", [], "*"),
        ("*", ["agent.example.com"], "*"),
        ([], [], []),
        ([], ["agent.example.com"], ["agent.example.com"]),
        (["benchmark.example.com"], [], ["benchmark.example.com"]),
        (
            ["shared.example.com", "benchmark.example.com"],
            ["shared.example.com", "agent.example.com"],
            ["shared.example.com", "benchmark.example.com", "agent.example.com"],
        ),
    ],
)
def test_combine_run_egress_policies(
    benchmark_policy: EgressPolicy | None,
    legacy_agent_allowlist: list[str],
    expected: EgressPolicy,
    task_folder: bool,
) -> None:
    if task_folder and benchmark_policy is None:
        expected = "*"
    policy = combine_run_egress_policies(benchmark_policy, legacy_agent_allowlist, default_allow_all=task_folder)
    assert json.dumps(select_policy(policy, "run")) == json.dumps(expected)
    assert (policy is None) is (benchmark_policy is None and (task_folder or not legacy_agent_allowlist))


@pytest.mark.description("An agent contract with the obsolete egress field is rejected.")
@pytest.mark.parametrize("egress", [None, {"install": "*"}, {"run": []}])
def test_agent_contract_rejects_obsolete_egress(egress: object) -> None:
    with pytest.raises(ValidationError, match="install_egress"):
        AgentContractRequest.model_validate({"name": "agent", "egress": egress})


@pytest.mark.description("The agent install stage gets its own egress policy.")
@pytest.mark.parametrize(
    ("install_egress", "expected"),
    [
        (None, None),
        ("*", "*"),
        ([], []),
        (["packages.example.com"], ["packages.example.com"]),
    ],
)
def test_agent_install_egress_policy(
    install_egress: EgressPolicy | None,
    expected: EgressPolicy | None,
) -> None:
    contract = AgentContractRequest(name="agent", install_egress=install_egress)

    assert contract.install_egress == expected
