import pytest

from tracker.egress import EgressPolicy
from tracker.services.datasets.egress import NO_INFERENCE_HOST_WARNING, allow_inference_hosts

GATEWAY = {"MODEL_GATEWAY_URL": "https://gateway.example.test", "MODEL_GATEWAY_API_KEY": "mgwt_scoped"}


@pytest.mark.description("The agent's model endpoints join a restricted run policy, and public access is left alone.")
class TestAllowInferenceHosts:
    @pytest.mark.description("Each model endpoint in the agent's environment is added to the task's run policy.")
    @pytest.mark.parametrize(
        ("policy", "env", "expected"),
        [
            ([], GATEWAY, ["gateway.example.test"]),
            (
                ["pypi.org"],
                {**GATEWAY, "VALKYRIE_GENERATION_MODEL_GATEWAY_URL": "https://generation.example.test:8443/v1"},
                ["pypi.org", "gateway.example.test", "generation.example.test"],
            ),
            (
                [],
                {"OPENAI_BASE_URL": "https://LLM.example.test/v1", "OPENAI_API_KEY": "sk", "ANTHROPIC_API_KEY": "sk"},
                ["llm.example.test", "api.anthropic.com"],
            ),
            ("*", GATEWAY, "*"),
        ],
        ids=["no-network-gateway", "allowlist-gateway", "provider-urls", "public"],
    )
    def test_model_endpoints_join_the_run_policy(
        self, policy: EgressPolicy, env: dict[str, str], expected: EgressPolicy
    ) -> None:
        logged: list[str] = []

        assert allow_inference_hosts(policy, env, logged.append) == expected
        assert logged == []

    @pytest.mark.description("A restricted run policy without a model endpoint stays as it is and the run log warns.")
    def test_missing_endpoint_leaves_the_policy_and_warns(self) -> None:
        logged: list[str] = []

        policy = allow_inference_hosts(
            [],
            {"MODEL_GATEWAY_URL": "gateway.example.test", "OPENAI_BASE_URL": "https://*.example.test"},
            logged.append,
        )

        assert policy == []
        assert logged == [NO_INFERENCE_HOST_WARNING]
