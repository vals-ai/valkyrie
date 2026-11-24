from typing import cast
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock

import pytest
from benchmark_service.sandbox.modal import ModalProviderConfig

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.exceptions import InvalidSandboxConfigurationError, SecretNotFoundError
from tracker.aws.secrets import SecretsManagerStore
from tracker.aws.cloudwatch_logs import CloudWatchBenchmarkLogSink
from tracker.types import AgentContractRequest, StartBenchmarkRequest


@pytest.mark.description("Read the persisted secret only when sandbox configuration is needed.")
async def test_runtime_resolves_provider_configuration_when_requested(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    resolve = AsyncMock(return_value={"MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"})
    monkeypatch.setattr(SecretsManagerStore, "get", resolve)
    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider="modal",
        sandbox_provider_secret_name="provider-reference",
    )
    resolve.assert_not_awaited()

    config = await runtime.get_sandbox_provider_config()

    resolve.assert_awaited_once_with("provider-reference")
    assert isinstance(config, ModalProviderConfig)
    assert config.MODAL_TOKEN_ID == "id"
    assert config.MODAL_TOKEN_SECRET == "secret"


@pytest.mark.description("Artifact locations work without touching AWS or sandbox credentials.")
async def test_read_only_runtime_needs_no_provider_credentials() -> None:
    clients = MagicMock(spec=AWSClientProvider)
    aws_runtime = AWSRuntime(AWSResources("us-east-1", "bucket", "logs", 30), cast(AWSClientProvider, clients))
    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    assert "bucket" in runtime.artifacts.object_location("result.json")
    with pytest.raises(InvalidSandboxConfigurationError, match="provider secret name"):
        await runtime.get_sandbox_provider_config()
    assert clients.mock_calls == []


@pytest.mark.description("A normal start still requires the agent credentials during cloud runtime preparation.")
async def test_start_still_requires_agent_secrets(monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> None:
    monkeypatch.setattr(CloudWatchBenchmarkLogSink, "create_benchmark", AsyncMock())
    monkeypatch.setattr(SecretsManagerStore, "get", AsyncMock(side_effect=SecretNotFoundError("deleted-agent-key")))
    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    request = StartBenchmarkRequest(
        benchmark_name="swe-bench",
        contract=AgentContractRequest(name="agent", secrets={"AGENT_KEY": "deleted-agent-key"}),
    )

    with pytest.raises(SecretNotFoundError, match="deleted-agent-key"):
        await runtime.prepare_execution(request, uuid4())
