import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from tracker._lambda import dry_run_lambda, invoke_lambda
from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import LambdaError


@pytest.mark.description("`invoke_lambda` calls the configured function and returns the decoded JSON payload.")
async def test_invoke_lambda_uses_provider_config_and_returns_parsed_payload() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.invoke.return_value = {
        "Payload": AsyncMock(
            read=AsyncMock(return_value=b'{"statusCode": 200, "reading_plan_url": "https://example.test/plan"}')
        )
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client
    config = Config(read_timeout=905)
    payload = {"benchmark_id": "benchmark-1"}

    result = await invoke_lambda(
        cast(AWSClientProvider, provider),
        "analyzer-function",
        payload,
        config=config,
    )

    provider.lambda_client.assert_called_once_with(config)
    client.invoke.assert_awaited_once_with(
        FunctionName="analyzer-function",
        Payload=json.dumps(payload),
    )
    assert result == {
        "statusCode": 200,
        "reading_plan_url": "https://example.test/plan",
    }


@pytest.mark.description("A Lambda dry run uses the provider's client.")
async def test_dry_run_lambda_uses_provider_client() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    await dry_run_lambda(cast(AWSClientProvider, provider), "benchmark-function")

    provider.lambda_client.assert_called_once_with()
    client.invoke.assert_awaited_once_with(FunctionName="benchmark-function", InvocationType="DryRun")


@pytest.mark.description("A Lambda dry run maps client errors.")
async def test_dry_run_lambda_maps_client_errors() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.invoke.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "access denied"}},
        "Invoke",
    )
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    with pytest.raises(LambdaError, match="Lambda invoke preflight failed for 'benchmark-function'"):
        await dry_run_lambda(cast(AWSClientProvider, provider), "benchmark-function")


@pytest.mark.description("A Lambda function error raises.")
async def test_invoke_lambda_raises_for_function_error() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.invoke.return_value = {
        "FunctionError": "Unhandled",
        "Payload": AsyncMock(read=AsyncMock(return_value=b'{"errorMessage": "analysis failed"}')),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    with pytest.raises(LambdaError, match="analysis failed"):
        await invoke_lambda(
            cast(AWSClientProvider, provider),
            "analyzer-function",
            {"benchmark_id": "benchmark-1"},
            config=Config(),
        )


@pytest.mark.description("A Lambda error status raises.")
async def test_invoke_lambda_raises_for_error_status() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.invoke.return_value = {
        "Payload": AsyncMock(
            read=AsyncMock(return_value=b'{"statusCode": 503, "errorMessage": "service unavailable"}')
        ),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    with pytest.raises(LambdaError, match="returned status 503"):
        await invoke_lambda(
            cast(AWSClientProvider, provider),
            "analyzer-function",
            {"benchmark_id": "benchmark-1"},
            config=Config(),
        )


@pytest.mark.description("A Lambda payload that is not an object is returned as is.")
async def test_invoke_lambda_returns_non_object_payload() -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.invoke.return_value = {
        "Payload": AsyncMock(read=AsyncMock(return_value=b'["result"]')),
    }
    provider = MagicMock(spec=AWSClientProvider)
    provider.lambda_client.return_value = client

    result = await invoke_lambda(
        cast(AWSClientProvider, provider),
        "analyzer-function",
        {"benchmark_id": "benchmark-1"},
        config=Config(),
    )

    assert result == ["result"]
