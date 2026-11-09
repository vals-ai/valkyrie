from unittest.mock import AsyncMock

import pytest

import tracker.aws.s3 as s3_module
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import S3ObjectCopy, copy_agent_to_benchmark


@pytest.mark.description("Agent source copies into benchmark workspaces.")
class TestCopyAgentToBenchmark:
    @pytest.mark.description(
        (
            "Copy an agent once so retries keep using the frozen benchmark version.\n"
            "\n"
            "Test cases:\n"
            "- A missing destination receives the current agent archive.\n"
            "- An existing destination is not overwritten during retry or resume."
        )
    )
    @pytest.mark.parametrize("destination_exists", [False, True])
    async def test_preserves_frozen_agent_copy(
        self,
        aws_runtime: AWSRuntime,
        monkeypatch: pytest.MonkeyPatch,
        destination_exists: bool,
    ) -> None:
        exists_mock = AsyncMock(return_value=destination_exists)
        copy_mock = AsyncMock(return_value="version-1")

        monkeypatch.setattr(s3_module, "s3_object_exists", exists_mock)
        monkeypatch.setattr(s3_module, "copy_s3_object", copy_mock)

        created = await copy_agent_to_benchmark(
            benchmark_id="bench-123",
            contract_name="my_agent",
            runtime=aws_runtime,
        )

        assert created == (None if destination_exists else S3ObjectCopy(version_id="version-1"))
        exists_mock.assert_awaited_once_with("benchmarks/bench-123/my_agent.zip", aws_runtime)
        if destination_exists:
            copy_mock.assert_not_awaited()
        else:
            copy_mock.assert_awaited_once_with(
                "agents/my_agent.zip",
                "benchmarks/bench-123/my_agent.zip",
                aws_runtime,
            )
