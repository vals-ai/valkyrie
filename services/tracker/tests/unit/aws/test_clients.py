import asyncio
import gc
import json
import re
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event
from typing import Any, cast
from unittest.mock import ANY, AsyncMock, MagicMock, call

import aioboto3
import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws import clients as aws_clients
from tracker.aws import cloudwatch_logs
from tracker.aws.clients import (
    AWSClientProvider,
    TenantRoleAWSClientProvider,
)
from tracker.aws.cloudwatch_logs import (
    CloudWatchBenchmarkLogLocations,
    CloudWatchBenchmarkLogSink,
    handle_cloudwatch_error,
    task_log_stream_name,
)
from tracker.aws.runtime import AWSResources
from tracker.aws.s3 import handle_s3_error
from tracker.exceptions import CloudWatchError, S3Error
from tracker.runtime.logs import TaskLogLine
from tracker.runtime.logs import sanitize_log_stream_name as _sanitize_log_stream_name

_AWS_RESOURCES = AWSResources(
    region="us-east-1",
    s3_bucket="test-bucket",
    log_group="/valkyrie/worker",
    log_retention_days=30,
)


def _lines(*messages: str) -> list[TaskLogLine]:
    return [TaskLogLine(received_at=1_700_000_000.0, message=message, fields={}) for message in messages]


@pytest.mark.description("ECS launch and recovery reuse separate clients with bounded standard retry budgets.")
def test_ecs_clients_cache_separate_retry_budgets(monkeypatch: pytest.MonkeyPatch) -> None:
    launch = MagicMock()
    recovery = MagicMock()
    factory = MagicMock(side_effect=[launch, recovery])
    monkeypatch.setattr(aws_clients.boto3, "client", factory)
    getattr(aws_clients, "_ecs_client").cache_clear()
    try:
        assert aws_clients.ecs_client() is launch
        assert aws_clients.ecs_client() is launch
        assert aws_clients.ecs_client(recovery=True) is recovery
        assert aws_clients.ecs_client(recovery=True) is recovery
        assert factory.call_args_list == [
            call("ecs", config=aws_clients.ECS_RUN_TASK_CONFIG),
            call("ecs", config=aws_clients.ECS_CONFIG),
        ]
        for config, attempts in [(aws_clients.ECS_RUN_TASK_CONFIG, 1), (aws_clients.ECS_CONFIG, 3)]:
            assert getattr(config, "retries") == {"mode": "standard", "total_max_attempts": attempts}
            assert getattr(config, "max_pool_connections") == 200
        for client in [launch, recovery]:
            client.meta.events.register_last.assert_called_once_with(
                "before-send", getattr(aws_clients, "_check_ecs_launch_deadline")
            )
    finally:
        getattr(aws_clients, "_ecs_client").cache_clear()


@pytest.mark.description("Concurrent cold ECS client lookups construct one client and register its deadline hook once.")
def test_ecs_client_concurrent_cold_lookups(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    ready = Barrier(16)
    creating = Event()
    release = Event()

    def create_client(*_args: Any, **_kwargs: Any) -> Any:
        creating.set()
        assert release.wait(timeout=5)
        return client

    def lookup() -> Any:
        ready.wait(timeout=5)
        return aws_clients.ecs_client()

    factory = MagicMock(side_effect=create_client)
    monkeypatch.setattr(aws_clients.boto3, "client", factory)
    getattr(aws_clients, "_ecs_client").cache_clear()
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(lookup) for _ in range(16)]
            try:
                assert creating.wait(timeout=5)
            finally:
                release.set()
            assert all(future.result(timeout=5) is client for future in futures)
        factory.assert_called_once_with("ecs", config=aws_clients.ECS_RUN_TASK_CONFIG)
        client.meta.events.register_last.assert_called_once_with(
            "before-send", getattr(aws_clients, "_check_ecs_launch_deadline")
        )
    finally:
        release.set()
        getattr(aws_clients, "_ecs_client").cache_clear()


@pytest.mark.description("Credential selection and presigned URL lifetime behavior.")
class TestAWSClientProviders:
    @pytest.mark.description("The default chain provider passes no explicit credentials.")
    async def test_default_chain_provider_omits_explicit_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = MagicMock()
        session_factory = MagicMock(return_value=session)
        boto_client_factory = MagicMock()
        monkeypatch.setattr(aws_clients.aioboto3, "Session", session_factory)
        monkeypatch.setattr(aws_clients.boto3, "client", boto_client_factory)

        region = "test-default-chain-region"
        provider = AWSClientProvider(region=region)

        provider.s3_client()
        provider.cloudwatch_logs_client()
        provider.cloudwatch_logs_async_client()
        provider.secretsmanager_async_client()
        provider.lambda_client()

        session_factory.assert_called_once_with(region_name=region)
        assert session.client.call_args_list == [
            call("s3", config=ANY),
            call("logs", config=ANY),
            call("secretsmanager"),
            call("lambda", config=None),
        ]
        assert {constructed.args[0] for constructed in boto_client_factory.call_args_list} == {
            "logs",
        }
        credential_arguments = {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}
        for constructed in boto_client_factory.call_args_list:
            assert constructed.kwargs["region_name"] == region
            assert credential_arguments.isdisjoint(constructed.kwargs)

    @pytest.mark.description("Two session lookups on one event loop return the same `aioboto3` session.")
    async def test_async_clients_on_one_event_loop_share_a_session(self) -> None:
        s3_session = getattr(AWSClientProvider(region="us-east-1"), "_task_session")

        assert s3_session() is s3_session()

    @pytest.mark.description("Separate event loops get separate `aioboto3` sessions.")
    def test_async_clients_on_different_event_loops_use_separate_sessions(self) -> None:
        s3_session = getattr(AWSClientProvider(region="us-east-1"), "_task_session")

        async def running_loop_session() -> aioboto3.Session:
            return s3_session()

        assert asyncio.run(running_loop_session()) is not asyncio.run(running_loop_session())

    @pytest.mark.description("Presigned URLs are capped at the maximum TTL.")
    @pytest.mark.parametrize(
        ("provider", "requested_seconds", "expected_seconds"),
        [
            (AWSClientProvider(region="us-east-1"), 86_400, 3_600),
            (AWSClientProvider(region="us-east-1"), 300, 300),
        ],
    )
    def test_maximum_presign_ttl(
        self,
        provider: AWSClientProvider,
        requested_seconds: int,
        expected_seconds: int,
    ) -> None:
        assert provider.maximum_presign_ttl(requested_seconds) == expected_seconds


def _role_credentials(key: str, lifetime: timedelta) -> dict[str, object]:
    expiration = datetime.now(timezone.utc) + lifetime
    return {
        "Credentials": {
            "AccessKeyId": key,
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": expiration,
        }
    }


@pytest.mark.description("S3 clients assume the tenant role with the organization's session tag.")
class TestTenantRoleAWSClientProvider:
    _ROLE_ARN = "arn:aws:iam::123456789012:role/ValkyrieTrackerTenantAccessRole-dev"

    def _provider(self) -> TenantRoleAWSClientProvider:
        return TenantRoleAWSClientProvider(
            region="us-east-1", role_arn=self._ROLE_ARN, org_id="org-1", shared_secrets=True
        )

    def _task_role_session(self, monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
        sts = MagicMock()
        sts.__aenter__.return_value = sts
        sts.assume_role = AsyncMock(return_value=_role_credentials("tenant-key", timedelta(hours=1)))
        session = MagicMock()
        session.client.return_value = sts
        monkeypatch.setattr(AWSClientProvider, "_task_session", lambda _provider: session)
        return session, sts

    @pytest.mark.description("S3 requests sign with renewed tenant credentials tagged with the org.")
    async def test_s3_signs_with_renewed_credentials_tagged_with_the_org(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, sts = self._task_role_session(monkeypatch)
        sts.assume_role.side_effect = [
            _role_credentials("expiring-key", timedelta(minutes=1)),
            sts.assume_role.return_value,
        ]
        provider = self._provider()

        keys: list[str] = []
        for _ in range(3):
            async with provider.s3_client() as s3:
                url = await s3.generate_presigned_url("get_object", Params={"Bucket": "b", "Key": "k"})
            keys.append(re.search(r"(?:AWSAccessKeyId|X-Amz-Credential)=([^%&]+)", url).group(1))  # pyright: ignore[reportOptionalMemberAccess]

        assert keys == ["expiring-key", "tenant-key", "tenant-key"]
        tagged = call(
            RoleArn=self._ROLE_ARN,
            RoleSessionName="valkyrie-org-1",
            Tags=[
                {"Key": "valkyrie:org-id", "Value": "org-1"},
                {"Key": "valkyrie:secret-scope", "Value": "shared-internal"},
            ],
        )
        assert sts.assume_role.await_args_list == [tagged, tagged]

    @pytest.mark.description(
        "Assuming the tenant role adds the `valkyrie:org-id` session tag only for shared secret orgs."
    )
    async def test_only_shared_orgs_send_the_secret_scope_tag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, sts = self._task_role_session(monkeypatch)
        provider = TenantRoleAWSClientProvider(
            region="us-east-1", role_arn=self._ROLE_ARN, org_id="org-1", shared_secrets=False
        )

        await getattr(provider, "_assume_role")()

        assert sts.assume_role.await_args.kwargs["Tags"] == [{"Key": "valkyrie:org-id", "Value": "org-1"}]

    @pytest.mark.description(
        "Providers for different orgs reuse the same task role session and CloudWatch Logs client."
    )
    async def test_task_role_clients_are_shared_across_orgs(self) -> None:
        other = TenantRoleAWSClientProvider(
            region="us-east-1", role_arn=self._ROLE_ARN, org_id="org-2", shared_secrets=False
        )

        assert self._provider().cloudwatch_logs_client() is other.cloudwatch_logs_client()
        assert getattr(self._provider(), "_task_session")() is getattr(other, "_task_session")()

    @pytest.mark.description("Sessions are reused within an event loop and released once that loop closes.")
    def test_sessions_are_released_with_their_event_loop(self) -> None:
        provider = self._provider()

        async def sessions() -> tuple[weakref.ref[asyncio.AbstractEventLoop], list[aioboto3.Session]]:
            return weakref.ref(asyncio.get_running_loop()), [
                getattr(provider, "_data_session")(),
                getattr(provider, "_data_session")(),
                getattr(provider, "_task_session")(),
            ]

        runs = [asyncio.run(sessions()) for _ in range(3)]
        asyncio.run(sessions())
        gc.collect()

        assert all(tenant is again and tenant is not task for _, (tenant, again, task) in runs)
        assert len({id(tenant) for _, (tenant, _, _) in runs}) == 3
        assert [loop() for loop, _ in runs] == [None, None, None]

    @pytest.mark.description("Tenant presigned URLs are capped to the credential refresh window.")
    def test_presigned_urls_are_capped_to_the_refresh_window(self) -> None:
        assert [self._provider().maximum_presign_ttl(seconds) for seconds in (3600, 300)] == [600, 300]

    @pytest.mark.description("Secrets Manager requests sign with tenant credentials.")
    async def test_secrets_requests_are_signed_with_tenant_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._task_role_session(monkeypatch)
        authorizations: list[str] = []

        class Captured(Exception):
            pass

        def capture(request: Any, **_kwargs: Any) -> None:
            header = request.headers["Authorization"]
            authorizations.append(header.decode() if isinstance(header, bytes) else header)
            raise Captured

        async with self._provider().secretsmanager_async_client() as client:
            client.meta.events.register("before-send.secrets-manager.GetSecretValue", capture)
            with pytest.raises(Captured):
                await client.get_secret_value(SecretId="valkyrie/tenants/org-1/daytona")

        assert "Credential=tenant-key/" in authorizations[0]

    @pytest.mark.description("CloudWatch Logs and Lambda clients keep the task role.")
    async def test_logs_and_lambda_keep_the_task_role(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session, sts = self._task_role_session(monkeypatch)
        provider = self._provider()

        provider.cloudwatch_logs_async_client()
        provider.lambda_client()

        assert session.client.call_args_list == [call("logs", config=ANY), call("lambda", config=None)]
        sts.assume_role.assert_not_called()


@pytest.mark.description("S3 exception translation at the client boundary.")
class TestS3DecoratorClient:
    @pytest.mark.description("`handle_s3_error` turns a ClientError into an S3Error that keeps it as the cause.")
    async def test_s3_error_with_client(self) -> None:

        client_error = ClientError({"Error": {"Code": "500", "Message": "Error"}}, "GetObject")

        @handle_s3_error(message="Failed to get object")
        async def failing_function() -> None:
            raise client_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to get object" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    @pytest.mark.description("`handle_s3_error` turns a BotoCoreError into an S3Error that keeps it as the cause.")
    async def test_s3_error_with_botocore(self) -> None:
        botocore_error = BotoCoreError()

        @handle_s3_error(message="Failed to connect to S3")
        async def failing_function() -> None:
            raise botocore_error

        with pytest.raises(S3Error) as exc_info:
            await failing_function()

        assert "Failed to connect to S3" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


@pytest.mark.description("S3 client retry configuration.")
class TestS3ClientRetry:
    @pytest.mark.description("The S3 client uses the standard retry mode.")
    @pytest.mark.parametrize(
        "provider",
        [
            pytest.param(AWSClientProvider("us-east-1"), id="default-chain"),
        ],
    )
    async def test_uses_standard_retry_mode(self, provider: AWSClientProvider) -> None:
        async with provider.s3_client() as client:
            assert client.meta.config.retries["mode"] == "standard"


@pytest.mark.description("CloudWatch clients back off on throttled log setup requests.")
class TestCloudWatchClientRetry:
    @pytest.mark.description("Sync and async Logs clients use adaptive retries with twenty total attempts.")
    async def test_logs_clients_use_adaptive_retries(self) -> None:
        provider = AWSClientProvider("us-east-1")

        sync_client = provider.cloudwatch_logs_client()
        async with provider.cloudwatch_logs_async_client() as async_client:
            for client in (sync_client, async_client):
                assert client.meta.config.retries == {"mode": "adaptive", "total_max_attempts": 20}
                assert client.meta.config.max_pool_connections == 200


@pytest.mark.description("CloudWatch exception translation at the client boundary.")
class TestCloudWatchClient:
    @pytest.mark.description(
        "`handle_cloudwatch_error` turns a ClientError into a CloudWatchError that keeps it as the cause."
    )
    def test_cloudwatch_error_with_client(self) -> None:
        client_error = ClientError({"Error": {"Code": "404", "Message": "Not found"}}, "CreateLogStream")

        @handle_cloudwatch_error(message="Failed to create log stream")
        def failing_function() -> None:
            raise client_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to create log stream" in str(exc_info.value)
        assert exc_info.value.__cause__ == client_error

    @pytest.mark.description(
        "`handle_cloudwatch_error` turns a BotoCoreError into a CloudWatchError that keeps it as the cause."
    )
    def test_cloudwatch_error_with_botocore(self) -> None:
        botocore_error = BotoCoreError()

        @handle_cloudwatch_error(message="Failed to connect to CloudWatch")
        def failing_function() -> None:
            raise botocore_error

        with pytest.raises(CloudWatchError) as exc_info:
            failing_function()

        assert "Failed to connect to CloudWatch" in str(exc_info.value)
        assert exc_info.value.__cause__ == botocore_error


@pytest.mark.description("logStreamName must satisfy AWS constraint [^:*]* (no ':' or '*').")
class TestSanitizeLogStreamName:
    @pytest.mark.description("Log stream names replace colons.")
    def test_replaces_colon(self) -> None:
        assert _sanitize_log_stream_name("provider/model:fast").startswith("provider/model%3Afast-")

    @pytest.mark.description("Log stream names replace asterisks.")
    def test_replaces_asterisk(self) -> None:
        assert _sanitize_log_stream_name("task*glob").startswith("task%2Aglob-")

    @pytest.mark.description("Log stream names replace every colon and asterisk.")
    def test_replaces_both_and_multiple(self) -> None:
        assert _sanitize_log_stream_name("a:b:c*d").startswith("a%3Ab%3Ac%2Ad-")

    @pytest.mark.description("Percent signs are escaped before CloudWatch metacharacters are replaced.")
    def test_escapes_percent_before_cloudwatch_metacharacters(self) -> None:
        percent_name = _sanitize_log_stream_name("task%3Aone:fast")
        colon_name = _sanitize_log_stream_name("task:one:fast")

        assert percent_name.startswith("task%253Aone%3Afast-")
        assert percent_name != colon_name

    @pytest.mark.description("A clean log stream name is left unchanged.")
    def test_preserves_clean_name(self) -> None:
        assert _sanitize_log_stream_name("water_intake_tracker") == "water_intake_tracker"
        assert _sanitize_log_stream_name("group/sub/name") == "group/sub/name"

    @pytest.mark.description("A sanitized log stream name meets the AWS naming rule.")
    def test_result_matches_aws_constraint(self) -> None:
        for raw in ["openai/gpt-5.5", "laguna-xs.2:fast", "x*:y", "plain_id"]:
            assert re.fullmatch(r"[^:*]*", _sanitize_log_stream_name(raw))


@pytest.mark.description("CloudWatch benchmark log URL construction.")
class TestGetBenchmarkLogUrl:
    @pytest.mark.description("The run log URL sanitizes the task ID.")
    def test_sanitizes_task_id_in_url(self) -> None:
        stream_name = task_log_stream_name("provider/model:fast", datetime(2026, 1, 1, tzinfo=timezone.utc))
        url = CloudWatchBenchmarkLogLocations(_AWS_RESOURCES).task_location("bench123", stream_name)

        assert "provider%2Fmodel%253Afast-" in url
        assert "model:fast" not in url

    @pytest.mark.description("Without a task ID, the run log URL leaves out the log events part.")
    def test_no_task_id_omits_log_events(self) -> None:
        url = CloudWatchBenchmarkLogLocations(_AWS_RESOURCES).benchmark_location("bench123")
        assert "log-events" not in url


@pytest.mark.description("CloudWatch stream creation and benchmark log writes.")
class TestWriteBenchmarkLogEvent:
    def _sink_with_mock_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> tuple[MagicMock, MagicMock, CloudWatchBenchmarkLogSink]:
        client = MagicMock()
        client_provider = MagicMock(spec=AWSClientProvider)
        client_provider.cloudwatch_logs_client.return_value = client
        client_provider.cloudwatch_logs_async_client.return_value = client
        client.__aenter__.return_value = client
        client.create_log_group = AsyncMock()
        client.put_retention_policy = AsyncMock()
        monkeypatch.setattr(cloudwatch_logs, "_created_streams", set[str]())
        return (
            client,
            client_provider,
            CloudWatchBenchmarkLogSink(
                cast(AWSClientProvider, client_provider),
                _AWS_RESOURCES.log_group,
            ),
        )

    @pytest.mark.description("Writing a log event creates the stream and puts the event under the sanitized name.")
    def test_creates_stream_and_puts_event_with_sanitized_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)

        stream_name = task_log_stream_name("provider/model:fast", datetime(2026, 1, 1, tzinfo=timezone.utc))
        sink.write(f"bench123:{stream_name}", _lines("hello"))

        client.create_log_stream.assert_called_once_with(
            logGroupName="/valkyrie/worker/bench123", logStreamName=stream_name
        )
        assert client.put_log_events.call_args.kwargs["logStreamName"] == stream_name

    @pytest.mark.description(
        "Each task log line is its own event, timestamped at receive time and stamped with its fields."
    )
    def test_puts_one_json_event_per_line_at_receive_time(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)

        sink.write(
            "bench123:task",
            [
                TaskLogLine(received_at=1_700_000_000.25, message="agent said hi", fields={"trace_id": "abc"}),
                TaskLogLine(received_at=1_700_000_001.5, message="done", fields={}),
            ],
        )

        events = client.put_log_events.call_args.kwargs["logEvents"]
        assert [event["timestamp"] for event in events] == [1_700_000_000_250, 1_700_000_001_500]
        assert [json.loads(event["message"]) for event in events] == [
            {"message": "agent said hi", "trace_id": "abc"},
            {"message": "done"},
        ]

    @pytest.mark.description("A botocore error while creating the stream reports the sanitized name.")
    def test_create_stream_botocore_error_reports_sanitized_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)
        client.create_log_stream.side_effect = BotoCoreError()

        stream_name = task_log_stream_name("provider/model:fast", datetime(2026, 1, 1, tzinfo=timezone.utc))
        with pytest.raises(CloudWatchError) as exc_info:
            sink.write(f"bench123:{stream_name}", _lines("hello"))

        assert stream_name in str(exc_info.value)
        client.put_log_events.assert_not_called()

    @pytest.mark.description("Creating a run's log group sets its retention.")
    async def test_create_benchmark_configures_group_retention(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)

        await sink.create_benchmark("bench123", retention_days=30)

        client.create_log_group.assert_awaited_once_with(logGroupName="/valkyrie/worker/bench123")
        client.put_retention_policy.assert_awaited_once_with(
            logGroupName="/valkyrie/worker/bench123",
            retentionInDays=30,
        )

    @pytest.mark.description("Retry retention when cancellation leaves an already-created log group.")
    async def test_create_benchmark_retries_cancelled_retention_for_existing_group(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)
        client.create_log_group.side_effect = [
            None,
            ClientError({"Error": {"Code": "ResourceAlreadyExistsException"}}, "CreateLogGroup"),
        ]
        client.put_retention_policy.side_effect = [asyncio.CancelledError(), None]

        with pytest.raises(asyncio.CancelledError):
            await sink.create_benchmark("bench123", retention_days=30)
        await sink.create_benchmark("bench123", retention_days=30)

        assert client.put_retention_policy.await_args_list == [
            call(logGroupName="/valkyrie/worker/bench123", retentionInDays=30),
            call(logGroupName="/valkyrie/worker/bench123", retentionInDays=30),
        ]

    @pytest.mark.description("A missing log group error is translated when creating a run's logs.")
    async def test_create_benchmark_translates_non_existing_group_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)
        client.create_log_group.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}},
            "CreateLogGroup",
        )

        with pytest.raises(CloudWatchError, match="Failed to create log group"):
            await sink.create_benchmark("bench123", retention_days=30)

    @pytest.mark.description("An empty log batch creates no client or stream.")
    def test_empty_batch_does_not_create_client_or_stream(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, client_provider, sink = self._sink_with_mock_client(monkeypatch)

        sink.write("bench123:task", [])

        client_provider.cloudwatch_logs_client.assert_not_called()
        client.assert_not_called()

    @pytest.mark.description("An invalid stream key is rejected before any client is created.")
    def test_invalid_stream_key_is_rejected_before_client_access(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, client_provider, sink = self._sink_with_mock_client(monkeypatch)

        with pytest.raises(CloudWatchError, match="Invalid stream key"):
            sink.write("bench123:", _lines("message"))

        client_provider.cloudwatch_logs_client.assert_not_called()
        client.assert_not_called()

    @pytest.mark.description("A created stream is reused, and a stream that already exists in AWS is fine.")
    def test_reuses_created_stream_and_tolerates_existing_aws_stream(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)
        client.create_log_stream.side_effect = ClientError(
            {"Error": {"Code": "ResourceAlreadyExistsException"}},
            "CreateLogStream",
        )

        sink.write("bench123:task", _lines("first"))
        sink.write("bench123:task", _lines("second"))

        client.create_log_stream.assert_called_once_with(
            logGroupName="/valkyrie/worker/bench123",
            logStreamName="task",
        )
        assert client.put_log_events.call_count == 2

    @pytest.mark.description("Missing stream errors and put errors are translated.")
    def test_non_existing_stream_error_and_put_error_are_translated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client, _, sink = self._sink_with_mock_client(monkeypatch)
        client.create_log_stream.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}},
            "CreateLogStream",
        )

        with pytest.raises(CloudWatchError, match="Failed to create cloudwatch stream"):
            sink.write("bench123:task", _lines("message"))

        client.create_log_stream.side_effect = None
        client.put_log_events.side_effect = BotoCoreError()
        with pytest.raises(CloudWatchError, match="Failed to put log event"):
            sink.write("bench123:other-task", _lines("message"))
