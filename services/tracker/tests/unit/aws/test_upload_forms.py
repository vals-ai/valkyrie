import base64
import hashlib
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiobotocore.stub import AioStubber
from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider, TenantRoleAWSClientProvider
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import S3ObjectStore
from tracker.exceptions import S3Error

_SHA256 = hashlib.sha256(b"archive").hexdigest()
_KEY = "dataset-uploads/example/archive.zip"


@pytest.mark.description(
    "Direct upload forms bind the destination, bytes, size, and expiration before a browser receives them."
)
class TestUploadForms:
    @pytest.mark.description(
        "A real S3 signature limits the form to one key, exact size, checksum, and credential lifetime."
    )
    @pytest.mark.parametrize("tenant", [False, True], ids=["default-credentials", "tenant-credentials"])
    async def test_signed_policy(self, monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime, tenant: bool) -> None:
        provider = (
            TenantRoleAWSClientProvider("us-east-1", "arn:aws:iam::123456789012:role/tenant", "org", False)
            if tenant
            else AWSClientProvider("us-east-1")
        )
        runtime = AWSRuntime(resources=aws_runtime.resources, clients=provider, expected_bucket_owner="123456789012")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "test-session")
        create_client = AWSClientProvider("us-east-1").s3_client

        @asynccontextmanager
        async def s3_client(
            _provider: AWSClientProvider, *, signature_version: str | None = None
        ) -> AsyncGenerator[Any, None]:
            async with create_client(signature_version=signature_version) as client:
                stubber = AioStubber(client)
                stubber.add_response(
                    "head_bucket", {}, {"Bucket": "test-bucket", "ExpectedBucketOwner": "123456789012"}
                )
                with stubber:
                    yield client
                    stubber.assert_no_pending_responses()

        monkeypatch.setattr(AWSClientProvider, "s3_client", s3_client)
        started = datetime.now(UTC)
        form = await S3ObjectStore(runtime).temporary_upload_form(_KEY, size=123, sha256=_SHA256, expires_in=86_400)

        policy = json.loads(base64.b64decode(form.fields["policy"]))
        assert form.fields["key"] == _KEY
        assert form.fields["x-amz-algorithm"] == "AWS4-HMAC-SHA256"
        assert form.fields["x-amz-security-token"] == "test-session"
        assert form.fields["x-amz-signature"]
        assert form.fields["x-amz-checksum-sha256"] == base64.b64encode(bytes.fromhex(_SHA256)).decode()
        assert ["content-length-range", 123, 123] in policy["conditions"]
        assert {"bucket": "test-bucket"} in policy["conditions"]
        for name in (
            "key",
            "Content-Type",
            "success_action_status",
            "x-amz-checksum-algorithm",
            "x-amz-checksum-sha256",
        ):
            assert {name: form.fields[name]} in policy["conditions"]
        assert form.expires_in == (600 if tenant else 3600)
        expiration = datetime.fromisoformat(policy["expiration"])
        assert started - timedelta(seconds=1) <= expiration - timedelta(seconds=form.expires_in) <= datetime.now(UTC)

    @pytest.mark.description("A refused bucket owner check prevents the storage client from issuing an upload form.")
    async def test_owner_denial(self, monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> None:
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.head_bucket.side_effect = ClientError({"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadBucket")

        def s3_client(_provider: AWSClientProvider, *, signature_version: str | None = None) -> AsyncMock:
            return client

        monkeypatch.setattr(AWSClientProvider, "s3_client", s3_client)

        with pytest.raises(S3Error, match="Failed to create an upload form"):
            await S3ObjectStore(aws_runtime).temporary_upload_form(_KEY, size=7, sha256=_SHA256, expires_in=60)

        client.generate_presigned_post.assert_not_called()

    @pytest.mark.description("Invalid upload limits and variable object keys fail before the storage client opens.")
    @pytest.mark.parametrize(
        ("key", "size", "sha256", "expires_in"),
        [
            ("", 7, _SHA256, 60),
            ("uploads/${filename}", 7, _SHA256, 60),
            (_KEY, 0, _SHA256, 60),
            (_KEY, 5 * 1024**3 + 1, _SHA256, 60),
            (_KEY, 7, "sha256:" + _SHA256, 60),
            (_KEY, 7, "g" * 64, 60),
            (_KEY, 7, _SHA256, 0),
        ],
        ids=[
            "empty-key",
            "variable-key",
            "empty-file",
            "oversize-file",
            "content-digest",
            "invalid-checksum",
            "expired",
        ],
    )
    async def test_invalid_request(
        self, aws_runtime: AWSRuntime, key: str, size: int, sha256: str, expires_in: int
    ) -> None:
        with pytest.raises(ValueError):
            await S3ObjectStore(aws_runtime).temporary_upload_form(key, size=size, sha256=sha256, expires_in=expires_in)
