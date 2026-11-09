from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.exceptions import S3Error
from tracker.logging import get_logger
from tracker.runtime.storage import ArtifactLocations, StoredObject, StoredObjectCopy
from tracker.runtime.artifacts import agent_bundle_key, benchmark_agent_bundle_key

logger = get_logger(__name__)

_P = ParamSpec("_P")
_R = TypeVar("_R")

S3_BENCHMARKS_PREFIX = "benchmarks"

_MULTIPART_PART_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class S3ObjectCopy:
    version_id: str | None


def s3_owner_arguments(runtime: AWSRuntime) -> dict[str, str]:
    if runtime.expected_bucket_owner is None:
        raise ValueError("Managed AWS runtime is missing the expected bucket owner")

    return {"ExpectedBucketOwner": runtime.expected_bucket_owner}


def get_benchmark_contract_s3_key(benchmark_id: str, contract_name: str) -> str:
    return benchmark_agent_bundle_key(benchmark_id, contract_name)


def handle_s3_error(message: str) -> Callable[[Callable[_P, Awaitable[_R]]], Callable[_P, Coroutine[Any, Any, _R]]]:

    def decorator(func: Callable[_P, Awaitable[_R]]) -> Callable[_P, Coroutine[Any, Any, _R]]:
        @wraps(func)
        async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return await func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise S3Error(f"{message}: {e}") from e

        return wrapper

    return decorator


@logfire.instrument("upload_to_s3", extract_args=("s3_key",))
@handle_s3_error(message="Failed to upload to S3")
async def upload_to_s3(file_content: bytes, s3_key: str, runtime: AWSRuntime) -> None:
    async with runtime.clients.s3_client() as client:
        await client.put_object(
            Bucket=runtime.resources.s3_bucket,
            Key=s3_key,
            Body=file_content,
            **s3_owner_arguments(runtime),
        )


@logfire.instrument("upload_stream_to_s3", extract_args=("s3_key",))
@handle_s3_error(message="Failed to upload stream to S3")
async def upload_stream_to_s3(
    chunks: AsyncIterable[bytes],
    s3_key: str,
    runtime: AWSRuntime,
    should_continue: Callable[[], bool] | None = None,
    *,
    overwrite: bool = True,
) -> int:
    total_bytes = 0
    s3_bucket = runtime.resources.s3_bucket
    owner_arguments = s3_owner_arguments(runtime)
    async with runtime.clients.s3_client() as client:
        multipart = await client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key, **owner_arguments)
        upload_id = multipart["UploadId"]
        try:
            parts: list[dict[str, Any]] = []
            buffer = bytearray()

            async def _upload_part() -> None:
                if should_continue is not None and not should_continue():
                    raise S3Error("S3 stream upload authority was revoked")
                part_number = len(parts) + 1
                response = await client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=bytes(buffer),
                    **owner_arguments,
                )
                parts.append({"ETag": response["ETag"], "PartNumber": part_number})
                buffer.clear()

            async for chunk in chunks:
                buffer.extend(chunk)
                total_bytes += len(chunk)
                if len(buffer) >= _MULTIPART_PART_BYTES:
                    await _upload_part()

            if buffer or not parts:
                await _upload_part()

            if should_continue is not None and not should_continue():
                raise S3Error("S3 stream upload authority was revoked")

            try:
                await client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                    **owner_arguments,
                    **({} if overwrite else {"IfNoneMatch": "*"}),
                )
            except ClientError as error:
                status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if not overwrite and status_code == 412:
                    raise FileExistsError(s3_key) from error
                if not overwrite and status_code == 409 and await s3_object_exists(s3_key, runtime):
                    raise FileExistsError(s3_key) from error
                raise
        except BaseException:
            with suppress(Exception):
                await client.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    **owner_arguments,
                )
            raise
    return total_bytes


@handle_s3_error(message="Failed to delete from S3")
async def delete_from_s3(s3_key: str, runtime: AWSRuntime, *, version_id: str | None = None) -> None:
    try:
        async with runtime.clients.s3_client() as client:
            if version_id is None:
                await client.delete_object(
                    Bucket=runtime.resources.s3_bucket,
                    Key=s3_key,
                    **s3_owner_arguments(runtime),
                )
            else:
                await client.delete_object(
                    Bucket=runtime.resources.s3_bucket,
                    Key=s3_key,
                    VersionId=version_id,
                    **s3_owner_arguments(runtime),
                )
    except (ClientError, BotoCoreError) as error:
        raise S3Error(f"Failed to delete object from S3: {error}") from error


async def copy_s3_object(source_key: str, dest_key: str, runtime: AWSRuntime) -> str | None:
    try:
        async with runtime.clients.s3_client() as client:
            response = await client.copy_object(
                Bucket=runtime.resources.s3_bucket,
                CopySource={"Bucket": runtime.resources.s3_bucket, "Key": source_key},
                Key=dest_key,
                **s3_owner_arguments(runtime),
            )
            version_id = response.get("VersionId")
            return str(version_id) if version_id is not None else None
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to copy S3 object from {source_key} to {dest_key}: {e}") from e


async def copy_agent_to_benchmark(benchmark_id: str, contract_name: str, runtime: AWSRuntime) -> S3ObjectCopy | None:
    source_key = agent_bundle_key(contract_name)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, contract_name)

    if await s3_object_exists(dest_key, runtime):
        return None

    version_id = await copy_s3_object(source_key, dest_key, runtime)
    return S3ObjectCopy(version_id=version_id)


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str, runtime: AWSRuntime) -> bool:
    async with runtime.clients.s3_client() as client:
        try:
            await client.head_object(
                Bucket=runtime.resources.s3_bucket,
                Key=s3_key,
                **s3_owner_arguments(runtime),
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":  # pyright: ignore[reportTypedDictNotRequiredAccess]
                return False

            raise


@handle_s3_error(message="Failed to create presigned URL")
async def create_presigned_url(s3_key: str, runtime: AWSRuntime, expiration: int = 86400) -> str:
    actual_expiration = runtime.clients.maximum_presign_ttl(expiration)
    async with runtime.clients.s3_client() as client:
        presigned_url: str = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": runtime.resources.s3_bucket, "Key": s3_key},
            ExpiresIn=actual_expiration,
        )

    return presigned_url


def create_console_url(s3_key: str, resources: AWSResources) -> str:
    return (
        f"https://{resources.region}.console.aws.amazon.com/s3/object/{resources.s3_bucket}"
        f"?region={resources.region}&prefix={s3_key}"
    )


def create_benchmark_url(benchmark_id: str, resources: AWSResources) -> str:
    prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    return (
        f"https://{resources.region}.console.aws.amazon.com/s3/buckets/{resources.s3_bucket}"
        f"?region={resources.region}&prefix={prefix}"
    )


@handle_s3_error(message="Failed to list agents from S3")
async def list_agents(runtime: AWSRuntime) -> list[tuple[str, datetime | None]]:
    agents: list[tuple[str, datetime | None]] = []
    async with runtime.clients.s3_client() as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=runtime.resources.s3_bucket,
            Prefix="agents/",
            **s3_owner_arguments(runtime),
        ):
            for s3_object in page.get("Contents", []):
                tail = s3_object["Key"][len("agents/") :]
                if not tail.endswith(".zip"):
                    continue
                agents.append((tail[: -len(".zip")], s3_object.get("LastModified")))

    return agents


class _S3ObjectReadSession:
    def __init__(self, client: Any, bucket: str, owner_arguments: dict[str, str]) -> None:
        self._client = client
        self._bucket = bucket
        self._owner_arguments = owner_arguments

    @handle_s3_error(message="Failed to download from S3")
    async def get_bytes(self, key: str) -> bytes:
        response = await self._client.get_object(Bucket=self._bucket, Key=key, **self._owner_arguments)
        async with response["Body"] as stream:
            return await stream.read()

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, **self._owner_arguments):
                for stored_object in page.get("Contents", []):
                    key = stored_object.get("Key")
                    if key is not None:
                        yield StoredObject(
                            key=key, last_modified=stored_object.get("LastModified"), size=stored_object["Size"]
                        )
        except (ClientError, BotoCoreError) as error:
            raise S3Error(f"Failed to list objects from S3: {error}") from error


class S3ObjectStore:
    def __init__(self, runtime: AWSRuntime) -> None:
        self._runtime = runtime

    @asynccontextmanager
    async def read_session(self) -> AsyncGenerator[_S3ObjectReadSession, None]:
        async with self._runtime.clients.s3_client() as client:
            yield _S3ObjectReadSession(
                client,
                self._runtime.resources.s3_bucket,
                s3_owner_arguments(self._runtime),
            )

    async def put_bytes(self, key: str, content: bytes) -> None:
        await upload_to_s3(content, key, self._runtime)

    async def put_stream(
        self,
        key: str,
        chunks: AsyncIterable[bytes],
        *,
        should_continue: Callable[[], bool] | None = None,
        overwrite: bool = True,
    ) -> int:
        return await upload_stream_to_s3(
            chunks, key, self._runtime, should_continue=should_continue, overwrite=overwrite
        )

    async def get_bytes(self, key: str) -> bytes:
        async with self.read_session() as reader:
            return await reader.get_bytes(key)

    async def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        owner_arguments = s3_owner_arguments(self._runtime)
        async with self._runtime.clients.s3_client() as client:
            async for key in keys:
                try:
                    response = await client.get_object(
                        Bucket=self._runtime.resources.s3_bucket,
                        Key=key,
                        **owner_arguments,
                    )
                    async with response["Body"] as stream:
                        yield key, await stream.read()
                except (ClientError, BotoCoreError) as error:
                    logger.warning("Failed to download %s from S3: %s", key, error)

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        await delete_from_s3(key, self._runtime, version_id=deletion_token)

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        return StoredObjectCopy(deletion_token=await copy_s3_object(source_key, destination_key, self._runtime))

    async def exists(self, key: str) -> bool:
        return await s3_object_exists(key, self._runtime)

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        async with self.read_session() as reader:
            async for stored_object in reader.list_objects(prefix):
                yield stored_object

    async def stat(self, key: str) -> StoredObject:
        async with self._runtime.clients.s3_client() as client:
            response = await client.head_object(
                Bucket=self._runtime.resources.s3_bucket,
                Key=key,
                **s3_owner_arguments(self._runtime),
            )
        return StoredObject(key, response.get("LastModified"), size=response["ContentLength"])

    async def list_objects_page(
        self, prefix: str, *, cursor: str | None, limit: int
    ) -> tuple[list[StoredObject], str | None]:
        arguments: dict[str, Any] = {"Bucket": self._runtime.resources.s3_bucket, "Prefix": prefix, "MaxKeys": limit}
        arguments.update(s3_owner_arguments(self._runtime))
        if cursor is not None:
            arguments["ContinuationToken"] = cursor
        async with self._runtime.clients.s3_client() as client:
            response = await client.list_objects_v2(**arguments)
        return [
            StoredObject(item["Key"], item.get("LastModified"), size=item["Size"])
            for item in response.get("Contents", [])
        ], response.get("NextContinuationToken")

    async def temporary_download_url(self, key: str, *, expires_in: int) -> str:
        return await create_presigned_url(key, self._runtime, expiration=expires_in)


class S3ArtifactLocations(ArtifactLocations):
    def __init__(self, resources: AWSResources) -> None:
        self._resources = resources

    def object_location(self, key: str) -> str:
        return create_console_url(key, self._resources)

    def prefix_location(self, prefix: str) -> str:
        return (
            f"https://{self._resources.region}.console.aws.amazon.com/s3/buckets/{self._resources.s3_bucket}"
            f"?region={self._resources.region}&prefix={prefix}"
        )
