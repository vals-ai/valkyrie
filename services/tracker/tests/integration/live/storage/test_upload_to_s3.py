import pytest

import asyncio
import io
import tarfile

from benchmark_service import ImageSource, Resources, SandboxProvider

from tests.utils import random_task_id
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import S3ObjectStore, delete_from_s3
from tracker.database.models import Benchmark
from tracker.runtime.artifacts import task_artifact_key
from tracker.sandbox import SandboxCreationState, archive_and_upload_output, create_sandbox


def _archive_members(archive_content: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(archive_content), mode="r:gz") as archive:
        members: dict[str, bytes] = {}
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted_file = archive.extractfile(member)
            if extracted_file is not None:
                members[member.name] = extracted_file.read()
        return members


def _member_content(members: dict[str, bytes], suffix: str) -> bytes:
    matching_members = [content for name, content in members.items() if name.endswith(suffix)]
    assert len(matching_members) == 1
    return matching_members[0]


@pytest.mark.description("Live archive uploads for files and directories.")
class TestUploadToS3:
    @pytest.mark.description(
        (
            "Sandbox output archives preserve file contents when uploaded to S3.\n"
            "\n"
            "Test cases:\n"
            "- A single output file is archived, uploaded, downloaded, and readable from S3.\n"
            "- A directory output preserves nested files in the uploaded archive."
        )
    )
    async def test_archive_and_upload_file_and_directory(
        self,
        example_benchmark_object: Benchmark,
        test_image: str,
        random_sandbox_name: str,
        test_resources: Resources,
        sandbox_provider: SandboxProvider,
        live_aws_runtime: AWSRuntime,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        file_path = "/tmp/test_output.json"
        file_content = '{"result": "success", "score": 95}'
        dir_path = "/tmp/test_output_dir"
        task_id = random_task_id()
        aws_runtime = live_aws_runtime
        object_store = S3ObjectStore(aws_runtime)
        file_s3_key = task_artifact_key(str(example_benchmark_object.id), task_id, "test_output.json")
        dir_s3_key = task_artifact_key(str(example_benchmark_object.id), task_id, "test_output_dir")

        try:
            async with create_sandbox(
                provider=sandbox_provider,
                sandbox_name=random_sandbox_name,
                source=ImageSource(image=test_image),
                resources=test_resources,
                creation_semaphore=creation_semaphore,
                creation_state=SandboxCreationState(cleanup_pending=False),
            ) as sandbox:
                await sandbox.exec(f"echo '{file_content}' > {file_path}")
                await archive_and_upload_output(sandbox, file_path, file_s3_key, object_store)

                await sandbox.exec(f"mkdir -p {dir_path}/nested")
                await sandbox.exec(f"echo 'file1 content' > {dir_path}/file1.txt")
                await sandbox.exec(f"echo 'file2 content' > {dir_path}/file2.txt")
                await sandbox.exec(f"echo 'nested content' > {dir_path}/nested/file3.txt")
                await archive_and_upload_output(sandbox, dir_path, dir_s3_key, object_store)

                file_members = _archive_members(await object_store.get_bytes(file_s3_key))
                dir_members = _archive_members(await object_store.get_bytes(dir_s3_key))

                assert _member_content(file_members, "tmp/test_output.json") == f"{file_content}\n".encode()
                assert _member_content(dir_members, "tmp/test_output_dir/file1.txt") == b"file1 content\n"
                assert _member_content(dir_members, "tmp/test_output_dir/file2.txt") == b"file2 content\n"
                assert _member_content(dir_members, "tmp/test_output_dir/nested/file3.txt") == b"nested content\n"
        finally:
            await delete_from_s3(file_s3_key, aws_runtime)
            await delete_from_s3(dir_s3_key, aws_runtime)
