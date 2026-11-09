import asyncio
from collections.abc import Awaitable, Callable

import pytest
from benchmark_service import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)

from tracker.sandbox import SandboxCreationState, create_sandbox


async def _wait_until_sandbox_not_found(
    sandbox_provider: SandboxProvider,
    sandbox_id: str,
) -> None:
    for _ in range(30):
        try:
            await sandbox_provider.get_sandbox(sandbox_id)
        except SandboxNotFoundError:
            return
        await asyncio.sleep(2)

    pytest.fail(f"Sandbox {sandbox_id} was still found after delete")


async def _wait_until_sandbox_listed(
    sandbox_provider: SandboxProvider,
    query: SandboxQuery,
    sandbox_id: str,
) -> None:
    for _ in range(30):
        listed_ids = [sandbox.id async for sandbox in sandbox_provider.list_sandboxes(query)]
        if sandbox_id in listed_ids:
            return
        await asyncio.sleep(2)

    pytest.fail(f"Sandbox {sandbox_id} was not listed before delete")


async def _consume_command(sandbox: Sandbox) -> None:
    async for _ in sandbox.command("echo after-delete"):
        pass


async def _read_stream_until_ready(sandbox: Sandbox, ready: asyncio.Event) -> None:
    async for chunk in sandbox.command("while true; do echo stream-ready; sleep 1; done"):
        if "stream-ready" in chunk:
            ready.set()


@pytest.mark.description("Live sandbox provider behavior after remote deletion and failure.")
class TestProvider:
    @pytest.mark.description(
        (
            "Deleted sandbox handles report the provider's SandboxNotFoundError.\n"
            "\n"
            "Test cases:\n"
            "- Provider lookup raises SandboxNotFoundError after delete.\n"
            "- Stale sandbox exec, stream, upload, and download operations preserve SandboxNotFoundError after delete."
        )
    )
    async def test_destroyed_sandbox_operations_raise_not_found(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            await sandbox.exec("echo ready")

        await _wait_until_sandbox_not_found(sandbox_provider, sandbox.id)

        operations: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("provider.get_sandbox", lambda: sandbox_provider.get_sandbox(sandbox.id)),
            ("sandbox.exec", lambda: sandbox.exec("echo after-delete")),
            ("sandbox.upload_file", lambda: sandbox.upload_file("/tmp/after-delete.txt", b"after-delete")),
            ("sandbox.download_file", lambda: sandbox.download_file("/tmp/after-delete.txt")),
            ("sandbox.command", lambda: _consume_command(sandbox)),
        ]

        for operation_name, operation in operations:
            try:
                await operation()
            except SandboxNotFoundError:
                continue
            except Exception as exc:
                pytest.fail(f"{operation_name} raised {type(exc).__name__} instead of SandboxNotFoundError: {exc}")
            else:
                pytest.fail(f"{operation_name} did not raise SandboxNotFoundError")

    @pytest.mark.description(
        (
            "Deleting a sandbox during provider command streaming reports not-found.\n"
            "\n"
            "Test cases:\n"
            "- The command stream starts returning output before deletion.\n"
            "- Deleting the sandbox while the command is streaming raises SandboxNotFoundError in the stream consumer."
        )
    )
    async def test_streaming_command_raises_not_found_when_sandbox_is_deleted(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
        ) as sandbox:
            ready = asyncio.Event()
            stream_task = asyncio.create_task(_read_stream_until_ready(sandbox, ready))
            await asyncio.wait_for(ready.wait(), timeout=30)
            await sandbox_provider.delete_sandbox(sandbox.id)

            with pytest.raises(SandboxNotFoundError):
                await stream_task

    @pytest.mark.description(
        (
            "`list_sandboxes` does not return a deleted sandbox.\n"
            "\n"
            "Test cases:\n"
            "- A live sandbox with a unique label appears in list_sandboxes.\n"
            "- The same sandbox no longer appears in list_sandboxes after deletion."
        )
    )
    async def test_destroyed_sandbox_does_not_show_up_in_list_sandboxes(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        test_image: str,
        random_sandbox_name: str,
        creation_semaphore: asyncio.Semaphore,
    ) -> None:
        labels = {"ProviderIntegrationTest": random_sandbox_name}
        query = SandboxQuery(labels=labels)

        async with create_sandbox(
            sandbox_provider,
            random_sandbox_name,
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            creation_state=SandboxCreationState(cleanup_pending=False),
            labels=labels,
        ) as sandbox:
            await _wait_until_sandbox_listed(sandbox_provider, query, sandbox.id)

        await _wait_until_sandbox_not_found(sandbox_provider, sandbox.id)

        listed_after_delete = [listed.id async for listed in sandbox_provider.list_sandboxes(query)]
        assert sandbox.id not in listed_after_delete

    @pytest.mark.description(
        (
            "Sandboxes that fail during image startup can still be deleted.\n"
            "\n"
            "Test cases:\n"
            "- A missing-image create leaves a provider-visible failed sandbox.\n"
            "- Deleting that errored sandbox succeeds and the sandbox becomes not found."
        )
    )
    async def test_error_state_sandbox_can_be_deleted(
        self,
        sandbox_provider: SandboxProvider,
        test_resources: Resources,
        random_sandbox_name: str,
    ) -> None:
        request = SandboxCreateRequest(
            source=ImageSource(image=f"vals-ai/missing-provider-test-image:{random_sandbox_name}"),
            resources=test_resources,
            name=random_sandbox_name,
            labels={"ProviderIntegrationTest": random_sandbox_name},
            env_vars={},
            auto_stop_interval=600,
            create_timeout=360,
        )

        with pytest.raises(SandboxError, match="Daytona sandbox provider error"):
            await sandbox_provider.create_sandbox(request)

        sandbox = await sandbox_provider.get_sandbox(random_sandbox_name)

        try:
            assert "ERROR" in sandbox.state or "FAILED" in sandbox.state
            await sandbox_provider.delete_sandbox(sandbox.id)
            await _wait_until_sandbox_not_found(sandbox_provider, sandbox.id)
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)
