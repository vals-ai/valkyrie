"""Live integration tests for the shared sandbox provider contract.

Run: uv run pytest tests/integration/test_sandbox_provider.py

Requires AWS_PROFILE, TEST_AWS_REGION, TEST_DAYTONA_SECRET_NAME, and TEST_MODAL_SECRET_NAME.
"""

import asyncio
import shlex
import time
from pathlib import Path
from collections.abc import Awaitable, Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, Literal, cast
from uuid import uuid4

import httpx
import pytest
from daytona import V0_API_URL
from daytona.v0 import CreateSecretParams
from modal import Sandbox as ModalSdkSandbox

from benchmark_service import (
    CheckpointKind,
    DockerfileSource,
    ExecResult,
    ImageSource,
    Resources,
    Sandbox,
    SandboxBuildError,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    VolumeMount,
)
from benchmark_service.sandbox.daytona import DaytonaSandboxProvider
from benchmark_service.sandbox.modal import ModalProviderConfig

_POLL_INTERVAL_SECONDS = 2
_POLL_TIMEOUT_SECONDS = 120
_TEST_IMAGE = "python:3.12-slim"
_TEST_RESOURCES = Resources(vcpu=1, memory=2, disk=5)


def _python_command(script: str) -> str:
    return f"python -c {shlex.quote(script)}"


_MEMORY_FAULT_COMMAND = _python_command(
    "import resource\n"
    "limit = 256 * 1024 * 1024\n"
    "resource.setrlimit(resource.RLIMIT_AS, (limit, limit))\n"
    "try:\n"
    "    bytearray(1024 * 1024 * 1024)\n"
    "except MemoryError:\n"
    "    raise SystemExit(42)\n"
    "raise SystemExit(1)\n"
)
_STORAGE_FAULT_COMMAND = _python_command(
    "import errno\n"
    "try:\n"
    '    with open("/dev/full", "wb", buffering=0) as full_device:\n'
    '        full_device.write(b"provider-storage-fault")\n'
    "except OSError as exc:\n"
    "    raise SystemExit(43 if exc.errno == errno.ENOSPC else 1)\n"
    "raise SystemExit(1)\n"
)


async def _exec_until_success(sandbox: Sandbox, command: str, attempts: int = 10) -> ExecResult:
    for _ in range(attempts - 1):
        result = await sandbox.exec(command)
        if result.exit_code == 0:
            return result
        await asyncio.sleep(3)
    return await sandbox.exec(command)


async def _policy_error(provider: SandboxProvider, sandbox_id: str) -> object:
    if not isinstance(provider, DaytonaSandboxProvider):
        return None
    api_key = provider._config.DAYTONA_API_KEY  # pyright: ignore[reportPrivateUsage]
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{V0_API_URL}/sandbox/{sandbox_id}", headers={"Authorization": f"Bearer {api_key}"}
            )
        return response.json().get("networkPolicyError")
    except (httpx.HTTPError, ValueError) as exc:
        return f"networkPolicyError lookup failed: {exc!r}"


def _egress_probe_command(allowed_url: str, blocked_url: str | None = None) -> str:
    script = (
        "import sys\n"
        "import urllib.error\n"
        "import urllib.request\n"
        "\n"
        "def can_reach(url):\n"
        '    request = urllib.request.Request(url, headers={"User-Agent": "cbs-provider-test"}, method="HEAD")\n'
        "    try:\n"
        "        with urllib.request.urlopen(request, timeout=10):\n"
        "            return True\n"
        "    except urllib.error.HTTPError:\n"
        "        return True\n"
        "    except (urllib.error.URLError, TimeoutError, OSError):\n"
        "        return False\n"
        "\n"
        "allowed = can_reach(sys.argv[1])\n"
        "blocked = can_reach(sys.argv[2]) if len(sys.argv) > 2 else False\n"
        'print(f"allowed={allowed} blocked={blocked}", flush=True)\n'
        "raise SystemExit(0 if allowed and not blocked else 1)\n"
    )
    arguments = [allowed_url]
    if blocked_url is not None:
        arguments.append(blocked_url)

    return shlex.join(["python", "-c", script, *arguments])


async def _wait_until_listed(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> Sandbox:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        matches = [sandbox async for sandbox in provider.list_sandboxes(query) if sandbox.id == sandbox_id]
        if len(matches) == 1:
            return matches[0]
        if matches:
            pytest.fail(f"Sandbox {sandbox_id} was listed more than once.")
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(
        f"Sandbox {sandbox_id} was not listed within {_POLL_TIMEOUT_SECONDS} seconds. "
        + await _listing_diagnosis(provider, query, sandbox_id)
    )


async def _listing_diagnosis(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> str:
    async def labelled() -> str:
        started = time.monotonic()
        ids = [sandbox.id async for sandbox in provider.list_sandboxes(query)]
        return f"returned {len(ids)} sandboxes in {time.monotonic() - started:.1f}s"

    async def unfiltered() -> str:
        ids = [sandbox.id async for sandbox in provider.list_sandboxes(SandboxQuery(labels={}, page_size=100))]
        return f"returned {len(ids)} with this one {'in' if sandbox_id in ids else 'not in'} it"

    async def lookup() -> str:
        try:
            found = await provider.get_sandbox(sandbox_id)
        except SandboxNotFoundError:
            return "does not find it"
        return f"finds it with labels {dict(found.labels or {})}"

    async def attempt(check: Callable[[], Awaitable[str]]) -> str:
        try:
            return await check()
        except SandboxError as exc:
            return f"failed with {exc!r}"

    return (
        f"Query {query.labels} {await attempt(labelled)}, "
        f"an unfiltered listing {await attempt(unfiltered)}, "
        f"and get_sandbox {await attempt(lookup)}."
    )


async def _wait_until_not_found(provider: SandboxProvider, sandbox_id: str) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            await provider.get_sandbox(sandbox_id)
        except SandboxNotFoundError:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandbox {sandbox_id} was still found after {_POLL_TIMEOUT_SECONDS} seconds.")


async def _wait_until_not_listed(provider: SandboxProvider, query: SandboxQuery, sandbox_id: str) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        listed_ids = [sandbox.id async for sandbox in provider.list_sandboxes(query)]
        if sandbox_id not in listed_ids:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandbox {sandbox_id} was still listed after {_POLL_TIMEOUT_SECONDS} seconds.")


async def _wait_until_none_listed(provider: SandboxProvider, query: SandboxQuery) -> None:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not [sandbox async for sandbox in provider.list_sandboxes(query)]:
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(f"Sandboxes were still listed after {_POLL_TIMEOUT_SECONDS} seconds.")


async def _consume_command(sandbox: Sandbox) -> str:
    return "".join([chunk async for chunk in sandbox.command("echo after-delete")])


async def _consume_download(sandbox: Sandbox, remote_path: str) -> bytes:
    return b"".join([chunk async for chunk in sandbox.stream_download(remote_path)])


async def _read_stream_until_ready(sandbox: Sandbox, ready: asyncio.Event) -> None:
    async for chunk in sandbox.command("echo stream-ready && sleep 300"):
        if "stream-ready" in chunk:
            ready.set()


class TestSandboxProviderIntegration:
    """Run the same live contract checks against every configured provider."""

    @pytest.mark.parametrize("provider_type", ["modal"], indirect=True)
    async def test_modal_gpu_with_vm_provider(
        self,
        sandbox_provider_config: ModalProviderConfig,
        record_property: Callable[[str, object], None],
    ) -> None:
        """Create and delete a GPU sandbox with a VM provider default."""
        provider = sandbox_provider_config.model_copy(update={"runtime": "vm"}).create_provider()
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=Resources(vcpu=1, memory=2, disk=5, gpu=1, gpu_type="T4"),
            name=f"cbs-modal-gpu-runtime-{uuid4().hex[:10]}",
            labels={"test": "modal-gpu-runtime"},
            env_vars={},
            auto_stop_interval=5,
            create_timeout=120,
        )
        try:
            sandbox = await provider.create_sandbox(request)
            record_property("sandbox_id", sandbox.id)
            record_property("provider_runtime", "vm")
            record_property("gpu_count", request.resources.gpu)
            try:
                result = await sandbox.exec("nvidia-smi -L", timeout=30)
                record_property("output", result.output)
                assert result.exit_code == 0
                assert "Tesla T4" in result.output
            finally:
                await provider.delete_sandbox(sandbox.id)
                await _wait_until_not_found(provider, sandbox.id)
                record_property("cleanup", "sandbox not found")
        finally:
            await provider.close()

    @pytest.mark.parametrize("provider_type", ["modal"], indirect=True)
    @pytest.mark.parametrize("cancel", [False, True], ids=["timeout", "cancel"])
    @pytest.mark.usefixtures("provider_type")
    async def test_modal_cleans_up_delayed_create(
        self,
        request: pytest.FixtureRequest,
        monkeypatch: pytest.MonkeyPatch,
        record_property: Callable[[str, object], None],
        cancel: bool,
    ) -> None:
        """Verify late cleanup cannot delete another provider's retry."""
        sandbox_provider_config = cast(ModalProviderConfig, request.getfixturevalue("sandbox_provider_config"))
        provider = sandbox_provider_config.create_provider()
        retry_provider = sandbox_provider_config.create_provider()
        retries: list[Sandbox] = []
        native_create = cast(Callable[..., Awaitable[ModalSdkSandbox]], ModalSdkSandbox.create.aio)  # pyright: ignore[reportUnknownMemberType]
        started = asyncio.Event()
        allocated = asyncio.Event()
        release = asyncio.Event()
        sandboxes: list[ModalSdkSandbox] = []

        async def delayed_create(**kwargs: Any) -> ModalSdkSandbox:
            first = not started.is_set()
            started.set()
            sandbox = await native_create(**kwargs)
            if first:
                sandboxes.append(sandbox)
                allocated.set()
                await release.wait()
            return sandbox

        monkeypatch.setattr(ModalSdkSandbox.create, "aio", delayed_create)
        create_request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=f"cbs-modal-create-cleanup-{uuid4().hex[:10]}",
            labels={"test": "modal-create-cleanup"},
            env_vars={},
            auto_stop_interval=5,
            create_timeout=1,
        )

        async def create_in_context() -> None:
            async with provider:
                await provider.create_sandbox(create_request)

        creation = asyncio.create_task(create_in_context())
        try:
            await asyncio.wait_for(started.wait(), timeout=30)
            before = time.monotonic()
            if cancel:
                creation.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(creation), timeout=3)
            else:
                with pytest.raises(SandboxError, match="within 1s"):
                    await asyncio.wait_for(asyncio.shield(creation), timeout=3)
            elapsed = time.monotonic() - before
            record_property("caller_wait_seconds", elapsed)
            assert elapsed < 2
            await asyncio.wait_for(allocated.wait(), timeout=120)
            retry = await retry_provider.create_sandbox(create_request.model_copy(update={"create_timeout": 120}))
            retries.append(retry)
            release.set()
            await provider.close()
            assert len(sandboxes) == 1
            original_id = sandboxes[0].object_id
            record_property("sandbox_id", original_id)
            record_property("retry_sandbox_id", retry.id)
            assert retry.id != original_id
            await _wait_until_not_found(retry_provider, original_id)
            assert not cast(Any, provider)._cleanup_tasks
            result = await retry.exec("echo retry-alive", timeout=30)
            assert result.exit_code == 0
            assert result.output.strip() == "retry-alive"
            record_property("cleanup", "original absent; retry still running")
        finally:
            release.set()
            await asyncio.gather(creation, return_exceptions=True)
            try:
                await provider.close()
                for original in sandboxes:
                    record_property("original_sandbox_id", original.object_id)
                    await _wait_until_not_found(retry_provider, original.object_id)
                    record_property("original_cleanup", "sandbox not found")
            finally:
                try:
                    for retry in retries:
                        await retry_provider.delete_sandbox(retry.id)
                        await _wait_until_not_found(retry_provider, retry.id)
                        record_property("retry_cleanup", "sandbox not found")
                finally:
                    await retry_provider.close()

    @pytest.mark.parametrize("provider_type", ["modal"], indirect=True)
    @pytest.mark.parametrize("fault", ["terminate_failure", "pooled_cleanup", "close_cancel"])
    @pytest.mark.usefixtures("provider_type")
    async def test_modal_cleanup_faults(
        self,
        request: pytest.FixtureRequest,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        record_property: Callable[[str, object], None],
        fault: Literal["terminate_failure", "pooled_cleanup", "close_cancel"],
    ) -> None:
        """Keep client closure alive through cleanup failure and cancellation."""
        config = cast(ModalProviderConfig, request.getfixturevalue("sandbox_provider_config"))
        provider = config.create_provider()
        checker = config.create_provider()
        create_request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=f"cbs-modal-cleanup-fault-{uuid4().hex[:10]}",
            labels={"test": "modal-cleanup-fault"},
            env_vars={},
            auto_stop_interval=5,
            create_timeout=120,
        )
        sandbox = await provider.create_sandbox(create_request)
        record_property("sandbox_id", sandbox.id)
        client = cast(Any, provider)._client
        native_close = client._close.aio
        closing = asyncio.Event()
        closed = asyncio.Event()
        release = asyncio.Event()
        native_create = cast(Callable[..., Awaitable[ModalSdkSandbox]], ModalSdkSandbox.create.aio)  # pyright: ignore[reportUnknownMemberType]
        inner = cast(ModalSdkSandbox, cast(Any, sandbox)._sandbox)
        native_terminate = inner.terminate

        async def held_close() -> None:
            closing.set()
            await release.wait()
            await native_close()
            closed.set()

        async def late_create(**_kwargs: Any) -> ModalSdkSandbox:
            await release.wait()
            return inner

        async def failed_terminate(*, wait: bool) -> None:
            raise RuntimeError("injected termination failure")

        close_task: asyncio.Task[None] | None = None
        cleaned_up = False
        try:
            if fault != "close_cancel":
                monkeypatch.setattr(ModalSdkSandbox.create, "aio", late_create)
                if fault == "terminate_failure":
                    monkeypatch.setattr(inner, "terminate", SimpleNamespace(aio=failed_terminate))
                with pytest.raises(SandboxError, match="within 1s"):
                    await provider.create_sandbox(create_request.model_copy(update={"create_timeout": 1}))
                if fault == "pooled_cleanup":
                    release.set()
                    await _wait_until_not_found(checker, sandbox.id)
                    cleaned_up = True
                    async with asyncio.timeout(30):
                        while cast(Any, provider)._cleanup_tasks:
                            await asyncio.sleep(0.05)
                    assert not cast(Any, provider)._cleanup_tasks
                    assert not client.is_closed()
                    record_property("completed_cleanup_pruned", True)
                await asyncio.wait_for(provider.close(), timeout=2)
                release.set()
            else:
                monkeypatch.setattr(client, "_close", SimpleNamespace(aio=held_close))
                close_task = asyncio.create_task(provider.close())
                await asyncio.wait_for(closing.wait(), timeout=10)
                close_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await close_task
                release.set()
                await asyncio.wait_for(closed.wait(), timeout=30)
            async with asyncio.timeout(30):
                while not client.is_closed():
                    await asyncio.sleep(0.05)
            record_property("native_client_closed", client.is_closed())
            if fault == "terminate_failure":
                assert "injected termination failure" in caplog.text
                assert not cast(Any, provider)._cleanup_tasks
                record_property("cleanup_failure_logged", True)
            elif fault == "close_cancel":
                record_property("cancelled_close_finished", True)
        finally:
            release.set()
            if close_task is not None:
                await asyncio.gather(close_task, return_exceptions=True)
            monkeypatch.setattr(ModalSdkSandbox.create, "aio", native_create)
            monkeypatch.setattr(inner, "terminate", native_terminate)
            try:
                if not cleaned_up:
                    await checker.delete_sandbox(sandbox.id)
                await _wait_until_not_found(checker, sandbox.id)
                record_property("cleanup", "sandbox not found")
            finally:
                await provider.close()
                await checker.close()

    @pytest.mark.parametrize("provider_type", ["daytona"], indirect=True)
    async def test_daytona_inventory_metadata_and_filtering(
        self,
        sandbox_provider: SandboxProvider,
    ) -> None:
        """Verify Daytona inventory metadata and inclusive cutoff filtering."""
        sandbox_name = f"cbs-daytona-inventory-{uuid4().hex[:10]}"
        labels = {"ProviderInventoryTest": sandbox_name}
        query = SandboxQuery(labels=labels)
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=sandbox_name,
            labels=labels,
            env_vars={},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        try:
            fetched = await sandbox_provider.get_sandbox(sandbox.id)
            listed = await _wait_until_listed(sandbox_provider, query, sandbox.id)

            created_labels = sandbox.labels
            created_at = sandbox.created_at
            assert created_labels is not None
            assert all(created_labels.get(key) == value for key, value in labels.items())
            assert created_at is not None
            assert created_at.utcoffset() == timedelta(0)
            assert fetched.labels == listed.labels == created_labels
            assert fetched.created_at == listed.created_at == created_at

            await _wait_until_listed(
                sandbox_provider,
                SandboxQuery(labels=labels, created_at_lte=created_at),
                sandbox.id,
            )
            await _wait_until_not_listed(
                sandbox_provider,
                SandboxQuery(
                    labels=labels,
                    created_at_lte=created_at - timedelta(microseconds=1),
                ),
                sandbox.id,
            )
        finally:
            await asyncio.wait_for(
                sandbox_provider.delete_sandbox(sandbox.id),
                timeout=_POLL_TIMEOUT_SECONDS,
            )

    @pytest.mark.parametrize("provider_type", ["daytona"], indirect=True)
    async def test_daytona_native_secret_and_volume_persistence(
        self,
        sandbox_provider: SandboxProvider,
    ) -> None:
        provider = cast(DaytonaSandboxProvider, sandbox_provider)
        daytona = provider._daytona  # pyright: ignore[reportPrivateUsage]
        probe_id = uuid4().hex[:10]
        secret_name = f"cbs-secret-{probe_id}"
        # Shared long-lived volume: Daytona rejects deleting volumes that have
        # not settled to ready/error, so runs isolate by subpath instead of
        # provisioning ad-hoc volumes that cleanup would race against.
        volume_name = "cbs-provider-integration"
        run_id = f"cbs-run-{probe_id}"
        labels = {"ProviderIntegrationTest": probe_id, "run-id": run_id}
        writer: Sandbox | None = None
        reader: Sandbox | None = None
        secret_id: str | None = None

        try:
            await daytona.volume.get(volume_name, create=True)
            secret = await daytona.secret.create(
                CreateSecretParams(
                    name=secret_name,
                    value=uuid4().hex,
                    description="Ephemeral CBS provider integration secret",
                    hosts=["example.com"],
                )
            )
            secret_id = secret.id
            mount = VolumeMount(
                name=volume_name,
                mount_path="/workspace/artifacts",
                subpath="runs/{run_id}",
            )
            writer = await provider.create_sandbox(
                SandboxCreateRequest(
                    source=ImageSource(image=_TEST_IMAGE),
                    resources=_TEST_RESOURCES,
                    name=f"cbs-secret-writer-{probe_id}",
                    labels=labels,
                    env_vars={},
                    sandbox_secrets={"CBS_SECRET_TEST": secret_name},
                    volumes=[mount],
                    auto_stop_interval=10,
                    create_timeout=360,
                )
            )

            secret_check = await writer.exec(
                f'test -n "$CBS_SECRET_TEST" && test "$CBS_SECRET_TEST" != {shlex.quote(secret_name)}'
            )
            assert secret_check.exit_code == 0
            write_result = await writer.exec("printf provider-volume-persistence > /workspace/artifacts/proof.txt")
            assert write_result.exit_code == 0
            await provider.delete_sandbox(writer.id)
            writer = None

            reader = await provider.create_sandbox(
                SandboxCreateRequest(
                    source=ImageSource(image=_TEST_IMAGE),
                    resources=_TEST_RESOURCES,
                    name=f"cbs-volume-reader-{probe_id}",
                    labels=labels,
                    env_vars={},
                    volumes=[mount],
                    auto_stop_interval=10,
                    create_timeout=360,
                )
            )

            read_result = await reader.exec("cat /workspace/artifacts/proof.txt")
            assert read_result.stdout == "provider-volume-persistence"
            secret_absence = await reader.exec('test -z "${CBS_SECRET_TEST+x}"')
            assert secret_absence.exit_code == 0
        finally:
            if reader is not None:
                await provider.delete_sandbox(reader.id)
            if writer is not None:
                await provider.delete_sandbox(writer.id)
            if secret_id is not None:
                await daytona.secret.delete(secret_id)

    @staticmethod
    def _dockerfile_request(
        dockerfile: Path, name: str, *, create_timeout: int = 360, build_timeout: float = 600
    ) -> SandboxCreateRequest:
        return SandboxCreateRequest(
            source=DockerfileSource(dockerfile=str(dockerfile), build_timeout=build_timeout),
            resources=_TEST_RESOURCES,
            name=name,
            labels={"ProviderIntegrationTest": name},
            env_vars={},
            auto_stop_interval=10,
            create_timeout=create_timeout,
        )

    async def test_files_checkpoint_restores_the_filesystem_into_a_new_sandbox(
        self, sandbox_provider: SandboxProvider
    ) -> None:
        """Checkpoint a sandbox's files, delete it, and read the same file back from a sandbox restored from it."""
        if CheckpointKind.FILES not in sandbox_provider.capabilities.checkpoints:
            pytest.skip("Provider does not offer filesystem checkpoints")
        name = f"cbs-checkpoint-test-{uuid4().hex[:10]}"
        remote_path = "/opt/checkpointed.txt"
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=name,
            labels={"ProviderIntegrationTest": name},
            env_vars={},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        try:
            await sandbox.upload_file(remote_path, name.encode())
            checkpoint = await sandbox.checkpoint(CheckpointKind.FILES, name)
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)

        try:
            restored = await sandbox_provider.create_sandbox(
                request.model_copy(update={"source": checkpoint, "name": f"{name}-restored"})
            )
            try:
                assert await restored.download_file(remote_path) == name.encode()
            finally:
                await sandbox_provider.delete_sandbox(restored.id)
        finally:
            await sandbox_provider.delete_checkpoint(checkpoint)

    async def test_memory_checkpoint_restores_with_fresh_command_env(self, sandbox_provider: SandboxProvider) -> None:
        """A sandbox restored from a memory checkpoint keeps its files and gives fresh env to each command."""
        if CheckpointKind.MEMORY not in sandbox_provider.capabilities.checkpoints:
            pytest.skip("Provider does not offer memory checkpoints")
        name = f"cbs-memory-test-{uuid4().hex[:10]}"
        remote_path = "/opt/checkpointed.txt"
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=name,
            labels={"ProviderIntegrationTest": name},
            env_vars={"RESTORE_VALUE": "saved"},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        try:
            await sandbox.upload_file(remote_path, name.encode())
            checkpoint = await sandbox.checkpoint(CheckpointKind.MEMORY, name)
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)

        try:
            restored = await sandbox_provider.create_sandbox(
                request.model_copy(
                    update={"source": checkpoint, "name": f"{name}-restored", "env_vars": {"RESTORE_VALUE": "fresh"}}
                )
            )
            try:
                assert await restored.download_file(remote_path) == name.encode()
                result = await restored.exec('printf "$RESTORE_VALUE"')
                assert (result.exit_code, result.stdout) == (0, "fresh")
                reopened = await sandbox_provider.get_sandbox(restored.id)
                assert (await reopened.exec('printf "$RESTORE_VALUE"')).stdout == "fresh"
            finally:
                await sandbox_provider.delete_sandbox(restored.id)
        finally:
            await sandbox_provider.delete_checkpoint(checkpoint)

    async def test_dockerfile_source_is_built_with_its_folder_as_the_context(
        self, sandbox_provider: SandboxProvider, tmp_path: Path
    ) -> None:
        """Build a Dockerfile whose COPY reads a file beside it, then delete the sandbox and see it gone."""
        name = f"cbs-dockerfile-test-{uuid4().hex[:10]}"
        (tmp_path / "Dockerfile").write_text(
            f"FROM python:3.12-slim\nRUN echo {name}\nCOPY seed.txt /seed.txt\nRUN echo built > /built.txt\n"
        )
        (tmp_path / "seed.txt").write_text("from the build context")

        sandbox = await sandbox_provider.create_sandbox(self._dockerfile_request(tmp_path / "Dockerfile", name))
        try:
            result = await sandbox.exec("cat /seed.txt /built.txt")
            assert result.exit_code == 0
            assert result.stdout == "from the build contextbuilt\n"
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)
            await _wait_until_not_found(sandbox_provider, sandbox.id)

    async def test_a_failing_dockerfile_build_raises_a_build_error_and_leaves_no_sandbox(
        self, sandbox_provider: SandboxProvider, tmp_path: Path
    ) -> None:
        """Report a failed build with the provider's output, and leave nothing running under the test's label."""
        name = f"cbs-dockerfile-test-{uuid4().hex[:10]}"
        (tmp_path / "Dockerfile").write_text(f"FROM python:3.12-slim\nRUN echo {name} && exit 3\n")

        with pytest.raises(SandboxBuildError) as error:
            await sandbox_provider.create_sandbox(self._dockerfile_request(tmp_path / "Dockerfile", name))

        assert error.value.build_log
        await _wait_until_none_listed(sandbox_provider, SandboxQuery(labels={"ProviderIntegrationTest": name}))

    async def test_a_slow_dockerfile_build_times_out_as_a_build_error_once(
        self, sandbox_provider: SandboxProvider, tmp_path: Path
    ) -> None:
        """Stop a build at its own timeout, report it as a build error without retrying it, and leave nothing behind."""
        name = f"cbs-dockerfile-test-{uuid4().hex[:10]}"
        (tmp_path / "Dockerfile").write_text(f"FROM python:3.12-slim\nRUN echo {name}\nRUN sleep 300\n")
        started = time.monotonic()

        with pytest.raises(SandboxBuildError):
            await sandbox_provider.create_sandbox(
                self._dockerfile_request(tmp_path / "Dockerfile", name, create_timeout=10, build_timeout=25)
            )

        assert time.monotonic() - started < 120
        await _wait_until_none_listed(sandbox_provider, SandboxQuery(labels={"ProviderIntegrationTest": name}))

    async def test_a_sandbox_created_blocked_accepts_a_hostname_rule(self, sandbox_provider: SandboxProvider) -> None:
        """A sandbox created with all egress blocked can't reach out until a hostname rule allows that host."""
        sandbox_name = f"cbs-provider-blocked-{uuid4().hex[:10]}"
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=sandbox_name,
            labels={"ProviderIntegrationTest": sandbox_name},
            env_vars={},
            auto_stop_interval=10,
            create_timeout=360,
            network_block_all=True,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        try:
            blocked_result = await sandbox.exec(_egress_probe_command("https://www.python.org"))
            assert "allowed=False" in blocked_result.stdout

            await sandbox.modify_egress_rules(["www.python.org"])
            allowed_result = await _exec_until_success(
                sandbox, _egress_probe_command("https://www.python.org", "https://example.com")
            )
            assert "allowed=True blocked=False" in allowed_result.stdout
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)

    @pytest.mark.parametrize("provider_type", ["daytona_v2"], indirect=True)
    async def test_a_sandbox_outlives_the_platform_default_timeout(self, sandbox_provider: SandboxProvider) -> None:
        """A sandbox running one long command is still there after seven minutes."""
        sandbox = await sandbox_provider.create_sandbox(
            SandboxCreateRequest(
                source=ImageSource(image=_TEST_IMAGE),
                resources=_TEST_RESOURCES,
                name=f"cbs-lifetime-test-{uuid4().hex[:10]}",
                labels={"test": "lifetime"},
                env_vars={},
                auto_stop_interval=10,
                create_timeout=360,
            )
        )
        try:
            result = await sandbox.exec("sleep 420 && echo survived", timeout=600)
            assert result.exit_code == 0, result.output
            assert "survived" in result.stdout
        finally:
            await sandbox_provider.delete_sandbox(sandbox.id)

    async def test_provider_contract(self, sandbox_provider: SandboxProvider) -> None:
        """Verify each provider implements the shared sandbox lifecycle and operation contract.

        Test cases:
        - Create-time and command-time environment variables reach sandbox commands.
        - Memory, storage, timeout, and OS-kill faults preserve their command exit codes.
        - Egress rules allow the configured host, block an off-list host, and restore unrestricted access.
        - A host on the same CDN as an allowed one stays blocked, and a `*.` wildcard allows its subdomains.
        - Deleting a sandbox during command streaming raises SandboxNotFoundError with the provider's last-known state.
        - Create, get, list, exec, file, and streaming download operations work.
        - Deleted sandboxes disappear and stale handles consistently raise SandboxNotFoundError.
        """
        sandbox_name = f"cbs-provider-test-{uuid4().hex[:10]}"
        labels = {"ProviderIntegrationTest": sandbox_name}
        query = SandboxQuery(labels=labels)
        remote_path = "/tmp/provider-integration.bin"
        payload = b"provider-integration-content"
        request = SandboxCreateRequest(
            source=ImageSource(image=_TEST_IMAGE),
            resources=_TEST_RESOURCES,
            name=sandbox_name,
            labels=labels,
            env_vars={"PROVIDER_CREATE_VALUE": "create-ready"},
            auto_stop_interval=10,
            create_timeout=360,
        )

        sandbox = await sandbox_provider.create_sandbox(request)
        stream_task: asyncio.Task[None] | None = None
        try:
            fetched = await sandbox_provider.get_sandbox(sandbox.id)
            assert fetched.id == sandbox.id

            await _wait_until_listed(sandbox_provider, query, sandbox.id)

            exec_result = await sandbox.exec('printf "$PROVIDER_CREATE_VALUE"', cwd="/tmp")
            assert exec_result.exit_code == 0
            assert exec_result.stdout == "create-ready"

            command_output = "".join(
                [
                    chunk
                    async for chunk in sandbox.command(
                        'printf "$PROVIDER_TEST_VALUE"',
                        env_vars={"PROVIDER_TEST_VALUE": "command-ready"},
                    )
                ]
            )
            assert "command-ready" in command_output

            await sandbox.upload_file(remote_path, payload)
            assert await sandbox.download_file(remote_path) == payload
            assert await _consume_download(sandbox, remote_path) == payload

            fault_cases: list[tuple[str, str, float | None, int]] = [
                ("memory", _MEMORY_FAULT_COMMAND, None, 42),
                ("storage", _STORAGE_FAULT_COMMAND, None, 43),
                ("timeout", "sleep 30", 1, 124),
                ("os-kill", "sh -c 'kill -KILL $$'", None, 137),
            ]
            for fault_name, fault_command, fault_timeout, expected_exit_code in fault_cases:
                with pytest.raises(SandboxCommandError) as error:
                    _ = "".join([chunk async for chunk in sandbox.command(fault_command, timeout=fault_timeout)])

                assert error.value.exit_code == expected_exit_code, fault_name

            egress_cases = [
                (["https://example.com"], "https://example.com", "https://www.python.org"),
                (["files.pythonhosted.org"], "https://files.pythonhosted.org", "https://www.python.org"),
                (["*.python.org"], "https://www.python.org", "https://example.com"),
            ]
            try:
                for allowlist, allowed_url, blocked_url in egress_cases:
                    await sandbox.modify_egress_rules(allowlist)
                    restricted_result = await _exec_until_success(
                        sandbox, _egress_probe_command(allowed_url, blocked_url)
                    )
                    assert "allowed=True blocked=False" in restricted_result.stdout, (
                        allowlist,
                        await _policy_error(sandbox_provider, sandbox.id),
                    )
                    assert restricted_result.exit_code == 0, allowlist
                await sandbox.block_all_egress()
                blocked_result = await sandbox.exec(_egress_probe_command("https://example.com"))
                assert "allowed=False" in blocked_result.stdout
            finally:
                await sandbox.clear_egress_rules()

            restored_result = await _exec_until_success(sandbox, _egress_probe_command("https://www.python.org"))
            assert restored_result.exit_code == 0
            assert "allowed=True" in restored_result.stdout

            stream_ready = asyncio.Event()
            stream_task = asyncio.create_task(_read_stream_until_ready(sandbox, stream_ready))
            await asyncio.wait_for(stream_ready.wait(), timeout=30)
            await sandbox_provider.delete_sandbox(sandbox.id)

            with pytest.raises(SandboxNotFoundError) as lost:
                await asyncio.wait_for(stream_task, timeout=60)
            assert "state=" in str(lost.value) or "poll_result=" in str(lost.value), str(lost.value)

        finally:
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
            await sandbox_provider.delete_sandbox(sandbox.id)

        await _wait_until_not_found(sandbox_provider, sandbox.id)
        await _wait_until_not_listed(sandbox_provider, query, sandbox.id)

        operations: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("provider.get_sandbox", lambda: sandbox_provider.get_sandbox(sandbox.id)),
            ("sandbox.exec", lambda: sandbox.exec("echo after-delete")),
            ("sandbox.command", lambda: _consume_command(sandbox)),
            ("sandbox.upload_file", lambda: sandbox.upload_file(remote_path, payload)),
            ("sandbox.download_file", lambda: sandbox.download_file(remote_path)),
            ("sandbox.stream_download", lambda: _consume_download(sandbox, remote_path)),
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
