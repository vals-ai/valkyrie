import hashlib
import io
import json
import zipfile
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, Mock

from benchmark_service import Sandbox

import pytest

from tracker.agent.releases import preserve_attempt_outputs, resolve_agent_release, retained_agent_key, validate_archive
from tracker.types import AgentContractRequest, AgentReleaseSelection
from tracker.local.storage import FilesystemObjectStore
from tracker.sandbox import upload_agent_artifacts


def archive(command: str) -> bytes:
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as bundle:
        bundle.writestr(
            "example/contract.yaml",
            f"name: example\ninstall_cmd: echo setup\nrun_cmd: {command} {{problem_statement_path}}\nkwargs:\n  budget:\n    type: int\n    required: false\n    default: 1\n  model:\n    type: str\n    required: false\n    default: default-model\n",
        )
        bundle.writestr("example/launch.sh", command)
    return content.getvalue()


class TestRetainedRelease:
    @pytest.mark.description("A saved version keeps the same files and configuration when the agent alias changes.")
    @pytest.mark.parametrize("model", [None, "evaluated-model"])
    @pytest.mark.parametrize("explicit_digest", [False, True])
    async def test_alias_moves_without_changing_selected_bytes(
        self, tmp_path: Path, model: str | None, explicit_digest: bool
    ) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        first, second = archive("echo A1"), archive("echo A2")
        selection = AgentReleaseSelection(
            reference="A1",
            archive_sha256=hashlib.sha256(first).hexdigest(),
            prescribed_configuration={"budget": "2"},
            overrides={"budget": "5"},
        )
        await store.put_bytes(
            retained_agent_key("example", selection).rsplit("/", 1)[0] + "/release.json",
            json.dumps(
                {"schema_version": 1, "agent": "example", "archive": "example.zip", "sha256": selection.archive_sha256}
            ).encode(),
        )
        await store.put_bytes(retained_agent_key("example", selection), first)
        await store.put_bytes("agents/example.zip", first)
        requested = AgentContractRequest(
            name="example",
            agent_release=selection if explicit_digest else selection.model_copy(update={"archive_sha256": None}),
            model=model,
        )

        resolved = await resolve_agent_release(store, requested)
        await store.put_bytes("agents/example.zip", second)
        retried = await resolve_agent_release(store, requested)

        sandbox = Mock()
        sandbox.upload_file = AsyncMock()
        sandbox.exec = AsyncMock(return_value=Mock(exit_code=0))
        await upload_agent_artifacts(cast(Sandbox, sandbox), resolved, "run", store)
        sandbox.upload_file.assert_any_await("/bundle/example/launch.sh", b"echo A1")

        assert resolved.run_cmd == retried.run_cmd == "echo A1 {problem_statement_path}"
        assert resolved.model == (model or "default-model")
        assert resolved.kwargs["budget"] == retried.kwargs["budget"] == "5"
        assert resolved.agent_release is not None
        assert resolved.agent_release.archive_sha256 == selection.archive_sha256
        assert await store.get_bytes(retained_agent_key("example", resolved.agent_release)) == first

        replacement = AgentReleaseSelection(reference="A2", archive_sha256=hashlib.sha256(second).hexdigest())
        await store.put_bytes(
            retained_agent_key("example", replacement).rsplit("/", 1)[0] + "/release.json",
            json.dumps(
                {
                    "schema_version": 1,
                    "agent": "example",
                    "archive": "example.zip",
                    "sha256": replacement.archive_sha256,
                }
            ).encode(),
        )
        await store.put_bytes(retained_agent_key("example", replacement), second)
        updated = await resolve_agent_release(store, requested.model_copy(update={"agent_release": replacement}))

        assert updated.run_cmd == "echo A2 {problem_statement_path}"
        assert await store.get_bytes(retained_agent_key("example", selection)) == first

        invalid = selection.model_copy(update={"overrides": {"budget": "not-a-number"}})
        with pytest.raises(ValueError, match="budget"):
            await resolve_agent_release(store, requested.model_copy(update={"agent_release": invalid}))

    @pytest.mark.description("Missing or damaged saved releases fail without using the current agent alias.")
    async def test_missing_and_corrupted_release_never_use_alias(self, tmp_path: Path) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        content = archive("echo A1")
        selection = AgentReleaseSelection(reference="A1", archive_sha256=hashlib.sha256(content).hexdigest())
        request = AgentContractRequest(name="example", agent_release=selection)
        await store.put_bytes("agents/example.zip", content)

        with pytest.raises(FileNotFoundError):
            await resolve_agent_release(store, request)

        await store.put_bytes(
            retained_agent_key("example", selection).rsplit("/", 1)[0] + "/release.json",
            json.dumps(
                {"schema_version": 1, "agent": "example", "archive": "example.zip", "sha256": selection.archive_sha256}
            ).encode(),
        )
        await store.put_bytes(retained_agent_key("example", selection), archive("echo wrong"))
        with pytest.raises(ValueError, match="digest mismatch"):
            await resolve_agent_release(store, request)

    @pytest.mark.description("Invalid release checksums fail before an archive can be selected.")
    @pytest.mark.parametrize("receipt_digest", [None, "../archive", "a" * 64])
    async def test_rejects_invalid_or_unexpected_receipt_digest(
        self, tmp_path: Path, receipt_digest: str | None
    ) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        selection = AgentReleaseSelection(
            reference="A1", archive_sha256="b" * 64 if receipt_digest == "a" * 64 else None
        )
        await store.put_bytes(
            "agent-releases/example/A1/release.json",
            json.dumps(
                {"schema_version": 1, "agent": "example", "archive": "example.zip", "sha256": receipt_digest}
            ).encode(),
        )
        with pytest.raises(ValueError, match="receipt does not match"):
            await resolve_agent_release(store, AgentContractRequest(name="example", agent_release=selection))

    @pytest.mark.description("Preserves outputs before replacement.")
    async def test_preserves_outputs_before_replacement(self, tmp_path: Path) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        key = "benchmarks/run/task/output.txt"
        await store.put_bytes(key, b"A1 output")

        retained = await preserve_attempt_outputs(store, "run", "attempt2")
        await store.put_bytes(key, b"A2 output")

        assert await store.get_bytes(retained + "task/output.txt") == b"A1 output"
        assert await store.get_bytes(key) == b"A2 output"

    @pytest.mark.description("Rejects archive traversal.")
    def test_rejects_archive_traversal(self) -> None:
        content = io.BytesIO()
        with zipfile.ZipFile(content, "w") as bundle:
            bundle.writestr("../escape", "bad")
        data = content.getvalue()
        selection = AgentReleaseSelection(reference="unsafe", archive_sha256=hashlib.sha256(data).hexdigest())

        with pytest.raises(ValueError, match="Unsafe"):
            validate_archive(data, selection)
