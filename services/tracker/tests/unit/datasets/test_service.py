from benchmark_service import SandboxProviders
import asyncio
import os
import io
import tarfile
from collections.abc import AsyncIterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import DockerfileSource, ImageSource, Resources, UnsupportedSandboxConfigError
from benchmark_service.sandbox import DockerProviderConfig, ModalProviderConfig, SandboxProviderConfig
from benchmark_service.remote_grading import RemoteGradingAuthorization

from tests.unit.datasets.support import FakeSandbox, OuterSandbox, store_archive, zip_bytes
from tracker.services.datasets import artifacts
from tracker.runtime.artifacts import task_artifact_key
from tracker.services.datasets.artifacts import (
    SAVED_ARTIFACTS_RUN,
    STAGING_ROOT,
    ArtifactSink,
    MissingSavedArtifactError,
)
from tracker.services.datasets.env import SecretReader
from tracker.services.datasets.grading import (
    REWARD_TEXT_PATH,
    RESET_STEP_LOG_DIRS,
    VERIFIER_OUTPUT_PATH,
    VerifierResultError,
)
from tracker.services.datasets.config import MultiStepRewardStrategy
from tracker.services.datasets.image_config import ImageConfig
from tracker.services.datasets.retrieve_task import TaskFolderResponse
from tracker.clients import dataset_tasks
from tracker.clients.dataset_tasks import DatasetTaskService, InvalidTaskError, TaskSelectionError
from tracker.services.datasets.source import ArchiveSource, LocalFolderSource, TaskSource
from tracker.exceptions import InvalidSandboxConfigurationError, SecretsError
from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.run_egress import ScopedGateway

TASK_TOML = """
[agent]
timeout_sec = 600

[verifier]
timeout_sec = 60

[environment]
docker_image = "ubuntu:24.04"
cpus = 2
memory_mb = 4096
workdir = "/app"
"""


@pytest.mark.description("In-place grading retains every verifier file even when grading fails.")
@pytest.mark.parametrize("reward,exit_code", [(b"1", 0), (b"bad", 0), (None, 1), (None, 0)])
async def test_in_place_verifier_archive(
    dataset: Path, monkeypatch: pytest.MonkeyPatch, reward: bytes | None, exit_code: int
) -> None:
    files = {
        "/logs/verifier/details.json": b'{"attempts": ["raw response"]}',
        "/logs/verifier/nested/extra.bin": b"extra",
        VERIFIER_OUTPUT_PATH: b"full stdout",
    }
    if reward is not None:
        files[REWARD_TEXT_PATH] = reward
    uploaded: dict[str, bytes] = {}

    async def sink(_task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
        uploaded[name] = b"".join([chunk async for chunk in chunks])

    service = service_for(
        LocalFolderSource(dataset), FakeSandbox(writes=files, exit_code=exit_code), monkeypatch, artifact_sink=sink
    )

    if reward == b"1":
        result = await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)
        assert isinstance(result, dict) and result["rewards"] == {"reward": 1.0}
    else:
        with pytest.raises(VerifierResultError):
            await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)

    with tarfile.open(fileobj=io.BytesIO(uploaded["verifier_output.tar.gz"])) as archive:
        assert {
            member.name: cast(io.BufferedReader, archive.extractfile(member)).read()
            for member in archive
            if member.isfile()
        } == {path.removeprefix("/logs/"): content for path, content in files.items()}


@pytest.mark.description("A required verifier upload failure fails an otherwise successful task.")
async def test_in_place_verifier_upload_failure(dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def sink(_task_id: str, _name: str, _chunks: AsyncIterable[bytes]) -> None:
        raise OSError("storage unavailable")

    service = service_for(
        LocalFolderSource(dataset), FakeSandbox(writes={REWARD_TEXT_PATH: b"1"}), monkeypatch, artifact_sink=sink
    )

    with pytest.raises(RuntimeError, match="Required artifact /logs/verifier could not be saved: storage unavailable"):
        await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)


@pytest.mark.description("An empty verifier directory is archived when grading produces no reward or output.")
async def test_empty_verifier_archive(dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded: dict[str, bytes] = {}

    async def sink(_task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
        uploaded[name] = b"".join([chunk async for chunk in chunks])

    service = service_for(LocalFolderSource(dataset), FakeSandbox(), monkeypatch, artifact_sink=sink)

    with pytest.raises(VerifierResultError, match="no reward file"):
        await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)

    with tarfile.open(fileobj=io.BytesIO(uploaded["verifier_output.tar.gz"])) as archive:
        assert not any(member.isfile() for member in archive)


@pytest.mark.description("A snapshot failure is logged with the task ID without replacing the original grading error.")
@pytest.mark.parametrize("failure", ["staging", "upload"])
async def test_grading_error_survives_snapshot_failure(
    dataset: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, failure: str
) -> None:
    if failure == "staging":
        monkeypatch.setattr(artifacts, "MAX_ARTIFACT_FILES", 0)

    async def sink(_task_id: str, _name: str, _chunks: AsyncIterable[bytes]) -> None:
        raise OSError("storage unavailable")

    service = service_for(LocalFolderSource(dataset), FakeSandbox(exit_code=23), monkeypatch, artifact_sink=sink)
    artifacts.logger.addHandler(caplog.handler)

    try:
        with pytest.raises(VerifierResultError, match="The verifier failed with exit code 23"):
            await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)
    finally:
        artifacts.logger.removeHandler(caplog.handler)

    (record,) = [record for record in caplog.records if record.name == "tracker.services.datasets.artifacts"]
    assert "task-a" in record.getMessage()
    assert "could not be saved after grading failed" in record.getMessage()
    assert record.exc_info is not None
    assert ("holds more than 0 files" if failure == "staging" else "storage unavailable") in str(record.exc_info[1])


@pytest.mark.description("A verifier archive that exceeds staging limits fails the task instead of being skipped.")
async def test_verifier_staging_failure(dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(artifacts, "MAX_ARTIFACT_FILES", 0)

    async def sink(_task_id: str, _name: str, _chunks: AsyncIterable[bytes]) -> None:
        pytest.fail("An incomplete verifier archive must not be uploaded")

    service = service_for(
        LocalFolderSource(dataset), FakeSandbox(writes={REWARD_TEXT_PATH: b"1"}), monkeypatch, artifact_sink=sink
    )

    with pytest.raises(RuntimeError, match="Verifier output could not be saved: it holds more than 0 files"):
        await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)


@pytest.mark.description(
    "Regrade saves fresh verifier output under the new run without replaying the old verifier archive."
)
async def test_regrade_verifier_archive(dataset: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (dataset / "task-a" / "task.toml").write_text(
        "artifacts = [{ source = '/workspace/output', destination = 'verifier' }]\n"
        + TASK_TOML.replace("[verifier]", "[verifier]\nenvironment_mode = 'separate'")
    )
    objects = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    old_key = task_artifact_key("old-run", "task-a", "verifier_output.tar.gz")
    new_key = task_artifact_key("new-run", "task-a", "verifier_output.tar.gz")
    await objects.put_bytes(old_key, b"old verifier output is not an input")
    recording = io.BytesIO()
    with tarfile.open(fileobj=recording, mode="w:gz") as archive:
        member = tarfile.TarInfo("output/audio.wav")
        member.size = len(b"saved recording")
        archive.addfile(member, io.BytesIO(b"saved recording"))
    await objects.put_bytes(task_artifact_key("old-run", "task-a", "artifacts/verifier.tar.gz"), recording.getvalue())
    sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1", "/logs/verifier/details.json": b"new response"})
    provider = SimpleNamespace(create_sandbox=AsyncMock(return_value=sandbox), delete_sandbox=AsyncMock())

    async def sink(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
        await objects.put_stream(task_artifact_key("new-run", task_id, name), chunks)

    service = DatasetTaskService(LocalFolderSource(dataset), artifact_sink=sink)
    monkeypatch.setattr(service, "get_sandbox_provider", lambda _config: provider)

    result = await service.regrade(
        "task-a", {SAVED_ARTIFACTS_RUN: "old-run"}, objects=objects, labels={}, sandbox_provider=PROVIDER
    )

    assert isinstance(result, dict) and result["rewards"] == {"reward": 1.0}
    assert await objects.get_bytes(old_key) == b"old verifier output is not an input"
    assert (
        await objects.get_bytes(task_artifact_key("old-run", "task-a", "artifacts/verifier.tar.gz"))
        == recording.getvalue()
    )
    with tarfile.open(fileobj=io.BytesIO(await objects.get_bytes(new_key))) as archive:
        assert cast(io.BufferedReader, archive.extractfile("verifier/details.json")).read() == b"new response"


PROVIDER = DockerProviderConfig(type="docker")
GATEWAY = ScopedGateway("https://gateway.example", "mgwt_grading")
DAYTONA = cast(SandboxProviderConfig, SimpleNamespace(type="daytona"))
MODAL_VM = ModalProviderConfig(runtime="vm", MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")
DOCKERFILE_TOML = "[environment]\ncpus = 1\nworkdir = '/app'"


@pytest.fixture(autouse=True)
def root_images(monkeypatch: pytest.MonkeyPatch) -> None:
    async def image_config(_image: str) -> ImageConfig:
        return ImageConfig(None, None)

    monkeypatch.setattr("tracker.clients.dataset_tasks.image_config", image_config)


def write_task(root: Path, name: str, toml: str = TASK_TOML) -> Path:
    task = root / name
    for relative, text in {
        "task.toml": toml,
        "instruction.md": f"Instruction for {name}.",
        "tests/test.sh": "#!/bin/bash\n",
        "tests/data.txt": "data",
    }.items():
        (task / relative).parent.mkdir(parents=True, exist_ok=True)
        (task / relative).write_text(text)
    return task


class FakeProvider:
    def __init__(self, sandbox: FakeSandbox) -> None:
        self.sandbox = sandbox
        self.requested: list[str] = []

    async def get_sandbox(self, instance_id: str) -> FakeSandbox:
        self.requested.append(instance_id)
        return self.sandbox


def service_for(
    source: TaskSource,
    sandbox: FakeSandbox,
    monkeypatch: pytest.MonkeyPatch,
    read_secret: SecretReader | None = None,
    artifact_sink: ArtifactSink | None = None,
) -> DatasetTaskService:
    service = DatasetTaskService(source, read_secret, artifact_sink)
    provider = FakeProvider(sandbox)

    def get_sandbox_provider(_config: object) -> FakeProvider:
        return provider

    monkeypatch.setattr(service, "get_sandbox_provider", get_sandbox_provider)
    return service


@pytest.fixture
def dataset(tmp_path: Path) -> Path:
    for name in ("task-c", "task-a", "task-b"):
        write_task(tmp_path, name)
    return tmp_path


LOG_DIRS_COMMAND = (
    "mkdir -p /logs/agent /logs/verifier /logs/artifacts && chmod 777 /logs/agent /logs/verifier /logs/artifacts"
    " && chmod 1777 /logs"
)

STEPS_TOML = (
    TASK_TOML.replace("timeout_sec = 600", 'timeout_sec = 600\nuser = "agent"')
    + """
[[steps]]
name = "one"
min_reward = 1.0
artifacts = ["/app/one-out"]
[[steps]]
name = "two"
[steps.verifier]
env = { MODE = "two" }
"""
)


def write_steps_task(root: Path, toml: str = STEPS_TOML) -> Path:
    task = write_task(root, "task-s", toml)
    for relative, text in {
        "steps/one/instruction.md": "First instruction.",
        "steps/one/workdir/data.txt": "data one",
        "steps/one/tests/test.sh": "#!/bin/bash\necho one",
        "steps/one/tests/own.txt": "own",
        "steps/two/instruction.md": "Second instruction.",
    }.items():
        (task / relative).parent.mkdir(parents=True, exist_ok=True)
        (task / relative).write_text(text)
    return root


@pytest.mark.description("Choosing which tasks a run covers.")
class TestVerifyTaskIds:
    @pytest.mark.description("A run covers every task in name order, the listed IDs once in their order, or a slice.")
    @pytest.mark.parametrize(
        ("task_ids", "slice_str", "expected"),
        [
            (None, None, ["task-a", "task-b", "task-c"]),
            (["task-c", "task-a", "task-c"], None, ["task-c", "task-a"]),
            (None, "1:", ["task-b", "task-c"]),
        ],
        ids=["every-task", "listed-ids", "slice"],
    )
    async def test_selection_picks_the_tasks_in_order(
        self, dataset: Path, task_ids: list[str] | None, slice_str: str | None, expected: list[str]
    ) -> None:
        response = await DatasetTaskService(LocalFolderSource(dataset)).verify_task_ids(task_ids, slice_str)

        assert response.task_ids == expected

    @pytest.mark.description("Selections that match nothing are refused.")
    @pytest.mark.parametrize(
        ("task_ids", "slice_str", "complaint"),
        [
            (["task-a", "nope", "gone"], None, "not found in the dataset: nope, gone"),
            (None, "a:b", "Invalid slice"),
            (None, "10:", "selects no tasks"),
        ],
    )
    async def test_selections_that_match_nothing_are_refused(
        self, dataset: Path, task_ids: list[str] | None, slice_str: str | None, complaint: str
    ) -> None:
        with pytest.raises(TaskSelectionError, match=complaint):
            await DatasetTaskService(LocalFolderSource(dataset)).verify_task_ids(task_ids, slice_str)

    @pytest.mark.description("A dataset without tasks is refused.")
    async def test_a_dataset_without_tasks_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(TaskSelectionError, match="no tasks"):
            await DatasetTaskService(LocalFolderSource(tmp_path)).verify_task_ids(None, None)


@pytest.mark.description("The sandbox contract for a task.")
class TestRetrieveTask:
    @pytest.mark.description("A valid task maps to its contract and is remembered.")
    async def test_a_valid_task_maps_to_its_contract_and_is_remembered(self, dataset: Path) -> None:
        service = DatasetTaskService(LocalFolderSource(dataset))

        first = await service.retrieve_task("task-a")
        (dataset / "task-a" / "task.toml").write_text(TASK_TOML.replace("cpus = 2", "cpus = 8"))
        second = await service.retrieve_task("task-a")

        assert first.source == ImageSource(image="ubuntu:24.04")
        assert first.resources == Resources(vcpu=2, memory=4, disk=15)
        assert first.problem_path == "/app/instruction.md"
        assert second is first

    @pytest.mark.description("Environment values are filled from secrets and the secret ones are named.")
    async def test_environment_values_are_filled_from_secrets_and_the_secret_ones_are_named(
        self, dataset: Path
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML + "env = { MODE = 'fast', KEY = '${API_KEY}', REGION = '${REGION:-us}' }"
        )

        async def read_secret(name: str) -> str | None:
            return {"API_KEY": "s3cret"}.get(name)

        response = await DatasetTaskService(LocalFolderSource(dataset), read_secret).retrieve_task("task-a")

        assert isinstance(response, TaskFolderResponse)
        assert response.env == {"MODE": "fast", "KEY": "s3cret", "REGION": "us"}
        assert response.secret_env == ["KEY"]

    @pytest.mark.description("A secret the run cannot read stops the task before any sandbox.")
    @pytest.mark.parametrize(
        "settings",
        ["env = { KEY = '${API_KEY}' }", "\n[verifier.env]\nKEY = '${API_KEY}'"],
        ids=["environment", "verifier"],
    )
    async def test_a_secret_the_run_cannot_read_stops_the_task_before_any_sandbox(
        self, dataset: Path, settings: str
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(TASK_TOML + settings)

        with pytest.raises(SecretsError, match="API_KEY"):
            await DatasetTaskService(LocalFolderSource(dataset)).retrieve_task("task-a")

    @pytest.mark.description("An invalid task is refused with its problems.")
    async def test_an_invalid_task_is_refused_with_its_problems(self, dataset: Path) -> None:
        (dataset / "task-a" / "tests" / "test.sh").unlink()

        with pytest.raises(InvalidTaskError, match="Task 'task-a' is not valid: tests/test.sh is missing"):
            await DatasetTaskService(LocalFolderSource(dataset)).retrieve_task("task-a")


@pytest.mark.description("A task that names no working directory runs in the one its image declares.")
class TestImageWorkdir:
    @pytest.mark.description("The instruction lands in the working directory.")
    @pytest.mark.parametrize(
        ("toml", "process_cwd", "expected_cwd"),
        [
            pytest.param("[environment]\ndocker_image = 'golang:1.22'", "/go", "/go", id="image-workdir"),
            pytest.param("[environment]\ndocker_image = 'python:3.12-slim'", "/root", "/workspace", id="no-workdir"),
            pytest.param("[environment]\ndocker_image = 'rooted:1'", "/root", "/root", id="registry-confirms-root"),
            pytest.param(TASK_TOML, "/go", "/app", id="task-workdir-wins"),
            pytest.param("[environment]\ndocker_image = 'golang:1.22'", "/", "/go", id="registry-workdir"),
            pytest.param("[environment]\ndocker_image = 'python:3.12-slim'", "/", "/workspace", id="registry-none"),
            pytest.param("[environment]\ncpus = 1", "/", "/go", id="dockerfile-base-from-registry"),
            pytest.param("[environment]\nsnapshot = 'env-build'", "/app", "/app", id="snapshot-workdir"),
            pytest.param("[environment]\nsnapshot = 'env-build'", "/", "/workspace", id="snapshot-without-workdir"),
        ],
    )
    async def test_the_instruction_lands_in_the_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, toml: str, process_cwd: str, expected_cwd: str
    ) -> None:
        task = write_task(tmp_path, "task-a", toml)
        if "docker_image" not in toml and "snapshot" not in toml and toml != TASK_TOML:
            (task / "environment").mkdir()
            (task / "environment" / "Dockerfile").write_text("FROM golang:1.22\nRUN true\n")

        async def registry(image: str) -> ImageConfig:
            return ImageConfig(None, {"golang:1.22": "/go", "rooted:1": "/root"}.get(image))

        monkeypatch.setattr("tracker.clients.dataset_tasks.image_config", registry)
        sandbox = FakeSandbox(process_cwd=process_cwd)
        service = service_for(LocalFolderSource(tmp_path), sandbox, monkeypatch)

        response = await service.use_image_workdir("task-a", "sandbox-1", PROVIDER)
        await service.setup_task("task-a", "sandbox-1", PROVIDER)

        assert (response.cwd, response.problem_path) == (expected_cwd, f"{expected_cwd}/instruction.md")
        assert list(sandbox.files) == [f"{expected_cwd}/instruction.md"]
        assert (await service.retrieve_task("task-a")).cwd == expected_cwd


@pytest.mark.description("A working directory Valkyrie creates belongs to the agent's user, who can then write to it.")
class TestWorkingDirectoryOwner:
    @pytest.mark.description("Setup makes the working directory for the agents user.")
    async def test_setup_makes_the_working_directory_for_the_agents_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_task(tmp_path, "task-a", TASK_TOML.replace("timeout_sec = 600", 'timeout_sec = 600\nuser = "agent"'))
        sandbox = FakeSandbox()

        await service_for(LocalFolderSource(tmp_path), sandbox, monkeypatch).setup_task("task-a", "sandbox-1", PROVIDER)

        assert "[ -d /app ] || { mkdir -p /app && chown agent /app; }" in sandbox.commands

    @pytest.mark.description("A non-root image `USER` gets a root sandbox, and the agent runs as that user.")
    @pytest.mark.parametrize(
        ("agent_user", "dockerfile", "image_user", "expected"),
        [
            pytest.param(None, None, "agent", "agent", id="image-user"),
            pytest.param("runner", None, "agent", "runner", id="agent-user-wins"),
            pytest.param(
                None,
                "FROM ubuntu:24.04\nUSER nobody:nogroup\n",
                "nobody:nogroup",
                "nobody:nogroup",
                id="dockerfile-user",
            ),
            pytest.param(None, "FROM ubuntu:24.04\nRUN true\n", "agent", "agent", id="dockerfile-base-user"),
        ],
    )
    async def test_non_root_image_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        agent_user: str | None,
        dockerfile: str | None,
        image_user: str,
        expected: str,
    ) -> None:
        toml = TASK_TOML
        if agent_user is not None:
            toml = toml.replace("timeout_sec = 600", f'timeout_sec = 600\nuser = "{agent_user}"')
        if dockerfile is not None:
            toml = toml.replace('docker_image = "ubuntu:24.04"\n', "")
        task = write_task(tmp_path, "task-a", toml)
        if dockerfile is not None:
            (task / "environment").mkdir()
            (task / "environment" / "Dockerfile").write_text(dockerfile)

        async def registry(image: str) -> ImageConfig:
            return ImageConfig({"ubuntu:24.04": "agent"}.get(image), None)

        monkeypatch.setattr("tracker.clients.dataset_tasks.image_config", registry)
        service = service_for(LocalFolderSource(tmp_path), FakeSandbox(), monkeypatch)

        try:
            response = cast(TaskFolderResponse, await service.retrieve_task("task-a"))
            dockerfile = Path(cast(DockerfileSource, response.source).dockerfile).read_text()
        finally:
            await service.close()

        assert dockerfile.endswith("USER root\n")
        assert (response.image_user, response.agent_user) == (image_user, expected)

    @pytest.mark.description(
        "An image whose user can't be read or checked runs as its own default user, and only a failed read is retried."
    )
    @pytest.mark.parametrize(
        ("unknown", "later_user"),
        [(None, "agent"), (ImageConfig("two words", None), None)],
        ids=["unreadable", "invalid"],
    )
    async def test_an_unknown_image_user_keeps_the_image_and_only_a_failed_read_is_retried(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unknown: ImageConfig | None, later_user: str | None
    ) -> None:
        write_task(tmp_path, "task-a")
        write_task(tmp_path, "task-b")
        answers = iter([unknown, ImageConfig("agent", None)])

        async def registry(_image: str) -> ImageConfig | None:
            return next(answers)

        monkeypatch.setattr("tracker.clients.dataset_tasks.image_config", registry)
        service = service_for(LocalFolderSource(tmp_path), FakeSandbox(), monkeypatch)

        try:
            first = await service.retrieve_task("task-a")
            second = await service.retrieve_task("task-b")
        finally:
            await service.close()

        assert isinstance(first, TaskFolderResponse)
        assert (first.source, first.image_user, first.agent_user) == (ImageSource(image="ubuntu:24.04"), None, None)
        assert isinstance(second, TaskFolderResponse)
        assert (second.image_user, second.agent_user) == (later_user, later_user)

    @pytest.mark.description("Tasks that share an image read its config once, even at the same time.")
    async def test_tasks_that_share_an_image_read_its_config_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_task(tmp_path, "task-a")
        write_task(tmp_path, "task-b")
        reads: list[str] = []

        async def registry(image: str) -> ImageConfig:
            reads.append(image)
            await asyncio.sleep(0)
            return ImageConfig("agent", None)

        monkeypatch.setattr("tracker.clients.dataset_tasks.image_config", registry)
        service = service_for(LocalFolderSource(tmp_path), FakeSandbox(), monkeypatch)

        try:
            responses = await asyncio.gather(service.retrieve_task("task-a"), service.retrieve_task("task-b"))
        finally:
            await service.close()

        assert reads == ["ubuntu:24.04"]
        assert [cast(TaskFolderResponse, response).image_user for response in responses] == ["agent", "agent"]


@pytest.mark.description("Putting the instruction in the sandbox.")
class TestSetupTask:
    @pytest.mark.description("Compose verifiers are validated before agent setup with their own sandbox resources.")
    @pytest.mark.parametrize("stepped", [False, True])
    @pytest.mark.parametrize(
        "config",
        [PROVIDER, ModalProviderConfig(runtime="gvisor", MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret"), MODAL_VM],
    )
    async def test_compose_verifier_preflight(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: SandboxProviderConfig, stepped: bool
    ) -> None:
        if stepped:
            task = (
                write_steps_task(tmp_path, STEPS_TOML + "\n[steps.verifier.environment]\ncpus = 3\nmemory_mb = 2048")
                / "task-s"
            )
            task_id = "task-s"
            tests = task / "steps" / "two" / "tests"
        else:
            toml = TASK_TOML.replace("[verifier]", "[verifier]\nenvironment_mode = 'separate'")
            task = write_task(tmp_path, "task-a", toml + "\n[verifier.environment]\ncpus = 3\nmemory_mb = 2048")
            task_id = "task-a"
            tests = task / "tests"
        tests.mkdir(exist_ok=True)
        (tests / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        sandbox = FakeSandbox()
        reach_sandbox = AsyncMock(return_value=sandbox)

        async with config.create_provider() as provider, DatasetTaskService(LocalFolderSource(tmp_path)) as service:
            validate = Mock(wraps=provider.validate_sandbox_config)
            create = AsyncMock()
            monkeypatch.setattr(provider, "validate_sandbox_config", validate)
            monkeypatch.setattr(provider, "create_sandbox", create)
            monkeypatch.setattr(service, "get_sandbox_provider", lambda _config: provider)
            monkeypatch.setattr(service, "_sandbox", reach_sandbox)

            if config == MODAL_VM:
                await service.setup_task(task_id, "agent-sandbox", config)
                reach_sandbox.assert_awaited_once()
            else:
                with pytest.raises(UnsupportedSandboxConfigError, match="Docker-in-Docker"):
                    await service.setup_task(task_id, "agent-sandbox", config)
                reach_sandbox.assert_not_awaited()
                assert sandbox.commands == []

            validate.assert_called_once()
            assert validate.call_args.kwargs["requires_nested_docker"] is True
            resources = validate.call_args.kwargs["resources"]
            assert resources.vcpu == 3
            assert resources.memory == 2
            create.assert_not_awaited()

    @pytest.mark.description("The instruction is uploaded to the problem path.")
    @pytest.mark.parametrize("authorization", [None, RemoteGradingAuthorization(issuer="dev", token="test-grant")])
    async def test_the_instruction_is_uploaded_to_the_problem_path(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch, authorization: RemoteGradingAuthorization | None
    ) -> None:
        sandbox = FakeSandbox()
        service = service_for(LocalFolderSource(dataset), sandbox, monkeypatch)

        response = await service.setup_task(
            "task-b",
            "sandbox-1",
            sandbox_provider=PROVIDER,
            on_message=lambda _message: None,
            dataset=None,
            child_egress={},
            remote_grading_authorization=authorization,
        )

        assert response.status == "ok"
        assert sandbox.files == {"/app/instruction.md": b"Instruction for task-b."}
        assert sandbox.commands == [LOG_DIRS_COMMAND, "mkdir -p /app"]

    @pytest.mark.description("Leading canary lines are not shown to the agent.")
    async def test_leading_canary_lines_are_not_shown_to_the_agent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        task = write_task(tmp_path, "task-a")
        (task / "instruction.md").write_text(
            "<!-- harbor-canary GUID 26b5c67b -->\n# canary GUID 26b5c67b\n\nSolve it.\n<!-- not a canary -->\n"
        )
        sandbox = FakeSandbox()
        service = service_for(LocalFolderSource(tmp_path), sandbox, monkeypatch)

        await service.setup_task("task-a", "sandbox-1", PROVIDER)

        assert sandbox.files == {"/app/instruction.md": b"Solve it.\n<!-- not a canary -->\n"}

    @pytest.mark.description("The healthcheck passes before the instruction is uploaded.")
    async def test_the_healthcheck_passes_before_the_instruction_is_uploaded(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(TASK_TOML + "\n[environment.healthcheck]\ncommand = 'ready?'")
        sandbox = FakeSandbox()
        service = service_for(LocalFolderSource(dataset), sandbox, monkeypatch)

        await service.setup_task("task-a", "sandbox-1", PROVIDER)

        assert sandbox.commands == [LOG_DIRS_COMMAND, "ready?", "mkdir -p /app"]

    @pytest.mark.description("A compose task starts its services before the instruction reaches main.")
    @pytest.mark.parametrize("provider", [DAYTONA, MODAL_VM])
    async def test_a_compose_task_starts_its_services_before_the_instruction_reaches_main(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch, provider: SandboxProviderConfig
    ) -> None:
        (dataset / "task-a" / "environment").mkdir()
        (dataset / "task-a" / "environment" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        outer = OuterSandbox()
        service = service_for(LocalFolderSource(dataset), outer, monkeypatch)

        await service.setup_task("task-a", "sandbox-1", provider)

        started = next(index for index, command in enumerate(outer.commands) if command.endswith("exec -T main true"))
        assert any("exec" in command and LOG_DIRS_COMMAND in command for command in outer.commands[started:])
        uploaded = next(
            index for index, command in enumerate(outer.commands) if "mkdir -p /app" in command and "exec" in command
        )
        assert started < uploaded
        assert b"Instruction for task-a." in outer.files.values()

    @pytest.mark.description("A stray compose file in the tests of a shared verifier asks nothing of the provider.")
    async def test_a_stray_compose_file_in_the_tests_of_a_shared_verifier_asks_nothing_of_the_provider(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "tests" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        outer = OuterSandbox()

        response = await service_for(LocalFolderSource(dataset), outer, monkeypatch).setup_task(
            "task-a", "sandbox-1", PROVIDER
        )

        assert response.status == "ok"

    @pytest.mark.description("A sandbox provider is required.")
    async def test_a_sandbox_provider_is_required(self, dataset: Path) -> None:
        with pytest.raises(ValueError, match="sandbox provider"):
            await DatasetTaskService(LocalFolderSource(dataset)).setup_task("task-a", "sandbox-1")


@pytest.mark.description("A task with steps is prepared and graded one step at a time.")
class TestSteps:
    @pytest.mark.description("The task response lists each steps plan and carries the reward strategy.")
    async def test_the_task_response_lists_each_steps_plan_and_carries_the_reward_strategy(
        self, tmp_path: Path
    ) -> None:
        service = DatasetTaskService(
            LocalFolderSource(write_steps_task(tmp_path, "multi_step_reward_strategy = 'final'\n" + STEPS_TOML))
        )

        response = await service.retrieve_task("task-s")

        assert isinstance(response, TaskFolderResponse)
        assert [(step.name, step.agent_user) for step in response.steps] == [("one", "agent"), ("two", "agent")]
        assert response.reward_strategy == MultiStepRewardStrategy.FINAL

    @pytest.mark.description("Setup leaves the instruction to each step.")
    async def test_setup_leaves_the_instruction_to_each_step(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox()

        await service_for(LocalFolderSource(write_steps_task(tmp_path)), sandbox, monkeypatch).setup_task(
            "task-s", "sandbox-1", PROVIDER
        )

        assert sandbox.files == {}

    @pytest.mark.description("Preparing a step brings its files and its own instruction.")
    async def test_preparing_a_step_brings_its_files_and_its_own_instruction(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox()
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), sandbox, monkeypatch)

        await service.prepare_step("task-s", "sandbox-1", "one", PROVIDER)
        await service.prepare_step("task-s", "sandbox-1", "two", PROVIDER)

        assert "[ -d /app ] || { mkdir -p /app && chown agent /app; }" in sandbox.commands
        assert sandbox.files["/app/data.txt"] == b"data one"
        assert sandbox.files["/app/instruction.md"] == b"Second instruction."
        assert sandbox.commands.count(RESET_STEP_LOG_DIRS) == 2
        assert sandbox.commands[0] == RESET_STEP_LOG_DIRS

    @pytest.mark.description("A step is graded with the tasks tests under its own and its artifacts kept apart.")
    async def test_a_step_is_graded_with_the_tasks_tests_under_its_own_and_its_artifacts_kept_apart(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(artifacts.uuid, "uuid4", lambda: UUID(int=1))
        staging = f"{STAGING_ROOT}/valkyrie-artifacts-{UUID(int=1).hex}"
        sandbox = FakeSandbox(
            files={f"{staging}/0.tar.gz": b"archive", f"{staging}/1.tar.gz": b"logs"},
            writes={REWARD_TEXT_PATH: b"1"},
            artifacts_present=True,
        )
        uploaded: list[tuple[str, str]] = []

        async def sink(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
            uploaded.append((task_id, name))
            _ = [chunk async for chunk in chunks]

        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), sandbox, monkeypatch, None, sink)

        grade = await service.grade_step("task-s", "sandbox-1", "one", PROVIDER)

        assert (grade.name, grade.rewards, grade.stopped) == ("one", {"reward": 1.0}, None)
        assert grade.metadata is not None and grade.metadata["reward_source"] == REWARD_TEXT_PATH
        assert sandbox.files["/tests/data.txt"] == b"data"
        assert sandbox.files["/tests/own.txt"] == b"own"
        assert sandbox.files["/tests/test.sh"] == b"#!/bin/bash\necho one"
        assert sandbox.commands.index("rm -rf /tests") < sandbox.commands.index("chmod +x /tests/test.sh")
        assert uploaded == [
            ("task-s", "steps/one/artifacts/app/one-out.tar.gz"),
            ("task-s", "steps/one/artifacts/logs/artifacts.tar.gz"),
            ("task-s", "steps/one/verifier_output.tar.gz"),
        ]

    @pytest.mark.description("A step below its minimum reward says why it stopped.")
    async def test_a_step_below_its_minimum_reward_says_why_it_stopped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"0.5"})
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), sandbox, monkeypatch)

        grade = await service.grade_step("task-s", "sandbox-1", "one", PROVIDER)

        assert grade.stopped == "reward is 0.5, below the minimum 1"

    @pytest.mark.description("A step whose verifier leaves no reward comes back with the problem.")
    async def test_a_step_whose_verifier_leaves_no_reward_comes_back_with_the_problem(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), FakeSandbox(), monkeypatch)

        grade = await service.grade_step("task-s", "sandbox-1", "one", PROVIDER)

        assert (grade.name, grade.rewards, grade.problem) == ("one", None, "The verifier wrote no reward file")

    @pytest.mark.description("A steps grader environment is its own.")
    async def test_a_steps_grader_environment_is_its_own(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), sandbox, monkeypatch)

        await service.grade_step("task-s", "sandbox-1", "two", PROVIDER)

        assert any("MODE=two" in command for command in sandbox.commands)

    @pytest.mark.description("A step whose verifier lists models gets the grading token.")
    async def test_a_step_whose_verifier_lists_models_gets_the_grading_token(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml = STEPS_TOML.replace('env = { MODE = "two" }', "models = ['openai/gpt-4o']")
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        service = service_for(LocalFolderSource(write_steps_task(tmp_path, toml)), sandbox, monkeypatch)

        await service.grade_step("task-s", "sandbox-1", "two", PROVIDER, grading_gateway=GATEWAY)

        assert await service.grading_requirements("task-s") == ["openai/gpt-4o"]
        assert any("MODEL_GATEWAY_API_KEY=mgwt_grading" in command for command in sandbox.commands)

    @pytest.mark.description("A task with steps cannot be graded as one.")
    async def test_a_task_with_steps_cannot_be_graded_as_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), FakeSandbox(), monkeypatch)

        with pytest.raises(InvalidTaskError, match="graded one at a time"):
            await service.evaluate_instance("task-s", "sandbox-1", PROVIDER)

    @pytest.mark.description("A step the task does not have is refused.")
    async def test_a_step_the_task_does_not_have_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service = service_for(LocalFolderSource(write_steps_task(tmp_path)), FakeSandbox(), monkeypatch)

        with pytest.raises(InvalidTaskError, match="no step named 'three'"):
            await service.prepare_step("task-s", "sandbox-1", "three", PROVIDER)


@pytest.mark.description("The oracle runs a step's own solution, or else the task's, with [solution].env resolved.")
class TestOracleSolution:
    @pytest.mark.description("Each step gets its own solution or else the tasks.")
    async def test_each_step_gets_its_own_solution_or_else_the_tasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = write_steps_task(tmp_path, STEPS_TOML + "[solution.env]\nMODE = 'oracle'\nKEY = '${ORACLE_KEY}'\n")
        for relative, text in {"solution/solve.sh": "echo task", "steps/two/solution/solve.sh": "echo two"}.items():
            (root / "task-s" / relative).parent.mkdir(parents=True, exist_ok=True)
            (root / "task-s" / relative).write_text(text)
        sandbox = FakeSandbox()

        async def read_secret(name: str) -> str | None:
            return "s3cret" if name == "ORACLE_KEY" else None

        service = service_for(LocalFolderSource(root), sandbox, monkeypatch, read_secret)

        resolved = await service.upload_solution("task-s", "sandbox-1", "one", PROVIDER)

        assert sandbox.files["/solution/solve.sh"] == b"echo task"
        assert resolved == ({"MODE": "oracle", "KEY": "s3cret"}, ["s3cret"])

        await service.upload_solution("task-s", "sandbox-1", "two", PROVIDER)

        assert sandbox.files["/solution/solve.sh"] == b"echo two"

    @pytest.mark.description("A task without a solve script is refused.")
    async def test_a_task_without_a_solve_script_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_task(tmp_path, "task-a")
        service = service_for(LocalFolderSource(tmp_path), FakeSandbox(), monkeypatch)

        with pytest.raises(InvalidTaskError, match="'task-a' has no solution/solve.sh"):
            await service.upload_solution("task-a", "sandbox-1", None, PROVIDER)


@pytest.mark.description("Grading in the agent's sandbox.")
class TestEvaluateInstance:
    @pytest.mark.description("A dataset task's grading models are the ones its verifier lists, with no remote grader.")
    @pytest.mark.parametrize("listed", [[], ["openai/gpt-4o", "anthropic/claude-haiku-4-5"]])
    async def test_grading_requirements(self, dataset: Path, listed: list[str]) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML.replace("[verifier]\n", f"[verifier]\nmodels = {listed!r}\n")
        )

        async with DatasetTaskService(LocalFolderSource(dataset)) as service:
            requirements = await service.grading_requirements_response("task-a", dataset="dataset")
            models = await service.grading_requirements("task-a", dataset="dataset")

        assert requirements.models == models == listed
        assert requirements.remote_grader_service is None

    @pytest.mark.description("The task is graded and its result returned.")
    async def test_the_task_is_graded_and_its_result_returned(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        messages: list[str] = []

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
            "task-a",
            "sandbox-1",
            sandbox_provider=PROVIDER,
            on_message=messages.append,
            on_sandbox_created=lambda _sandbox: None,
        )

        assert isinstance(result, dict)
        assert result["rewards"] == {"reward": 1.0}
        assert sandbox.files["/tests/data.txt"] == b"data"
        assert messages[0] == "Running the verifier for task-a"

    @pytest.mark.description(
        "A verifier that lists models gets the run's Gateway and grading token, masked, and only the Gateway host."
    )
    async def test_a_verifier_that_lists_models_gets_the_gateway_and_the_grading_token(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        applied: list[tuple[str, object]] = []

        async def apply(sandbox: FakeSandbox, policy: object) -> None:
            applied.append((sandbox.id, policy))

        monkeypatch.setattr(dataset_tasks, "apply_egress_policy", apply)
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML.replace("[verifier]\n", "[verifier]\nnetwork_mode = 'no-network'\nmodels = ['openai/gpt-4o']\n")
        )
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1", VERIFIER_OUTPUT_PATH: b"judging with mgwt_grading"})

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER, grading_gateway=GATEWAY
        )

        assert any(
            "MODEL_GATEWAY_URL=https://gateway.example" in command and "MODEL_GATEWAY_API_KEY=mgwt_grading" in command
            for command in sandbox.commands
        )
        assert applied == [("sandbox-1", ["gateway.example"]), ("sandbox-1", [])]
        assert isinstance(result, dict)
        assert result["metadata"]["verifier_output"] == "judging with [REDACTED]"

    @pytest.mark.description("The Gateway host is removed again when grading fails.")
    async def test_the_gateway_host_is_removed_again_when_grading_fails(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        applied: list[object] = []

        async def apply(_sandbox: FakeSandbox, policy: object) -> None:
            applied.append(policy)

        monkeypatch.setattr(dataset_tasks, "apply_egress_policy", apply)
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML.replace("[verifier]\n", "[verifier]\nnetwork_mode = 'no-network'\nmodels = ['openai/gpt-4o']\n")
        )

        with pytest.raises(VerifierResultError):
            await service_for(LocalFolderSource(dataset), FakeSandbox(), monkeypatch).evaluate_instance(
                "task-a", "sandbox-1", PROVIDER, grading_gateway=GATEWAY
            )

        assert applied == [["gateway.example"], []]

    @pytest.mark.description("A verifier that lists models is not run when the run has no grading token.")
    async def test_a_verifier_that_lists_models_is_not_run_without_a_grading_token(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML.replace("[verifier]\n", "[verifier]\nmodels = ['openai/gpt-4o']\n")
        )
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        with pytest.raises(InvalidSandboxConfigurationError, match="no Model Gateway grading token"):
            await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
                "task-a", "sandbox-1", PROVIDER
            )

        assert sandbox.commands == []

    @pytest.mark.description("A separate verifier is handed the artifacts convention folder once when it holds files.")
    @pytest.mark.parametrize(
        ("present", "declared", "expected"),
        [
            (True, "", ["/logs/artifacts"]),
            (False, "", []),
            (True, "artifacts = [{ source = '/logs/artifacts', destination = 'out' }]\n", ["/logs/artifacts"]),
        ],
        ids=["holds files", "empty", "already declared"],
    )
    async def test_a_separate_verifier_is_handed_the_artifacts_convention_folder_once_when_it_holds_files(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch, present: bool, declared: str, expected: list[str]
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            declared + TASK_TOML.replace("[verifier]\n", "[verifier]\nenvironment_mode = 'separate'\n")
        )
        carried: list[str] = []

        async def grade_separately(*args: Any, **_kwargs: Any) -> dict[str, Any]:
            carried.extend(artifact.source for artifact in args[9])
            return {"rewards": {"reward": 1.0}}

        monkeypatch.setattr("tracker.clients.dataset_tasks.grade_separately", grade_separately)

        await service_for(
            LocalFolderSource(dataset), FakeSandbox(artifacts_present=present), monkeypatch
        ).evaluate_instance("task-a", "sandbox-1", PROVIDER)

        assert carried == expected

    @pytest.mark.description("The grader runs in the environments workdir.")
    async def test_the_grader_runs_in_the_environments_workdir(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER
        )

        assert sandbox.cwds == ["/app"]
        assert "mkdir -p /logs/verifier /app" in sandbox.commands

    @pytest.mark.description("Without `[environment].workdir`, the grader runs in the Dockerfile's `WORKDIR`.")
    async def test_the_grader_runs_in_the_dockerfile_workdir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        task = write_task(tmp_path, "task-w", "[environment]\ncpus = 1")
        (task / "environment").mkdir()
        (task / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /root\n")
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        await service_for(LocalFolderSource(tmp_path), sandbox, monkeypatch).evaluate_instance(
            "task-w", "sandbox-1", PROVIDER
        )

        assert sandbox.cwds == ["/root"]

    @pytest.mark.description("When nothing names a working directory, the grader runs where the sandbox starts it.")
    async def test_without_a_named_directory_the_grader_runs_where_the_sandbox_starts_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        write_task(tmp_path, "task-a", "[environment]\ndocker_image = 'python:3.12-slim'")
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        service = service_for(LocalFolderSource(tmp_path), sandbox, monkeypatch)

        await service.use_image_workdir("task-a", "sandbox-1", PROVIDER)
        await service.evaluate_instance("task-a", "sandbox-1", PROVIDER)

        assert sandbox.cwds == [None]

    @pytest.mark.description("The grader environment is filled from secrets.")
    async def test_the_grader_environment_is_filled_from_secrets(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(TASK_TOML + "\n[verifier.env]\nJUDGE_KEY = '${OPENAI_API_KEY}'")
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        async def read_secret(name: str) -> str | None:
            return {"OPENAI_API_KEY": "sk-judge"}.get(name)

        await service_for(LocalFolderSource(dataset), sandbox, monkeypatch, read_secret).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER
        )

        assert any("JUDGE_KEY=sk-judge" in command for command in sandbox.commands)

    @pytest.mark.description("A secret the grader prints is masked in what is saved.")
    async def test_a_secret_the_grader_prints_is_masked_in_what_is_saved(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(TASK_TOML + "\n[verifier.env]\nJUDGE_KEY = '${OPENAI_API_KEY}'")
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1", VERIFIER_OUTPUT_PATH: b"calling with sk-judge now"})

        async def read_secret(name: str) -> str | None:
            return {"OPENAI_API_KEY": "sk-judge"}.get(name)

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch, read_secret).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER
        )

        assert isinstance(result, dict)
        assert result["metadata"]["verifier_output"] == "calling with [REDACTED] now"

    @pytest.mark.description("Hooks run and artifacts are uploaded before the grader.")
    async def test_hooks_run_and_artifacts_are_uploaded_before_the_grader(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            "artifacts = ['/app/out']\n" + TASK_TOML + "\n[[verifier.collect]]\ncommand = 'snapshot'"
        )
        monkeypatch.setattr(artifacts.uuid, "uuid4", lambda: UUID(int=1))
        staging = f"{STAGING_ROOT}/valkyrie-artifacts-{UUID(int=1).hex}"
        sandbox = FakeSandbox(files={f"{staging}/0.tar.gz": b"archive"}, writes={REWARD_TEXT_PATH: b"1"})
        uploaded: list[tuple[str, str, bytes]] = []

        async def sink(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
            uploaded.append((task_id, name, b"".join([chunk async for chunk in chunks])))

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch, None, sink).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER
        )

        assert isinstance(result, dict)
        assert result["rewards"] == {"reward": 1.0}
        assert uploaded[0] == ("task-a", "artifacts/app/out.tar.gz", b"archive")
        assert uploaded[1][:2] == ("task-a", "verifier_output.tar.gz")
        assert sandbox.commands[0] == "snapshot"
        assert sandbox.commands.index(f"rm -rf {staging}") < sandbox.commands.index("chmod +x /tests/test.sh")

    @pytest.mark.description("A hook on a sidecar runs in that service of a compose task.")
    async def test_a_hook_on_a_sidecar_runs_in_that_service_of_a_compose_task(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (dataset / "task-a" / "environment").mkdir()
        (dataset / "task-a" / "environment" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML + "\n[[verifier.collect]]\ncommand = 'dump'\nservice = 'db'"
        )
        outer = OuterSandbox()
        outer.writes[REWARD_TEXT_PATH] = b"1"

        await service_for(LocalFolderSource(dataset), outer, monkeypatch).evaluate_instance(
            "task-a", "sandbox-1", DAYTONA
        )

        assert any(" exec " in command and " db sh -lc dump" in command for command in outer.commands)

    @pytest.mark.description("An invalid task is not graded.")
    async def test_an_invalid_task_is_not_graded(self, dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (dataset / "task-a" / "task.toml").write_text("[environment")
        sandbox = FakeSandbox()

        with pytest.raises(InvalidTaskError, match="not valid TOML"):
            await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
                "task-a", "sandbox-1", PROVIDER
            )

        assert sandbox.files == {}


SEPARATE_TOML = "artifacts = ['/app/out']\n" + TASK_TOML.replace(
    "[verifier]\n", "[verifier]\nenvironment_mode = 'separate'\n"
)
SAVED_RUN = "11111111-1111-1111-1111-111111111111"


@pytest.mark.description("A regrade grades a dataset task again from the artifacts its source run saved.")
class TestRegrade:
    @pytest.mark.description("Regrade skips agent environment credentials while retaining verifier credentials.")
    async def test_only_verifier_environment_is_resolved(self, dataset: Path) -> None:
        (dataset / "task-a" / "task.toml").write_text(
            SEPARATE_TOML + "env = { AGENT_KEY = '${deleted-agent-key}' }\n" + "[verifier.env]\nKEY = '${GRADER_KEY}'\n"
        )

        async def read_secret(name: str) -> str | None:
            assert name == "GRADER_KEY"
            return "grading-value"

        service = DatasetTaskService(LocalFolderSource(dataset), read_secret, regrade_only=True)
        response = await service.retrieve_task("task-a")

        assert isinstance(response, TaskFolderResponse)
        assert response.env == {} and response.secret_env == []
        assert await service.regrade_requirements(["task-a"]) == []
        await service.close()

    @pytest.fixture
    def store(self, tmp_path: Path) -> FilesystemObjectStore:
        return FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")

    @pytest.fixture
    def graded(self, monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
        calls: list[tuple[Any, ...]] = []

        async def grade_separately(*args: Any, **_kwargs: Any) -> dict[str, Any]:
            calls.append(args)
            return {"rewards": {"reward": 1.0}}

        monkeypatch.setattr("tracker.clients.dataset_tasks.grade_separately", grade_separately)
        return calls

    @pytest.mark.description(
        "A saved artifact reaches a fresh separate verifier with the new run's labels, over an older absent marker."
    )
    async def test_a_saved_artifact_reaches_a_fresh_separate_verifier(
        self,
        dataset: Path,
        tmp_path: Path,
        store: FilesystemObjectStore,
        graded: list[tuple[Any, ...]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(SEPARATE_TOML)
        key = task_artifact_key(SAVED_RUN, "task-a", "artifacts/app/out")
        await store.put_bytes(f"{key}.absent", b"")
        os.utime(tmp_path / "objects" / f"{key}.absent", (0, 0))
        await store.put_bytes(f"{key}.tar.gz", b"archive")
        service = service_for(LocalFolderSource(dataset), FakeSandbox(), monkeypatch)

        result = await service.regrade(
            "task-a",
            {SAVED_ARTIFACTS_RUN: SAVED_RUN},
            objects=store,
            labels={"Id": "new-run"},
            sandbox_provider=PROVIDER,
        )

        (call,) = graded
        labels, staged, skipped = call[5], call[9], call[10]
        assert result == {"rewards": {"reward": 1.0}}
        assert labels == {"Id": "new-run"}
        assert [artifact.source for artifact in staged] == ["/app/out"]
        assert b"".join([chunk async for chunk in staged[0].read()]) == b"archive"
        assert skipped == []

    @pytest.mark.description(
        "An artifact the agent never wrote is regraded as absent over an older archive, and one never saved is refused."
    )
    async def test_an_unwritten_artifact_is_absent_and_an_unsaved_one_is_refused(
        self,
        dataset: Path,
        tmp_path: Path,
        store: FilesystemObjectStore,
        graded: list[tuple[Any, ...]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(SEPARATE_TOML)
        write_task(dataset, "task-b", SEPARATE_TOML)
        key = task_artifact_key(SAVED_RUN, "task-a", "artifacts/app/out")
        await store.put_bytes(f"{key}.tar.gz", b"archive")
        os.utime(tmp_path / "objects" / f"{key}.tar.gz", (0, 0))
        await store.put_bytes(f"{key}.absent", b"")
        service = service_for(LocalFolderSource(dataset), FakeSandbox(), monkeypatch)

        await service.regrade(
            "task-a", {SAVED_ARTIFACTS_RUN: SAVED_RUN}, objects=store, labels={}, sandbox_provider=PROVIDER
        )

        (call,) = graded
        assert call[9] == []
        assert [(artifact.source, artifact.absent) for artifact in call[10]] == [("/app/out", True)]

        with pytest.raises(MissingSavedArtifactError, match="saved no artifact /app/out for task task-b"):
            await service.regrade(
                "task-b", {SAVED_ARTIFACTS_RUN: SAVED_RUN}, objects=store, labels={}, sandbox_provider=PROVIDER
            )

    @pytest.mark.description("A task that grades in the agent's sandbox or has steps is not regraded.")
    @pytest.mark.parametrize(
        ("toml", "reason"),
        [
            (TASK_TOML, "grades in the agent's sandbox"),
            (SEPARATE_TOML + "\n[[steps]]\nname = 'one'\n", "has steps"),
        ],
        ids=["shared verifier", "steps"],
    )
    async def test_a_task_that_cannot_be_regraded_is_refused(
        self,
        dataset: Path,
        store: FilesystemObjectStore,
        graded: list[tuple[Any, ...]],
        monkeypatch: pytest.MonkeyPatch,
        toml: str,
        reason: str,
    ) -> None:
        (dataset / "task-a" / "task.toml").write_text(toml)
        (dataset / "task-a" / "steps" / "one").mkdir(parents=True)
        (dataset / "task-a" / "steps" / "one" / "instruction.md").write_text("Step one.")
        service = service_for(LocalFolderSource(dataset), FakeSandbox(), monkeypatch)

        with pytest.raises(InvalidTaskError, match=reason):
            await service.regrade(
                "task-a", {SAVED_ARTIFACTS_RUN: SAVED_RUN}, objects=store, labels={}, sandbox_provider=PROVIDER
            )

        assert graded == []


@pytest.mark.description("A hosted dataset: the same answers from task archives in storage.")
class TestFromAnArchive:
    @pytest.mark.description("A task in storage is selected retrieved and graded.")
    async def test_a_task_in_storage_is_selected_retrieved_and_graded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        dataset_id = uuid4()
        archive = zip_bytes(
            {
                "task.toml": TASK_TOML,
                "instruction.md": "From storage.",
                "tests/test.sh": "#!/bin/bash\n",
                "tests/data.txt": "data",
                "environment/data/huge.bin": "x" * 1000,
            }
        )
        digest = await store_archive(store, dataset_id, archive)
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        service = service_for(ArchiveSource(store, dataset_id, {"stored-task": digest}), sandbox, monkeypatch)

        selected = await service.verify_task_ids(None, None)
        await service.setup_task("stored-task", "sandbox-1", PROVIDER)
        result = await service.evaluate_instance("stored-task", "sandbox-1", PROVIDER)

        assert selected.task_ids == ["stored-task"]
        assert sandbox.files["/app/instruction.md"] == b"From storage."
        assert isinstance(result, dict)
        assert result["rewards"] == {"reward": 1.0}
        assert "/tests/data.txt" in sandbox.files


def _dockerfile_archive(toml: str = DOCKERFILE_TOML) -> bytes:
    return zip_bytes(
        {
            "task.toml": toml,
            "instruction.md": "Build it.",
            "tests/test.sh": "#!/bin/bash\n",
            "environment/Dockerfile": "FROM ubuntu:24.04\n",
        }
    )


@pytest.mark.description("A task built from its Dockerfile keeps its build context for the length of the run.")
class TestDockerfileTasks:
    @pytest.mark.description("A hosted tasks context is kept until the service closes.")
    async def test_a_hosted_tasks_context_is_kept_until_the_service_closes(self, tmp_path: Path) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        dataset_id = uuid4()
        archive = zip_bytes(
            {
                "task.toml": DOCKERFILE_TOML,
                "instruction.md": "Build it.",
                "tests/test.sh": "#!/bin/bash\n",
                "environment/Dockerfile": "FROM ubuntu:24.04\nCOPY data.txt /data.txt\n",
                "environment/data.txt": "context",
            }
        )
        digest = await store_archive(store, dataset_id, archive)
        service = DatasetTaskService(ArchiveSource(store, dataset_id, {"built-task": digest}))

        response = await service.retrieve_task("built-task")

        assert isinstance(response.source, DockerfileSource)
        dockerfile = Path(response.source.dockerfile)
        assert dockerfile.read_text() == "FROM ubuntu:24.04\nCOPY data.txt /data.txt\n"
        assert (dockerfile.parent / "data.txt").read_text() == "context"

        await service.close()

        assert not dockerfile.exists()

    @pytest.mark.description("A local tasks own folder is the context and stays after the service closes.")
    async def test_a_local_tasks_own_folder_is_the_context_and_stays_after_the_service_closes(
        self, tmp_path: Path
    ) -> None:
        task = write_task(tmp_path, "built-task", DOCKERFILE_TOML)
        (task / "environment").mkdir()
        (task / "environment" / "Dockerfile").write_text("FROM ubuntu:24.04\n")
        service = DatasetTaskService(LocalFolderSource(tmp_path))

        response = await service.retrieve_task("built-task")
        await service.close()

        assert isinstance(response.source, DockerfileSource)
        assert Path(response.source.dockerfile) == (task / "environment" / "Dockerfile").resolve()
        assert (task / "environment" / "Dockerfile").is_file()

    @pytest.mark.description("Tasks retrieved at once leave nothing behind after the service closes.")
    async def test_tasks_retrieved_at_once_leave_nothing_behind_after_the_service_closes(self, tmp_path: Path) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        dataset_id = uuid4()
        digest = await store_archive(store, dataset_id, _dockerfile_archive())
        service = DatasetTaskService(
            ArchiveSource(store, dataset_id, {f"task-{number}": digest for number in range(3)})
        )

        responses = await asyncio.gather(*(service.retrieve_task(f"task-{number}") for number in range(3)))

        dockerfiles = [
            Path(response.source.dockerfile) for response in responses if isinstance(response.source, DockerfileSource)
        ]
        assert len(dockerfiles) == 3 and all(path.is_file() for path in dockerfiles)
        scratch = {path.parents[3] for path in dockerfiles}

        await service.close()

        assert not any(path.exists() for path in scratch)

    @pytest.mark.description("A task that fails after its environment is extracted fails the same way again.")
    async def test_a_task_that_fails_after_its_environment_is_extracted_fails_the_same_way_again(
        self, tmp_path: Path
    ) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        dataset_id = uuid4()
        digest = await store_archive(store, dataset_id, _dockerfile_archive("[environment.env]\nTOKEN = '${TOKEN}'"))
        service = DatasetTaskService(ArchiveSource(store, dataset_id, {"unreadable": digest}))

        for _ in range(2):
            with pytest.raises(SecretsError):
                await service.retrieve_task("unreadable")

        await service.close()

    @pytest.mark.description("The environment is removed even when closing the client fails.")
    async def test_the_environment_is_removed_even_when_closing_the_client_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        dataset_id = uuid4()
        digest = await store_archive(store, dataset_id, _dockerfile_archive())
        service = DatasetTaskService(ArchiveSource(store, dataset_id, {"built-task": digest}))
        response = await service.retrieve_task("built-task")
        assert isinstance(response.source, DockerfileSource)

        async def failing_close(_providers: SandboxProviders) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(SandboxProviders, "close", failing_close)

        with pytest.raises(RuntimeError, match="close failed"):
            await service.close()

        assert not Path(response.source.dockerfile).exists()
