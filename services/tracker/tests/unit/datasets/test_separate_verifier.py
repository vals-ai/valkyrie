import io
import logging
import os
from functools import partial
import tarfile
from collections.abc import AsyncIterable, Awaitable, Callable, Sequence
from contextvars import ContextVar
from pathlib import Path
from typing import Any, cast

import pytest
from benchmark_service import (
    ComposeSource,
    DockerfileSource,
    ImageSource,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    UnsupportedSandboxConfigError,
)
from benchmark_service.sandbox.created import AnnouncedSandboxProvider, notify_sandbox_created
from benchmark_service.schemas import SandboxCreatedData
from benchmark_service.sandbox.types import SandboxSource

from tests.unit.datasets.support import FakeSandbox, OuterSandbox, ShellSandbox
from tracker.services.datasets import artifacts, separate_verifier
from tracker import egress
from tracker.services.datasets.artifacts import ArtifactSink, SkippedArtifact, StagedArtifact, stage_artifacts
from tracker.runtime.artifacts import task_artifact_key
from tracker.services.datasets.compose import ComposeRuntime
from tracker.services.datasets.config import ArtifactConfig, TaskConfig
from tracker.services.datasets.grading import OPEN_LOG_DIRS, REWARD_TEXT_PATH, GradingSandbox, VerifierResultError
from tracker.services.datasets.separate_verifier import (
    SeparateVerifierError,
    VerifierEnvironment,
    _carry_artifacts,  # pyright: ignore[reportPrivateUsage]
    _clean_archive as clean_archive,  # pyright: ignore[reportPrivateUsage]
    grade_separately,
    verifier_environment,
)

AGENT_SOURCE = ImageSource(image="agent:1")


@pytest.mark.description("Separate verifier policy events retain attempt context for implicit and explicit policies.")
@pytest.mark.parametrize("network", ["", "network_mode = 'public'", "network_mode = 'no-network'"])
async def test_separate_verifier_policy_event(
    tests_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, network: str
) -> None:
    context = {"attempt_id": "attempt-1", "benchmark_id": "run-1", "task_id": "task-1", "org_id": "org-1"}
    monkeypatch.setattr(egress, "policy_context", ContextVar("test_policy_context", default=context), raising=False)
    logger = logging.getLogger("tracker.egress")
    logger.addHandler(caplog.handler)
    try:
        await grade(
            TaskConfig.parse(f"[verifier]\nenvironment_mode = 'separate'\n{network}"),
            FakeProvider([verifier_sandbox()]),
            tests_dir,
        )
    finally:
        logger.removeHandler(caplog.handler)
    records = [record for record in caplog.records if getattr(record, "event", None) == "task.egress.policy"]
    assert len(records) == 1
    assert getattr(records[0], "stage") == "evaluation"
    assert getattr(records[0], "phase") == "separate_verifier"
    assert getattr(records[0], "implicit_allow_all") is (network == "")
    assert all(getattr(records[0], key) == value for key, value in context.items())


class FakeProvider:
    def __init__(
        self, sandboxes: Sequence[FakeSandbox | SandboxError], announces: AnnouncedSandboxProvider | None = "daytona"
    ) -> None:
        self.to_create = list(sandboxes)
        self.announces: AnnouncedSandboxProvider | None = announces
        self.requests: list[SandboxCreateRequest] = []
        self.deleted: list[str] = []

    async def create_sandbox(self, request: SandboxCreateRequest) -> FakeSandbox:
        self.requests.append(request)
        if isinstance(request.source, ComposeSource):
            raise SandboxError("ComposeSource must be unwrapped before provider.create_sandbox")
        created = self.to_create.pop(0)
        if isinstance(created, SandboxError):
            raise created
        if self.announces is not None:
            await notify_sandbox_created(self.announces, created.id)
        return created

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleted.append(instance_id)


async def grade(
    config: TaskConfig,
    provider: FakeProvider,
    tests_dir: Path,
    *,
    agent: FakeSandbox | None = None,
    created: list[SandboxCreatedData] | None = None,
    agent_source: SandboxSource = AGENT_SOURCE,
    agent_compose: ComposeRuntime | None = None,
    gateway_url: str | None = None,
    default_user: str | None = None,
    image_workdir: Callable[[GradingSandbox, SandboxSource], Awaitable[str | None]] | None = None,
    staged: Sequence[StagedArtifact] = (),
    artifact_sink: ArtifactSink | None = None,
) -> dict[str, object]:
    return await grade_separately(
        "task-1",
        config,
        {},
        {},
        tests_dir,
        (agent or labelled_agent()).labels or {},
        agent_source,
        agent_compose,
        cast(Any, provider),
        staged,
        [],
        None,
        created.append if created is not None else None,
        gateway_url=gateway_url,
        default_user=default_user,
        image_workdir=image_workdir,
        artifact_sink=artifact_sink,
    )


@pytest.fixture
def tests_dir(tmp_path: Path) -> Path:
    folder = tmp_path / "tests"
    folder.mkdir()
    (folder / "test.sh").write_text("#!/bin/bash\n")
    return folder


def labelled_agent() -> FakeSandbox:
    agent = FakeSandbox(id="agent-1")
    agent.labels = {"Benchmark": "run-1"}
    return agent


def verifier_sandbox() -> FakeSandbox:
    return FakeSandbox(id="verifier-1", writes={REWARD_TEXT_PATH: b"1"})


@pytest.mark.description("Separate grading retains verifier files before deletion, including after grading fails.")
@pytest.mark.parametrize("reward", [b"1", b"bad", None])
async def test_separate_verifier_archive(tests_dir: Path, reward: bytes | None) -> None:
    files = {
        "/logs/verifier/details.json": b'{"attempts": ["complete response"]}',
        "/logs/verifier/test-stdout.txt": b"complete stdout",
    }
    if reward is not None:
        files[REWARD_TEXT_PATH] = reward
    provider = FakeProvider([FakeSandbox(writes=files)])
    uploaded: dict[str, bytes] = {}

    async def sink(_task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
        assert not provider.deleted
        uploaded[name] = b"".join([chunk async for chunk in chunks])

    if reward == b"1":
        await grade(
            TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir, artifact_sink=sink
        )
    else:
        with pytest.raises(VerifierResultError):
            await grade(
                TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir, artifact_sink=sink
            )

    assert provider.deleted == ["sandbox-1"]
    with tarfile.open(fileobj=io.BytesIO(uploaded["verifier_output.tar.gz"])) as archive:
        assert {
            member.name: cast(io.BufferedReader, archive.extractfile(member)).read()
            for member in archive
            if member.isfile()
        } == {path.removeprefix("/logs/"): content for path, content in files.items()}


@pytest.mark.description("A verifier upload failure fails separate grading and the sandbox is still deleted.")
async def test_separate_verifier_upload_failure(tests_dir: Path) -> None:
    provider = FakeProvider([verifier_sandbox()])

    async def sink(_task_id: str, _name: str, _chunks: AsyncIterable[bytes]) -> None:
        raise OSError("storage unavailable")

    with pytest.raises(RuntimeError, match="Required artifact /logs/verifier could not be saved: storage unavailable"):
        await grade(
            TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir, artifact_sink=sink
        )

    assert provider.deleted == ["verifier-1"]


COMPOSE_AGENT = ComposeSource(outer=ImageSource(image="docker:dind"), compose_command="docker compose -p agent")


def agent_runtime(tmp_path: Path) -> ComposeRuntime:
    return ComposeRuntime(environment_dir=str(tmp_path / "environment"), overlays={}, build_timeout_sec=900)


@pytest.mark.description("The environment the grader runs in, and whether the tests still have to be uploaded.")
class TestVerifierEnvironment:
    @pytest.mark.description("A prebuilt image has its tests baked in.")
    def test_a_prebuilt_image_has_its_tests_baked_in(self, tests_dir: Path) -> None:
        config = TaskConfig.parse("[verifier.environment]\ndocker_image = 'grader:1'")

        assert verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None) == VerifierEnvironment(
            ImageSource(image="grader:1"), False
        )

    @pytest.mark.description("A dockerfile beside the tests is built with the tests as context.")
    def test_a_dockerfile_beside_the_tests_is_built_with_the_tests_as_context(self, tests_dir: Path) -> None:
        (tests_dir / "Dockerfile").write_text("FROM grader\nCOPY . /tests\n")
        config = TaskConfig.parse("[verifier.environment]\nbuild_timeout_sec = 90")

        environment = verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None)

        assert environment.source == DockerfileSource(
            dockerfile=str((tests_dir / "Dockerfile").absolute()), build_timeout=90
        )
        assert environment.upload_tests is False

    @pytest.mark.description("A compose file beside the tests runs them baked in with the verifiers own settings.")
    def test_a_compose_file_beside_the_tests_runs_them_baked_in_with_the_verifiers_own_settings(
        self, tests_dir: Path
    ) -> None:
        (tests_dir / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        config = TaskConfig.parse("[verifier.environment]\nmemory_mb = 2048")

        environment = verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None)

        assert isinstance(environment.source, ComposeSource)
        assert environment.upload_tests is False
        assert environment.compose is not None
        assert environment.compose.environment_dir == str(tests_dir.absolute())
        assert "2048M" in environment.compose.overlays["resources.json"]

    @pytest.mark.description("A compose file wins over a dockerfile and takes the image as its main service.")
    def test_a_compose_file_wins_over_a_dockerfile_and_takes_the_image_as_its_main_service(
        self, tests_dir: Path
    ) -> None:
        (tests_dir / "Dockerfile").write_text("FROM grader\n")
        (tests_dir / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        built = TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'")
        pulled = TaskConfig.parse("[verifier.environment]\ndocker_image = 'grader:1'")

        built_environment = verifier_environment("task-1", tests_dir, built, AGENT_SOURCE, None)
        pulled_environment = verifier_environment("task-1", tests_dir, pulled, AGENT_SOURCE, None)

        assert isinstance(built_environment.source, ComposeSource)
        assert "PREBUILT_IMAGE_NAME" not in built_environment.source.compose_command
        assert isinstance(pulled_environment.source, ComposeSource)
        assert "PREBUILT_IMAGE_NAME=grader:1" in pulled_environment.source.compose_command

    @pytest.mark.description("An image wins over a dockerfile.")
    def test_an_image_wins_over_a_dockerfile(self, tests_dir: Path) -> None:
        (tests_dir / "Dockerfile").write_text("FROM grader\n")
        config = TaskConfig.parse("[verifier.environment]\ndocker_image = 'grader:1'")

        environment = verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None)

        assert environment.source == ImageSource(image="grader:1")

    @pytest.mark.description("The compose project of a verifier takes nothing from the agents environment.")
    def test_the_compose_project_of_a_verifier_takes_nothing_from_the_agents_environment(self, tests_dir: Path) -> None:
        (tests_dir / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        config = TaskConfig.parse(
            "[environment]\ndocker_image = 'agent:1'\nnetwork_mode = 'no-network'\ncpus = 4\n"
            "[verifier]\nenvironment_mode = 'separate'"
        )

        environment = verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None)

        assert isinstance(environment.source, ComposeSource)
        assert "PREBUILT_IMAGE_NAME" not in environment.source.compose_command
        assert "no-network.yaml" not in environment.source.compose_command
        assert environment.compose is not None
        assert "cpus" not in environment.compose.overlays["resources.json"]

    @pytest.mark.description("Otherwise a clean copy of the agents environment gets the tests uploaded.")
    def test_otherwise_a_clean_copy_of_the_agents_environment_gets_the_tests_uploaded(self, tests_dir: Path) -> None:
        config = TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'")

        assert verifier_environment("task-1", tests_dir, config, AGENT_SOURCE, None) == VerifierEnvironment(
            AGENT_SOURCE, True
        )


@pytest.mark.description("The grader runs in its own sandbox, which is always deleted.")
class TestGradeSeparately:
    @pytest.mark.description("The task is graded in the new sandbox and it is deleted.")
    async def test_the_task_is_graded_in_the_new_sandbox_and_it_is_deleted(self, tests_dir: Path) -> None:
        provider = FakeProvider([verifier_sandbox()])
        created: list[SandboxCreatedData] = []
        config = TaskConfig.parse("[verifier]\ntimeout_sec = 600\n[verifier.environment]\ndocker_image = 'grader:1'")

        result = await grade(config, provider, tests_dir, created=created)

        assert result["rewards"] == {"reward": 1.0}
        assert cast(dict[str, object], result["metadata"])["verifier_environment_mode"] == "separate"
        assert provider.deleted == ["verifier-1"]
        assert created == [SandboxCreatedData(provider="daytona", sandbox_id="verifier-1")]
        (request,) = provider.requests
        assert request.source == ImageSource(image="grader:1")
        assert request.name.startswith("task-1-verifier-")
        assert request.labels == {"Benchmark": "run-1", "valkyrie-role": "verifier"}
        assert request.auto_stop_interval == 10 + 30
        assert request.network_block_all is False

    @pytest.mark.description("A compose verifier starts its services and grades in main with its network closed.")
    async def test_a_compose_verifier_starts_its_services_and_grades_in_main_with_its_network_closed(
        self, tests_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tests_dir / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        outer = OuterSandbox()
        outer.id = "verifier-1"
        outer.writes[REWARD_TEXT_PATH] = b"1"
        provider = FakeProvider([cast(FakeSandbox, outer)])
        applied: list[tuple[str, object]] = []

        async def apply(sandbox: FakeSandbox, policy: object) -> None:
            applied.append((sandbox.id, policy))

        monkeypatch.setattr(separate_verifier, "apply_egress_policy", apply)
        config = TaskConfig.parse("[verifier]\nnetwork_mode = 'no-network'\nenvironment_mode = 'separate'")

        result = await grade(config, provider, tests_dir)

        assert result["rewards"] == {"reward": 1.0}
        assert provider.requests[0].network_block_all is False
        assert provider.requests[0].source == ImageSource(image="docker:28.3.3-dind")
        assert provider.requests[0].requires_nested_docker is True
        assert separate_verifier.compose_verifier_resources(config, tests_dir.parent) == [
            provider.requests[0].resources
        ]
        assert any(command.endswith("up --detach --wait") for command in outer.commands)
        assert applied == [("verifier-1", [])]
        assert provider.deleted == ["verifier-1"]

    @pytest.mark.description("A compose agent environment is started again for the grader and gets the tests.")
    async def test_a_compose_agent_environment_is_started_again_for_the_grader_and_gets_the_tests(
        self, tests_dir: Path, tmp_path: Path
    ) -> None:
        outer = OuterSandbox()
        outer.id = "verifier-1"
        outer.writes[REWARD_TEXT_PATH] = b"1"
        provider = FakeProvider([cast(FakeSandbox, outer)])
        (tmp_path / "environment").mkdir()

        await grade(
            TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"),
            provider,
            tests_dir,
            agent_source=COMPOSE_AGENT,
            agent_compose=agent_runtime(tmp_path),
        )

        assert any(command.endswith("docker compose -p agent up --detach --wait") for command in outer.commands)
        assert provider.requests[0].source == COMPOSE_AGENT.outer
        assert provider.requests[0].requires_nested_docker is True
        assert provider.deleted == ["verifier-1"]

    @pytest.mark.description(
        "The tests run in the verifier's workdir, else the environment's in a copy of it, else the verifier image's own."
        " The image's workdir is read before an artifact replaces that folder."
    )
    async def test_the_tests_run_in_the_verifiers_workdir_and_otherwise_the_environments(
        self, tests_dir: Path, tmp_path: Path
    ) -> None:
        own, inherited, image = verifier_sandbox(), verifier_sandbox(), verifier_sandbox()
        archive = tmp_path / "srv.tar.gz"
        write_archive(archive, {"srv/out.txt": b"x"})

        async def image_workdir(_sandbox: GradingSandbox, source: SandboxSource) -> str | None:
            if any("tar -xzf" in command for command in image.commands):
                return "/srv (deleted)"
            return "/srv" if source == ImageSource(image="grader:1") else "/elsewhere"

        await grade(
            TaskConfig.parse("[environment]\nworkdir = '/app'\n[verifier.environment]\nworkdir = '/grader'"),
            FakeProvider([own]),
            tests_dir,
        )
        await grade(
            TaskConfig.parse("[environment]\nworkdir = '/app'\n[verifier]\nenvironment_mode = 'separate'"),
            FakeProvider([inherited]),
            tests_dir,
        )
        await grade(
            TaskConfig.parse("[environment]\nworkdir = '/app'\n[verifier.environment]\ndocker_image = 'grader:1'"),
            FakeProvider([image]),
            tests_dir,
            image_workdir=image_workdir,
            staged=[
                StagedArtifact(
                    "srv.tar.gz",
                    "/srv",
                    str(archive),
                    "main",
                    partial(FakeSandbox(files={str(archive): archive.read_bytes()}).stream_download, str(archive)),
                )
            ],
        )

        assert (own.cwds, inherited.cwds, image.cwds) == (["/grader"], ["/app"], ["/srv"])
        assert any("tar -xzf" in command for command in image.commands)

    @pytest.mark.description(
        "A verifier in a copy of the agent's sandbox runs as the image user, and one in its own image runs as that image's."
    )
    @pytest.mark.parametrize(
        ("toml", "switched"),
        [
            ("[verifier]\nenvironment_mode = 'separate'", True),
            ("[verifier.environment]\ndocker_image = 'grader:1'", False),
        ],
        ids=["agent-copy", "own-image"],
    )
    async def test_the_verifier_runs_as_the_image_user_only_in_a_copy_of_the_agents_sandbox(
        self, tests_dir: Path, toml: str, switched: bool
    ) -> None:
        verifier = verifier_sandbox()

        await grade(TaskConfig.parse(toml), FakeProvider([verifier]), tests_dir, default_user="agent")

        assert any("getent passwd agent" in command for command in verifier.commands) is switched

    @pytest.mark.description("The agent environments tests are uploaded only when they are not baked in.")
    async def test_the_agent_environments_tests_are_uploaded_only_when_they_are_not_baked_in(
        self, tests_dir: Path
    ) -> None:
        copied, baked = verifier_sandbox(), verifier_sandbox()

        await grade(TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), FakeProvider([copied]), tests_dir)
        await grade(
            TaskConfig.parse("[verifier.environment]\ndocker_image = 'grader:1'"), FakeProvider([baked]), tests_dir
        )

        assert "/tests/test.sh" in copied.files
        assert "/tests/test.sh" not in baked.files

    @pytest.mark.description("A verifier without network is created with egress blocked.")
    async def test_a_verifier_without_network_is_created_with_egress_blocked(self, tests_dir: Path) -> None:
        provider = FakeProvider([verifier_sandbox()])
        config = TaskConfig.parse("[verifier]\nnetwork_mode = 'no-network'\nenvironment_mode = 'separate'")

        await grade(config, provider, tests_dir)

        assert provider.requests[0].network_block_all is True
        assert provider.requests[0].requires_nested_docker is False

    @pytest.mark.description("An allowlist is applied to the new sandbox.")
    async def test_an_allowlist_is_applied_to_the_new_sandbox(
        self, tests_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        applied: list[tuple[str, object]] = []

        async def apply(sandbox: FakeSandbox, policy: object) -> None:
            applied.append((sandbox.id, policy))

        monkeypatch.setattr(separate_verifier, "apply_egress_policy", apply)
        config = TaskConfig.parse(
            "[verifier]\nnetwork_mode = 'allowlist'\nallowed_hosts = ['api.example.com']\nenvironment_mode = 'separate'"
        )

        await grade(config, FakeProvider([verifier_sandbox()]), tests_dir)

        assert applied == [("verifier-1", ["api.example.com"])]

    @pytest.mark.description("A verifier without network starts blocked and then can reach only the run's Gateway.")
    async def test_a_verifier_that_lists_models_can_reach_only_the_gateway(
        self, tests_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        applied: list[tuple[str, object]] = []

        async def apply(sandbox: FakeSandbox, policy: object) -> None:
            applied.append((sandbox.id, policy))

        monkeypatch.setattr(separate_verifier, "apply_egress_policy", apply)
        provider = FakeProvider([verifier_sandbox()])
        config = TaskConfig.parse(
            "[verifier]\nnetwork_mode = 'no-network'\nenvironment_mode = 'separate'\nmodels = ['openai/gpt-4o']"
        )

        await grade(config, provider, tests_dir, gateway_url="https://gateway.example")

        assert provider.requests[0].network_block_all is True
        assert applied == [("verifier-1", ["gateway.example"])]

    @pytest.mark.description("A failed create is retried under a new name when it may work next time.")
    async def test_a_failed_create_is_retried_under_a_new_name_when_it_may_work_next_time(
        self, tests_dir: Path
    ) -> None:
        provider = FakeProvider([SandboxError("Sandbox name already exists"), verifier_sandbox()])

        await grade(TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir)

        assert len({request.name for request in provider.requests}) == 2

    @pytest.mark.description("A create that cannot work is not retried.")
    @pytest.mark.parametrize(
        "error", [SandboxError("image not found"), UnsupportedSandboxConfigError("entered error state")]
    )
    async def test_a_create_that_cannot_work_is_not_retried(self, tests_dir: Path, error: SandboxError) -> None:
        provider = FakeProvider([error])

        with pytest.raises(type(error), match=str(error)):
            await grade(TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir)

        assert len(provider.requests) == 1

    @pytest.mark.description("Only a verifier its provider announces is reported, under the provider's own name.")
    @pytest.mark.parametrize("announces", ["daytona_v2", None])
    async def test_the_provider_names_the_verifier_it_announces(
        self, tests_dir: Path, announces: AnnouncedSandboxProvider | None
    ) -> None:
        provider = FakeProvider([verifier_sandbox()], announces=announces)
        created: list[SandboxCreatedData] = []
        config = TaskConfig.parse("[verifier]\ntimeout_sec = 600\n[verifier.environment]\ndocker_image = 'grader:1'")

        await grade(config, provider, tests_dir, created=created)

        expected = [] if announces is None else [SandboxCreatedData(provider=announces, sandbox_id="verifier-1")]
        assert created == expected

    @pytest.mark.description("The sandbox is deleted even if announcing it fails.")
    async def test_the_sandbox_is_deleted_even_if_announcing_it_fails(self, tests_dir: Path) -> None:
        provider = FakeProvider([verifier_sandbox()])

        def fail(_created: SandboxCreatedData) -> None:
            raise RuntimeError("could not record the sandbox")

        with pytest.raises(RuntimeError, match="could not record"):
            await grade_separately(
                "task-1",
                TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"),
                {},
                {},
                tests_dir,
                labelled_agent().labels or {},
                AGENT_SOURCE,
                None,
                cast(Any, provider),
                [],
                [],
                None,
                fail,
            )

        assert provider.deleted == ["verifier-1"]

    @pytest.mark.description("The sandbox is deleted when grading fails.")
    async def test_the_sandbox_is_deleted_when_grading_fails(self, tests_dir: Path) -> None:
        provider = FakeProvider([FakeSandbox(id="verifier-1")])

        with pytest.raises(VerifierResultError):
            await grade(TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir)

        assert provider.deleted == ["verifier-1"]


@pytest.fixture
def shell(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Sandbox:
    monkeypatch.setattr(artifacts, "STAGING_ROOT", str(tmp_path))
    monkeypatch.setattr(separate_verifier, "TRANSFER_DIR", str(tmp_path / "transfer"))
    return cast(Sandbox, ShellSandbox())


def write_archive(
    path: Path,
    members: dict[str, bytes],
    links: dict[str, str] | None = None,
    hardlinks: dict[str, str] | None = None,
    modes: dict[str, int] | None = None,
) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, target in (links or {}).items():
            info = tarfile.TarInfo(name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            archive.addfile(info)
        for name, target in (hardlinks or {}).items():
            info = tarfile.TarInfo(name)
            info.type = tarfile.LNKTYPE
            info.linkname = target
            archive.addfile(info)
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = (modes or {}).get(name, 0o644)
            info.uid = 4242
            archive.addfile(info, io.BytesIO(content))


@pytest.mark.description("What the agent leaves is carried to the verifier without trusting the archive.")
class TestCarryArtifacts:
    @pytest.mark.description("A legacy symlink chain cannot escape through paths that change meaning after rebasing.")
    def test_rebased_symlink_chain_cannot_escape(self, tmp_path: Path) -> None:
        archive = tmp_path / "legacy.tar.gz"
        write_archive(
            archive,
            {"workspace/output/escape/planted.txt": b"must not escape"},
            links={"workspace/output/deep/link": "..", "workspace/output/escape": "deep/link/../../outside"},
        )
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        (scratch / "outside").mkdir()

        with pytest.raises(SeparateVerifierError, match="can't be carried"):
            clean_archive(archive, "output", "/workspace/output", scratch, "workspace/output")

        assert not (scratch / "outside" / "planted.txt").exists()

    @pytest.mark.description("A legacy archive restores only the declared subtree and drops unrelated members.")
    @pytest.mark.parametrize("single_file", [False, True])
    async def test_legacy_subtree(self, shell: Sandbox, tmp_path: Path, single_file: bool) -> None:
        archive = tmp_path / "legacy.tar.gz"
        write_archive(
            archive,
            {
                "workspace/output/audio.wav": b"recording",
                "workspace/output/audio.json": b"metadata",
                "workspace/output-other/ignored": b"sibling",
                "workspace/private.txt": b"unrelated",
            },
            links={"workspace/output/alias": "audio.wav", "unrelated-link": "/private"},
        )
        destination = tmp_path / "restored" / ("audio.wav" if single_file else "output")
        staged = StagedArtifact(
            "recording.tar.gz",
            str(destination),
            str(archive),
            "main",
            partial(shell.stream_download, str(archive)),
            "workspace/output/audio.wav" if single_file else "workspace/output",
        )

        await _carry_artifacts(shell, [staged], [])

        if single_file:
            assert destination.read_bytes() == b"recording"
            assert sorted(path.name for path in destination.parent.iterdir()) == ["audio.wav"]
        else:
            assert (destination / "audio.wav").read_bytes() == b"recording"
            assert (destination / "audio.json").read_bytes() == b"metadata"
            assert (destination / "alias").is_symlink()
            assert (destination / "alias").read_bytes() == b"recording"
            assert sorted(path.name for path in destination.parent.iterdir()) == ["output"]

    @pytest.mark.description("A missing legacy subtree names the source run, task and source before restoring files.")
    @pytest.mark.parametrize("source", ["/workspace/output", "/logs/artifacts"])
    async def test_missing_legacy_subtree(self, shell: Sandbox, tmp_path: Path, source: str) -> None:
        archive = tmp_path / "legacy.tar.gz"
        write_archive(archive, {"workspace/elsewhere/audio.wav": b"not the source"})
        staged = StagedArtifact(
            "recording.tar.gz",
            source,
            task_artifact_key("old-run", "task-1", "agent_output.tar.gz"),
            "main",
            partial(shell.stream_download, str(archive)),
            source.lstrip("/"),
        )

        with pytest.raises(SeparateVerifierError) as caught:
            await _carry_artifacts(shell, [staged], [])

        assert all(value in str(caught.value) for value in ["old-run", "task-1", source, "missing subtree"])

    @pytest.mark.description("Legacy subtree selection preserves path escape and link checks.")
    @pytest.mark.parametrize(
        "members,links,hardlinks",
        [
            ({"../../escaped": b"x"}, {}, {}),
            ({}, {"workspace/output/link": "../private"}, {}),
            ({}, {"workspace/output/link": "/etc/passwd"}, {}),
            ({}, {}, {"workspace/output/link": "workspace/private"}),
        ],
    )
    def test_legacy_escape_rejected(
        self, tmp_path: Path, members: dict[str, bytes], links: dict[str, str], hardlinks: dict[str, str]
    ) -> None:
        archive = tmp_path / "legacy.tar.gz"
        write_archive(archive, members, links, hardlinks)
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        with pytest.raises(SeparateVerifierError):
            clean_archive(archive, "output", "/workspace/output", scratch, "workspace/output")

    @pytest.mark.description("Dropped legacy members still count toward entry and decompressed size limits.")
    @pytest.mark.parametrize("limit", ["entries", "bytes"])
    def test_legacy_limits(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str) -> None:
        monkeypatch.setattr(separate_verifier, "MAX_ARTIFACT_FILES" if limit == "entries" else "MAX_UNPACKED_BYTES", 1)
        archive = tmp_path / "legacy.tar.gz"
        write_archive(archive, {"ignored": b"outside", "workspace/output/audio.wav": b"recording"})
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        with pytest.raises(SeparateVerifierError):
            clean_archive(archive, "output", "/workspace/output", scratch, "workspace/output")

    @pytest.mark.description(
        "A folder arrives at its declared path with its contents' modes, and the folder is open to every user."
    )
    async def test_a_folder_arrives_at_its_declared_path_with_its_modes(self, shell: Sandbox, tmp_path: Path) -> None:
        out = tmp_path / "out"
        (out / "nested").mkdir(parents=True)
        (out / "nested" / "run.sh").write_text("#!/bin/sh\n")
        os.chmod(out / "nested" / "run.sh", 0o755)
        (out / "result.txt").write_text("42")
        (out / "alias").symlink_to("result.txt")
        staged, skipped = await stage_artifacts({"main": shell}, [ArtifactConfig(source=str(out))], str(tmp_path))
        for path in sorted(out.rglob("*"), reverse=True):
            path.unlink() if path.is_file() or path.is_symlink() else path.rmdir()
        out.rmdir()

        await _carry_artifacts(shell, staged, skipped)

        assert (out / "result.txt").read_text() == "42"
        assert os.access(out / "nested" / "run.sh", os.X_OK)
        assert out.stat().st_mode & 0o777 == 0o777
        assert (out / "alias").is_symlink() and os.readlink(out / "alias") == "result.txt"

    @pytest.mark.description("A carried folder replaces what the verifier image already had at that path.")
    async def test_a_carried_folder_replaces_what_was_there(self, shell: Sandbox, tmp_path: Path) -> None:
        out = tmp_path / "out"
        out.mkdir()
        (out / "kept.py").write_text("agent")
        staged, skipped = await stage_artifacts({"main": shell}, [ArtifactConfig(source=str(out))], str(tmp_path))
        (out / "deleted.py").write_text("image")

        await _carry_artifacts(shell, staged, skipped)

        assert sorted(path.name for path in out.iterdir()) == ["kept.py"]

    @pytest.mark.description("A single file lands in its folder, which opens to every user and keeps its sticky bit.")
    async def test_a_single_file_opens_its_folder_and_keeps_its_sticky_bit(
        self, shell: Sandbox, tmp_path: Path
    ) -> None:
        box = tmp_path / "box"
        box.mkdir()
        (box / "report.txt").write_text("42")
        staged, skipped = await stage_artifacts(
            {"main": shell}, [ArtifactConfig(source=str(box / "report.txt"))], str(tmp_path)
        )
        (box / "report.txt").unlink()
        box.chmod(0o1755)

        await _carry_artifacts(shell, staged, skipped)

        assert (box / "report.txt").read_text() == "42"
        assert box.stat().st_mode & 0o7777 == 0o1777

    @pytest.mark.description("An artifact the agent never wrote is carried as nothing.")
    async def test_an_artifact_the_agent_never_wrote_is_carried_as_nothing(
        self, shell: Sandbox, tmp_path: Path
    ) -> None:
        staged, skipped = await stage_artifacts(
            {"main": shell}, [ArtifactConfig(source=str(tmp_path / "nope"))], str(tmp_path)
        )

        await _carry_artifacts(shell, staged, skipped)

        assert not (tmp_path / "nope").exists()

    @pytest.mark.description("An artifact that could not be staged is an error not a zero.")
    async def test_an_artifact_that_could_not_be_staged_is_an_error_not_a_zero(self, shell: Sandbox) -> None:
        skipped = [SkippedArtifact("/app/big", "it is over 5 bytes", absent=False, name="app/big")]

        with pytest.raises(SeparateVerifierError, match="/app/big can't be carried: it is over 5 bytes"):
            await _carry_artifacts(shell, [], skipped)

    @pytest.mark.description("The artifacts folder that cannot be carried is reported and grading goes on.")
    async def test_the_artifacts_folder_that_cannot_be_carried_is_reported_and_grading_goes_on(self) -> None:
        verifier = FakeSandbox(id="verifier-1")
        messages: list[str] = []

        await _carry_artifacts(
            cast(Sandbox, verifier),
            [],
            [SkippedArtifact("/logs/artifacts", "it holds too many files", False, "logs/artifacts")],
            messages.append,
        )

        assert messages == ["The folder /logs/artifacts could not be carried to the verifier: it holds too many files"]

    @pytest.mark.description("The reward folder is wiped after the artifacts land.")
    async def test_the_reward_folder_is_wiped_after_the_artifacts_land(self) -> None:
        verifier = FakeSandbox(id="verifier-1")

        await _carry_artifacts(cast(Sandbox, verifier), [], [])

        assert verifier.commands[-2:] == ["rm -rf /logs/verifier", OPEN_LOG_DIRS]

    @pytest.mark.description("A hostile archive is refused before anything is written.")
    @pytest.mark.parametrize(
        ("members", "links", "hardlinks", "complaint"),
        [
            ({"out/ok.txt": b"x", "tests/answers.txt": b"planted"}, {}, {}, "outside itself"),
            ({"out/ok.txt": b"x"}, {"out/escape": "/tests"}, {}, "can't be carried"),
            ({"out/ok.txt": b"x"}, {"out/up": "../../tests"}, {}, "can't be carried"),
            ({"out/ok.txt": b"x"}, {"out/answer": "../tests/expected.json"}, {}, "link that leaves it"),
            ({"out/ok.txt": b"x"}, {"out": "."}, {}, "link that leaves it"),
            ({"out/ok.txt": b"x"}, {}, {"out/answer": "tests/expected.json"}, "link that leaves it"),
        ],
        ids=["member-outside", "absolute-link", "link-out", "link-to-sibling", "root-link", "hard-link"],
    )
    async def test_a_hostile_archive_is_refused_before_anything_is_written(
        self,
        shell: Sandbox,
        tmp_path: Path,
        members: dict[str, bytes],
        links: dict[str, str],
        hardlinks: dict[str, str],
        complaint: str,
    ) -> None:
        archive = tmp_path / "hostile.tar.gz"
        write_archive(archive, members, links, hardlinks)
        path = str(archive)
        staged = [
            StagedArtifact("out.tar.gz", f"{tmp_path}/work/out", path, "main", partial(shell.stream_download, path))
        ]

        with pytest.raises(SeparateVerifierError, match=complaint):
            await _carry_artifacts(shell, staged, [])

        assert not (tmp_path / "work").exists()

    @pytest.mark.description("What is carried is rebuilt with root ownership and no special modes.")
    def test_what_is_carried_is_rebuilt_with_root_ownership_and_no_special_modes(self, tmp_path: Path) -> None:
        archive = tmp_path / "agent.tar.gz"
        write_archive(archive, {"out/run": b"x", "out/setuid": b"y"}, modes={"out/run": 0o4755, "out/setuid": 0o2755})
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        rebuilt, _folder = clean_archive(archive, "out", "/work/out", scratch)

        with tarfile.open(rebuilt) as tar:
            members = {member.name: member for member in tar.getmembers() if member.isfile()}
        assert {name: (member.mode, member.uid, member.gid) for name, member in members.items()} == {
            "out/run": (0o755, 0, 0),
            "out/setuid": (0o755, 0, 0),
        }

    @pytest.mark.description("An archive that unpacks past the limit is stopped while reading.")
    def test_an_archive_that_unpacks_past_the_limit_is_stopped_while_reading(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(separate_verifier, "MAX_UNPACKED_BYTES", 10_000)
        archive = tmp_path / "bomb.tar.gz"
        write_archive(archive, {"out/zeros": bytes(1_000_000)})
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        with pytest.raises(SeparateVerifierError, match="more than this run will carry"):
            clean_archive(archive, "out", "/work/out", scratch)

    @pytest.mark.description("A truncated archive is an error and not a crash.")
    def test_a_truncated_archive_is_an_error_and_not_a_crash(self, tmp_path: Path) -> None:
        archive = tmp_path / "cut.tar.gz"
        write_archive(archive, {"out/data": os.urandom(5000)})
        archive.write_bytes(archive.read_bytes()[:-200])
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        with pytest.raises(SeparateVerifierError, match="can't be carried"):
            clean_archive(archive, "out", "/work/out", scratch)

    @pytest.mark.description("A download past the limit is stopped.")
    async def test_a_download_past_the_limit_is_stopped(
        self, shell: Sandbox, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(separate_verifier, "MAX_ARTIFACT_BYTES", 100)
        big = tmp_path / "big.tar.gz"
        big.write_bytes(os.urandom(1000))
        path = str(big)
        staged = [
            StagedArtifact("out.tar.gz", f"{tmp_path}/work/out", path, "main", partial(shell.stream_download, path))
        ]

        with pytest.raises(SeparateVerifierError, match="is over 100 bytes"):
            await _carry_artifacts(shell, staged, [])
