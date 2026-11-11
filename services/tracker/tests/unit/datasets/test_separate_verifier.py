import io
import os
import tarfile
from collections.abc import Sequence
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
)
from benchmark_service.sandbox.created import AnnouncedSandboxProvider, notify_sandbox_created
from benchmark_service.schemas import SandboxCreatedData
from benchmark_service.sandbox.types import SandboxSource

from tests.unit.datasets.support import FakeSandbox, OuterSandbox, ShellSandbox
from tracker.services.datasets import artifacts, separate_verifier
from tracker.services.datasets.artifacts import SkippedArtifact, StagedArtifact, stage_artifacts
from tracker.services.datasets.compose import ComposeRuntime
from tracker.services.datasets.config import ArtifactConfig, TaskConfig
from tracker.services.datasets.grading import OPEN_LOG_DIRS, REWARD_TEXT_PATH, VerifierResultError
from tracker.services.datasets.separate_verifier import (
    SeparateVerifierError,
    VerifierEnvironment,
    _carry_artifacts,  # pyright: ignore[reportPrivateUsage]
    _clean_archive as clean_archive,  # pyright: ignore[reportPrivateUsage]
    grade_separately,
    verifier_environment,
)

AGENT_SOURCE = ImageSource(image="agent:1")


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
) -> dict[str, object]:
    return await grade_separately(
        "task-1",
        config,
        {},
        {},
        tests_dir,
        {"main": cast(Sandbox, agent or labelled_agent())},
        agent_source,
        agent_compose,
        cast(Any, provider),
        [],
        [],
        None,
        created.append if created is not None else None,
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
        assert provider.deleted == ["verifier-1"]

    @pytest.mark.description("The tests run in the verifiers workdir and otherwise the environments.")
    async def test_the_tests_run_in_the_verifiers_workdir_and_otherwise_the_environments(self, tests_dir: Path) -> None:
        own, inherited = verifier_sandbox(), verifier_sandbox()

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

        assert (own.cwds, inherited.cwds) == (["/grader"], ["/app"])

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

    @pytest.mark.description("A failed create is retried under a new name when it may work next time.")
    async def test_a_failed_create_is_retried_under_a_new_name_when_it_may_work_next_time(
        self, tests_dir: Path
    ) -> None:
        provider = FakeProvider([SandboxError("Sandbox name already exists"), verifier_sandbox()])

        await grade(TaskConfig.parse("[verifier]\nenvironment_mode = 'separate'"), provider, tests_dir)

        assert len({request.name for request in provider.requests}) == 2

    @pytest.mark.description("A create that cannot work is not retried.")
    async def test_a_create_that_cannot_work_is_not_retried(self, tests_dir: Path) -> None:
        provider = FakeProvider([SandboxError("image not found")])

        with pytest.raises(SandboxError, match="image not found"):
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
                {"main": cast(Sandbox, labelled_agent())},
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


def clean(archive: Path, name: str, scratch: Path) -> Path:
    return clean_archive(archive, name, f"/work/{name}", scratch)


@pytest.mark.description("What the agent leaves is carried to the verifier without trusting the archive.")
class TestCarryArtifacts:
    @pytest.mark.description("A folder arrives at its declared path with its modes.")
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

        await _carry_artifacts({"main": shell}, shell, staged, skipped)

        assert (out / "result.txt").read_text() == "42"
        assert os.access(out / "nested" / "run.sh", os.X_OK)
        assert (out / "alias").is_symlink() and os.readlink(out / "alias") == "result.txt"

    @pytest.mark.description("An artifact the agent never wrote is carried as nothing.")
    async def test_an_artifact_the_agent_never_wrote_is_carried_as_nothing(
        self, shell: Sandbox, tmp_path: Path
    ) -> None:
        staged, skipped = await stage_artifacts(
            {"main": shell}, [ArtifactConfig(source=str(tmp_path / "nope"))], str(tmp_path)
        )

        await _carry_artifacts({"main": shell}, shell, staged, skipped)

        assert not (tmp_path / "nope").exists()

    @pytest.mark.description("An artifact that could not be staged is an error not a zero.")
    async def test_an_artifact_that_could_not_be_staged_is_an_error_not_a_zero(self, shell: Sandbox) -> None:
        skipped = [SkippedArtifact("/app/big", "it is over 5 bytes", absent=False)]

        with pytest.raises(SeparateVerifierError, match="/app/big can't be carried: it is over 5 bytes"):
            await _carry_artifacts({"main": shell}, shell, [], skipped)

    @pytest.mark.description("The artifacts folder that cannot be carried is reported and grading goes on.")
    async def test_the_artifacts_folder_that_cannot_be_carried_is_reported_and_grading_goes_on(self) -> None:
        verifier = FakeSandbox(id="verifier-1")
        messages: list[str] = []

        await _carry_artifacts(
            {"main": cast(Sandbox, FakeSandbox())},
            cast(Sandbox, verifier),
            [],
            [SkippedArtifact("/logs/artifacts", "it holds too many files", False)],
            messages.append,
        )

        assert messages == ["The folder /logs/artifacts could not be carried to the verifier: it holds too many files"]

    @pytest.mark.description("The reward folder is wiped after the artifacts land.")
    async def test_the_reward_folder_is_wiped_after_the_artifacts_land(self) -> None:
        verifier = FakeSandbox(id="verifier-1")

        await _carry_artifacts({"main": cast(Sandbox, FakeSandbox())}, cast(Sandbox, verifier), [], [])

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
        staged = [StagedArtifact("out.tar.gz", str(tmp_path / "work" / "out"), str(archive), "main")]

        with pytest.raises(SeparateVerifierError, match=complaint):
            await _carry_artifacts({"main": shell}, shell, staged, [])

        assert not (tmp_path / "work").exists()

    @pytest.mark.description("What is carried is rebuilt with root ownership and no special modes.")
    def test_what_is_carried_is_rebuilt_with_root_ownership_and_no_special_modes(self, tmp_path: Path) -> None:
        archive = tmp_path / "agent.tar.gz"
        write_archive(archive, {"out/run": b"x", "out/setuid": b"y"}, modes={"out/run": 0o4755, "out/setuid": 0o2755})
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        rebuilt = clean_archive(archive, "out", "/work/out", scratch)

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
        staged = [StagedArtifact("out.tar.gz", str(tmp_path / "work" / "out"), str(big), "main")]

        with pytest.raises(SeparateVerifierError, match="is over 100 bytes"):
            await _carry_artifacts({"main": shell}, shell, staged, [])
