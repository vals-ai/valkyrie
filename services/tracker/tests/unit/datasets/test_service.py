from benchmark_service import SandboxProviders
import asyncio
from collections.abc import AsyncIterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from benchmark_service import DockerfileSource, ImageSource, Resources
from benchmark_service.sandbox import DockerProviderConfig, ModalProviderConfig, SandboxProviderConfig

from tests.unit.datasets.support import FakeSandbox, OuterSandbox, store_archive, zip_bytes
from tracker.services.datasets import artifacts
from tracker.services.datasets.artifacts import STAGING_ROOT, ArtifactSink
from tracker.services.datasets.env import SecretReader
from tracker.services.datasets.grading import REWARD_TEXT_PATH, RESET_STEP_LOG_DIRS, VERIFIER_OUTPUT_PATH
from tracker.services.datasets.config import MultiStepRewardStrategy
from tracker.services.datasets.retrieve_task import TaskFolderResponse
from tracker.clients.dataset_tasks import DatasetTaskService, InvalidTaskError, TaskSelectionError
from tracker.services.datasets.source import ArchiveSource, LocalFolderSource, TaskSource
from tracker.exceptions import InvalidSandboxConfigurationError, SecretsError
from tracker.local.storage import FilesystemObjectStore

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
PROVIDER = DockerProviderConfig(type="docker")
DAYTONA = cast(SandboxProviderConfig, SimpleNamespace(type="daytona"))
MODAL_VM = ModalProviderConfig(runtime="vm", MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")
MODAL_GVISOR = ModalProviderConfig(runtime="gvisor", MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")
DOCKERFILE_TOML = "[environment]\ncpus = 1\nworkdir = '/app'"


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
            pytest.param(TASK_TOML, "/go", "/app", id="task-workdir-wins"),
            pytest.param("[environment]\ndocker_image = 'golang:1.22'", "/", "/go", id="registry-workdir"),
            pytest.param("[environment]\ndocker_image = 'python:3.12-slim'", "/", "/workspace", id="registry-none"),
            pytest.param("[environment]\ncpus = 1", "/", "/go", id="dockerfile-base-from-registry"),
        ],
    )
    async def test_the_instruction_lands_in_the_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, toml: str, process_cwd: str, expected_cwd: str
    ) -> None:
        task = write_task(tmp_path, "task-a", toml)
        if "docker_image" not in toml and toml != TASK_TOML:
            (task / "environment").mkdir()
            (task / "environment" / "Dockerfile").write_text("FROM golang:1.22\nRUN true\n")

        async def registry_workdir(image: str) -> str | None:
            return {"golang:1.22": "/go"}.get(image)

        monkeypatch.setattr("tracker.clients.dataset_tasks.image_workdir", registry_workdir)
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


@pytest.mark.description("Putting the instruction in the sandbox.")
class TestSetupTask:
    @pytest.mark.description("The instruction is uploaded to the problem path.")
    async def test_the_instruction_is_uploaded_to_the_problem_path(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox()
        service = service_for(LocalFolderSource(dataset), sandbox, monkeypatch)

        response = await service.setup_task("task-b", "sandbox-1", PROVIDER)

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

    @pytest.mark.description("A compose task is refused on a provider that cannot run docker in docker.")
    @pytest.mark.parametrize("provider", [PROVIDER, MODAL_GVISOR])
    async def test_a_compose_task_is_refused_on_a_provider_that_cannot_run_docker_in_docker(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch, provider: SandboxProviderConfig
    ) -> None:
        (dataset / "task-a" / "environment").mkdir()
        (dataset / "task-a" / "environment" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        outer = OuterSandbox()

        with pytest.raises(
            InvalidSandboxConfigurationError,
            match="needs Docker-in-Docker; only the daytona sandbox provider and the modal sandbox provider on its vm runtime",
        ):
            await service_for(LocalFolderSource(dataset), outer, monkeypatch).setup_task(
                "task-a", "sandbox-1", provider
            )

        assert outer.commands == []

    @pytest.mark.description(
        "A task that grades in a compose verifier is refused before its agent runs on such a provider."
    )
    @pytest.mark.parametrize("provider", [PROVIDER, MODAL_GVISOR])
    async def test_a_task_that_grades_in_a_compose_verifier_is_refused_before_its_agent_runs_on_such_a_provider(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch, provider: SandboxProviderConfig
    ) -> None:
        (dataset / "task-a" / "tests" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        (dataset / "task-a" / "task.toml").write_text(
            TASK_TOML.replace("[verifier]\n", "[verifier]\nenvironment_mode = 'separate'\n")
        )
        outer = OuterSandbox()

        with pytest.raises(
            InvalidSandboxConfigurationError, match="grades in a compose file, which needs Docker-in-Docker"
        ):
            await service_for(LocalFolderSource(dataset), outer, monkeypatch).setup_task(
                "task-a", "sandbox-1", provider
            )

        assert outer.commands == []

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
    @pytest.mark.description("The task is graded and its result returned.")
    async def test_the_task_is_graded_and_its_result_returned(
        self, dataset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        messages: list[str] = []

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER, messages.append
        )

        assert isinstance(result, dict)
        assert result["rewards"] == {"reward": 1.0}
        assert sandbox.files["/tests/data.txt"] == b"data"
        assert messages[0] == "Running the verifier for task-a"

    @pytest.mark.description("A grading token from the Tracker is accepted and the task is still graded.")
    async def test_a_grading_token_is_accepted(self, dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        result = await service_for(LocalFolderSource(dataset), sandbox, monkeypatch).evaluate_instance(
            "task-a", "sandbox-1", PROVIDER, grading_token="token"
        )

        assert isinstance(result, dict)
        assert result["rewards"] == {"reward": 1.0}

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

        async def grade_separately(*args: Any) -> dict[str, Any]:
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
        assert uploaded == [("task-a", "artifacts/app/out.tar.gz", b"archive")]
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
