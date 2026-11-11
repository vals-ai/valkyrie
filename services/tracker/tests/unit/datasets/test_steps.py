from pathlib import Path
from typing import cast

import pytest
from benchmark_service import Sandbox
from benchmark_service.sandbox.types import ExecResult

from tests.unit.datasets.support import FakeSandbox
from tracker.services.datasets.config import MultiStepRewardStrategy, StepConfig, TaskConfig
from tracker.services.datasets.healthcheck import HealthcheckError
from tracker.services.datasets.steps import (
    StepOutcome,
    StepSetupError,
    below_min_reward,
    combine_steps,
    merged_tests,
    prepare_step,
    step_agent,
    step_config,
    step_verifier,
)

TASK = """
[agent]
timeout_sec = 600
user = "root"
[verifier]
timeout_sec = 60
env = { MODE = "base" }
[[artifacts]]
source = "/app/out"
[[steps]]
name = "one"
[[steps]]
name = "two"
artifacts = [{ source = "/app/step-two" }]
[steps.agent]
timeout_sec = 30
[steps.verifier]
timeout_sec = 5
"""


def two_steps() -> tuple[TaskConfig, StepConfig, StepConfig]:
    config = TaskConfig.parse(TASK)
    return config, config.steps[0], config.steps[1]


class RecordingSandbox(FakeSandbox):
    def __init__(self, *, setup_exit: int = 0) -> None:
        super().__init__()
        self.calls: list[tuple[str, str | None]] = []
        self._setup_exit = setup_exit

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.calls.append((command, cwd))
        if "setup.sh" in command:
            return ExecResult(exit_code=self._setup_exit, output="setup said no")
        return await super().exec(command, cwd=cwd, timeout=timeout)


@pytest.mark.description("A step changes only what it names.")
class TestStepSettings:
    @pytest.mark.description("The fields a step sets replace the tasks and the rest are kept.")
    def test_the_fields_a_step_sets_replace_the_tasks_and_the_rest_are_kept(self) -> None:
        config, one, two = two_steps()

        assert (step_agent(config, one).timeout_sec, step_agent(config, one).user) == (600, "root")
        assert (step_agent(config, two).timeout_sec, step_agent(config, two).user) == (30, "root")
        assert (step_verifier(config, two).timeout_sec, step_verifier(config, two).env) == (5, {"MODE": "base"})

    @pytest.mark.description("A steps verifier env adds to the tasks and wins on a clash.")
    def test_a_steps_verifier_env_adds_to_the_tasks_and_wins_on_a_clash(self) -> None:
        config = TaskConfig.parse(
            "[verifier]\nenv = { TOKEN = '${SECRET_TOKEN}', MODE = 'base' }\n"
            "[[steps]]\nname = 'one'\n[steps.verifier]\nenv = { MODE = 'one', EXTRA = 'x' }"
        )

        assert step_verifier(config, config.steps[0]).env == {"TOKEN": "${SECRET_TOKEN}", "MODE": "one", "EXTRA": "x"}

    @pytest.mark.description("A steps own environment makes it separate unless it says shared.")
    @pytest.mark.parametrize(
        ("task", "step", "separate"),
        [
            ("environment_mode = 'shared'", "[steps.verifier.environment]\ndocker_image = 'grader:1'", True),
            ("", "[steps.verifier.environment]\ndocker_image = 'grader:1'", True),
            ("environment_mode = 'separate'", "environment_mode = 'shared'", False),
            ("environment_mode = 'separate'", "timeout_sec = 5", True),
            ("environment_mode = 'shared'", "timeout_sec = 5", False),
        ],
    )
    def test_a_steps_own_environment_makes_it_separate_unless_it_says_shared(
        self, task: str, step: str, separate: bool
    ) -> None:
        config = TaskConfig.parse(f"[verifier]\n{task}\n[[steps]]\nname = 'one'\n[steps.verifier]\n{step}")

        assert step_verifier(config, config.steps[0]).is_separate is separate

    @pytest.mark.description("A steps config carries its settings and the tasks artifacts plus its own.")
    def test_a_steps_config_carries_its_settings_and_the_tasks_artifacts_plus_its_own(self) -> None:
        config, _, two = two_steps()

        stepped = step_config(config, two)

        assert stepped.agent.timeout_sec == 30
        assert stepped.verifier.timeout_sec == 5
        assert [artifact.source for artifact in stepped.artifacts] == ["/app/out", "/app/step-two"]
        assert stepped.steps == []

    @pytest.mark.description("A step artifact replaces a task artifact stored under the same name.")
    def test_a_step_artifact_replaces_a_task_artifact_stored_under_the_same_name(self) -> None:
        config = TaskConfig.parse(
            "artifacts = [{ source = '/a/out' }, { source = '/keep' }]\n"
            "[[steps]]\nname = 'one'\nartifacts = [{ source = '/b/out', destination = 'a/out' }]"
        )

        stepped = step_config(config, config.steps[0])

        assert [artifact.source for artifact in stepped.artifacts] == ["/keep", "/b/out"]


@pytest.mark.description("A step may ask for a reward before the task goes on.")
class TestBelowMinReward:
    @pytest.mark.description("Each named reward must reach its minimum.")
    @pytest.mark.parametrize(
        ("minimum", "rewards", "stopped"),
        [
            (None, {"reward": 0.0}, None),
            (0.5, {"reward": 0.5}, None),
            (0.5, {"reward": 0.4}, "reward is 0.4, below the minimum 0.5"),
            (0.5, {"other": 1.0}, "reward is missing, below the minimum 0.5"),
            ({"accuracy": 0.9, "safety": 1.0}, {"accuracy": 0.95, "safety": 1.0}, None),
            ({"accuracy": 0.9, "safety": 1.0}, {"accuracy": 0.95, "safety": 0.0}, "safety is 0, below the minimum 1"),
        ],
    )
    def test_each_named_reward_must_reach_its_minimum(
        self, minimum: float | dict[str, float] | None, rewards: dict[str, float], stopped: str | None
    ) -> None:
        step = StepConfig(name="one", min_reward=minimum)

        assert below_min_reward(step, rewards) == stopped


@pytest.mark.description("A step grades with the task's tests and then its own over them.")
class TestMergedTests:
    @pytest.mark.description("The steps files win over the tasks.")
    def test_the_steps_files_win_over_the_tasks(self, tmp_path: Path) -> None:
        base, own = tmp_path / "base", tmp_path / "own"
        (base / "lib").mkdir(parents=True)
        (own / "lib").mkdir(parents=True)
        (base / "test.sh").write_text("base")
        (base / "lib" / "helpers.py").write_text("helpers")
        (own / "test.sh").write_text("step")
        (own / "lib" / "extra.py").write_text("extra")

        with merged_tests(base, own) as merged:
            files = {
                path.relative_to(merged).as_posix(): path.read_text() for path in merged.rglob("*") if path.is_file()
            }

        assert files == {"test.sh": "step", "lib/helpers.py": "helpers", "lib/extra.py": "extra"}

    @pytest.mark.description("Either side may be missing.")
    def test_either_side_may_be_missing(self, tmp_path: Path) -> None:
        own = tmp_path / "own"
        own.mkdir()
        (own / "test.sh").write_text("step")

        with merged_tests(tmp_path / "absent", own) as only_step, merged_tests(own, tmp_path / "none") as only_base:
            assert (only_step / "test.sh").read_text() == "step"
            assert (only_base / "test.sh").read_text() == "step"


@pytest.mark.description("The files, the setup script and the healthcheck that make a step ready for the agent.")
class TestPrepareStep:
    async def prepare(
        self, tmp_path: Path, sandbox: RecordingSandbox, *, user: str | None = None, healthcheck: str = ""
    ) -> list[str]:
        step_folder = tmp_path / "steps" / "one"
        (step_folder / "workdir").mkdir(parents=True)
        (step_folder / "workdir" / "data.txt").write_text("data")
        (step_folder / "workdir" / "setup.sh").write_text("echo hi")
        step = StepConfig.model_validate(
            {"name": "one", **({"healthcheck": {"command": healthcheck, "retries": 1}} if healthcheck else {})}
        )
        said: list[str] = []

        await prepare_step(
            cast(Sandbox, sandbox), step, step_folder, "Do step one.", "/app/instruction.md", "/app", user, said.append
        )

        return said

    @pytest.mark.description("The files arrive then the setup runs in the workdir then the instruction is written.")
    async def test_the_files_arrive_then_the_setup_runs_in_the_workdir_then_the_instruction_is_written(
        self, tmp_path: Path
    ) -> None:
        sandbox = RecordingSandbox()

        said = await self.prepare(tmp_path, sandbox)

        assert sandbox.files["/app/data.txt"] == b"data"
        assert sandbox.files["/app/setup.sh"] == b"echo hi"
        assert ("bash /app/setup.sh", "/app") in sandbox.calls
        assert sandbox.files["/app/instruction.md"] == b"Do step one."
        assert said == ["Copying the files of step 'one' into /app", "Running the setup of step 'one'"]

    @pytest.mark.description("The step users gets the files and runs the setup.")
    async def test_the_step_users_gets_the_files_and_runs_the_setup(self, tmp_path: Path) -> None:
        sandbox = RecordingSandbox()

        await self.prepare(tmp_path, sandbox, user="agent")

        commands = [command for command, _ in sandbox.calls]
        assert any(command.startswith("chown agent ") and "/app/data.txt" in command for command in commands)
        assert any("su -m" in command and "bash /app/setup.sh" in command for command in commands)

    @pytest.mark.description("A failing setup stops the step with its output and writes no instruction.")
    async def test_a_failing_setup_stops_the_step_with_its_output_and_writes_no_instruction(
        self, tmp_path: Path
    ) -> None:
        sandbox = RecordingSandbox(setup_exit=3)

        with pytest.raises(StepSetupError, match=r"exited with code 3: setup said no"):
            await self.prepare(tmp_path, sandbox)

        assert "/app/instruction.md" not in sandbox.files

    @pytest.mark.description("The healthcheck runs after the setup and before the instruction.")
    async def test_the_healthcheck_runs_after_the_setup_and_before_the_instruction(self, tmp_path: Path) -> None:
        sandbox = RecordingSandbox()

        await self.prepare(tmp_path, sandbox, healthcheck="ready?")

        commands = [command for command, _ in sandbox.calls]
        assert commands.index("ready?") > commands.index("bash /app/setup.sh")
        assert "/app/instruction.md" in sandbox.files

    @pytest.mark.description("A step that never becomes healthy stops before the agent.")
    async def test_a_step_that_never_becomes_healthy_stops_before_the_agent(self, tmp_path: Path) -> None:
        class Unhealthy(RecordingSandbox):
            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                if command == "ready?":
                    return ExecResult(exit_code=1, output="no")
                return await super().exec(command, cwd=cwd, timeout=timeout)

        sandbox = Unhealthy()

        with pytest.raises(HealthcheckError):
            await self.prepare(tmp_path, sandbox, healthcheck="ready?")

        assert "/app/instruction.md" not in sandbox.files


def graded(name: str, reward: float, **extra: float) -> StepOutcome:
    return StepOutcome(name, {"reward": reward, **extra}, metadata={"verifier_exit_code": 0})


@pytest.mark.description("The task's reward from the rewards of the steps that ran.")
class TestCombineSteps:
    @pytest.mark.description("The mean strategy averages each key over the graded steps with a missing key as zero.")
    def test_the_mean_strategy_averages_each_key_over_the_graded_steps_with_a_missing_key_as_zero(self) -> None:
        outcomes = [graded("one", 1.0, speed=2.0), graded("two", 0.5), StepOutcome("three", None, problem="no")]

        result = combine_steps("task-a", MultiStepRewardStrategy.MEAN, outcomes)

        assert result["rewards"] == {"reward": 0.75, "speed": 1.0}
        assert result["verifier_result"] == {"rewards": result["rewards"]}
        assert result["task_id"] == "task-a"

    @pytest.mark.description("A step that never started keeps its problem in the result.")
    def test_a_step_that_never_started_keeps_its_problem_in_the_result(self) -> None:
        outcomes = [graded("one", 1.0), StepOutcome("two", None, problem="setup failed")]

        result = combine_steps("task-a", MultiStepRewardStrategy.MEAN, outcomes)

        assert result["rewards"] == {"reward": 1.0}
        assert result["metadata"]["steps"][1] == {
            "name": "two",
            "rewards": None,
            "stopped": None,
            "problem": "setup failed",
            "metadata": None,
        }

    @pytest.mark.description("The final strategy takes the last executed steps rewards.")
    def test_the_final_strategy_takes_the_last_executed_steps_rewards(self) -> None:
        outcomes = [graded("one", 1.0), graded("two", 0.25)]

        assert combine_steps("task-a", MultiStepRewardStrategy.FINAL, outcomes)["rewards"] == {"reward": 0.25}

    @pytest.mark.description("Steps that leave no reward to count are an error.")
    @pytest.mark.parametrize(
        ("strategy", "outcomes", "message"),
        [
            (
                MultiStepRewardStrategy.FINAL,
                [graded("one", 1.0), StepOutcome("two", None, problem="setup failed")],
                r"two: setup failed",
            ),
            (MultiStepRewardStrategy.FINAL, [graded("one", 1.0), StepOutcome("two", {})], r"produced no reward$"),
            (MultiStepRewardStrategy.MEAN, [StepOutcome("one", {})], r"produced a reward$"),
        ],
    )
    def test_steps_that_leave_no_reward_to_count_are_an_error(
        self, strategy: MultiStepRewardStrategy, outcomes: list[StepOutcome], message: str
    ) -> None:
        with pytest.raises(StepSetupError, match=message):
            combine_steps("task-a", strategy, outcomes)

    @pytest.mark.description("Each steps stop reason and grader metadata are kept.")
    def test_each_steps_stop_reason_and_grader_metadata_are_kept(self) -> None:
        stopped = StepOutcome("one", {"reward": 0.0}, "below the minimum", metadata={"verifier_exit_code": 1})

        (step,) = combine_steps("task-a", MultiStepRewardStrategy.MEAN, [stopped])["metadata"]["steps"]

        assert step == {
            "name": "one",
            "rewards": {"reward": 0.0},
            "stopped": "below the minimum",
            "problem": None,
            "metadata": {"verifier_exit_code": 1},
        }
