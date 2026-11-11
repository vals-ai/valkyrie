import shlex
import shutil
import tempfile
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NamedTuple, TypeVar

from benchmark_service.sandbox.types import Sandbox
from pydantic import BaseModel

from tracker.services.datasets.config import (
    AgentConfig,
    EnvironmentConfig,
    MultiStepRewardStrategy,
    StepConfig,
    TaskConfig,
    VerifierConfig,
    VerifierEnvironmentMode,
)
from tracker.services.datasets.grading import RESET_STEP_LOG_DIRS, exec_in_batches, upload_dir
from tracker.services.datasets.healthcheck import wait_until_healthy
from tracker.services.datasets.rewards import RewardMap
from tracker.services.datasets.user import run_as
from tracker.sandbox import working_directory_command

STEPS_DIR = "steps"
STEP_WORKDIR = "workdir"
STEP_SETUP_SCRIPT = "setup.sh"
STEP_TESTS = "tests"
SETUP_TIMEOUT_SEC = 600

Model = TypeVar("Model", bound=BaseModel)


class StepSetupError(RuntimeError):
    pass


def overlay(base: Model, override: Model) -> Model:
    return base.model_copy(update={name: getattr(override, name) for name in override.model_fields_set})


def step_agent(config: TaskConfig, step: StepConfig) -> AgentConfig:
    return overlay(config.agent, step.agent)


def step_verifier(config: TaskConfig, step: StepConfig) -> VerifierConfig:
    own = step.verifier
    update: dict[str, Any] = {"env": {**config.verifier.env, **own.env}}
    if own.environment is not None and "environment_mode" not in own.model_fields_set:
        update["environment_mode"] = VerifierEnvironmentMode.SEPARATE
    return overlay(config.verifier, own).model_copy(update=update)


def step_config(config: TaskConfig, step: StepConfig) -> TaskConfig:
    own = {artifact.name for artifact in step.artifacts}
    return config.model_copy(
        update={
            "agent": step_agent(config, step),
            "verifier": step_verifier(config, step),
            "artifacts": [artifact for artifact in config.artifacts if artifact.name not in own] + step.artifacts,
            "steps": [],
        }
    )


def step_dir(task_dir: Path, step: StepConfig) -> Path:
    return task_dir / STEPS_DIR / step.name


def step_layers(config: TaskConfig, step: StepConfig, task_dir: Path) -> list[tuple[EnvironmentConfig | None, Path]]:
    return [
        (step.verifier.environment, step_dir(task_dir, step) / STEP_TESTS),
        (config.verifier.environment, task_dir / STEP_TESTS),
    ]


def below_min_reward(step: StepConfig, rewards: RewardMap) -> str | None:
    if step.min_reward is None:
        return None
    thresholds = step.min_reward if isinstance(step.min_reward, Mapping) else {"reward": step.min_reward}
    for name, threshold in thresholds.items():
        actual = rewards.get(name)
        if actual is None or actual < threshold:
            shown = "missing" if actual is None else f"{actual:g}"
            return f"{name} is {shown}, below the minimum {threshold:g}"
    return None


@contextmanager
def merged_tests(base: Path, step_tests: Path) -> Generator[Path]:
    with tempfile.TemporaryDirectory(prefix="step-tests-") as scratch:
        merged = Path(scratch) / STEP_TESTS
        if base.is_dir():
            shutil.copytree(base, merged)
        if step_tests.is_dir():
            shutil.copytree(step_tests, merged, dirs_exist_ok=True)
        yield merged


async def prepare_step(
    sandbox: Sandbox,
    step: StepConfig,
    step_folder: Path,
    instruction: str,
    problem_path: str,
    cwd: str,
    user: str | None,
    say: Callable[[str], None],
) -> None:
    await sandbox.exec(RESET_STEP_LOG_DIRS)
    await sandbox.exec(working_directory_command(cwd, user))
    workdir = step_folder / STEP_WORKDIR
    if workdir.is_dir():
        say(f"Copying the files of step {step.name!r} into {cwd}")
        await upload_dir(sandbox, workdir, cwd)
        if user is not None:
            files = sorted(f"{cwd.rstrip('/')}/{path.relative_to(workdir).as_posix()}" for path in workdir.rglob("*"))
            await exec_in_batches(sandbox, f"chown {shlex.quote(user)}", files)
    if (workdir / STEP_SETUP_SCRIPT).is_file():
        say(f"Running the setup of step {step.name!r}")
        script = f"{cwd.rstrip('/')}/{STEP_SETUP_SCRIPT}"
        command = f"bash {shlex.quote(script)}"
        result = await sandbox.exec(
            command if user is None else run_as(user, command), cwd=cwd, timeout=SETUP_TIMEOUT_SEC
        )
        if result.exit_code != 0:
            raise StepSetupError(
                f"The setup of step {step.name!r} exited with code {result.exit_code}: {result.output[-2000:]}"
            )
    if step.healthcheck is not None:
        await wait_until_healthy(sandbox, step.healthcheck)
    await sandbox.upload_file(problem_path, instruction.encode())


class StepOutcome(NamedTuple):
    name: str
    rewards: RewardMap | None
    stopped: str | None = None
    problem: str | None = None
    metadata: dict[str, Any] | None = None


def combine_steps(task_id: str, strategy: MultiStepRewardStrategy, outcomes: Sequence[StepOutcome]) -> dict[str, Any]:
    executed = [outcome.rewards for outcome in outcomes]
    graded = [rewards for rewards in executed if rewards is not None]
    keys = sorted({key for rewards in graded for key in rewards})
    if strategy == MultiStepRewardStrategy.FINAL:
        rewards = (executed[-1] if executed else None) or None
    else:
        rewards = {key: sum(r.get(key, 0.0) for r in graded) / len(graded) for key in keys} if graded and keys else None
    if rewards is None:
        problems = "; ".join(f"{outcome.name}: {outcome.problem}" for outcome in outcomes if outcome.problem)
        subject = (
            "The last step of the task produced no reward"
            if strategy == MultiStepRewardStrategy.FINAL
            else "No step of the task produced a reward"
        )
        raise StepSetupError(f"{subject}{f' ({problems})' if problems else ''}")
    return {
        "task_id": task_id,
        "rewards": rewards,
        "verifier_result": {"rewards": rewards},
        "metadata": {
            "multi_step_reward_strategy": strategy.value,
            "steps": [outcome._asdict() for outcome in outcomes],
        },
    }
