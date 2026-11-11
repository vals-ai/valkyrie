import posixpath
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any, NamedTuple, TypeVar

from benchmark_service import (
    ComposeSandbox,
    ComposeSource,
    DockerfileSource,
    ImageSource,
    SandboxProvider,
    SandboxProviders,
    SandboxRecoveryAttempt,
    run_with_sandbox_recovery,
)
from benchmark_service.sandbox import SandboxProviderConfig
from benchmark_service.sandbox.types import Sandbox, SandboxSource
from benchmark_service.schemas import (
    ChildEgress,
    JsonValue,
    MonitoringRequirementsResponse,
    RetrieveTaskResponse,
    SandboxCreatedData,
    SetupTaskResponse,
    TaskFilter,
    VerifyTaskIdsResponse,
)

from tracker.services.datasets.compose import runs_compose, start_compose_runtime
from tracker.services.datasets.config import MAIN_SERVICE, StepConfig, TaskConfig, VerifierConfig
from tracker.services.datasets.artifacts import (
    CONVENTION_ARTIFACT,
    ArtifactSink,
    discard_staged,
    run_collect_hooks,
    stage_artifacts,
    upload_artifacts,
)
from tracker.services.datasets.env import SecretReader, resolve_env
from tracker.services.datasets.folder import (
    ENVIRONMENT_DIR,
    INSTRUCTION_FILE,
    SandboxKind,
    read_instruction,
    sandbox_kind,
)
from tracker.services.datasets.grading import ARTIFACTS_DIR, OPEN_LOG_DIRS, VerifierResultError, grade_task, upload_dir
from tracker.services.datasets.healthcheck import wait_until_healthy
from tracker.services.datasets.image_config import dockerfile_base_image, image_workdir
from tracker.services.datasets.oracle import SOLUTION_DIR, SOLUTION_REMOTE, SOLVE_SCRIPT
from tracker.services.datasets.ref import DatasetRef, LocalDataset
from tracker.services.datasets.retrieve_task import TaskFolderResponse, retrieve_task_response
from tracker.services.datasets.rewards import extract_rewards
from tracker.services.datasets.separate_verifier import Layers, grade_separately, verifier_runs_compose
from tracker.services.datasets.source import ArchiveSource, LocalFolderSource, TaskSource
from tracker.services.datasets.steps import (
    STEP_TESTS,
    STEPS_DIR,
    StepOutcome,
    below_min_reward,
    merged_tests,
    prepare_step,
    step_agent,
    step_config,
    step_dir,
    step_layers,
    step_verifier,
)
from tracker.services.datasets.validate import validate_task
from tracker.runtime.storage import ObjectStore
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.sandbox import create_agent_error_redactor, runtime_sandbox, working_directory_command

_Result = TypeVar("_Result")
TESTS_PART = "tests"


class TaskSelectionError(ValueError):
    pass


class InvalidTaskError(ValueError):
    pass


PROCESS_CWD_COMMAND = "readlink /proc/1/cwd"
UNREADABLE_IMAGE_CWDS = ("", "/")
UNDECLARED_IMAGE_CWDS = (*UNREADABLE_IMAGE_CWDS, "/root")


class ResolvedStep(NamedTuple):
    instruction: str
    verifier_env: dict[str, str]
    verifier_sandbox_env: dict[str, str]


class ResolvedTask(NamedTuple):
    response: TaskFolderResponse
    instruction: str
    verifier_env: dict[str, str]
    verifier_sandbox_env: dict[str, str]
    secret_values: list[str]
    compose_verifier: bool
    steps: dict[str, ResolvedStep]


class DatasetTaskService:
    def __init__(
        self, source: TaskSource, read_secret: SecretReader | None = None, artifact_sink: ArtifactSink | None = None
    ) -> None:
        self._sandbox_providers = SandboxProviders()
        self.source = source
        self._read_secret = read_secret
        self._artifact_sink = artifact_sink
        self._tasks: dict[str, ResolvedTask] = {}
        self._image_workdirs: dict[str, str | None] = {}
        self._build_contexts = AsyncExitStack()

    async def verify_task_ids(
        self, task_ids: list[str] | None, slice_str: str | None, dataset: str | None = None
    ) -> VerifyTaskIdsResponse:
        available = await self.source.task_ids()
        if not available:
            raise TaskSelectionError("The dataset has no tasks")
        if task_ids:
            missing = [task_id for task_id in dict.fromkeys(task_ids) if task_id not in available]
            if missing:
                raise TaskSelectionError(f"Tasks not found in the dataset: {', '.join(missing)}")
            return VerifyTaskIdsResponse(task_ids=list(dict.fromkeys(task_ids)))
        if slice_str:
            try:
                selected = available[TaskFilter(slice_str=slice_str).parse_slice()]
            except ValueError as error:
                raise TaskSelectionError(f"Invalid slice {slice_str!r}: {error}") from error
            if not selected:
                raise TaskSelectionError(f"The slice {slice_str!r} selects no tasks")
            return VerifyTaskIdsResponse(task_ids=selected)
        return VerifyTaskIdsResponse(task_ids=available)

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        return (await self._task(task_id)).response

    async def _task(self, task_id: str) -> ResolvedTask:
        if task_id not in self._tasks:
            async with self.source.open_task(task_id) as task_dir:
                report = validate_task(task_dir)
                if report.config is None or not report.ok:
                    raise InvalidTaskError(f"Task {task_id!r} is not valid: {'; '.join(report.errors)}")
                build_dir = task_dir
                async with AsyncExitStack() as pending:
                    if sandbox_kind(task_dir, report.config) in (SandboxKind.DOCKERFILE, SandboxKind.COMPOSE):
                        build_dir = await pending.enter_async_context(self.source.open_task(task_id, [ENVIRONMENT_DIR]))
                    response = retrieve_task_response(build_dir, report.config)
                    env, secret_env = await resolve_env(
                        response.env, self._read_secret, f"Task {task_id!r} [environment].env"
                    )
                    verifier_env, sandbox_env, secret_values = await self._resolve_grader(
                        f"Task {task_id!r}", report.config.verifier
                    )
                    secret_values.extend(env[name] for name in secret_env)
                    steps: dict[str, ResolvedStep] = {}
                    for step in report.config.steps:
                        step_env, step_sandbox_env, step_secrets = await self._resolve_grader(
                            f"Task {task_id!r} step {step.name!r}", step_verifier(report.config, step)
                        )
                        instruction = read_instruction(step_dir(task_dir, step) / INSTRUCTION_FILE)
                        steps[step.name] = ResolvedStep(instruction, step_env, step_sandbox_env)
                        secret_values.extend(step_secrets)
                    instruction_path = task_dir / INSTRUCTION_FILE
                    self._tasks[task_id] = ResolvedTask(
                        response.model_copy(update={"env": env, "secret_env": secret_env}),
                        read_instruction(instruction_path) if instruction_path.is_file() else "",
                        verifier_env,
                        sandbox_env,
                        secret_values,
                        verifier_runs_compose(report.config, task_dir),
                        steps,
                    )
                    self._build_contexts.push_async_exit(pending.pop_all())
        return self._tasks[task_id]

    async def _resolve_grader(
        self, scope: str, grader: VerifierConfig
    ) -> tuple[dict[str, str], dict[str, str], list[str]]:
        env, from_secrets = await resolve_env(grader.env, self._read_secret, f"{scope} [verifier].env")
        sandbox_env, sandbox_secrets = await resolve_env(
            grader.environment.env if grader.environment else {},
            self._read_secret,
            f"{scope} [verifier.environment].env",
        )
        return (
            env,
            sandbox_env,
            [*(env[name] for name in from_secrets), *(sandbox_env[name] for name in sandbox_secrets)],
        )

    async def resume_evaluation(self, task_id: str, **_options: Any) -> Any:
        raise InvalidSandboxConfigurationError(f"Task {task_id} of a dataset has no evaluation to resume")

    def get_sandbox_provider(self, provider: SandboxProviderConfig) -> SandboxProvider:
        return self._sandbox_providers.get(provider)

    async def run_with_sandbox_recovery(
        self,
        task_id: str,
        run_id: str,
        operation: Callable[[SandboxRecoveryAttempt], Awaitable[_Result]],
        *,
        dataset: str | None = None,
        retryable_attempt_errors: tuple[type[Exception], ...] = (),
        default_max_attempts: int = 1,
        retry_delay_s: float = 2.0,
        on_retry: Callable[[SandboxRecoveryAttempt, Exception], Awaitable[None] | None] | None = None,
        attempt_id: str | None = None,
        email: str | None = None,
    ) -> _Result:
        return await run_with_sandbox_recovery(
            lambda: self.retrieve_task(task_id, dataset=dataset),
            task_id,
            run_id,
            operation,
            retryable_attempt_errors=retryable_attempt_errors,
            default_max_attempts=default_max_attempts,
            retry_delay_s=retry_delay_s,
            on_retry=on_retry,
            attempt_id=attempt_id,
            email=email,
        )

    async def __aenter__(self) -> "DatasetTaskService":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        async with self._build_contexts:
            await self._sandbox_providers.close()

    async def setup_task(
        self,
        task_id: str,
        instance_id: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        child_egress: dict[str, ChildEgress] | None = None,
    ) -> SetupTaskResponse:
        task = await self._task(task_id)
        outer = await self._sandbox(instance_id, sandbox_provider)
        agent_compose = isinstance(task.response.source, ComposeSource) and task.response.compose is not None
        if (agent_compose or task.compose_verifier) and not runs_compose(sandbox_provider):
            subject = "brings a compose file" if agent_compose else "grades in a compose file"
            raise InvalidSandboxConfigurationError(
                f"Task {task_id!r} {subject}, which needs Docker-in-Docker; "
                "only the daytona sandbox provider and the modal sandbox provider on its vm runtime run it"
            )
        if isinstance(task.response.source, ComposeSource) and task.response.compose is not None:
            await start_compose_runtime(outer, task.response.source, task.response.compose, task.response.env)
        sandbox = runtime_sandbox(outer, task.response.source)
        await sandbox.exec(OPEN_LOG_DIRS)
        if task.response.healthcheck is not None:
            await wait_until_healthy(sandbox, task.response.healthcheck)
        if not task.response.steps:
            await sandbox.exec(working_directory_command(task.response.cwd, task.response.agent_user))
            await sandbox.upload_file(task.response.problem_path, task.instruction.encode())
        return SetupTaskResponse(status="ok")

    async def use_image_workdir(
        self, task_id: str, instance_id: str, sandbox_provider: SandboxProviderConfig | None = None
    ) -> TaskFolderResponse:
        task = await self._task(task_id)
        if not task.response.cwd_from_image:
            return task.response
        result = await (await self._sandbox(instance_id, sandbox_provider)).exec(PROCESS_CWD_COMMAND)
        image_cwd = result.output.strip() if result.exit_code == 0 else ""
        if image_cwd in UNREADABLE_IMAGE_CWDS:
            image_cwd = await self._registry_workdir(task.response.source) or ""
        cwd = image_cwd if image_cwd not in UNDECLARED_IMAGE_CWDS else task.response.cwd
        response = task.response.model_copy(
            update={"cwd": cwd, "problem_path": f"{cwd}/{INSTRUCTION_FILE}", "cwd_from_image": False}
        )
        self._tasks[task_id] = task._replace(response=response)
        return response

    async def _registry_workdir(self, source: SandboxSource) -> str | None:
        match source:
            case ImageSource(image=image):
                pass
            case DockerfileSource(dockerfile=dockerfile):
                image = dockerfile_base_image(Path(dockerfile))
            case _:
                image = None
        if image is None:
            return None
        if image not in self._image_workdirs:
            self._image_workdirs[image] = await image_workdir(image)
        return self._image_workdirs[image]

    async def prepare_step(
        self,
        task_id: str,
        instance_id: str,
        step_name: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
    ) -> None:
        task = await self._task(task_id)
        outer = await self._sandbox(instance_id, sandbox_provider)
        async with self.source.open_task(task_id, [f"{STEPS_DIR}/{step_name}"]) as task_dir:
            config = self._config(task_id, task_dir)
            step = _find_step(config, step_name)
            await prepare_step(
                runtime_sandbox(outer, task.response.source),
                step,
                step_dir(task_dir, step),
                task.steps[step.name].instruction,
                task.response.problem_path,
                task.response.cwd,
                step_agent(config, step).user,
                lambda message: on_message(message) if on_message is not None else None,
            )

    async def upload_solution(
        self,
        task_id: str,
        instance_id: str,
        step_name: str | None = None,
        sandbox_provider: SandboxProviderConfig | None = None,
    ) -> tuple[dict[str, str], list[str]]:
        task = await self._task(task_id)
        step_solution = f"{STEPS_DIR}/{step_name}/{SOLUTION_DIR}" if step_name else None
        async with self.source.open_task(
            task_id, [SOLUTION_DIR, *([step_solution] if step_solution else [])]
        ) as task_dir:
            config = self._config(task_id, task_dir)
            solution = step_solution if step_solution and (task_dir / step_solution).is_dir() else SOLUTION_DIR
            if not (task_dir / solution / SOLVE_SCRIPT).is_file():
                raise InvalidTaskError(f"Task {task_id!r} has no {solution}/{SOLVE_SCRIPT} for the oracle to run")
            agent = runtime_sandbox(await self._sandbox(instance_id, sandbox_provider), task.response.source)
            await agent.exec(f"rm -rf {SOLUTION_REMOTE}")
            await upload_dir(agent, task_dir / solution, SOLUTION_REMOTE)
            await agent.exec(f"chmod -R a+rX {SOLUTION_REMOTE}")
        env, from_secrets = await resolve_env(
            config.solution.env, self._read_secret, f"Task {task_id!r} [solution].env"
        )
        return env, [env[name] for name in from_secrets]

    async def grade_step(
        self,
        task_id: str,
        instance_id: str,
        step_name: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
        on_sandbox_created: Callable[[SandboxCreatedData], None] | None = None,
    ) -> StepOutcome:
        provider = self._provider(sandbox_provider)
        outer = await provider.get_sandbox(instance_id)
        task = await self._task(task_id)
        parts = [TESTS_PART, f"{STEPS_DIR}/{step_name}/{STEP_TESTS}"]
        async with self.source.open_task(task_id, parts) as task_dir:
            config = self._config(task_id, task_dir)
            step = _find_step(config, step_name)
            own_tests = step_dir(task_dir, step) / STEP_TESTS
            with merged_tests(task_dir / TESTS_PART, own_tests) as tests_dir:
                resolved = task.steps[step.name]
                try:
                    result = await self._grade(
                        task_id,
                        step_config(config, step),
                        task,
                        tests_dir,
                        resolved.verifier_env,
                        resolved.verifier_sandbox_env,
                        outer,
                        provider,
                        on_message,
                        on_sandbox_created,
                        folder=f"{STEPS_DIR}/{step.name}/artifacts",
                        reset_tests=True,
                        layers=step_layers(config, step, task_dir),
                    )
                except VerifierResultError as error:
                    return StepOutcome(step.name, None, problem=str(error))
        rewards = extract_rewards(result)
        stopped = below_min_reward(step, rewards) if rewards is not None else None
        return StepOutcome(step.name, rewards, stopped, metadata=result.get("metadata"))

    async def grading_requirements(self, task_id: str, dataset: str | None = None) -> list[str]:
        return []

    async def monitoring_requirements(
        self, task_ids: list[str], dataset: str | None = None
    ) -> MonitoringRequirementsResponse:
        return MonitoringRequirementsResponse()

    def _config(self, task_id: str, task_dir: Path) -> TaskConfig:
        report = validate_task(task_dir)
        if report.config is None or not report.ok:
            raise InvalidTaskError(f"Task {task_id!r} is not valid: {'; '.join(report.errors)}")
        return report.config

    async def evaluate_instance(
        self,
        task_id: str,
        instance_id: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
        on_sandbox_created: Callable[[SandboxCreatedData], None] | None = None,
        child_egress: dict[str, ChildEgress] | None = None,
        grading_token: str | None = None,
    ) -> JsonValue:
        provider = self._provider(sandbox_provider)
        outer = await provider.get_sandbox(instance_id)
        task = await self._task(task_id)
        if task.response.steps:
            raise InvalidTaskError(f"Task {task_id!r} has steps, which are graded one at a time")
        async with self.source.open_task(task_id, [TESTS_PART]) as task_dir:
            config = self._config(task_id, task_dir)
            return await self._grade(
                task_id,
                config,
                task,
                task_dir / TESTS_PART,
                task.verifier_env,
                task.verifier_sandbox_env,
                outer,
                provider,
                on_message,
                on_sandbox_created,
            )

    async def _grade(
        self,
        task_id: str,
        config: TaskConfig,
        task: ResolvedTask,
        tests_dir: Path,
        verifier_env: dict[str, str],
        verifier_sandbox_env: dict[str, str],
        outer: Sandbox,
        provider: SandboxProvider,
        on_message: Callable[[str], None] | None,
        on_created: Callable[[SandboxCreatedData], None] | None,
        *,
        folder: str = "artifacts",
        reset_tests: bool = False,
        layers: Layers | None = None,
    ) -> dict[str, Any]:
        def say(message: str) -> None:
            if on_message is not None:
                on_message(message)

        redact = create_agent_error_redactor(task.secret_values)
        services = _services(outer, task.response.source, config)
        sandbox = services[MAIN_SERVICE]

        await run_collect_hooks(services, config.verifier.collect, say, task.response.cwd)
        artifacts = list(config.artifacts)
        declared = any(
            posixpath.normpath(artifact.source) == ARTIFACTS_DIR and artifact.service in (None, MAIN_SERVICE)
            for artifact in artifacts
        )
        if not declared and (await sandbox.exec(f"find {ARTIFACTS_DIR} -mindepth 1 -print -quit")).output.strip():
            artifacts.append(CONVENTION_ARTIFACT)
        staged, skipped = await stage_artifacts(services, artifacts, task.response.cwd) if artifacts else ([], [])
        try:
            for artifact in skipped:
                say(f"Artifact {artifact.source} was skipped: {artifact.reason}")
            if self._artifact_sink is not None:
                await upload_artifacts(task_id, services, staged, self._artifact_sink, say, folder)
            if not config.verifier.is_separate:
                await discard_staged(services, staged)
                return await grade_task(
                    task_id,
                    config,
                    verifier_env,
                    tests_dir,
                    sandbox,
                    on_message,
                    redact=redact,
                    reset_tests=reset_tests,
                )
            return await grade_separately(
                task_id,
                config,
                verifier_env,
                verifier_sandbox_env,
                tests_dir,
                services,
                task.response.source,
                task.response.compose,
                provider,
                staged,
                skipped,
                on_message,
                on_created,
                redact,
                layers,
            )
        finally:
            await discard_staged(services, staged)

    def _provider(self, config: SandboxProviderConfig | None) -> SandboxProvider:
        if config is None:
            raise ValueError("A sandbox provider is needed to reach the task's sandbox")
        return self.get_sandbox_provider(config)

    async def _sandbox(self, instance_id: str, config: SandboxProviderConfig | None) -> Sandbox:
        return await self._provider(config).get_sandbox(instance_id)


def _find_step(config: TaskConfig, name: str) -> StepConfig:
    for step in config.steps:
        if step.name == name:
            return step
    raise InvalidTaskError(f"The task has no step named {name!r}")


def _services(outer: Sandbox, source: SandboxSource, config: TaskConfig) -> dict[str, Sandbox]:
    if not isinstance(source, ComposeSource):
        return {MAIN_SERVICE: outer}
    named = {MAIN_SERVICE, *(hook.service for hook in config.verifier.collect)}
    named |= {artifact.service for artifact in config.artifacts if artifact.service}
    return {name: ComposeSandbox(outer, source.model_copy(update={"service": name})) for name in named}


def dataset_service(
    ref: DatasetRef,
    objects: ObjectStore,
    read_secret: SecretReader | None = None,
    artifact_sink: ArtifactSink | None = None,
    tasks: Mapping[str, str] | None = None,
) -> DatasetTaskService:
    if isinstance(ref, LocalDataset):
        return DatasetTaskService(LocalFolderSource(Path(ref.path)), read_secret, artifact_sink)
    if ref.dataset_id is None or tasks is None:
        raise ValueError(f"The dataset {ref.benchmark}/{ref.dataset} has not been resolved to a version")
    return DatasetTaskService(ArchiveSource(objects, ref.dataset_id, tasks), read_secret, artifact_sink)
