import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from uuid import UUID

from benchmark_service import SandboxProviderConfig, SandboxCreatedData
from benchmark_service.schemas import EgressPolicy

from tracker.clients.benchmark_service import run_benchmark_service_websocket
from tracker.domain import AgentCausedExitReason, FailurePhase
from tracker.services.datasets.healthcheck import HealthcheckError
from tracker.services.datasets.retrieve_task import StepPlan, TaskFolderResponse
from tracker.clients.dataset_tasks import DatasetTaskService
from tracker.services.datasets.steps import StepOutcome, StepSetupError
from tracker.exceptions import AgentRunFailedError, OutputArtifactError
from tracker.external_service_gateway import (
    ExternalServiceDeadlineController,
    GenerationAccountingSummary,
    combine_accounting,
)
from tracker.logging import get_logger
from tracker.logging.events import LogEvent
from tracker.runtime.task_logs import TaskLogBuffer

logger = get_logger(__name__)

AccountingSink = Callable[[GenerationAccountingSummary], Awaitable[None]]
StepGeneration = Callable[
    [StepPlan, ExternalServiceDeadlineController, AccountingSink],
    Awaitable[tuple[AgentCausedExitReason | None, float]],
]


@dataclass
class StepRun:
    service: DatasetTaskService
    task: TaskFolderResponse
    task_id: str
    benchmark_id: UUID
    sandbox_id: str
    sandbox_name: str
    sandbox_provider: SandboxProviderConfig
    task_logs: TaskLogBuffer
    generate: StepGeneration
    persist_accounting: AccountingSink
    on_sandbox_created: Callable[[SandboxCreatedData], None]
    set_phase: Callable[[FailurePhase], None]
    redact: Callable[[str], str]
    restrict_egress: Callable[[EgressPolicy], Awaitable[None]] | None


@dataclass
class StepsResult:
    outcomes: list[StepOutcome] = field(default_factory=list)
    abnormal_exit: AgentCausedExitReason | None = None
    agent_time: float = 0.0
    evaluation_time: float = 0.0
    agent_failure: Exception | None = None


async def run_steps(run: StepRun, controller: ExternalServiceDeadlineController) -> StepsResult:
    result = StepsResult()
    accounting: list[GenerationAccountingSummary] = []
    counter = controller.snapshot
    log_output = run.task_logs.write

    async def persist_accounting(summary: GenerationAccountingSummary) -> None:
        await run.persist_accounting(combine_accounting([*accounting, summary]))

    for index, step in enumerate(run.task.steps):
        if run.restrict_egress is not None:
            await run.restrict_egress(run.task.egress.setup_task)
        run.set_phase(FailurePhase.SETUP if index == 0 else FailurePhase.EVALUATION)
        run.task_logs.last_log_time = time.monotonic()
        try:
            await run_benchmark_service_websocket(
                run.service.prepare_step(
                    run.task_id,
                    run.sandbox_id,
                    step.name,
                    on_message=log_output,
                    sandbox_provider=run.sandbox_provider,
                )
            )
        except (StepSetupError, HealthcheckError) as error:
            problem = run.redact(str(error))
            if index == 0:
                raise type(error)(problem) from None
            log_output(f"[WARNING] Step {step.name!r} could not start, so the task stops here: {problem}")
            result.outcomes.append(StepOutcome(step.name, None, problem=problem))
            break
        step_controller = replace(
            controller,
            base_allowance_seconds=step.agent_timeout,
            snapshot=counter,
            baseline_overhead_ms=counter.cumulative_neutral_overhead_ms if counter is not None else 0,
        )
        try:
            step_exit, step_run_time = await run.generate(step, step_controller, persist_accounting)
        except (AgentRunFailedError, OutputArtifactError) as error:
            if index == 0:
                raise
            log_output(f"[WARNING] Step {step.name!r} failed, so the task stops here: {error}")
            result.outcomes.append(StepOutcome(step.name, None, problem=str(error)))
            result.agent_failure = error
            result.agent_time += step_controller.active_elapsed_seconds
            break
        counter = step_controller.snapshot
        accounting.append(step_controller.summary())
        result.agent_time += step_run_time
        result.abnormal_exit = result.abnormal_exit or step_exit
        run.set_phase(FailurePhase.EVALUATION)
        evaluation_started = time.perf_counter()
        logger.info(
            "task.evaluation.start",
            extra={
                "event": LogEvent.TASK_EVALUATION_START,
                "benchmark_id": str(run.benchmark_id),
                "task_id": run.task_id,
                "sandbox_id": run.sandbox_id,
                "sandbox_name": run.sandbox_name,
                "step": step.name,
            },
        )
        if run.restrict_egress is not None:
            await run.restrict_egress(step.egress_evaluation)
        run.task_logs.last_log_time = time.monotonic()
        outcome = await run_benchmark_service_websocket(
            run.service.grade_step(
                run.task_id,
                run.sandbox_id,
                step.name,
                on_message=log_output,
                on_sandbox_created=run.on_sandbox_created,
                sandbox_provider=run.sandbox_provider,
            )
        )
        result.evaluation_time += time.perf_counter() - evaluation_started
        if outcome.problem is not None:
            outcome = outcome._replace(problem=run.redact(outcome.problem))
        result.outcomes.append(outcome)
        if outcome.problem is not None:
            log_output(f"[WARNING] Step {step.name!r} could not be graded, so the task stops here")
            break
        if outcome.stopped is not None:
            log_output(f"Step {step.name!r} stopped the task: {outcome.stopped}")
            break
    return result
