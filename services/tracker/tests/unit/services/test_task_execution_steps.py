import socket
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from benchmark_service import DockerProviderConfig, ImageSource, Resources
from benchmark_service.schemas import EgressPolicy, RetrieveTaskResponse
from sqlmodel import Session, select

from tests.unit.services.task_execution_support import (
    create_task_environment,
    finalize_agent_generation,
    run_process_task,
)
from tracker.database.models import ErrorResult, TaskBreakdown
from tracker.domain import FailureCategory, TaskStatus
from tracker.types import AgentContractRequest
from tracker.services.failures import FailurePhase
from tracker.services.datasets.config import MultiStepRewardStrategy
from tracker.services.datasets.healthcheck import HealthcheckError
from tracker.services.datasets.oracle import ORACLE_CONTRACT
from tracker.services.datasets.retrieve_task import StepPlan, TaskFolderResponse
from tracker.clients.dataset_tasks import DatasetTaskService
from tracker.services.datasets.source import LocalFolderSource
from tracker.services.datasets.steps import StepOutcome, StepSetupError
from tracker.exceptions import AgentRunFailedError, OutputArtifactError
from tracker.external_service_gateway import (
    AccountingSnapshot,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
)
from tracker.runtime.services import RuntimeServices
from tracker.executor.control_tasks import TaskControl
from tracker.services.tasks import steps as task_steps


def _grade(reward: float, stopped: str | None = None) -> StepOutcome:
    return StepOutcome("", {"reward": reward}, stopped, metadata={})


def _response(
    *names: str,
    strategy: MultiStepRewardStrategy = MultiStepRewardStrategy.MEAN,
    egress_run: EgressPolicy = "*",
    egress_evaluation: EgressPolicy = "*",
    env: dict[str, str] | None = None,
) -> RetrieveTaskResponse:
    return TaskFolderResponse(
        source=ImageSource(image="test-image:latest"),
        problem_path="/testbed/instruction.md",
        cwd="/testbed",
        resources=Resources(vcpu=1, memory=1, disk=5),
        steps=[
            StepPlan(
                name=name,
                agent_timeout=float(10 * (index + 1)),
                agent_user=f"user-{name}",
                egress_run=egress_run,
                egress_evaluation=egress_evaluation,
            )
            for index, name in enumerate(names)
        ],
        reward_strategy=strategy,
        env=env or {},
        secret_env=list(env or {}),
    )


class StepRun:
    def __init__(self, response: RetrieveTaskResponse, grades: list[StepOutcome | Exception], tmp_path: Path) -> None:
        self.events: list[str] = []
        self.agent_runs: list[dict[str, Any]] = []
        self.agent_envs: list[dict[str, str]] = []
        self.bundle_uploads = 0
        self.controllers: list[ExternalServiceDeadlineController] = []
        self.gateway_urls: list[str | None] = []
        self.evaluating_after: list[list[str]] = []
        self.failing_agent: str | None = None
        self.agent_error: Exception = AgentRunFailedError("agent crashed")
        self.service = DatasetTaskService(LocalFolderSource(tmp_path))
        self._grades = iter(grades)
        self._response = response

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def retrieve_task(_service: DatasetTaskService, *_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
            return self._response

        async def setup_task(_service: DatasetTaskService, *_args: Any, **_kwargs: Any) -> Mock:
            self.events.append("setup")
            return Mock()

        async def prepare_step(
            _service: DatasetTaskService, _task_id: str, _instance_id: str, step_name: str, **_kwargs: Any
        ) -> None:
            self.events.append(f"prepare {step_name}")
            if step_name == "broken":
                raise HealthcheckError("not healthy")
            if step_name == "flaky":
                raise socket.gaierror("temporary failure in name resolution")
            if step_name == "leaky":
                raise StepSetupError("setup printed s3cret")

        async def grade_step(
            _service: DatasetTaskService, _task_id: str, _instance_id: str, step_name: str, **_kwargs: Any
        ) -> StepOutcome:
            self.events.append(f"grade {step_name}")
            grade = next(self._grades)
            if isinstance(grade, Exception):
                raise grade
            return grade._replace(name=step_name)

        async def upload_solution(
            _service: DatasetTaskService, _task_id: str, _instance_id: str, step_name: str | None, *_args: Any
        ) -> tuple[dict[str, str], list[str]]:
            self.events.append(f"solution {step_name}")
            return {"MODE": "oracle"}, []

        async def upload_agent_artifacts(*_args: Any) -> None:
            self.bundle_uploads += 1

        async def run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
            self.events.append("agent")
            self.agent_envs.append(kwargs["extra_env"])
            self.agent_runs.append(
                {
                    "allowance": kwargs["plan"].allowance_seconds,
                    "user": kwargs["user"],
                    "base": kwargs["deadline_controller"].base_allowance_seconds,
                    "folder": kwargs["artifact_folder"],
                }
            )
            self.controllers.append(kwargs["deadline_controller"])
            self.gateway_urls.append(kwargs["generation_gateway_url"])
            if kwargs["user"] == f"user-{self.failing_agent}":
                kwargs["deadline_controller"].begin_generation(now=0.0)
                kwargs["deadline_controller"].end_generation(now=3.0)
                await finalize_agent_generation(
                    deadline_controller=kwargs["deadline_controller"],
                    on_external_service_finalized=kwargs["on_external_service_finalized"],
                )
                raise self.agent_error
            await finalize_agent_generation(
                deadline_controller=kwargs["deadline_controller"],
                on_external_service_finalized=kwargs["on_external_service_finalized"],
            )
            return None, 2.0

        record_breakdown = TaskControl.breakdown

        async def record_transition(control: TaskControl, request: Any) -> bool:
            self.evaluating_after.append(list(self.events))
            return await record_breakdown(control, request)

        monkeypatch.setattr(TaskControl, "breakdown", record_transition)
        monkeypatch.setattr(DatasetTaskService, "retrieve_task", retrieve_task)
        monkeypatch.setattr(DatasetTaskService, "setup_task", setup_task)
        monkeypatch.setattr(DatasetTaskService, "prepare_step", prepare_step)
        monkeypatch.setattr(DatasetTaskService, "grade_step", grade_step)
        monkeypatch.setattr(DatasetTaskService, "upload_solution", upload_solution)
        monkeypatch.setattr("tracker.services.tasks.execute.upload_agent_artifacts", upload_agent_artifacts)
        monkeypatch.setattr("tracker.services.tasks.execute.run_agent", run_agent)


@asynccontextmanager
async def _sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
    sandbox = AsyncMock()
    sandbox.id = "sandbox-1"
    sandbox.name = "sandbox-1"
    yield sandbox


StepStarter = Callable[[RetrieveTaskResponse, list[StepOutcome | Exception]], StepRun]


@pytest.fixture
def database_session(control_postgres_session: Session) -> Session:
    return control_postgres_session


@pytest.fixture
def stepped(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> StepStarter:
    def start(response: RetrieveTaskResponse, grades: list[StepOutcome | Exception]) -> StepRun:
        run = StepRun(response, grades, tmp_path)
        run.install(monkeypatch)
        monkeypatch.setattr("tracker.services.tasks.execute.TaskLogBuffer.buffer_logs", Mock())
        monkeypatch.setattr("tracker.services.tasks.execute.create_sandbox", _sandbox)
        monkeypatch.setattr("tracker.services.tasks.execute.install_agent_dependencies", AsyncMock())
        return run

    return start


async def _run(
    run: StepRun,
    contract: AgentContractRequest,
    database_session: Session,
    runtime_services: RuntimeServices,
    bound_controls: Any,
) -> dict[str, Any] | None:
    request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)
    result = await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)
    database_session.refresh(task_row)
    assert task_row.status == TaskStatus.FINISHED
    return result["task_0"]


@pytest.mark.description("Each step is prepared, run by the agent and graded before the next begins.")
@pytest.mark.usefixtures("process_benchmark_env")
class TestSteps:
    @pytest.mark.description("Every step is prepared run and graded in turn and the rewards are averaged.")
    async def test_every_step_is_prepared_run_and_graded_in_turn_and_the_rewards_are_averaged(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(0.5)])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.events == ["setup", "prepare one", "agent", "grade one", "prepare two", "agent", "grade two"]
        assert result is not None
        assert result["rewards"] == {"reward": 0.75}
        assert [step["name"] for step in result["metadata"]["steps"]] == ["one", "two"]

    @pytest.mark.description("The oracle runs each steps solution with its env in place of an installed agent.")
    async def test_the_oracle_runs_each_steps_solution_with_its_env_in_place_of_an_installed_agent(
        self, stepped: StepStarter, database_session: Session, bound_controls: Any, runtime_services: RuntimeServices
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(1.0)])

        await _run(run, ORACLE_CONTRACT, database_session, runtime_services, bound_controls)

        assert run.events == [
            "setup",
            "prepare one",
            "solution one",
            "agent",
            "grade one",
            "prepare two",
            "solution two",
            "agent",
            "grade two",
        ]
        assert [env["MODE"] for env in run.agent_envs] == ["oracle", "oracle"]
        assert run.bundle_uploads == 0

    @pytest.mark.description("Each agent run gets its steps timeout user and output folder.")
    async def test_each_agent_run_gets_its_steps_timeout_user_and_output_folder(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(1.0)])

        await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.agent_runs == [
            {"allowance": 10.0, "user": "user-one", "base": 10.0, "folder": "steps/one"},
            {"allowance": 20.0, "user": "user-two", "base": 20.0, "folder": "steps/two"},
        ]

    @pytest.mark.description("The final strategy keeps only the last steps rewards.")
    async def test_the_final_strategy_keeps_only_the_last_steps_rewards(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two", strategy=MultiStepRewardStrategy.FINAL), [_grade(1.0), _grade(0.25)])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert result is not None
        assert result["rewards"] == {"reward": 0.25}

    @pytest.mark.description("A step below its minimum ends the task with the steps so far.")
    async def test_a_step_below_its_minimum_ends_the_task_with_the_steps_so_far(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(0.0, "reward is 0, below the minimum 1")])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.events == ["setup", "prepare one", "agent", "grade one"]
        assert result is not None
        assert result["rewards"] == {"reward": 0.0}
        assert result["metadata"]["steps"][0]["stopped"] == "reward is 0, below the minimum 1"

    @pytest.mark.description("A later step that cannot start ends the task and scores the earlier ones.")
    async def test_a_later_step_that_cannot_start_ends_the_task_and_scores_the_earlier_ones(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "broken", "three"), [_grade(1.0)])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.events == ["setup", "prepare one", "agent", "grade one", "prepare broken"]
        assert result is not None
        assert result["rewards"] == {"reward": 1.0}
        assert result["metadata"]["steps"][1]["problem"] == "not healthy"

    @pytest.mark.description("A grader that crashes in a step errors the task.")
    async def test_a_grader_that_crashes_in_a_step_errors_the_task(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one"), [RuntimeError("grader crashed")])
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        database_session.refresh(task_row)
        assert result == {"task_0": None}
        assert task_row.status == TaskStatus.ERROR
        assert run.events == ["setup", "prepare one", "agent", "grade one"]

    @pytest.mark.description("A steps agent run gets the contracts egress allowlist added to its own.")
    async def test_a_steps_agent_run_gets_the_contracts_egress_allowlist_added_to_its_own(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        applied: list[EgressPolicy] = []

        async def record(_sandbox: object, policy: EgressPolicy) -> None:
            applied.append(policy)

        monkeypatch.setattr("tracker.services.tasks.execute.apply_egress_policy", record)
        monkeypatch.setattr("tracker.runtime.run_egress.apply_egress_policy", record)
        contract = contract.model_copy(update={"egress_allowlist": ["api.model.example"]})
        run = stepped(_response("one", egress_run=["step.example"]), [_grade(1.0)])

        await _run(run, contract, database_session, runtime_services, bound_controls)

        assert ["step.example", "api.model.example"] in applied

    @pytest.mark.description("Local docker refuses a step that restricts egress before creating a sandbox.")
    @pytest.mark.parametrize(
        ("egress_run", "egress_evaluation"), [([], "*"), ("*", ["example.com"]), (["example.com"], ["example.com"])]
    )
    async def test_local_docker_refuses_a_step_that_restricts_egress_before_creating_a_sandbox(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        monkeypatch: pytest.MonkeyPatch,
        egress_run: EgressPolicy,
        egress_evaluation: EgressPolicy,
    ) -> None:
        run = stepped(_response("one", egress_run=egress_run, egress_evaluation=egress_evaluation), [_grade(1.0)])
        monkeypatch.setattr(
            runtime_services, "get_sandbox_provider_config", AsyncMock(return_value=DockerProviderConfig())
        )
        provider = AsyncMock()
        provider.__aenter__.return_value = provider

        def create_provider(_config: DockerProviderConfig) -> AsyncMock:
            return provider

        monkeypatch.setattr(DockerProviderConfig, "create_provider", create_provider)
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert result == {"task_0": None}
        assert "requires unrestricted egress for every stage" in error.error_message
        assert run.events == []

    @pytest.mark.description("A transient failure preparing a later step does not run the agent of the first again.")
    async def test_a_transient_failure_preparing_a_later_step_does_not_run_the_agent_of_the_first_again(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "flaky"), [_grade(1.0)])
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        assert result == {"task_0": None}
        assert run.events == ["setup", "prepare one", "agent", "grade one", "prepare flaky"]

    @pytest.mark.description("Each steps grading logs the evaluation start event with the step name.")
    async def test_each_steps_grading_logs_the_evaluation_start_event_with_the_step_name(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(1.0)])
        starts: list[dict[str, Any]] = []

        def record(message: str, *_args: object, extra: dict[str, Any] | None = None, **_kwargs: Any) -> None:
            if message == "task.evaluation.start" and extra is not None:
                starts.append(extra)
                run.events.append(f"start {extra['step']}")

        monkeypatch.setattr(task_steps.logger, "info", record)

        await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.events == [
            "setup",
            "prepare one",
            "agent",
            "start one",
            "grade one",
            "prepare two",
            "agent",
            "start two",
            "grade two",
        ]
        assert [(item["task_id"], item["sandbox_id"], item["sandbox_name"], item["step"]) for item in starts] == [
            ("task_0", "sandbox-1", "sandbox-1", "one"),
            ("task_0", "sandbox-1", "sandbox-1", "two"),
        ]
        assert all(item["benchmark_id"] for item in starts)

    @pytest.mark.description("The task stays in progress until every step is graded.")
    async def test_the_task_stays_in_progress_until_every_step_is_graded(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(1.0)])

        await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.evaluating_after == [
            ["setup", "prepare one", "agent", "grade one", "prepare two", "agent", "grade two"]
        ]

    @pytest.mark.description("A later step whose agent run fails ends the task and scores the earlier ones.")
    @pytest.mark.parametrize(
        ("error", "problem"),
        [
            (AgentRunFailedError("agent crashed"), "Sandbox error: agent crashed"),
            (OutputArtifactError("result.json is missing"), "Output artifact error: result.json is missing"),
        ],
    )
    async def test_a_later_step_whose_agent_run_fails_ends_the_task_and_scores_the_earlier_ones(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        error: Exception,
        problem: str,
    ) -> None:
        run = stepped(_response("one", "two", "three"), [_grade(1.0)])
        run.failing_agent = "two"
        run.agent_error = error
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        outcome = await run_process_task(
            request, task_row, benchmark_id, runtime_services, control, service=run.service
        )

        database_session.refresh(task_row)
        breakdown = database_session.get(TaskBreakdown, task_row.task_breakdown)
        result = outcome["task_0"]
        assert task_row.status == TaskStatus.FINISHED
        assert run.events == ["setup", "prepare one", "agent", "grade one", "prepare two", "agent"]
        assert result is not None
        assert result["rewards"] == {"reward": 1.0}
        assert result["metadata"]["steps"][1]["problem"] == problem
        assert breakdown is not None
        assert breakdown.agent_run_duration == 5.0

    @pytest.mark.description("A later steps grader that leaves no reward ends the task and scores the earlier ones.")
    async def test_a_later_steps_grader_that_leaves_no_reward_ends_the_task_and_scores_the_earlier_ones(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        missing = StepOutcome("", None, problem="The verifier wrote no reward file for s3cret")
        run = stepped(_response("one", "two", "three", env={"TOKEN": "s3cret"}), [_grade(0.5), missing])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert run.events[-2:] == ["agent", "grade two"]
        assert result is not None
        assert result["rewards"] == {"reward": 0.5}
        assert result["metadata"]["steps"][1]["problem"] == "The verifier wrote no reward file for [REDACTED]"

    @pytest.mark.description("A failure in the first step errors the task.")
    @pytest.mark.parametrize(
        ("failure", "message", "category"),
        [
            ("agent", "AgentRunFailedError: Sandbox error: agent crashed", FailureCategory.AGENT),
            ("grader", "No step of the task produced a reward (one: no reward)", FailureCategory.BENCHMARK_SERVICE),
        ],
    )
    async def test_a_failure_in_the_first_step_errors_the_task(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        failure: str,
        message: str,
        category: FailureCategory,
    ) -> None:
        run = stepped(_response("one", "two"), [StepOutcome("", None, problem="no reward")])
        run.failing_agent = "one" if failure == "agent" else None
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        result = await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        database_session.refresh(task_row)
        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert result == {"task_0": None}
        assert task_row.status == TaskStatus.ERROR
        assert message in error.error_message
        assert error.category == category

    @pytest.mark.description("The final strategy with a failing last step errors the task as that failure.")
    @pytest.mark.parametrize(
        ("error", "message"),
        [
            (AgentRunFailedError("agent crashed"), "AgentRunFailedError: Sandbox error: agent crashed"),
            (OutputArtifactError("result.json is missing"), "OutputArtifactError: Output artifact error: result.json"),
        ],
    )
    async def test_the_final_strategy_with_a_failing_last_step_errors_the_task_as_that_failure(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        error: Exception,
        message: str,
    ) -> None:
        run = stepped(_response("one", "two", strategy=MultiStepRewardStrategy.FINAL), [_grade(1.0)])
        run.failing_agent = "two"
        run.agent_error = error
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        row = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert message in row.error_message
        assert row.phase == FailurePhase.AGENT

    @pytest.mark.description("A later steps setup output is redacted in its problem and the log.")
    async def test_a_later_steps_setup_output_is_redacted_in_its_problem_and_the_log(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        logged: list[str] = []
        monkeypatch.setattr(
            "tracker.services.tasks.execute.TaskLogBuffer.write", lambda _buffer, data: logged.append(data)
        )
        run = stepped(_response("one", "leaky", env={"TOKEN": "s3cret"}), [_grade(1.0)])

        result = await _run(run, contract, database_session, runtime_services, bound_controls)

        assert result is not None
        assert result["metadata"]["steps"][1]["problem"] == "setup printed [REDACTED]"
        assert any("setup printed [REDACTED]" in line for line in logged)
        assert not any("s3cret" in line for line in logged)

    @pytest.mark.description("A task that errors after its first step keeps the timing so far.")
    async def test_a_task_that_errors_after_its_first_step_keeps_the_timing_so_far(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("one", "two"), [StepOutcome("", None, problem="no reward")])
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        database_session.refresh(task_row)
        breakdown = database_session.get(TaskBreakdown, task_row.task_breakdown)
        assert breakdown is not None
        assert breakdown.agent_run_duration == 2.0

    @pytest.mark.description("The first steps setup output is redacted in the error.")
    async def test_the_first_steps_setup_output_is_redacted_in_the_error(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
    ) -> None:
        run = stepped(_response("leaky", env={"TOKEN": "s3cret"}), [])
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).one()
        assert "setup printed [REDACTED]" in error.error_message
        assert "s3cret" not in error.error_message

    @pytest.mark.description("Each step gets its own gateway credit and the accounting row adds them up.")
    async def test_each_step_gets_its_own_gateway_credit_and_the_accounting_row_adds_them_up(
        self,
        stepped: StepStarter,
        contract: AgentContractRequest,
        database_session: Session,
        bound_controls: Any,
        runtime_services: RuntimeServices,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        overheads = iter([2000, 5000])

        def counter(_request: httpx.Request) -> httpx.Response:
            snapshot = AccountingSnapshot(
                accounting_id="digest", cumulative_neutral_overhead_ms=next(overheads), revision=1
            )
            return httpx.Response(200, json=snapshot.model_dump())

        client = ExternalServiceGatewayClient(
            "http://gateway.test", "tracker-control", client=httpx.AsyncClient(transport=httpx.MockTransport(counter))
        )

        async def gateway_controller(*_args: Any) -> ExternalServiceDeadlineController:
            return ExternalServiceDeadlineController(
                base_allowance_seconds=5.0,
                credit_cap_seconds=60.0,
                client=client,
                snapshot=AccountingSnapshot(accounting_id="digest", cumulative_neutral_overhead_ms=0, revision=0),
            )

        monkeypatch.setattr("tracker.services.tasks.execute._create_generation_deadline_controller", gateway_controller)
        monkeypatch.setattr("tracker.services.tasks.execute.EXTERNAL_SERVICE_GATEWAY_URL", "http://gateway.test")
        run = stepped(_response("one", "two"), [_grade(1.0), _grade(1.0)])
        request, task_row, benchmark_id, control = create_task_environment(contract, database_session, bound_controls)

        await run_process_task(request, task_row, benchmark_id, runtime_services, control, service=run.service)

        database_session.refresh(task_row)
        breakdown = database_session.get(TaskBreakdown, task_row.task_breakdown)
        assert run.gateway_urls == ["http://gateway.test", "http://gateway.test"]
        assert [controller.applied_credit_seconds() for controller in run.controllers] == [2.0, 3.0]
        assert breakdown is not None
        assert breakdown.base_generation_allowance_seconds == 30.0
        assert breakdown.external_service_overhead_seconds == 5.0
        assert breakdown.external_service_credit_applied_seconds == 5.0
