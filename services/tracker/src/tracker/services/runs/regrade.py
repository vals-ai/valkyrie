from collections.abc import Sequence
from asyncio import to_thread
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from benchmark_service.schemas import DatasetVersion

from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSResources
from tracker.clients.dataset_tasks import dataset_service
from tracker.clients.model_gateway import MODEL_GATEWAY_API_KEY, MODEL_GATEWAY_URL
from tracker.config import AUTH_REQUIRED
from tracker.database.models import Benchmark, ErrorResult, Org, Task
from tracker.domain import TaskStatus
from tracker.exceptions import SecretConfigurationError, SecretNotFoundError
from tracker.local.resources import LocalResources, reads_secrets_file
from tracker.local.secrets import InMemorySecretStore, load_execution_secrets
from tracker.runtime.secrets import SecretStore, resolve_secrets, secret_set_instruction
from tracker.runtime.services import RuntimeServices
from tracker.secret_access import OrgSecretStore, selected_project_gateway_reference
from tracker.services.datasets.artifacts import SAVED_ARTIFACTS_RUN
from tracker.services.datasets.ref import HostedDataset
from tracker.services.runs.start import task_secret_reader
from tracker.types import OrgContext, RunExecutionRequest


@dataclass(frozen=True)
class RegradeTasks:
    eval_resume_states: dict[str, dict[str, Any]]
    skipped_task_ids: list[str]

    @property
    def task_ids(self) -> list[str]:
        return list(self.eval_resume_states)


def regrade_tasks(source_tasks: Sequence[Task], saved_artifacts_run: UUID | None = None) -> RegradeTasks:
    def state(task: Task) -> dict[str, Any] | None:
        if task.eval_resume_state is not None:
            return task.eval_resume_state
        if saved_artifacts_run is not None and (
            task.status == TaskStatus.FINISHED
            or (task.status == TaskStatus.ERROR and task.evaluation_phase_token is not None)
        ):
            return {SAVED_ARTIFACTS_RUN: str(saved_artifacts_run)}
        return None

    states = {task.task_id: found for task in source_tasks if (found := state(task)) is not None}
    return RegradeTasks(
        eval_resume_states=states,
        skipped_task_ids=[task.task_id for task in source_tasks if task.task_id not in states],
    )


def regrade_run(
    source: Benchmark,
    tasks: RegradeTasks,
    run_starter: RequestIdentity,
    *,
    dataset: str | None = None,
    dataset_version: DatasetVersion | None = None,
    concurrency: int | None = None,
    dataset_ref: HostedDataset | None = None,
) -> Benchmark:
    return Benchmark(
        org_id=source.org_id,
        name=source.name if dataset_ref is None else dataset_ref.benchmark,
        label=source.label,
        custom_benchmark_service=source.custom_benchmark_service,
        arguments=source.arguments.model_copy(
            update={
                "contract": source.arguments.contract.model_copy(update={"secrets": _grading_secrets(source)}),
                "dataset": dataset,
                "dataset_version": dataset_version,
                "task_ids": tasks.task_ids,
                "slice_str": None,
                "concurrency": source.arguments.concurrency if concurrency is None else concurrency,
                "lambda_function": None,
                "regraded_from_run_id": source.id,
                "agent_attempts": [],
                "regrade_skipped_task_ids": tasks.skipped_task_ids,
                "dataset_ref": dataset_ref,
            }
        ),
        started_by_id=run_starter.access_key_id or run_starter.user_id,
        started_by_email=run_starter.email,
    )


def _grading_secrets(source: Benchmark) -> dict[str, str]:
    secrets = {
        name: reference
        for name, reference in source.arguments.contract.secrets.items()
        if name in {MODEL_GATEWAY_API_KEY, MODEL_GATEWAY_URL}
    }
    if (reference := source.arguments.selected_project_gateway_secret_ref) is not None:
        secrets.update(dict.fromkeys((MODEL_GATEWAY_API_KEY, MODEL_GATEWAY_URL), reference))
    return secrets


async def validate_regrade(
    run: Benchmark,
    runtime: RuntimeServices,
    org: Org,
    task_digests: dict[str, str] | None = None,
) -> None:
    request = _execution_request(run)
    try:
        if isinstance(ref := run.arguments.dataset_ref, HostedDataset):
            if not await _grading_models(run, ref, request, runtime, org, task_digests):
                run.arguments = run.arguments.model_copy(
                    update={"contract": run.arguments.contract.model_copy(update={"secrets": {}})}
                )
            else:
                _require_gateway_key(run, request, org)
        await _check_secrets(run.arguments.contract.secrets, run.arguments.properties, runtime, org)
        await runtime.get_sandbox_provider_config()
    except SecretConfigurationError as error:
        raise SecretConfigurationError(f"Valkyrie cannot regrade: {error.args[0]}") from error


def _execution_request(run: Benchmark) -> RunExecutionRequest:
    return (
        run.local_start_benchmark_request(service_headers={})
        if run.arguments.environment == "local"
        else run.managed_start_benchmark_request()
    )


async def _grading_models(
    run: Benchmark,
    dataset_ref: HostedDataset,
    request: RunExecutionRequest,
    runtime: RuntimeServices,
    org: Org,
    task_digests: dict[str, str] | None,
) -> list[str]:
    reader = task_secret_reader(request, runtime, OrgContext(id=org.id, name=org.name, storage_key=org.storage_key))

    async def read_secret(name: str) -> str | None:
        try:
            return await reader(name)
        except SecretNotFoundError as error:
            raise SecretNotFoundError(f"{error.args[0]}. {secret_set_instruction(name.partition('#')[0])}") from error

    service = dataset_service(
        dataset_ref,
        runtime.objects,
        read_secret,
        tasks=task_digests,
        regrade_only=True,
    )
    try:
        return await service.regrade_requirements(run.arguments.task_ids or [])
    finally:
        await service.close()


def _require_gateway_key(run: Benchmark, request: RunExecutionRequest, org: Org) -> None:
    if (
        selected_project_gateway_reference(
            request.contract.secrets,
            org,
            run.arguments.selected_project_gateway_secret_ref,
            run.name,
            run.custom_benchmark_service,
            dataset_task=True,
        )
        is None
    ):
        raise SecretConfigurationError(
            "Grading needs a selected project Gateway key. "
            "Run `valkyrie run start` in a project that has a Gateway key instead."
        )


async def _check_secrets(
    references: dict[str, str],
    properties: AWSResources | LocalResources | None,
    runtime: RuntimeServices,
    org: Org,
) -> None:
    store: SecretStore
    if isinstance(properties, LocalResources) and reads_secrets_file(properties):
        values = await to_thread(load_execution_secrets, properties.secrets_file, references)
        for name, reference in references.items():
            if name not in values:
                raise SecretConfigurationError(f"Secret '{reference}' is not set. {secret_set_instruction(reference)}")
        store = InMemorySecretStore(references, values)
    else:
        store = OrgSecretStore(
            runtime.secrets, references.values(), org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED
        )
    await resolve_secrets(references, store)


def regrade_task_rows(
    run: Benchmark, tasks: RegradeTasks, started_at: datetime
) -> tuple[list[Task], list[ErrorResult]]:
    copied = [
        Task(
            org_id=run.org_id,
            benchmark=run.id,
            task_id=task_id,
            status=TaskStatus.EVALUATING,
            started_at=started_at,
            eval_resume_state=state,
        )
        for task_id, state in tasks.eval_resume_states.items()
    ]
    skipped = [
        Task(
            org_id=run.org_id,
            benchmark=run.id,
            task_id=task_id,
            status=TaskStatus.ERROR,
            started_at=started_at,
            finished_at=started_at,
        )
        for task_id in tasks.skipped_task_ids
    ]
    errors = [
        ErrorResult(
            org_id=run.org_id,
            task=task.id,
            error_message=f"Source run {run.arguments.regraded_from_run_id} saved no eval_resume_state for this task.",
            producer="tracker",
            operation="regrade",
            transient=False,
        )
        for task in skipped
    ]
    return [*copied, *skipped], errors
