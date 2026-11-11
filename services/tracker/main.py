import asyncio
import io
import tarfile
from collections.abc import AsyncGenerator, AsyncIterator, Buffer, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
import json
from datetime import UTC, datetime
from typing import Annotated, Any, TypeVar, cast
from uuid import UUID, uuid4

import httpx
import logfire
import sentry_sdk
from benchmark_service import BenchmarkServiceClient
from benchmark_service.client import BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from benchmark_service.schemas import ResolveDatasetResponse, VerifyTaskIdsResponse
from benchmark_service.schemas import DatasetVersion as BenchmarkDatasetVersion
from botocore.config import Config
from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.routing import APIRoute
from opentelemetry.propagate import inject
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker._lambda import dry_run_lambda, invoke_lambda
from tracker.api.login import router as login_router
from tracker.api.organizations import router as organizations_router
from tracker.api.agents import router as agents_router
from tracker.api.benchmark_services import router as benchmark_services_router
from tracker.api.benchmarks_status import router as benchmarks_status_router
from tracker.api.dependencies import TrackedBenchmarkId, bind_benchmark_id, get_agent_library_runtime
from tracker.api.dependencies import RunBenchmarkDependency, RunRuntimeDependency
from tracker.api.costs import router as costs_router
from tracker.api.filter_options import router as filter_options_router
from tracker.api.logs import monitor_router
from tracker.api.logs import router as logs_router
from tracker.api.scheduler_overview import router as scheduler_overview_router
from tracker.api.single_benchmark import router as single_benchmark_router
from tracker.api.single_task import router as single_task_router
from tracker.api.run_artifacts import router as run_artifacts_router
from tracker.api.providers import router as providers_router
from tracker.repositories.provider_secret import ProviderSecretRepository
from tracker.api.run_settings import router as run_settings_router, user_run_settings
from tracker.api.benchmark_definitions import router as benchmark_definitions_router
from tracker.api.dataset_versions import router as dataset_versions_router
from tracker.api.datasets import router as datasets_router
from tracker.repositories.benchmark import BenchmarkRepository, BenchmarkRepositoryDependency, Order
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.project import ProjectRepository, ProjectRepositoryDependency
from tracker.repositories.run_settings import RunSettingsRepository
from tracker.repositories.scheduler import SchedulerRepository
from tracker.api.projects import (
    find_project,
    reject_archived_project,
    reject_missing_project,
    router as projects_router,
)
from tracker.api.secrets import router as secrets_router, secret_store
from tracker.auth import (
    RequestIdentity,
    forward_tracker_api_key,
    get_current_identity,
    get_current_org,
    get_current_runner,
    get_current_starter,
)
from tracker.aws.resolver import (
    resolve_run_metadata_aws_runtime,
    resolve_run_aws_runtime,
    resolve_start_aws_runtime,
)
from tracker.aws.services import CloudRuntimeFactory, CloudRuntimeServices
from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    S3ObjectStore,
    create_console_url,
    copy_s3_object,
    create_presigned_url,
    s3_object_exists,
)
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.project_gateway import prepare_project_gateway_key, selects_project_gateway_key
from tracker.repositories.benchmark_definition import BenchmarkDefinitionRepository
from tracker.repositories.dataset import DatasetRepository
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.database.models import FinalEvaluation as FinalEvaluationRow
from tracker.runtime.artifacts import (
    agent_bundle_key,
    benchmark_agent_bundle_key,
    benchmark_prefix as benchmark_artifact_prefix,
    copy_agent_to_benchmark,
)
from tracker.runtime.secrets import resolve_secrets
from tracker.runtime.services import RuntimeServices
from tracker.runtime.storage import ObjectStore, StoredObjectCopy
from tracker.agent.schemas import AgentConfig, validate_agent_name
from tracker.config import (
    AUTH_REQUIRED,
    ENVIRONMENT,
    SANDBOX_QUEUE_ENABLED,
    BenchmarkServiceDestination,
    classify_benchmark_service_destination,
    create_benchmark_service_url,
)
from tracker.database.models import (
    Benchmark,
    Dataset,
    DatasetVersion,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    Org,
    RunAccessKey,
    RunProject,
)
from tracker.domain import BenchmarkStatus, DocentReadingStatus, ExecutorDispatchKind, RetryMode, TaskStatus
from tracker.database.scoping import assert_org
from tracker.executor.dispatch_control import (
    EnqueueFailureResolution,
    admit_recovery_dispatch,
    admit_start_dispatch,
    resolve_enqueue_failure,
    validate_managed_execution_release,
)
from tracker.executor.dispatch_payload import PayloadKey, generate_payload_key, seal_payload
from tracker.executor.launcher import launch_dispatch
from tracker.database.session import check_database_connection, database_call, get_session
from tracker.services.datasets.oracle import ORACLE_AGENT, ORACLE_CONTRACT
from tracker.services.datasets.ref import DatasetRef, HostedDataset, LocalDataset
from tracker.services.datasets.scoring import scorer_for
from tracker.clients.dataset_tasks import TaskSelectionError, dataset_service
from tracker.services.datasets.version_ref import parse_version_ref
from tracker.services.datasets.versions import UnresolvedDatasetError, dataset_tasks
from tracker.docent_analysis import (
    analyze_event_stream,
)
from tracker.exceptions import (
    AmbiguousDigestError,
    InvalidSandboxConfigurationError,
    LambdaError,
    SecretNotFoundError,
    TrackerServiceError,
)
from tracker.local import config as local_config
from tracker.local.resources import LocalResources, reads_secrets_file
from tracker.local.runtime import LocalRuntimeFactory
from executor_protocol import ExecutorTelemetryContext
from tracker.logging import configure_logging, get_logger, request_id_var
from tracker.executor.release_control import (
    MaintenanceModeError,
    ReleaseControlError,
    get_executor_admission,
    lock_executor_admission,
)
from tracker.executor.dispatch_recovery import AutomaticDispatchRecovery
from tracker.scheduler.coordinator import SharedPoolScheduler
from tracker.scheduler.service_capacity import API_WAKE_SECONDS, ServiceCapacityScheduler, wake_benchmark_service
from tracker.executor.release_retirement import AutomaticReleaseRetirement
from tracker.middleware import LocalTrustedHostMiddleware, RequestContextMiddleware, RunRequestSizeLimitMiddleware
from tracker.observability import configure_observability
from tracker.secret_access import may_use_shared_secrets, resolve_secret_name, validate_secret_names
from tracker.run_access_keys import IssuedKeys, IssuedRunKey, run_service_key
from tracker.sandbox_cost import AutomaticSandboxCost
from tracker.outbound_security import (
    validate_custom_service_destination,
    validate_service_headers,
    validate_service_url_syntax,
)
from tracker.executor.control_queue import queue_pool_id
from tracker.types import (
    AgentContractRequest,
    AnalyzeBenchmarkRequest,
    AWSRuntimeResponse,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FetchBenchmarkTasksRequest,
    FinalEvaluation,
    FinalViewResponse,
    ManagedExecutionContext,
    OrgContext,
    ProjectResponse,
    RetrieveResultsResponse,
    RunExecutionRequest,
    RunMonitoringRules,
    RunProjectRequest,
    RescoreBenchmarkResponse,
    RegradeBenchmarkRequest,
    RegradeBenchmarkResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    ShedBenchmarkRequest,
    ShedBenchmarkResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
    UpdateBenchmarkConcurrencyRequest,
    UpdateBenchmarkConcurrencyResponse,
    validate_managed_execution_request,
)
from tracker.services.benchmarks import (
    BenchmarkConcurrencyUpdate,
    fetch_benchmark_row,
    start_benchmark_request_to_benchmark,
    update_benchmark_concurrency,
    update_benchmark_resume_arguments,
)
from tracker.clients.benchmark_service import create_benchmark_service_client, fetch_sandbox_provider_config
from tracker.services.results import (
    InvalidCursorError,
    fetch_benchmark_response,
    build_benchmark_table_rows,
    fetch_filtered_benchmark_rows,
    fetch_final_score_inputs,
    stream_benchmark_results,
)
from tracker.services.failures import is_rejected_request
from tracker.services.final_views import create_final_view, final_view_s3_key, upload_final_view
from tracker.api.storage_errors import storage_errors
from tracker.services.runs.attempts import record_prior_attempts
from tracker.services.runs.regrade import RegradeTasks, regrade_run, regrade_task_rows, regrade_tasks
from tracker.services.runs.retry import (
    RetryState,
    prepare_retry_state,
    regrade_rerun_error,
    reset_to_in_progress_status,
)
from tracker.services.runs.stop import apply_shed_benchmark, initiate_stop_benchmark
from tracker.services.sandboxes import ForceStopError, capture_stop_sandboxes, force_stop_sandboxes, sandbox_generator
from tracker.services.tasks.execute import task_attempt_id

configure_logging()
configure_observability("valkyrie-tracker", environment=ENVIRONMENT)

logger = get_logger(__name__)

_COMPLETION_CALLBACK_CONFIG = Config(read_timeout=60, retries={"total_max_attempts": 1})

_TaskResult = TypeVar("_TaskResult")


def _operation_id(route: APIRoute) -> str:
    return route.name


@asynccontextmanager
async def tracker_lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    retirement = AutomaticReleaseRetirement()
    dispatch_recovery = AutomaticDispatchRecovery()
    sandbox_cost = AutomaticSandboxCost()
    scheduler = SharedPoolScheduler()
    service_capacity = ServiceCapacityScheduler()
    retirement.start()
    dispatch_recovery.start()
    sandbox_cost.start()
    scheduler.start()
    service_capacity.start()
    try:
        yield
    finally:
        await service_capacity.stop()
        await scheduler.stop()
        sandbox_cost.stop()
        dispatch_recovery.stop()
        retirement.stop()


app = FastAPI(generate_unique_id_function=_operation_id, redirect_slashes=False, lifespan=tracker_lifespan)

logfire.instrument_fastapi(app, excluded_urls="/health$")

app.add_middleware(RunRequestSizeLimitMiddleware)
app.add_middleware(RequestContextMiddleware)
app.add_middleware(LocalTrustedHostMiddleware)

app.include_router(agents_router)
app.include_router(login_router)
app.include_router(organizations_router)
app.include_router(benchmark_services_router)
app.include_router(benchmarks_status_router)
app.include_router(costs_router)
app.include_router(filter_options_router)
app.include_router(logs_router)
app.include_router(monitor_router)
app.include_router(scheduler_overview_router)
app.include_router(single_benchmark_router)
app.include_router(single_task_router)
app.include_router(run_artifacts_router)
app.include_router(providers_router)
app.include_router(run_settings_router)
app.include_router(secrets_router)
app.include_router(projects_router)
v1 = APIRouter(prefix="/v1")
v1.include_router(benchmark_definitions_router)
v1.include_router(datasets_router)
v1.include_router(dataset_versions_router)
app.include_router(v1)


def _executor_telemetry_context() -> ExecutorTelemetryContext:
    trace_headers: dict[str, str] = {}
    inject(trace_headers)
    return {
        "request_id": request_id_var.get(),
        "trace_headers": trace_headers,
    }


def _process_benchmark_kwargs(
    benchmark_row: Benchmark,
    request: RunExecutionRequest,
    verified_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "execution_context_json": ManagedExecutionContext(
            version=3,
            benchmark_id=benchmark_row.id,
            verified_task_ids=verified_task_ids,
            start_benchmark_request=request,
        ).model_dump(mode="json")
    }


def _persist_dispatch_payload(
    session: Session,
    dispatch: ExecutorDispatch,
    payload: dict[str, Any],
    telemetry_context: ExecutorTelemetryContext,
    payload_key: PayloadKey,
) -> None:
    sealed = seal_payload(dispatch.id, {**payload, "telemetry_context_json": telemetry_context}, payload_key)
    BenchmarkRepository(session).save(
        ExecutorDispatchPayload(
            dispatch_id=dispatch.id,
            ciphertext=sealed.ciphertext,
            encrypted_data_key=sealed.encrypted_data_key,
            nonce=sealed.nonce,
        )
    )


def _resolve_enqueue_failure(
    bind: Engine | Connection,
    benchmark_id: UUID,
    dispatch_id: UUID,
    task_ids: list[str],
) -> EnqueueFailureResolution:
    with Session(bind) as session:
        return resolve_enqueue_failure(session, benchmark_id=benchmark_id, dispatch_id=dispatch_id, task_ids=task_ids)


class _TaskFailedAfterCancellation(Exception):
    def __init__(self, task_error: Exception, cancellation: asyncio.CancelledError) -> None:
        super().__init__(str(task_error))
        self.task_error = task_error
        self.cancellation = cancellation


async def _await_before_cancellation(
    task: asyncio.Task[_TaskResult],
    cancellation: asyncio.CancelledError | None = None,
) -> tuple[_TaskResult, asyncio.CancelledError | None]:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except Exception:
            pass
    try:
        result = task.result()
    except Exception as error:
        if cancellation is not None:
            raise _TaskFailedAfterCancellation(error, cancellation) from error
        raise
    return result, cancellation


async def _launch_executor_dispatch(
    dispatch: ExecutorDispatch,
    *,
    session: Session,
    verified_task_ids: list[str],
) -> None:
    try:
        await launch_dispatch(dispatch)
    except Exception as exc:
        logger.exception(
            "Executor dispatch launch acknowledgement failed",
            extra={"executor_dispatch_id": str(dispatch.id)},
        )
        resolution = await asyncio.to_thread(
            _resolve_enqueue_failure,
            session.get_bind(),
            dispatch.benchmark_id,
            dispatch.id,
            verified_task_ids,
        )
        if resolution == EnqueueFailureResolution.DELIVERED:
            return
        if resolution == EnqueueFailureResolution.SUPERSEDED:
            raise HTTPException(status_code=409, detail="Executor dispatch was superseded by a newer Retry") from exc
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Executor dispatch enqueue acknowledgement failed; use Retry to continue",
                "benchmark_id": str(dispatch.benchmark_id),
                "executor_dispatch_id": str(dispatch.id),
            },
        ) from exc


async def _delete_uncommitted_agent_copy(
    *,
    created_copy: StoredObjectCopy | None,
    benchmark_id: UUID,
    request: StartBenchmarkRequest,
    object_store: ObjectStore,
) -> None:
    if created_copy is None:
        return
    try:
        await object_store.delete(
            benchmark_agent_bundle_key(str(benchmark_id), request.contract.name),
            deletion_token=created_copy.deletion_token,
        )
    except Exception:
        logger.exception(
            "Failed to delete uncommitted benchmark agent copy",
            extra={"benchmark_id": str(benchmark_id)},
        )


def _start_admission_is_absent(
    bind: Engine | Connection,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
) -> bool:
    try:
        with Session(bind) as verification_session:
            benchmark = BenchmarkRepository(verification_session).get_by_id(benchmark_id)
            dispatch = ExecutorDispatchRepository(verification_session).get(dispatch_id)
    except Exception:
        logger.exception(
            "Could not verify failed benchmark admission; retaining its agent copy",
            extra={"benchmark_id": str(benchmark_id), "executor_dispatch_id": str(dispatch_id)},
        )
        return False
    return benchmark is None and dispatch is None


async def _rollback_failed_start_admission(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    created_copy: StoredObjectCopy | None,
    request: StartBenchmarkRequest,
    object_store: ObjectStore,
) -> None:
    try:
        session.rollback()
    except Exception:
        logger.exception(
            "Failed to roll back benchmark admission",
            extra={"benchmark_id": str(benchmark_id), "executor_dispatch_id": str(dispatch_id)},
        )
        return
    if not await asyncio.to_thread(
        _start_admission_is_absent,
        session.get_bind(),
        benchmark_id=benchmark_id,
        dispatch_id=dispatch_id,
    ):
        return
    await _delete_uncommitted_agent_copy(
        created_copy=created_copy,
        benchmark_id=benchmark_id,
        request=request,
        object_store=object_store,
    )


@app.exception_handler(SecretNotFoundError)
async def secret_not_found_handler(_request: Request, exc: SecretNotFoundError):
    raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc


@app.exception_handler(TrackerServiceError)
async def tracker_service_error_handler(request: Request, exc: TrackerServiceError):
    logger.error("Tracker service operation failed on %s %s: %s", request.method, request.url.path, exc, exc_info=exc)
    sentry_sdk.capture_exception(exc)
    return JSONResponse(status_code=500, content={"detail": "Tracker service operation failed"})


@app.exception_handler(BenchmarkServiceUnauthenticatedError)
async def benchmark_service_unauth_error_handler(_request: Request, exc: BenchmarkServiceUnauthenticatedError):
    logger.warning("Benchmark service authentication failed: %s", exc)
    raise HTTPException(status_code=502, detail="Benchmark service authentication failed") from exc


@app.exception_handler(BenchmarkServiceError)
async def benchmark_service_error_handler(request: Request, exc: BenchmarkServiceError):
    logger.error("Benchmark service request failed on %s %s: %s", request.method, request.url.path, exc, exc_info=exc)
    sentry_sdk.capture_exception(exc)
    return JSONResponse(status_code=502, content={"detail": "Benchmark service request failed"})


async def _monitoring_rules(
    benchmark_service: BenchmarkServiceClient, task_ids: list[str], dataset: str | None
) -> RunMonitoringRules | None:
    try:
        rules = await benchmark_service.monitoring_requirements(task_ids, dataset=dataset)
    except BenchmarkServiceError as exc:
        if exc.status_code == 404:
            return None
        raise HTTPException(status_code=502, detail="Failed to read the benchmark's monitoring rules") from exc
    return RunMonitoringRules(**rules.model_dump())


def _verify_task_ids_failure(benchmark_name: str, exc: Exception) -> HTTPException:
    if isinstance(exc, BenchmarkServiceError) and is_rejected_request(exc.status_code):
        logger.warning("Benchmark service rejected the task ids for %s: %s", benchmark_name, exc)
        return HTTPException(
            status_code=400,
            detail=exc.detail if isinstance(exc.detail, str) else "Benchmark service rejected the task IDs",
        )
    logger.error("Failed to verify task ids for %s", benchmark_name, exc_info=exc)
    return HTTPException(status_code=502, detail="Failed to verify task ids")


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(request: Request, exc: RequestValidationError):
    if request.url.path.startswith(f"{secrets_router.prefix}/"):
        exc = RequestValidationError([{k: v for k, v in error.items() if k != "input"} for error in exc.errors()])
    return await request_validation_exception_handler(request, exc)


@app.get(
    "/health",
    description="Report whether the Tracker and its database are up. Returns 503 when the database is unreachable.",
)
def health_check() -> dict[str, str]:
    if not check_database_connection():
        raise HTTPException(status_code=503, detail="Database is not accessible")
    return {"status": "ok"}


def _org_context(org: Org) -> OrgContext:
    return OrgContext(id=org.id, name=org.name, storage_key=org.storage_key)


@app.get(
    "/aws-runtime",
    response_model=AWSRuntimeResponse,
    description="Return managed AWS resource locations without credential material.",
)
def fetch_aws_runtime_metadata(org: Org = Depends(get_current_org)) -> AWSRuntimeResponse:
    resources = resolve_start_aws_runtime(_org_context(org)).resources
    return AWSRuntimeResponse(
        mode="managed",
        region=resources.region,
        s3_bucket=resources.s3_bucket,
    )


async def _resolve_contract_from_s3(request: StartBenchmarkRequest, object_store: ObjectStore) -> AgentContractRequest:
    try:
        zip_bytes = await object_store.get_bytes(agent_bundle_key(request.contract.name))
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail=f"Agent '{request.contract.name}' not found") from error
    agent_config = AgentConfig(model=request.contract.model, kwargs=dict(request.contract.kwargs))
    resolved = get_contract_from_zip_bytes(request.contract.name, zip_bytes, agent_config)
    if request.contract.secrets:
        resolved.secrets = {**resolved.secrets, **request.contract.secrets}
    resolved.inference_settings_attested = True
    return resolved


def _authorize_custom_benchmark_destination(url: str, org: Org) -> None:
    try:
        validate_custom_service_destination(
            url,
            org_name=org.name,
            auth_required=AUTH_REQUIRED,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _replace_headers(headers: Mapping[str, str], replacements: Mapping[str, str]) -> dict[str, str]:
    names = {name.lower() for name in replacements}
    return {name: value for name, value in headers.items() if name.lower() not in names} | dict(replacements)


def _resolve_secret_name(name: str, org: Org) -> str:
    return resolve_secret_name(name, org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)


def _authorize_secret_names(request: StartBenchmarkRequest, org: Org) -> StartBenchmarkRequest:
    secrets = {env_name: _resolve_secret_name(name, org) for env_name, name in request.contract.secrets.items()}
    request = request.model_copy(
        update={
            "contract": request.contract.model_copy(update={"secrets": secrets}),
            "sandbox_provider_secret_name": request.sandbox_provider_secret_name
            and _resolve_secret_name(request.sandbox_provider_secret_name, org),
            "service_auth_secret_name": request.service_auth_secret_name
            and _resolve_secret_name(request.service_auth_secret_name, org),
        }
    )
    names = [*secrets.values()]
    names += [name for name in (request.sandbox_provider_secret_name, request.service_auth_secret_name) if name]
    try:
        validate_secret_names(names, org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return request


async def _with_project_gateway_secrets(
    request: StartBenchmarkRequest, project_id: UUID, org: Org, bind: Engine | Connection
) -> StartBenchmarkRequest:
    references = await prepare_project_gateway_key(bind, project_id, org, secret_store(org))
    return request.model_copy(
        update={"contract": request.contract.model_copy(update={"secrets": request.contract.secrets | references})}
    )


async def _wake_hosted_service(
    benchmark_name: str,
    custom_benchmark_service: str | None,
    client: BenchmarkServiceClient,
) -> None:
    if (
        classify_benchmark_service_destination(benchmark_name, custom_benchmark_service).value
        != BenchmarkServiceDestination.HOSTED.value
    ):
        return
    try:
        await wake_benchmark_service(benchmark_name, client, timeout_seconds=API_WAKE_SECONDS)
    except TrackerServiceError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Benchmark service '{benchmark_name}' is starting, retry in a minute",
            headers={"Retry-After": "60"},
        ) from exc


def _reject_docker_with_hosted_service(sandbox_provider: str, destination: BenchmarkServiceDestination) -> None:
    if sandbox_provider == "docker" and destination == BenchmarkServiceDestination.HOSTED:
        raise HTTPException(status_code=400, detail="The Docker sandbox provider requires a local benchmark service")


class _StartAdmissionRollbackError(Exception):
    pass


def _validate_start_release(bind: Engine | Connection) -> None:
    with Session(bind) as session:
        validate_managed_execution_release(session)


def _commit_start(
    bind: Engine | Connection,
    benchmark_json: str,
    request: RunExecutionRequest,
    dispatch_id: UUID,
    task_ids: list[str],
    queue_pool_id: str | None,
    telemetry_context: ExecutorTelemetryContext,
    issued_keys: Sequence[IssuedRunKey],
    project_id: UUID | None,
    selected_project_gateway_root: bool,
) -> tuple[str, "AdmissionResult"]:
    payload_key = generate_payload_key(dispatch_id)
    with Session(bind, expire_on_commit=False) as session:
        benchmark = Benchmark.model_validate(json.loads(benchmark_json))
        benchmark.arguments = benchmark.arguments.model_copy(
            update={
                "priority": request.priority,
                "queue_pool_id": queue_pool_id,
                "dataset_ref": request.dataset_ref,
                "selected_project_gateway_secret_ref": (
                    request.contract.secrets["MODEL_GATEWAY_API_KEY"] if selected_project_gateway_root else None
                ),
            }
        )
        try:
            if project_id is not None:
                reject_archived_project(ProjectRepository(session).lock(project_id))
            repository = BenchmarkRepository(session)
            repository.add_tasks(benchmark.org_id, benchmark.id, task_ids)
            dispatch = admit_start_dispatch(session, benchmark=benchmark, dispatch_id=dispatch_id, task_ids=task_ids)
            payload = _process_benchmark_kwargs(benchmark, request, task_ids)
            _persist_dispatch_payload(session, dispatch, payload, telemetry_context, payload_key)
            repository.save(*(RunAccessKey(key_id=key.key_id, benchmark_id=benchmark.id) for key in issued_keys))
            if project_id is not None:
                repository.save(
                    RunProject(
                        benchmark_id=benchmark.id,
                        project_id=project_id,
                        created_by_email=benchmark.started_by_email,
                    )
                )
            session.commit()
            return benchmark.model_dump_json(), _admission_result(dispatch, task_ids)
        except Exception as exc:
            try:
                session.rollback()
            except Exception:
                logger.exception("Failed to roll back benchmark admission")
                raise _StartAdmissionRollbackError() from exc
            raise


async def _resolve_dataset_version(
    benchmark_service: BenchmarkServiceClient,
    benchmark_name: str,
    dataset: str,
    dataset_version: str | None,
) -> ResolveDatasetResponse | None:
    try:
        try:
            version_metadata = await benchmark_service.version(dataset=dataset)
        except BenchmarkServiceError as exc:
            if exc.status_code not in {404, 405, 501}:
                raise
            if dataset_version is not None:
                raise HTTPException(
                    status_code=400, detail="This benchmark service cannot select a dataset version"
                ) from exc
            logger.info("Benchmark service does not expose version metadata; starting run without a pinned version")
            return None
        if version_metadata.dataset_version_selection:
            return await benchmark_service.resolve_dataset(dataset, version=dataset_version)
        if dataset_version is not None:
            raise HTTPException(status_code=400, detail="This benchmark service cannot select a dataset version")
        return None
    except (BenchmarkServiceError, httpx.HTTPError, ValueError) as exc:
        logger.exception("Failed to select dataset version for %s", benchmark_name)
        raise HTTPException(status_code=502, detail="Failed to select dataset version") from exc


async def _verify_through_benchmark_service(
    request: StartBenchmarkRequest, execution_request: RunExecutionRequest
) -> VerifyTaskIdsResponse:
    benchmark_service = request.benchmark_service

    try:
        try:
            await _wake_hosted_service(
                request.benchmark_name,
                request.custom_benchmark_service,
                benchmark_service,
            )
            await benchmark_service.health_check()
        except httpx.ConnectError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Benchmark service '{request.benchmark_name}' is not reachable",
            ) from exc

        resolved_dataset = await _resolve_dataset_version(
            benchmark_service, request.benchmark_name, request.dataset or "default", request.dataset_version
        )
        if resolved_dataset is not None:
            execution_request.dataset = resolved_dataset.dataset
            execution_request.resolved_dataset_version = resolved_dataset.version
            await benchmark_service.close()
            benchmark_service = execution_request.benchmark_service

        try:
            verify_response = await benchmark_service.verify_task_ids(
                task_ids=request.task_ids, slice_str=request.slice_str, dataset=execution_request.dataset
            )
        except BenchmarkServiceUnauthenticatedError as exc:
            logger.warning("Benchmark service authentication failed for %s: %s", request.benchmark_name, exc)
            raise HTTPException(status_code=502, detail="Benchmark service authentication failed") from exc
        except (BenchmarkServiceError, httpx.HTTPError, ValueError) as exc:
            raise _verify_task_ids_failure(request.benchmark_name, exc) from exc

        if execution_request.monitoring is not None:
            execution_request.monitoring_rules = await _monitoring_rules(
                benchmark_service, verify_response.task_ids, execution_request.dataset
            )
    finally:
        try:
            await benchmark_service.close()
        except Exception:
            logger.exception("Failed to close benchmark service client for %s", request.benchmark_name)

    return verify_response


def _find_dataset(
    bind: Engine | Connection, org_id: UUID, ref: HostedDataset
) -> tuple[Dataset, DatasetVersion | None] | None:
    with Session(bind=bind) as session:
        benchmark = BenchmarkDefinitionRepository(session).get(org_id, ref.benchmark)
        dataset = None if benchmark is None else DatasetRepository(session).get(benchmark.id, ref.dataset)
        if dataset is None:
            return None
        return dataset, DatasetVersionRepository(session).get(dataset.id, parse_version_ref(ref.ref))


async def _resolve_dataset(ref: DatasetRef, bind: Engine | Connection, org_id: UUID) -> DatasetRef:
    if isinstance(ref, LocalDataset):
        return ref
    try:
        found = await asyncio.to_thread(_find_dataset, bind, org_id, ref)
    except AmbiguousDigestError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    if found is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{ref.benchmark}/{ref.dataset}' not found")
    dataset, version = found
    if dataset.archived_at is not None:
        raise HTTPException(
            status_code=409, detail=f"Dataset '{ref.benchmark}/{ref.dataset}' is archived; restore it first"
        )
    if version is None:
        raise HTTPException(
            status_code=404, detail=f"Version '{ref.ref}' of dataset '{ref.benchmark}/{ref.dataset}' not found"
        )
    return ref.model_copy(update={"dataset_id": dataset.id, "revision": version.revision})


async def _verify_dataset_task_ids(
    ref: DatasetRef, bind: Engine | Connection, objects: ObjectStore, task_ids: list[str] | None, slice_str: str | None
) -> VerifyTaskIdsResponse:
    try:
        tasks = await dataset_tasks(bind, ref) if isinstance(ref, HostedDataset) else None
    except UnresolvedDatasetError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    service = dataset_service(ref, objects, tasks=tasks)
    try:
        return await service.verify_task_ids(task_ids, slice_str)
    except (TaskSelectionError, FileNotFoundError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    finally:
        await service.close()


@app.post("/start-benchmark", description="Start a benchmark run from an uploaded agent or a complete contract.")
async def start_benchmark(
    http_request: Request,
    request: StartBenchmarkRequest,
    issued_keys: IssuedKeys,
    session: Session = Depends(get_session),
    run_starter: RequestIdentity = Depends(get_current_runner),
) -> StartBenchmarkResponse:
    settings = await asyncio.to_thread(user_run_settings, RunSettingsRepository(session), run_starter)
    request.use_settings(
        settings, await asyncio.to_thread(ProviderSecretRepository(session).list_all, run_starter.org.id)
    )
    if request.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(request.custom_benchmark_service, run_starter.org)
    try:
        validate_agent_name(request.contract.name)
        validate_service_headers(request.service_headers)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    reject_missing_project(run_starter.org, request.project)
    oracle = request.contract.name == ORACLE_AGENT
    if oracle:
        if request.dataset_ref is None:
            raise HTTPException(status_code=400, detail="The oracle agent runs only on a dataset")
        request = request.model_copy(update={"contract": ORACLE_CONTRACT})
    projects = ProjectRepository(session)
    named_project = find_project(projects, run_starter.org.id, request.project) if request.project is not None else None
    project = named_project or projects.get_or_create_default(run_starter.org.id, run_starter.email)
    selected_project_gateway_root = False

    bind = session.get_bind()
    session.close()
    aws_runtime: AWSRuntime | None = None
    if local_config.resources is not None:
        if request.properties is not None:
            raise HTTPException(status_code=400, detail="Local resources are configured by the server")
        try:
            request = StartBenchmarkRequest.model_validate(
                {
                    **request.model_dump(),
                    "environment": "local",
                    "properties": local_config.resources,
                    **({"sandbox_provider_secret_name": None} if reads_secrets_file(local_config.resources) else {}),
                }
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        if not reads_secrets_file(local_config.resources):
            if named_project is not None and selects_project_gateway_key(run_starter.org):
                request = await _with_project_gateway_secrets(request, named_project.id, run_starter.org, bind)
                selected_project_gateway_root = True
            request = _authorize_secret_names(request, run_starter.org)
        runtime = LocalRuntimeFactory.create_runtime(
            local_config.resources,
            run_starter.org.id,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.sandbox_provider_secret_name,
        )
        try:
            await runtime.get_sandbox_provider_config()
        except InvalidSandboxConfigurationError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
    else:
        if request.environment == "local":
            raise HTTPException(status_code=400, detail="Server has no local configuration")
        if isinstance(request.dataset_ref, LocalDataset):
            raise HTTPException(status_code=400, detail="A dataset folder can only run on a local Tracker")
        if request.sandbox_provider == "docker":
            raise HTTPException(status_code=400, detail="AWS execution does not support the Docker sandbox provider")
        if request.sandbox_provider_secret_name is None:
            provider = request.sandbox_provider
            raise HTTPException(
                status_code=400,
                detail=f"No secret is set for provider {provider}; run `valkyrie provider set {provider} SECRET`, "
                "or choose another with --provider or `valkyrie provider default`",
            )
        assert not isinstance(request.properties, LocalResources)
        aws_runtime = resolve_start_aws_runtime(_org_context(run_starter.org), request.properties)
        runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    object_store = runtime.objects

    if aws_runtime is not None:
        try:
            validate_managed_execution_request(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if named_project is not None and selects_project_gateway_key(run_starter.org):
            request = await _with_project_gateway_secrets(request, named_project.id, run_starter.org, bind)
            selected_project_gateway_root = True
        request = _authorize_secret_names(request, run_starter.org)

        try:
            await asyncio.to_thread(_validate_start_release, bind)
        except ReleaseControlError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    service_headers = dict(request.service_headers)
    if request.service_auth_header_name and request.service_auth_secret_name:
        resolved = await resolve_secrets(
            {request.service_auth_header_name: request.service_auth_secret_name},
            runtime.secrets,
        )
        service_headers = _replace_headers(service_headers, resolved)

    if request.dataset_ref is None:
        destination = classify_benchmark_service_destination(request.benchmark_name, request.custom_benchmark_service)
        _reject_docker_with_hosted_service(request.sandbox_provider, destination)
        run_key = await run_service_key(http_request, run_starter.org, destination, service_headers, issued_keys)
        service_headers = forward_tracker_api_key(service_headers, run_key, destination=destination)
    request = request.model_copy(
        update={
            **({"properties": aws_runtime.resources} if aws_runtime is not None else {}),
            "service_headers": service_headers,
        }
    )

    if aws_runtime is not None:
        try:
            validate_managed_execution_request(request)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    request = request.model_copy(
        update={"contract": request.contract.model_copy(update={"inference_settings_attested": False})}
    )

    resolved_queue_pool_id: str | None = None
    if not SANDBOX_QUEUE_ENABLED:
        if request.priority is not None:
            raise HTTPException(
                status_code=400,
                detail="Queue priority requires sandbox queue to be enabled",
            )
    elif request.sandbox_provider in {"modal", "docker"}:
        if request.priority is not None:
            raise HTTPException(
                status_code=400,
                detail="Queue priority requires a sandbox provider configured for admission",
            )
    else:
        if request.environment == "local":
            provider_config = await runtime.get_sandbox_provider_config()
        else:
            provider_secret_name = request.sandbox_provider_secret_name
            assert provider_secret_name is not None
            provider_config = await fetch_sandbox_provider_config(
                provider_secret_name,
                runtime.secrets,
                request.sandbox_provider,
            )
        provider = provider_config.create_provider()
        try:
            provider_pool_id = provider.admission_pool_id
        finally:
            await provider.close()

        if provider_pool_id is not None:
            if request.priority is None:
                request = request.model_copy(update={"priority": 3})
            resolved_queue_pool_id = queue_pool_id(provider_pool_id)
        elif request.priority is not None:
            raise HTTPException(
                status_code=400,
                detail="Queue priority requires a sandbox provider configured for admission",
            )

    if (
        aws_runtime is not None
        and not oracle
        and not await object_store.exists(agent_bundle_key(request.contract.name))
    ):
        raise HTTPException(
            status_code=404,
            detail=f"Agent '{request.contract.name}' is not in your organization's agent library. Push it first.",
        )

    if not request.contract.install_cmd and not request.contract.run_cmd:
        request = request.model_copy(update={"contract": await _resolve_contract_from_s3(request, object_store)})
        if aws_runtime is not None:
            try:
                validate_managed_execution_request(request)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not reads_secrets_file(request.properties):
            request = _authorize_secret_names(request, run_starter.org)

    logger.info("Starting benchmark run - contract: %s, benchmark: %s", request.contract.name, request.benchmark_name)

    execution_request = RunExecutionRequest.model_validate(request.model_dump(mode="python"))
    if request.dataset_ref is None:
        verify_response = await _verify_through_benchmark_service(request, execution_request)
    else:
        execution_request.dataset_ref = await _resolve_dataset(request.dataset_ref, bind, run_starter.org.id)
        verify_response = await _verify_dataset_task_ids(
            execution_request.dataset_ref, bind, object_store, request.task_ids, request.slice_str
        )

    benchmark_row = start_benchmark_request_to_benchmark(
        execution_request,
        run_starter,
        queue_pool_id=resolved_queue_pool_id,
    )
    dispatch_id = uuid4()
    created_agent_copy: StoredObjectCopy | None = None
    try:
        if not oracle:
            created_agent_copy = await copy_agent_to_benchmark(
                object_store,
                str(benchmark_row.id),
                request.contract.name,
            )
        telemetry_context = _executor_telemetry_context()
        commit_task = asyncio.create_task(
            asyncio.to_thread(
                _commit_start,
                bind,
                benchmark_row.model_dump_json(),
                execution_request,
                dispatch_id,
                verify_response.task_ids,
                resolved_queue_pool_id,
                telemetry_context,
                issued_keys,
                project.id,
                selected_project_gateway_root,
            )
        )
        (benchmark_json, result), cancellation = await _await_before_cancellation(commit_task)
        benchmark_row = Benchmark.model_validate(json.loads(benchmark_json))
        benchmark_row.arguments = benchmark_row.arguments.model_copy(
            update={"dataset_ref": execution_request.dataset_ref}
        )
        executor_dispatch = ExecutorDispatch.model_validate(json.loads(result.dispatch_json))
    except Exception as error:
        cancellation_after_failure: asyncio.CancelledError | None = None
        exc = error
        if isinstance(exc, _TaskFailedAfterCancellation):
            cancellation_after_failure = exc.cancellation
            exc = exc.task_error
        if isinstance(exc, _StartAdmissionRollbackError):
            assert exc.__cause__ is not None
            exc = exc.__cause__
        else:
            await _rollback_failed_start_admission(
                session,
                benchmark_id=benchmark_row.id,
                dispatch_id=dispatch_id,
                created_copy=created_agent_copy,
                request=request,
                object_store=object_store,
            )
        if cancellation_after_failure is not None:
            raise cancellation_after_failure from exc
        if isinstance(exc, ReleaseControlError):
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if isinstance(exc, HTTPException):
            raise exc
        raise TrackerServiceError("Failed to admit benchmark execution") from exc
    bind_failure: Exception | None = None
    try:
        await bind_benchmark_id(benchmark_row.id)
    except Exception as error:
        if cancellation is None:
            raise
        logger.exception(
            "Failed to bind benchmark context after cancelled admission",
            extra={"benchmark_id": str(benchmark_row.id), "executor_dispatch_id": str(dispatch_id)},
        )
        bind_failure = error

    if run_starter.access_key_id is not None and run_starter.email is None:
        logger.warning(
            "Access key %s resolved no user email; run attribution for this run will be empty",
            run_starter.access_key_id,
        )

    launch_task = asyncio.create_task(
        _launch_executor_dispatch(
            executor_dispatch,
            session=session,
            verified_task_ids=verify_response.task_ids,
        )
    )
    try:
        _, cancellation = await _await_before_cancellation(launch_task, cancellation)
    except _TaskFailedAfterCancellation as failure:
        raise failure.cancellation from failure.task_error
    if cancellation is not None:
        if bind_failure is not None:
            raise cancellation from bind_failure
        raise cancellation

    pinned_version = benchmark_row.arguments.with_hosted_dataset().dataset_version
    return StartBenchmarkResponse(
        benchmark_name=benchmark_row.name,
        agent_name=request.contract.name,
        benchmark_id=benchmark_row.id,
        concurrency=request.concurrency,
        started_at=benchmark_row.started_at,
        task_count=len(verify_response.task_ids),
        dataset_version=pinned_version,
        dataset_version_warning=(
            "Unversioned — dataset consistency is not guaranteed." if pinned_version is None else None
        ),
        cloudwatch_url=(
            str(http_request.url_for("get_logs", benchmark_id=benchmark_row.id))
            if request.environment == "local"
            else _console_location(run_starter.org, runtime.log_locations.benchmark_location(str(benchmark_row.id)))
        ),
        s3_bucket_url=(
            str(http_request.url_for("list_run_artifacts", benchmark_id=benchmark_row.id))
            if request.environment == "local"
            else _console_location(
                run_starter.org, runtime.artifacts.prefix_location(benchmark_artifact_prefix(str(benchmark_row.id)))
            )
        ),
        storage_bucket=aws_runtime.resources.s3_bucket if aws_runtime is not None else None,
        executor_release_id=benchmark_row.executor_release_id,
        current_execution_release_id=benchmark_row.current_execution_release_id,
        executor_artifact_digest=benchmark_row.executor_artifact_digest,
        executor_protocol_version=benchmark_row.executor_protocol_version,
        project=ProjectResponse.from_row(project),
    )


@app.post("/fetch-benchmark-tasks", description="List every task ID in a benchmark's dataset.")
async def fetch_benchmark_tasks(
    http_request: Request,
    request: FetchBenchmarkTasksRequest,
    issued_keys: IssuedKeys,
    identity: RequestIdentity = Depends(get_current_identity),
    session: Session = Depends(get_session),
) -> VerifyTaskIdsResponse:
    credential = request.use_saved_service(
        await asyncio.to_thread(user_run_settings, RunSettingsRepository(session), identity)
    )
    session.close()
    if request.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(request.custom_benchmark_service, identity.org)

    service_headers = dict(request.service_headers)
    if credential:
        header, name = credential
        org = identity.org
        reference = _resolve_secret_name(name, org)
        try:
            validate_secret_names([reference], org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        service_headers = _replace_headers(
            service_headers, await resolve_secrets({header: reference}, secret_store(org))
        )

    destination = classify_benchmark_service_destination(request.benchmark_name, request.custom_benchmark_service)
    service_key = await run_service_key(
        http_request, identity.org, destination, service_headers, issued_keys, for_run=False
    )
    try:
        benchmark_service = create_benchmark_service_client(
            url=request.custom_benchmark_service or create_benchmark_service_url(request.benchmark_name),
            service_headers=forward_tracker_api_key(service_headers, service_key, destination=destination),
        )
        try:
            await _wake_hosted_service(
                request.benchmark_name,
                request.custom_benchmark_service,
                benchmark_service,
            )
            return await benchmark_service.verify_task_ids(
                task_ids=None,
                slice_str=None,
                dataset=request.dataset,
            )
        finally:
            await benchmark_service.close()
    except (BenchmarkServiceError, httpx.HTTPError) as exc:
        logger.warning("Failed to fetch task ids from benchmark service %s: %s", request.benchmark_name, exc)
        raise HTTPException(status_code=502, detail="Failed to fetch task ids from benchmark service") from exc


@app.get(
    "/fetch-benchmark",
    response_model=None,
    description=(
        "Return a run with its progress by its ID. With `connect`, it streams the run's updates as server-sent events "
        "instead. Returns 404 when the run does not exist."
    ),
)
async def fetch_benchmark(
    http_request: Request,
    benchmark_id: TrackedBenchmarkId,
    runtime: RunRuntimeDependency,
    benchmark_row: RunBenchmarkDependency,
    connect: bool = Query(default=False),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarkResponse | StreamingResponse:
    s3_bucket_url = (
        str(http_request.url_for("list_run_artifacts", benchmark_id=benchmark_row.id))
        if benchmark_row.arguments.environment == "local"
        else _console_location(org, runtime.artifacts.prefix_location(benchmark_artifact_prefix(str(benchmark_row.id))))
    )
    storage_bucket = runtime.aws_runtime.resources.s3_bucket if isinstance(runtime, CloudRuntimeServices) else None
    if connect:
        bind = session.bind
        org_id = org.id
        session.close()
        return StreamingResponse(
            stream_benchmark_results(benchmark_id, bind, s3_bucket_url, org_id, storage_bucket=storage_bucket),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return fetch_benchmark_response(benchmark_row, session, s3_bucket_url, storage_bucket)


@app.post(
    "/analyze-benchmark/{benchmark_id}",
    response_model=None,
    description=(
        "Analyze a run with Docent and stream progress as server-sent events. The last event is `done` with the reading plan URL, or `error` with a message.\n"
        "\n"
        "A run that was already analyzed returns its existing reading plan URL unless no_cache is set."
    ),
)
async def analyze_benchmark(
    benchmark_id: TrackedBenchmarkId,
    benchmark_row: RunBenchmarkDependency,
    body: AnalyzeBenchmarkRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> dict[str, str] | StreamingResponse:
    if isinstance(benchmark_row.arguments.properties, LocalResources):
        raise HTTPException(status_code=400, detail="Docent analysis requires S3 storage")
    aws_runtime = resolve_run_aws_runtime(
        org=_org_context(org),
        properties=benchmark_row.arguments.properties,
    )

    if benchmark_row.status != BenchmarkStatus.FINISHED:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot analyze run {benchmark_id}: status is {benchmark_row.status.value} (must be FINISHED).",
        )

    if (
        not body.no_cache
        and benchmark_row.docent_reading_status == DocentReadingStatus.DONE
        and benchmark_row.docent_reading_url
    ):
        return {
            "status": "done",
            "reading_plan_url": benchmark_row.docent_reading_url,
        }

    if not body.lambda_function:
        raise HTTPException(
            status_code=400,
            detail=(
                f"No ingest_lambda provided for agent '{benchmark_row.arguments.contract.name}'. "
                "The CLI normally resolves this from the agent's pushed contract — if you're "
                "calling this endpoint directly, supply `lambda_function` in the request body."
            ),
        )

    payload: dict[str, Any] = {
        "benchmark_id": str(benchmark_id),
        "benchmark_name": benchmark_row.name,
        "s3_bucket": aws_runtime.resources.s3_bucket,
        "contract": {"name": benchmark_row.arguments.contract.name},
    }

    return StreamingResponse(
        analyze_event_stream(
            benchmark_id=benchmark_row.id,
            lambda_function=body.lambda_function,
            payload=payload,
            clients=aws_runtime.clients,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _filter_final_view_to_tasks(final_view: FinalViewResponse, task_ids: set[str]) -> None:
    def _filter_task_map(task_map: dict[str, Any] | None) -> dict[str, Any] | None:
        return {task_id: value for task_id, value in (task_map or {}).items() if task_id in task_ids} or None

    final_view.evaluation_results = _filter_task_map(final_view.evaluation_results)
    final_view.task_errors = _filter_task_map(final_view.task_errors)
    final_view.task_failure_categories = _filter_task_map(final_view.task_failure_categories)


async def _recompute_final_evaluation(
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    http_request: Request,
    issued_keys: list[IssuedRunKey],
    task_ids: set[str] | None,
) -> FinalEvaluation:
    scored_results = fetch_final_score_inputs(session, benchmark_row, org)
    if task_ids is not None:
        scored_results = {task_id: result for task_id, result in scored_results.items() if task_id in task_ids}

    if benchmark_row.arguments.dataset_ref is not None:
        ref = benchmark_row.arguments.dataset_ref
        script_digest = (
            DatasetVersionRepository(session).metric_digest(ref.dataset_id, ref.revision)
            if isinstance(ref, HostedDataset) and ref.dataset_id is not None and ref.revision is not None
            else None
        )
        scorer = await scorer_for(ref, org.id, script_digest, get_agent_library_runtime(org).objects)
        scored = await scorer.score(scored_results)
        return FinalEvaluation(
            org_id=org.id, benchmark=benchmark_row.id, final_score=scored.final_score, properties=scored.properties
        )

    if benchmark_row.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(benchmark_row.custom_benchmark_service, org)

    destination = classify_benchmark_service_destination(benchmark_row.name, benchmark_row.custom_benchmark_service)
    service_key = await run_service_key(http_request, org, destination, None, issued_keys, for_run=False)
    benchmark_service = benchmark_row.benchmark_service(
        service_headers=forward_tracker_api_key(None, service_key, destination=destination)
    )
    try:
        await _wake_hosted_service(
            benchmark_row.name,
            benchmark_row.custom_benchmark_service,
            benchmark_service,
        )
        response = await benchmark_service.final_score(
            evaluation_results=scored_results,
            dataset=benchmark_row.arguments.dataset,
        )
    finally:
        await benchmark_service.close()

    return FinalEvaluation(
        org_id=org.id,
        benchmark=benchmark_row.id,
        final_score=response.final_score,
        properties=response.metadata,
    )


async def _invoke_preview_lambda(benchmark_row: Benchmark, aws_runtime: AWSRuntime) -> None:
    lambda_function = benchmark_row.arguments.lambda_function
    if lambda_function is None:
        return

    lambda_payload = benchmark_row.arguments.model_dump()
    lambda_payload.update(
        {
            "benchmark_id": str(benchmark_row.id),
            "benchmark_name": benchmark_row.name,
            "bucket": aws_runtime.resources.s3_bucket,
            "preview": True,
        }
    )
    await invoke_lambda(
        aws_runtime.clients,
        lambda_function,
        lambda_payload,
        config=_COMPLETION_CALLBACK_CONFIG,
    )


async def _archive_final_view(benchmark_row: Benchmark, aws_runtime: AWSRuntime) -> None:
    s3_key = final_view_s3_key(benchmark_row)
    if not await s3_object_exists(s3_key, aws_runtime):
        return
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    archive_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_row.id}/archive/{timestamp}/{benchmark_row.name}.json"
    await copy_s3_object(s3_key, archive_key, aws_runtime)


async def _s3_results_response(s3_key: str, aws_runtime: AWSRuntime) -> S3UploadResultsResponse:
    expires_in = aws_runtime.clients.maximum_presign_ttl(86400)
    return S3UploadResultsResponse(
        s3_url=f"s3://{aws_runtime.resources.s3_bucket}/{s3_key}",
        presigned_url=await create_presigned_url(s3_key, aws_runtime, expiration=expires_in),
        console_url=create_console_url(s3_key, aws_runtime.resources),
        expires_in=expires_in,
    )


async def _retrieve_results(
    benchmark_id: UUID,
    http_request: Request,
    issued_keys: list[IssuedRunKey],
    s3: bool,
    preview: bool,
    task_ids: list[str] | None,
    session: Session,
    org: Org,
) -> RetrieveResultsResponse:
    repository = BenchmarkRepository(session)
    benchmark_row = assert_org(repository.get(org.id, benchmark_id), org)

    final_view = create_final_view(benchmark_row, session, org)
    task_ids_set = set(task_ids) if task_ids else None

    if task_ids_set is not None:
        unknown_task_ids = task_ids_set - repository.task_ids(org.id, benchmark_id)
        if unknown_task_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Task IDs not found in this run: {', '.join(sorted(unknown_task_ids))}",
            )

        _filter_final_view_to_tasks(final_view, task_ids_set)

    if task_ids_set is not None or preview:
        final_view.final_evaluation = await _recompute_final_evaluation(
            benchmark_row,
            session,
            org,
            http_request,
            issued_keys,
            task_ids_set,
        )

    if not s3:
        return final_view

    if isinstance(benchmark_row.arguments.properties, LocalResources):
        raise HTTPException(status_code=400, detail="S3 export requires S3 storage")
    aws_runtime = resolve_run_aws_runtime(
        properties=benchmark_row.arguments.properties,
        org=_org_context(org),
    )

    if preview:
        await _archive_final_view(benchmark_row, aws_runtime)
    s3_key = await upload_final_view(final_view, S3ObjectStore(aws_runtime))
    if preview:
        await _invoke_preview_lambda(benchmark_row, aws_runtime)
    return await _s3_results_response(s3_key, aws_runtime)


@app.get(
    "/retrieve-results", description="Retrieve a benchmark's current results, optionally filtered or uploaded to S3."
)
async def retrieve_results(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    issued_keys: IssuedKeys,
    s3: bool = Query(default=False),
    task_ids: list[str] | None = Query(default=None),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> RetrieveResultsResponse:
    return await _retrieve_results(benchmark_id, http_request, issued_keys, s3, False, task_ids, session, org)


@app.post(
    "/rescore-benchmark/{benchmark_id}",
    description=(
        "Score a finished dataset run again, running its metric script again and replacing the stored score. "
        "Returns 502 and keeps the stored score when the metric script fails."
    ),
)
async def rescore_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    issued_keys: IssuedKeys,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> RescoreBenchmarkResponse:
    try:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if benchmark_row.arguments.dataset_ref is None:
        raise HTTPException(status_code=400, detail="Only a run on a dataset has a scorer to run again")
    if benchmark_row.status != BenchmarkStatus.FINISHED:
        raise HTTPException(status_code=409, detail="Only a finished run can be scored again")
    rescored = await _recompute_final_evaluation(benchmark_row, session, org, http_request, issued_keys, None)
    scorer_error = rescored.properties.get("scorer", {}).get("error")
    if scorer_error is not None:
        raise HTTPException(status_code=502, detail=f"The metric script failed: {scorer_error}")
    stored = benchmark_row.final_evaluation
    if stored is None:
        stored = FinalEvaluationRow(org_id=org.id, benchmark=benchmark_row.id, final_score=None)
        BenchmarkRepository(session).save(stored)
    stored.final_score = rescored.final_score
    stored.properties = rescored.properties
    session.commit()
    final_view = create_final_view(benchmark_row, session, org)
    final_view.final_evaluation = FinalEvaluation.model_validate(stored)
    await upload_final_view(final_view, _run_objects(benchmark_row, org))
    return RescoreBenchmarkResponse(
        benchmark_id=benchmark_row.id, final_score=stored.final_score, properties=stored.properties
    )


@app.get(
    "/preview-results",
    description="Archive the current S3 final view, replace it with a recomputed one, and run the completion callback.",
)
async def preview_results(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    issued_keys: IssuedKeys,
    task_ids: list[str] | None = Query(default=None),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> S3UploadResultsResponse:
    response = await _retrieve_results(benchmark_id, http_request, issued_keys, True, True, task_ids, session, org)
    return cast(S3UploadResultsResponse, response)


@app.get(
    "/check-results-exist",
    description="Report whether a run's final results already exist in S3.",
)
async def check_results_exist(
    benchmark_id: TrackedBenchmarkId,
    runtime: RunRuntimeDependency,
    benchmark_row: RunBenchmarkDependency,
) -> dict[str, bool]:
    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{benchmark_row.name}.json"
    exists = await runtime.objects.exists(s3_key)
    return {"exists": exists}


async def validate_tasks_exist(
    benchmark_row: Benchmark,
    task_ids: list[str],
    session: Session,
    org: Org,
) -> list[str]:
    if not task_ids:
        raise HTTPException(status_code=400, detail="task_ids cannot be empty")

    requested_task_ids = list(dict.fromkeys(task_ids))
    existing_task_ids = await database_call(
        lambda: BenchmarkRepository(session).task_ids(org.id, benchmark_row.id, requested_task_ids)
    )
    missing_task_ids = [task_id for task_id in requested_task_ids if task_id not in existing_task_ids]
    if missing_task_ids:
        raise HTTPException(
            status_code=400,
            detail=f"Task IDs are not part of run {benchmark_row.id}: {', '.join(missing_task_ids)}",
        )

    return requested_task_ids


def _resolve_force_stop_provider_secret_name(benchmark_row: Benchmark) -> str:
    provider_secret_name = benchmark_row.arguments.sandbox_provider_secret_name
    if provider_secret_name:
        return provider_secret_name
    raise HTTPException(status_code=400, detail="The run does not have a sandbox provider secret name.")


@app.get(
    "/benchmarks/{benchmark_id}/monitor/stop-receipt",
    description="Report whether any provider sandbox labeled for the task remains, for a current attempt.",
)
async def monitor_stop_receipt(
    benchmark_id: TrackedBenchmarkId,
    task_id: str = Query(min_length=1),
    expected_attempt_id: str = Query(min_length=1),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> dict[str, str]:
    repository = BenchmarkRepository(session)
    benchmark_row = assert_org(repository.get(org.id, benchmark_id), org)
    task = repository.get_task(org.id, benchmark_id, task_id)
    if task is None or task_attempt_id(task.id, task.started_at) != expected_attempt_id:
        raise HTTPException(status_code=409, detail="Task attempt has changed")

    async def inspect() -> str:
        runtime = _force_stop_runtime(benchmark_row, org)
        config = await runtime.get_sandbox_provider_config()
        async with config.create_provider() as provider:
            sandboxes = [sandbox async for sandbox in sandbox_generator(benchmark_row, provider, [task_id])]
        return "tracked_sandboxes_present" if sandboxes else "tracked_sandboxes_absent"

    try:
        state = await asyncio.wait_for(inspect(), timeout=15)
    except Exception:
        logger.warning("Provider inventory inspection failed", exc_info=True)
        raise HTTPException(status_code=503, detail="Provider inventory unavailable") from None
    session.expire_all()
    current = repository.get_task(org.id, benchmark_id, task_id, refresh=True)
    if current is None or task_attempt_id(current.id, current.started_at) != expected_attempt_id:
        raise HTTPException(status_code=409, detail="Task attempt has changed")
    return {"state": state}


@app.post(
    "/stop-benchmark/{benchmark_id}",
    description=(
        "Stop a run by its ID.\n"
        "\n"
        "With force, the run stops right away and sandboxes are killed without waiting for teardown."
    ),
)
async def stop_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    force: bool = Query(default=False),
    task_ids: list[str] | None = Body(default=None, embed=True),
    expected_attempt_ids: dict[str, str] | None = Body(default=None, embed=True),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> StopBenchmarkResponse:
    benchmark_row = assert_org(await database_call(lambda: BenchmarkRepository(session).get(org.id, benchmark_id)), org)

    if expected_attempt_ids is not None:
        if not task_ids or set(expected_attempt_ids) != set(task_ids):
            raise HTTPException(status_code=422, detail="expected_attempt_ids must cover exactly the selected task_ids")
        if any(not attempt.strip() for attempt in expected_attempt_ids.values()):
            raise HTTPException(status_code=422, detail="expected_attempt_ids values must not be blank")

    get_executor_admission(session, for_update=True)

    valid_stop_states = [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPING]
    if force and expected_attempt_ids is not None:
        valid_stop_states.append(BenchmarkStatus.STOPPED)

    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status.value} state. Can only pause an in progress or error run.",
        )

    selected_task_ids = (
        await validate_tasks_exist(benchmark_row, task_ids, session, org) if task_ids is not None else None
    )

    runtime = _force_stop_runtime(benchmark_row, org) if force else None

    benchmark_row = await database_call(lambda: fetch_benchmark_row(benchmark_id, session, org, for_update=True))
    if benchmark_row.status not in valid_stop_states:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status.value} state. Can only pause an in progress or error run.",
        )

    captured_sandboxes = None
    if expected_attempt_ids is not None:
        tasks = await database_call(
            lambda: BenchmarkRepository(session).lock_tasks(org.id, benchmark_id, task_ids=selected_task_ids or [])
        )
        if len(tasks) != len(expected_attempt_ids) or any(
            task_attempt_id(task.id, task.started_at) != expected_attempt_ids[task.task_id] for task in tasks
        ):
            raise HTTPException(status_code=409, detail="Task attempt has changed")
        if runtime is not None:
            try:
                captured_sandboxes = await capture_stop_sandboxes(
                    benchmark_row, runtime, selected_task_ids or [], strict=True
                )
            except ForceStopError:
                raise HTTPException(
                    status_code=503, detail="Unable to capture force-stop targets; retry the request"
                ) from None

    await initiate_stop_benchmark(benchmark_row, session, force, org, task_ids=selected_task_ids)

    if runtime is not None:
        if expected_attempt_ids is None:
            await force_stop_sandboxes(benchmark_row, runtime, org, task_ids=selected_task_ids)
        else:
            try:
                await force_stop_sandboxes(
                    benchmark_row, runtime, org, task_ids=selected_task_ids, sandboxes=captured_sandboxes, strict=True
                )
            except ForceStopError:
                raise HTTPException(
                    status_code=503, detail="Unable to send force-stop signals; retry the request"
                ) from None

    return StopBenchmarkResponse(status="success")


def _update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    try:
        with session.no_autoflush:
            lock_executor_admission(session)
        benchmark_row = update_benchmark_concurrency(benchmark_id, concurrency, session, org)
    except MaintenanceModeError as exc:
        session.rollback()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Not found") from exc

    if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
        raise HTTPException(
            status_code=409,
            detail=f"Run {benchmark_id} is currently in the {benchmark_row.status.value} state.",
        )
    return benchmark_row


def _console_location(org: Org, location: str) -> str:
    return location if may_use_shared_secrets(org.name) else ""


def _run_objects(benchmark_row: Benchmark, org: Org) -> ObjectStore:
    properties = benchmark_row.arguments.properties
    if isinstance(properties, LocalResources):
        return LocalRuntimeFactory.create_runtime(properties, org.id).objects
    return S3ObjectStore(resolve_run_aws_runtime(properties=properties, org=org))


def _force_stop_runtime(benchmark_row: Benchmark, org: Org) -> RuntimeServices:
    if benchmark_row.arguments.environment == "local":
        return LocalRuntimeFactory.create_runtime(
            benchmark_row.arguments.properties,
            org.id,
            sandbox_provider=benchmark_row.arguments.sandbox_provider,
            sandbox_provider_secret_name=benchmark_row.arguments.sandbox_provider_secret_name,
        )
    aws_runtime = resolve_run_aws_runtime(
        properties=benchmark_row.arguments.properties,
        org=_org_context(org),
    )
    return CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider=benchmark_row.arguments.sandbox_provider,
        sandbox_provider_secret_name=_resolve_force_stop_provider_secret_name(benchmark_row),
    )


def _commit_shed(
    bind: Engine | Connection,
    org_id: UUID,
    benchmark_id: UUID,
    request: ShedBenchmarkRequest,
) -> list[str]:
    with Session(bind) as session:
        org = OrganizationRepository(session).get(org_id)
        assert org is not None
        try:
            with session.no_autoflush:
                lock_executor_admission(session)
        except MaintenanceModeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            raise HTTPException(
                status_code=409,
                detail=f"Run {benchmark_id} is currently in the {benchmark_row.status.value} state.",
            )
        current = benchmark_row.arguments.concurrency
        if request.concurrency >= current:
            raise HTTPException(
                status_code=400,
                detail=f"Run {benchmark_id} concurrency is {current}; shed can only lower it.",
            )

        task_ids = apply_shed_benchmark(benchmark_row, request.concurrency, session, org)
        if not request.dry_run:
            session.commit()
        return task_ids


@app.post(
    "/benchmarks/{benchmark_id}/shed",
    description="Lower an active run's concurrency and force stop the newest tasks above the new limit.",
)
async def shed_benchmark(
    benchmark_id: TrackedBenchmarkId,
    request: ShedBenchmarkRequest,
    http_request: Request,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> ShedBenchmarkResponse:
    benchmark_row = assert_org(BenchmarkRepository(session).get(org.id, benchmark_id), org)
    runtime = None if request.dry_run else _force_stop_runtime(benchmark_row, org)
    session.close()
    task_ids = await asyncio.to_thread(_commit_shed, session.get_bind(), org.id, benchmark_id, request)
    if runtime is not None and task_ids:
        await force_stop_sandboxes(benchmark_row, runtime, org, task_ids=task_ids)
    return ShedBenchmarkResponse(benchmark_id=benchmark_id, concurrency=request.concurrency, task_ids=task_ids)


@app.patch("/benchmarks/{benchmark_id}/concurrency", description="Set how many of a run's tasks may execute at once.")
def patch_benchmark_concurrency(
    benchmark_id: TrackedBenchmarkId,
    request: UpdateBenchmarkConcurrencyRequest,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> UpdateBenchmarkConcurrencyResponse:
    benchmark_row = _update_benchmark_concurrency(benchmark_id, request.concurrency, session, org)
    return UpdateBenchmarkConcurrencyResponse(
        benchmark_id=benchmark_row.benchmark_id,
        status=benchmark_row.status,
        concurrency=benchmark_row.concurrency,
    )


@app.post(
    "/benchmarks/{benchmark_id}/project",
    description="Put a run in another project, in any run state, by adding a link. Earlier links are kept.",
)
def retag_benchmark(
    benchmark_id: TrackedBenchmarkId,
    request: RunProjectRequest,
    session: Session = Depends(get_session),
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    benchmark_row = assert_org(BenchmarkRepository(session).get(identity.org.id, benchmark_id), identity.org)
    project = find_project(ProjectRepository(session), identity.org.id, request.project)
    BenchmarkRepository(session).save(
        RunProject(benchmark_id=benchmark_row.id, project_id=project.id, created_by_email=identity.email)
    )
    session.commit()
    return ProjectResponse.from_row(project)


@dataclass(frozen=True)
class AdmissionResult:
    dispatch_json: str
    verified_task_ids: tuple[str, ...]


def _admission_result(dispatch: ExecutorDispatch, task_ids: list[str]) -> AdmissionResult:
    return AdmissionResult(dispatch.model_dump_json(), tuple(task_ids))


@dataclass(frozen=True)
class RecoveryPreparation:
    state: RetryState | None
    benchmark_name: str
    service_url: str | None
    dataset: str | None
    dataset_version: BenchmarkDatasetVersion | None
    dataset_ref: DatasetRef | None
    queued_recovery: bool
    agent_name: str
    sandbox_provider: str
    properties: AWSResources | LocalResources | None = None
    resolved_properties: AWSResources | None = None


def _prepare_recovery(
    bind: Engine | Connection,
    benchmark_id: UUID,
    org_id: UUID,
    retry: bool,
    retry_mode: RetryMode,
    concurrency: int | None,
    task_ids: list[str],
    benchmark_url: str | None,
    secrets: dict[str, str],
) -> RecoveryPreparation:
    with Session(bind) as session:
        org = OrganizationRepository(session).get(org_id)
        assert org is not None
        benchmark = assert_org(BenchmarkRepository(session).get(org.id, benchmark_id), org)
        if benchmark.arguments.dataset_version is not None and benchmark_url is not None:
            raise HTTPException(status_code=400, detail="A pinned run cannot change its benchmark service URL")
        if benchmark.status == BenchmarkStatus.STOPPING:
            raise HTTPException(
                status_code=400,
                detail=f"Run {benchmark_id} is in the {benchmark.status.value} state. Cannot continue a run that is stopping.",
            )
        if benchmark.status == BenchmarkStatus.IN_PROGRESS and not retry and secrets:
            raise HTTPException(
                status_code=409, detail="Secret overrides require retry=true while a run is in progress."
            )
        effective_url = benchmark_url if benchmark_url is not None else benchmark.custom_benchmark_service
        if effective_url is not None:
            _authorize_custom_benchmark_destination(effective_url, org)
        queued = (
            benchmark.status == BenchmarkStatus.IN_PROGRESS
            and not retry
            and concurrency is None
            and benchmark.arguments.queue_pool_id is not None
        )
        state = None
        if benchmark.status != BenchmarkStatus.IN_PROGRESS or retry or queued:
            state = prepare_retry_state(benchmark, session, retry, task_ids, org, queued_recovery=queued)
        if not queued and (error := regrade_rerun_error(benchmark, session, retry, task_ids, org, retry_mode)):
            raise HTTPException(status_code=400, detail=error)
        return RecoveryPreparation(
            state,
            benchmark.name,
            effective_url,
            benchmark.arguments.dataset,
            benchmark.arguments.dataset_version,
            benchmark.arguments.dataset_ref,
            queued,
            benchmark.arguments.contract.name,
            benchmark.arguments.sandbox_provider,
            benchmark.arguments.properties,
        )


def _recovery_objects(preparation: RecoveryPreparation, aws_runtime: AWSRuntime | None, org_id: UUID) -> ObjectStore:
    if isinstance(preparation.properties, LocalResources):
        return LocalRuntimeFactory.create_runtime(preparation.properties, org_id).objects
    if aws_runtime is None:
        raise TrackerServiceError("Managed recovery has no resolved AWS resources")
    return S3ObjectStore(aws_runtime)


def _commit_recovery(
    bind: Engine | Connection,
    org_id: UUID,
    *,
    benchmark_id: UUID,
    issued_keys: Sequence[IssuedRunKey],
    retry: bool,
    retry_mode: RetryMode,
    concurrency: int | None,
    task_ids: list[str],
    service_headers: dict[str, str],
    secrets: dict[str, str],
    benchmark_url: str | None,
    lambda_function: str | None,
    preparation: RecoveryPreparation,
    verified_task_ids: list[str],
    telemetry_context: ExecutorTelemetryContext,
    update_agent: bool = False,
) -> AdmissionResult | None:
    with Session(bind, expire_on_commit=False) as session:
        org = OrganizationRepository(session).get(org_id)
        assert org is not None
        return _apply_recovery(
            benchmark_id,
            issued_keys,
            retry,
            retry_mode,
            concurrency,
            task_ids,
            service_headers,
            secrets,
            benchmark_url,
            lambda_function,
            session,
            org,
            preparation,
            verified_task_ids,
            telemetry_context,
            update_agent=update_agent,
        )


_UPDATE_AGENT_IN_PROGRESS = "Updating the agent of an in-progress run requires stopping the run first."


async def _discard_attempt_records(store: ObjectStore | None, benchmark_id: UUID, keys: Sequence[str]) -> None:
    if store is None:
        return
    for outcome in await asyncio.gather(*(store.delete(key) for key in keys), return_exceptions=True):
        if isinstance(outcome, Exception):
            logger.error(
                "Attempt record of a retry that did not commit was not deleted",
                exc_info=outcome,
                extra={"benchmark_id": str(benchmark_id)},
            )
            sentry_sdk.capture_exception(outcome)


async def _refresh_recovered_agent(
    copier: ObjectStore,
    *,
    benchmark_id: UUID,
    agent_name: str,
    session: Session,
    admission: AdmissionResult,
) -> None:
    try:
        await copier.copy(agent_bundle_key(agent_name), benchmark_agent_bundle_key(str(benchmark_id), agent_name))
    except Exception as exc:
        dispatch = ExecutorDispatch.model_validate(json.loads(admission.dispatch_json))
        logger.exception(
            "Failed to refresh the agent bundle for an admitted recovery",
            extra={"executor_dispatch_id": str(dispatch.id)},
        )
        _ = await asyncio.to_thread(
            _resolve_enqueue_failure,
            session.get_bind(),
            dispatch.benchmark_id,
            dispatch.id,
            list(admission.verified_task_ids),
        )
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Agent bundle refresh failed; use Retry to continue",
                "benchmark_id": str(dispatch.benchmark_id),
                "executor_dispatch_id": str(dispatch.id),
            },
        ) from exc


@app.post(
    "/retry-or-resume-benchmark/{benchmark_id}",
    dependencies=[Depends(get_current_runner)],
    description=(
        "Retry a run's errored tasks or resume its stopped ones.\n"
        "\n"
        "The request can change concurrency, refresh the agent, pick task IDs, replace the benchmark URL, or replace the Lambda callback. A valid task ID with no row yet gets a new pending task."
    ),
)
async def retry_or_resume_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    issued_keys: IssuedKeys,
    retry: bool = Query(default=False),
    retry_mode: RetryMode = Query(default=RetryMode.AUTO),
    update_agent: bool = False,
    concurrency: int | None = Query(default=None),
    task_ids: list[str] = Body(default=[]),
    service_headers: dict[str, str] = Body(default={}),
    secrets: dict[str, str] = Body(default={}),
    benchmark_url: str | None = Body(default=None),
    lambda_function: Annotated[str | None, Body(min_length=1)] = None,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> RetryOrResumeBenchmarkResponse:
    org_id, bind = org.id, session.get_bind()
    try:
        validate_service_headers(service_headers)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    project = ProjectRepository(session).get_for_run(
        assert_org(BenchmarkRepository(session).get(org.id, benchmark_id), org).id
    )
    if project is not None:
        reject_archived_project(project)
    session.close()
    if benchmark_url is not None:
        try:
            benchmark_url = validate_service_url_syntax(benchmark_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    if concurrency is not None and concurrency < 1:
        raise HTTPException(status_code=400, detail="Concurrency must be greater than 0.")
    preparation = await asyncio.to_thread(
        _prepare_recovery,
        bind,
        benchmark_id,
        org_id,
        retry,
        retry_mode,
        concurrency,
        task_ids,
        benchmark_url,
        secrets,
    )
    if preparation.dataset_ref is not None and (service_headers or benchmark_url is not None):
        raise HTTPException(
            status_code=400, detail="A dataset run has no benchmark service to send headers or a URL to"
        )
    aws_runtime: AWSRuntime | None = None
    if isinstance(preparation.properties, LocalResources):
        if local_config.resources is None:
            raise HTTPException(status_code=400, detail="Server has no local configuration")
        if lambda_function:
            raise HTTPException(status_code=400, detail="Lambda callbacks require S3 storage")
    else:
        aws_runtime = resolve_run_aws_runtime(
            properties=preparation.properties,
            org=_org_context(org),
        )
        preparation = replace(preparation, resolved_properties=aws_runtime.resources)
        if lambda_function:
            try:
                await dry_run_lambda(aws_runtime.clients, lambda_function)
            except LambdaError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    effective_headers = service_headers
    if preparation.dataset_ref is None:
        destination = classify_benchmark_service_destination(preparation.benchmark_name, preparation.service_url)
        _reject_docker_with_hosted_service(preparation.sandbox_provider, destination)
        run_key = await run_service_key(http_request, org, destination, service_headers, issued_keys)
        effective_headers = forward_tracker_api_key(service_headers, run_key, destination=destination)
    verified_task_ids = list(preparation.state.task_ids) if preparation.state is not None else []
    if verified_task_ids and not preparation.queued_recovery:
        if preparation.dataset_ref is None:
            service = create_benchmark_service_client(
                preparation.service_url or create_benchmark_service_url(preparation.benchmark_name),
                effective_headers,
                preparation.dataset_version,
                sandbox_provider=preparation.sandbox_provider,
            )
            try:
                await _wake_hosted_service(preparation.benchmark_name, preparation.service_url, service)
                verified = await service.verify_task_ids(
                    task_ids=verified_task_ids, slice_str=None, dataset=preparation.dataset
                )
                verified_task_ids = verified.task_ids
            except BenchmarkServiceUnauthenticatedError:
                raise
            except (BenchmarkServiceError, httpx.HTTPError, ValueError) as exc:
                raise _verify_task_ids_failure(preparation.benchmark_name, exc) from exc
            finally:
                await service.close()
        else:
            objects = _recovery_objects(preparation, aws_runtime, org_id)
            verified_task_ids = (
                await _verify_dataset_task_ids(preparation.dataset_ref, bind, objects, verified_task_ids, None)
            ).task_ids
    telemetry_context = _executor_telemetry_context()
    prior_attempts = preparation.state.attempts if preparation.state is not None else ()
    run_store: ObjectStore | None = None
    if update_agent or prior_attempts:
        run_store = _recovery_objects(preparation, aws_runtime, org_id)
    library_store: ObjectStore | None = None
    if update_agent:
        assert run_store is not None
        library_store = run_store
        if not await library_store.exists(agent_bundle_key(preparation.agent_name)):
            raise HTTPException(
                status_code=404,
                detail=f"Agent {preparation.agent_name!r} was not found. Push the agent before using --update-agent.",
            )
    recorded_attempts: list[str] = []
    if prior_attempts:
        assert run_store is not None
        with storage_errors("Task attempt"):
            recorded_attempts = await record_prior_attempts(run_store, str(benchmark_id), prior_attempts)
    commit_task = asyncio.create_task(
        asyncio.to_thread(
            _commit_recovery,
            bind,
            org_id,
            benchmark_id=benchmark_id,
            issued_keys=issued_keys,
            retry=retry,
            retry_mode=retry_mode,
            concurrency=concurrency,
            task_ids=task_ids,
            service_headers=effective_headers,
            secrets=secrets,
            benchmark_url=benchmark_url,
            lambda_function=lambda_function,
            preparation=preparation,
            verified_task_ids=verified_task_ids,
            telemetry_context=telemetry_context,
            update_agent=update_agent,
        )
    )
    try:
        result, cancellation = await _await_before_cancellation(commit_task)
    except _TaskFailedAfterCancellation as failure:
        await _discard_attempt_records(run_store, benchmark_id, recorded_attempts)
        raise failure.cancellation from failure.task_error
    except Exception:
        await _discard_attempt_records(run_store, benchmark_id, recorded_attempts)
        raise
    if result is not None:
        if library_store is not None:
            refresh_task = asyncio.create_task(
                _refresh_recovered_agent(
                    library_store,
                    benchmark_id=benchmark_id,
                    agent_name=preparation.agent_name,
                    session=session,
                    admission=result,
                )
            )
            try:
                _, cancellation = await _await_before_cancellation(refresh_task, cancellation)
            except _TaskFailedAfterCancellation as failure:
                raise failure.cancellation from failure.task_error
        launch_task = asyncio.create_task(
            _launch_executor_dispatch(
                ExecutorDispatch.model_validate(json.loads(result.dispatch_json)),
                session=session,
                verified_task_ids=list(result.verified_task_ids),
            )
        )
        try:
            _, cancellation = await _await_before_cancellation(launch_task, cancellation)
        except _TaskFailedAfterCancellation as failure:
            raise failure.cancellation from failure.task_error
    if cancellation is not None:
        raise cancellation
    return RetryOrResumeBenchmarkResponse(status="success")


@dataclass(frozen=True)
class RegradeSource:
    run: Benchmark
    tasks: RegradeTasks


def _regrade_source(session: Session, org: Org, source_run_id: UUID, *, for_update: bool = False) -> RegradeSource:
    repository = BenchmarkRepository(session)
    source = assert_org(
        repository.lock(org.id, source_run_id) if for_update else repository.get(org.id, source_run_id), org
    )
    if source.status != BenchmarkStatus.FINISHED:
        raise HTTPException(
            status_code=409,
            detail=f"Run {source_run_id} is {source.status.value}; only a finished run can be regraded.",
        )
    tasks = regrade_tasks(repository.list_tasks(org.id, source.id))
    if not tasks.eval_resume_states:
        raise HTTPException(status_code=400, detail=f"Run {source_run_id} has no tasks with saved evaluation state.")
    return RegradeSource(Benchmark.model_validate(source.model_dump()), tasks)


def _prepare_regrade(bind: Engine | Connection, org: Org, source_run_id: UUID) -> RegradeSource:
    with Session(bind) as session:
        return _regrade_source(session, org, source_run_id)


def _commit_regrade(
    bind: Engine | Connection,
    org: Org,
    source_id: UUID,
    run_json: str,
    service_headers: dict[str, str],
    tasks: RegradeTasks,
    dispatch_id: UUID,
    telemetry_context: ExecutorTelemetryContext,
    issued_keys: Sequence[IssuedRunKey],
) -> AdmissionResult:
    payload_key = generate_payload_key(dispatch_id)
    with Session(bind, expire_on_commit=False) as session:
        run = Benchmark.model_validate(json.loads(run_json))
        run.arguments = run.arguments.model_copy(
            update={"regraded_from_run_id": source_id, "regrade_skipped_task_ids": tasks.skipped_task_ids}
        )
        try:
            if _regrade_source(session, org, source_id, for_update=True).tasks != tasks:
                raise HTTPException(status_code=409, detail="Source run changed during regrade; retry the request.")
            dispatch = admit_start_dispatch(session, benchmark=run, dispatch_id=dispatch_id, task_ids=tasks.task_ids)
            repository = BenchmarkRepository(session)
            repository.add_tasks_with_errors(*regrade_task_rows(run, tasks, dispatch.created_at))
            project = ProjectRepository(session).get_for_run(source_id)
            if project is not None:
                reject_archived_project(ProjectRepository(session).lock(project.id))
                repository.save(
                    RunProject(benchmark_id=run.id, project_id=project.id, created_by_email=run.started_by_email)
                )
            request = (
                run.local_start_benchmark_request(service_headers=service_headers)
                if run.arguments.environment == "local"
                else run.managed_start_benchmark_request(service_headers=service_headers)
            )
            _persist_dispatch_payload(
                session,
                dispatch,
                _process_benchmark_kwargs(run, request, tasks.task_ids),
                telemetry_context,
                payload_key,
            )
            repository.save(*(RunAccessKey(key_id=key.key_id, benchmark_id=run.id) for key in issued_keys))
            session.commit()
        except ReleaseControlError as exc:
            session.rollback()
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception:
            session.rollback()
            raise
        return _admission_result(dispatch, tasks.task_ids)


@app.post(
    "/regrade-benchmark/{benchmark_id}",
    description=(
        "Regrade a finished run's saved task states into a new run on another dataset.\n"
        "\n"
        "The source run, its tasks and its evaluation stay unchanged. Tasks without saved evaluation state are skipped and listed on the new run."
    ),
)
async def regrade_benchmark(
    benchmark_id: TrackedBenchmarkId,
    http_request: Request,
    request: RegradeBenchmarkRequest,
    issued_keys: IssuedKeys,
    session: Session = Depends(get_session),
    run_starter: RequestIdentity = Depends(get_current_runner),
) -> RegradeBenchmarkResponse:
    org, bind = run_starter.org, session.get_bind()
    try:
        validate_service_headers(request.service_headers)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    session.close()
    source = await asyncio.to_thread(_prepare_regrade, bind, org, benchmark_id)
    arguments = source.run.arguments
    if source.run.custom_benchmark_service is not None:
        _authorize_custom_benchmark_destination(source.run.custom_benchmark_service, org)
    resolved_properties: AWSResources | None = None
    if isinstance(arguments.properties, LocalResources):
        if local_config.resources is None:
            raise HTTPException(status_code=400, detail="Server has no local configuration")
    else:
        resolved_properties = resolve_start_aws_runtime(org).resources
    destination = classify_benchmark_service_destination(source.run.name, source.run.custom_benchmark_service)
    _reject_docker_with_hosted_service(arguments.sandbox_provider, destination)
    run_key = await run_service_key(http_request, org, destination, request.service_headers, issued_keys)
    service_headers = forward_tracker_api_key(request.service_headers, run_key, destination=destination)
    benchmark_url = source.run.custom_benchmark_service or create_benchmark_service_url(source.run.name)
    service = create_benchmark_service_client(
        benchmark_url, service_headers, None, sandbox_provider=arguments.sandbox_provider
    )
    try:
        await _wake_hosted_service(source.run.name, source.run.custom_benchmark_service, service)
        resolved_dataset = await _resolve_dataset_version(
            service, source.run.name, request.dataset, request.dataset_version
        )
        dataset = resolved_dataset.dataset if resolved_dataset is not None else request.dataset
        dataset_version = resolved_dataset.version if resolved_dataset is not None else None
        if dataset_version is not None:
            await service.close()
            service = create_benchmark_service_client(
                benchmark_url, service_headers, dataset_version, sandbox_provider=arguments.sandbox_provider
            )
        verified = await service.verify_task_ids(task_ids=source.tasks.task_ids, slice_str=None, dataset=dataset)
    except BenchmarkServiceUnauthenticatedError:
        raise
    except (BenchmarkServiceError, httpx.HTTPError, ValueError) as exc:
        raise _verify_task_ids_failure(source.run.name, exc) from exc
    finally:
        await service.close()
    missing = sorted(set(source.tasks.task_ids) - set(verified.task_ids))
    if missing:
        raise HTTPException(status_code=400, detail=f"Dataset {dataset} has no tasks {', '.join(missing)}")
    run = regrade_run(
        source.run,
        source.tasks,
        run_starter,
        dataset=dataset,
        dataset_version=dataset_version,
        concurrency=request.concurrency,
    )
    if resolved_properties is not None:
        run.arguments = run.arguments.model_copy(update={"properties": resolved_properties})
    if not reads_secrets_file(run.arguments.properties):
        prospective_request = run.managed_start_benchmark_request(service_headers=service_headers)
        if prospective_request.environment == "aws":
            try:
                validate_managed_execution_request(prospective_request)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        _ = _authorize_secret_names(prospective_request, org)
    commit_task = asyncio.create_task(
        asyncio.to_thread(
            _commit_regrade,
            bind,
            org,
            source.run.id,
            run.model_dump_json(),
            service_headers,
            source.tasks,
            uuid4(),
            _executor_telemetry_context(),
            issued_keys,
        )
    )
    try:
        result, cancellation = await _await_before_cancellation(commit_task)
    except _TaskFailedAfterCancellation as failure:
        raise failure.cancellation from failure.task_error
    launch_task = asyncio.create_task(
        _launch_executor_dispatch(
            ExecutorDispatch.model_validate(json.loads(result.dispatch_json)),
            session=session,
            verified_task_ids=list(result.verified_task_ids),
        )
    )
    try:
        _, cancellation = await _await_before_cancellation(launch_task, cancellation)
    except _TaskFailedAfterCancellation as failure:
        raise failure.cancellation from failure.task_error
    if cancellation is not None:
        raise cancellation
    return RegradeBenchmarkResponse(
        benchmark_id=run.id,
        regraded_from_run_id=source.run.id,
        task_ids=source.tasks.task_ids,
        skipped_task_ids=source.tasks.skipped_task_ids,
    )


def _apply_recovery(
    benchmark_id: UUID,
    issued_keys: Sequence[IssuedRunKey],
    retry: bool,
    retry_mode: RetryMode,
    concurrency: int | None,
    task_ids: list[str],
    service_headers: dict[str, str],
    secrets: dict[str, str],
    benchmark_url: str | None,
    lambda_function: str | None,
    session: Session,
    org: Org,
    preparation: RecoveryPreparation,
    verified_task_ids: list[str],
    telemetry_context: ExecutorTelemetryContext,
    *,
    update_agent: bool = False,
) -> AdmissionResult | None:
    get_executor_admission(session, for_update=True)
    benchmark_row = assert_org(BenchmarkRepository(session).get(org.id, benchmark_id), org)
    current_project = ProjectRepository(session).get_for_run(benchmark_row.id)
    if current_project is not None:
        reject_archived_project(ProjectRepository(session).lock(current_project.id))
    if selects_project_gateway_key(org) and not reads_secrets_file(benchmark_row.arguments.properties):
        if current_project is not None:
            secrets = {
                env_name: name
                for env_name, name in secrets.items()
                if env_name not in {"MODEL_GATEWAY_API_KEY", "MODEL_GATEWAY_URL"}
            }
    secrets = {env_name: _resolve_secret_name(name, org) for env_name, name in secrets.items()}

    if benchmark_row.status == BenchmarkStatus.STOPPING:
        raise HTTPException(
            status_code=400,
            detail=f"Run {benchmark_id} is in the {benchmark_row.status.value} state. Cannot continue a run that is stopping.",
        )

    if update_agent and benchmark_row.status == BenchmarkStatus.IN_PROGRESS:
        raise HTTPException(status_code=409, detail=_UPDATE_AGENT_IN_PROGRESS)

    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and not retry and secrets:
        raise HTTPException(
            status_code=409,
            detail="Secret overrides require retry=true while a run is in progress.",
        )

    if benchmark_url is not None:
        try:
            benchmark_url = validate_service_url_syntax(benchmark_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    effective_benchmark_url = benchmark_url if benchmark_url is not None else benchmark_row.custom_benchmark_service
    if effective_benchmark_url is not None:
        _authorize_custom_benchmark_destination(effective_benchmark_url, org)

    queued_running_recovery = (
        benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        and not retry
        and concurrency is None
        and benchmark_row.arguments.queue_pool_id is not None
    )
    recovery_task_ids: list[str] | None = None
    if (
        preparation.state is None
        and benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        and not retry
        and concurrency is None
        and not queued_running_recovery
    ):
        if secrets or benchmark_url is not None or lambda_function is not None:
            update_benchmark_resume_arguments(
                benchmark_id,
                session,
                org,
                secrets=secrets,
                concurrency=None,
                benchmark_url=benchmark_url,
                lambda_function=lambda_function,
            )
            session.commit()
        return None

    if concurrency is not None and concurrency < 1:
        raise HTTPException(status_code=400, detail="Concurrency must be greater than 0.")

    if (
        preparation.state is None
        and benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        and not retry
        and concurrency is not None
    ):
        _update_benchmark_concurrency(benchmark_id, concurrency, session, org)
        if secrets or benchmark_url is not None or lambda_function is not None:
            update_benchmark_resume_arguments(
                benchmark_id,
                session,
                org,
                secrets=secrets,
                concurrency=None,
                benchmark_url=benchmark_url,
                lambda_function=lambda_function,
            )
            session.commit()
        return None

    session.rollback()
    dispatch_id = uuid4()
    payload_key = generate_payload_key(dispatch_id)
    pre_action_status: BenchmarkStatus | None = None
    try:
        with session.no_autoflush:
            lock_executor_admission(session)
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        effective_benchmark_url = benchmark_url if benchmark_url is not None else benchmark_row.custom_benchmark_service
        if effective_benchmark_url is not None:
            _authorize_custom_benchmark_destination(effective_benchmark_url, org)
        if benchmark_row.status == BenchmarkStatus.STOPPING:
            raise HTTPException(
                status_code=400,
                detail=f"Run {benchmark_id} is in the {benchmark_row.status.value} state. Cannot continue a run that is stopping.",
            )
        pre_action_status = benchmark_row.status

        try:
            current_state = prepare_retry_state(
                benchmark_row,
                session,
                retry,
                task_ids,
                org,
                queued_recovery=preparation.queued_recovery,
                for_update=True,
            )
        except TrackerServiceError as exc:
            raise HTTPException(
                status_code=409, detail="Run changed during task verification; retry the request."
            ) from exc
        if current_state != preparation.state:
            raise HTTPException(status_code=409, detail="Run changed during task verification; retry the request.")
        if queued_running_recovery and pre_action_status == BenchmarkStatus.IN_PROGRESS:
            scheduler_rows = BenchmarkRepository(session).lock_tasks(
                org.id,
                benchmark_row.id,
                statuses=[TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING],
            )
            if any(
                task_row.status == TaskStatus.IN_PROGRESS
                or (task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is None)
                for task_row in scheduler_rows
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Run has active tasks that cannot be resumed safely; stop the run before resuming",
                )
            recovery_task_ids = [task_row.task_id for task_row in scheduler_rows]

        if not reads_secrets_file(benchmark_row.arguments.properties):
            prospective_request = benchmark_row.managed_start_benchmark_request(
                service_headers=service_headers,
            )
            if secrets:
                prospective_contract = prospective_request.contract.model_copy(
                    update={"secrets": {**prospective_request.contract.secrets, **secrets}}
                )
                prospective_request = prospective_request.model_copy(update={"contract": prospective_contract})
            if prospective_request.environment == "aws":
                try:
                    validate_managed_execution_request(prospective_request)
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            _ = _authorize_secret_names(prospective_request, org)

        if recovery_task_ids is not None or retry_mode == RetryMode.AUTO:
            evaluation_candidates = BenchmarkRepository(session).lock_tasks(
                org.id,
                benchmark_row.id,
                task_ids=recovery_task_ids if recovery_task_ids is not None else verified_task_ids,
                statuses=[TaskStatus.EVALUATING],
            )
            for task in evaluation_candidates:
                if task.eval_resume_state is None or task.evaluation_phase_token is None:
                    continue
                if BenchmarkRepository(session).has_active_evaluation_owner(task.evaluation_phase_dispatch_id):
                    raise HTTPException(
                        status_code=409,
                        detail="Run has an evaluation that is already owned by an active executor",
                    )

        if recovery_task_ids is not None:
            verified_task_ids = recovery_task_ids
        else:
            verified_task_ids = reset_to_in_progress_status(
                benchmark_row=benchmark_row,
                session=session,
                retry=retry,
                retry_mode=retry_mode,
                rerun_task_ids=task_ids,
                org=org,
                verified_task_ids=verified_task_ids,
            )

        if pre_action_status == BenchmarkStatus.IN_PROGRESS and not verified_task_ids and recovery_task_ids is None:
            if secrets or concurrency is not None or benchmark_url is not None or lambda_function is not None:
                update_benchmark_resume_arguments(
                    benchmark_id,
                    session,
                    org,
                    secrets=secrets,
                    concurrency=concurrency,
                    benchmark_url=benchmark_url,
                    lambda_function=lambda_function,
                )
                session.commit()
            else:
                session.rollback()
            return None

        resumable_evaluations = [
            task
            for task in BenchmarkRepository(session).lock_tasks(
                org.id, benchmark_row.id, task_ids=verified_task_ids, statuses=[TaskStatus.EVALUATING]
            )
            if task.eval_resume_state is not None
        ]
        for task in resumable_evaluations:
            if not SchedulerRepository(session).try_task_evaluation_transaction_lock(task.id):
                raise HTTPException(
                    status_code=409,
                    detail="Run has an evaluation that is already owned by an active executor",
                )

        if secrets or concurrency is not None or benchmark_url is not None or lambda_function is not None:
            benchmark_row = update_benchmark_resume_arguments(
                benchmark_id,
                session,
                org,
                secrets=secrets,
                concurrency=concurrency,
                benchmark_url=benchmark_url,
                lambda_function=lambda_function,
            )

        if benchmark_row.arguments.environment == "local":
            resume_request = benchmark_row.local_start_benchmark_request(service_headers=service_headers)
        else:
            if benchmark_row.arguments.properties is None:
                if preparation.resolved_properties is None:
                    raise TrackerServiceError("Managed recovery has no resolved AWS resources")
                benchmark_row.arguments = benchmark_row.arguments.model_copy(
                    update={"properties": preparation.resolved_properties}
                )

            resume_request = benchmark_row.managed_start_benchmark_request(
                service_headers=service_headers,
            )
        dispatch_kind = ExecutorDispatchKind.RETRY if retry else ExecutorDispatchKind.RESUME
        executor_dispatch = admit_recovery_dispatch(
            session,
            benchmark=benchmark_row,
            pre_action_status=pre_action_status,
            dispatch_id=dispatch_id,
            kind=dispatch_kind,
            task_ids=verified_task_ids,
        )
        if resumable_evaluations:
            transferred = BenchmarkRepository(session).restart_evaluations(
                [task.id for task in resumable_evaluations], executor_dispatch.created_at
            )
            if transferred != len(resumable_evaluations):
                raise TrackerServiceError("Recovery evaluation ownership changed before dispatch admission")
        executor_payload = _process_benchmark_kwargs(benchmark_row, resume_request, verified_task_ids)
        _persist_dispatch_payload(session, executor_dispatch, executor_payload, telemetry_context, payload_key)
        BenchmarkRepository(session).save(
            *(RunAccessKey(key_id=key.key_id, benchmark_id=benchmark_row.id) for key in issued_keys)
        )
        session.commit()
    except ReleaseControlError as exc:
        session.rollback()
        status_code = (
            503
            if isinstance(exc, MaintenanceModeError)
            else (409 if pre_action_status == BenchmarkStatus.IN_PROGRESS else 503)
        )
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except Exception:
        session.rollback()
        raise

    return _admission_result(executor_dispatch, verified_task_ids)


@app.get(
    "/fetch-benchmarks",
    description="List runs that match the filters, a page at a time.",
)
def fetch_benchmarks(
    agent_name: list[str] | None = Query(default=None),
    benchmark_name: list[str] | None = Query(default=None),
    status: list[BenchmarkStatus] | None = Query(default=None),
    started_by: list[str] | None = Query(default=None),
    model: str | None = Query(default=None),
    dataset: str | None = Query(default=None),
    label: str | None = Query(default=None),
    started_after: datetime | None = Query(default=None),
    started_before: datetime | None = Query(default=None),
    order_by: Order = Query(default=Order.DESC),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> FetchBenchmarksResponse:
    text_filters = [model, dataset, label, *(agent_name or []), *(benchmark_name or []), *(started_by or [])]
    if any("\x00" in value for value in text_filters if value is not None):
        raise HTTPException(status_code=400, detail="Filters must not contain NUL characters")

    request = FetchBenchmarksRequest(
        agent_name=agent_name,
        benchmark_name=benchmark_name,
        status=status,
        started_by=started_by,
        model=model,
        dataset=dataset,
        label=label,
        started_after=started_after,
        started_before=started_before,
        order_by=order_by,
        cursor=cursor,
        limit=limit,
        offset=offset,
    )

    try:
        benchmark_rows, total_count, next_cursor = fetch_filtered_benchmark_rows(request, session, org)
    except InvalidCursorError as error:
        raise HTTPException(status_code=400, detail="Invalid cursor") from error

    return FetchBenchmarksResponse(
        benchmarks=build_benchmark_table_rows(benchmark_rows, session),
        total_count=total_count,
        next_cursor=next_cursor,
    )


@app.get(
    "/fetch-benchmark-metadata/{benchmark_id}",
    description="Return a run's metadata by its ID.",
)
def fetch_benchmark_metadata(
    benchmark_id: TrackedBenchmarkId,
    benchmarks: BenchmarkRepositoryDependency,
    projects: ProjectRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> FetchBenchmarkMetadataResponse:
    benchmark_row = assert_org(benchmarks.get(org.id, benchmark_id), org)
    project = projects.get_for_run(benchmark_row.id)
    metadata = benchmark_row.benchmark_metadata.model_copy(
        update={"project": ProjectResponse.from_row(project) if project is not None else None}
    )
    if benchmark_row.arguments.environment == "local":
        return metadata

    assert not isinstance(benchmark_row.arguments.properties, LocalResources)
    aws_runtime = resolve_run_metadata_aws_runtime(
        properties=benchmark_row.arguments.properties,
        org=_org_context(org),
    )

    return metadata.model_copy(
        update={"storage_bucket": aws_runtime.resources.s3_bucket if aws_runtime is not None else None}
    )


def _safe_output_tar_member(s3_key: str, benchmark_prefix: str) -> str | None:
    if not s3_key.startswith(benchmark_prefix):
        return None

    relative_path = s3_key[len(benchmark_prefix) :]
    parts = relative_path.split("/")
    if (
        not relative_path
        or "\x00" in relative_path
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in parts)
        or (len(parts[0]) >= 2 and parts[0][1] == ":")
    ):
        return None

    return relative_path


async def _output_keys(
    benchmark_prefix: str,
    task_ids: list[str] | None,
    object_store: ObjectStore,
    benchmark_id: TrackedBenchmarkId,
) -> AsyncIterator[str]:
    prefixes = [f"{benchmark_prefix}{task_id}/" for task_id in task_ids] if task_ids else [benchmark_prefix]
    for prefix in prefixes:
        async for stored_object in object_store.list_objects(prefix):
            if _safe_output_tar_member(stored_object.key, benchmark_prefix) is None:
                logger.warning("Skipping unsafe output archive member for benchmark %s", benchmark_id)
                continue
            yield stored_object.key


async def _output_keys_with_first(first_key: str, keys: AsyncIterator[str]) -> AsyncIterator[str]:
    yield first_key
    async for key in keys:
        yield key


class YieldingWriter(io.RawIOBase):
    def __init__(self):
        super().__init__()
        self._buffer: bytearray = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, b: Buffer) -> int:
        data = bytes(b)
        self._buffer.extend(data)

        return len(data)

    def pop(self) -> bytes:
        if not self._buffer:
            return b""

        chunk = bytes(self._buffer)
        self._buffer.clear()

        return chunk


async def _tar_output_stream(
    keys: AsyncIterator[str],
    benchmark_prefix: str,
    object_store: ObjectStore,
) -> AsyncIterator[bytes]:
    writer: YieldingWriter = YieldingWriter()

    with tarfile.open(fileobj=writer, mode="w|") as tar:
        async for s3_key, data in object_store.get_many(keys):
            relative_path = s3_key.removeprefix(benchmark_prefix)
            tarinfo = tarfile.TarInfo(name=relative_path)
            tarinfo.size = len(data)
            tar.addfile(tarinfo, fileobj=io.BytesIO(data))

            chunk = writer.pop()
            if chunk:
                yield chunk

    final_chunk = writer.pop()
    if final_chunk:
        yield final_chunk


@app.get(
    "/fetch-run-outputs/{benchmark_id}",
    response_model=None,
    description="Stream a run's outputs as a tar file.",
)
async def fetch_run_outputs(
    benchmark_id: TrackedBenchmarkId,
    runtime: RunRuntimeDependency,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
    task_ids: list[str] | None = Query(default=None),
) -> StreamingResponse:
    object_store = runtime.objects

    benchmark_prefix = benchmark_artifact_prefix(str(benchmark_id))

    keys = _output_keys(benchmark_prefix, task_ids, object_store, benchmark_id)
    first_key = await anext(keys, None)
    if first_key is None:
        raise HTTPException(status_code=404, detail=f"No outputs found for run '{benchmark_id}'")

    return StreamingResponse(
        _tar_output_stream(_output_keys_with_first(first_key, keys), benchmark_prefix, object_store),
        media_type="application/x-tar",
        headers={"Content-Disposition": f"attachment; filename=benchmark_{benchmark_id}_outputs.tar"},
    )
