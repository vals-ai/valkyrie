from __future__ import annotations

import pytest
import inspect
from collections.abc import Awaitable, Callable
from importlib import import_module
from uuid import UUID

import httpx

from valkyrie.sdk.client import ValkyrieClient  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.errors import ValkyrieInputError, ValkyrieSDKError  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.models import FetchBenchmarksRequest  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.config import ValkyrieConfig  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.login import LoginSession, SessionAuth, device_login  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.agents import AgentsResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.benchmarks import BenchmarksResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.logs import LogsResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.runs import RunsResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.scheduler import SchedulerResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.secrets import SecretsResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.settings import ProvidersResource, SettingsResource  # pyright: ignore[reportMissingImports]
from valkyrie.sdk.resources.services import BenchmarkServicesResource  # pyright: ignore[reportMissingImports]

EXPECTED_ALL = [
    "AccessKeyResponse",
    "IdentityResponse",
    "InviteResponse",
    "MemberEntry",
    "MembersResponse",
    "OrganizationEntry",
    "OrganizationResponse",
    "OrganizationsResponse",
    "RunArtifactEntry",
    "RunArtifactsResponse",
    "RunArtifactDownloadResponse",
    "AgentContract",
    "AgentContractRequest",
    "AgentDownloadURLResponse",
    "AgentEntry",
    "AgentsResponse",
    "AnalyzeEvent",
    "BenchmarkServiceCatalogResponse",
    "BenchmarkServiceEntry",
    "BenchmarkServiceHealth",
    "BenchmarkServicesResponse",
    "BenchmarkStatus",
    "BenchmarkStatusEntry",
    "BenchmarkStatusResponse",
    "DatasetVersion",
    "FailureCategory",
    "FetchBenchmarkResponse",
    "FetchBenchmarkMetadataResponse",
    "FetchBenchmarksRequest",
    "FetchBenchmarksResponse",
    "FilterOptionsResponse",
    "FetchTasksRequest",
    "FinalViewResponse",
    "LogEvent",
    "LogPage",
    "LoginSession",
    "Order",
    "Parameter",
    "NewProjectRequest",
    "ProjectCreateRequest",
    "ProjectResponse",
    "ProjectsResponse",
    "RetryMode",
    "RegradeBenchmarkRequest",
    "RegradeBenchmarkResponse",
    "RetryOrResumeBenchmarkResponse",
    "RunSnapshot",
    "RunStreamEvent",
    "ResultsExistResponse",
    "S3UploadResultsResponse",
    "SchedulerActiveEntryResponse",
    "SchedulerActiveStatus",
    "SchedulerCapacityDomainResponse",
    "SchedulerCapacityResponse",
    "SchedulerOverviewResponse",
    "SchedulerPoolResponse",
    "SchedulerResourceCapacityResponse",
    "SchedulerSummaryResponse",
    "SchedulerWaitingEntryResponse",
    "SecretEntry",
    "SecretsResponse",
    "ProvidersResponse",
    "RunSettingsResponse",
    "SavedBenchmarkService",
    "SessionAuth",
    "SingleBenchmarkResponse",
    "SingleTaskResponse",
    "ShedBenchmarkRequest",
    "ShedBenchmarkResponse",
    "SPEND_SUBCATEGORIES",
    "SpendType",
    "RunMonitoring",
    "RunMonitoringRules",
    "StartBenchmarkResponse",
    "StopBenchmarkResponse",
    "UpdateBenchmarkConcurrencyRequest",
    "UpdateBenchmarkConcurrencyResponse",
    "TaskArtifactsResponse",
    "TasksResponse",
    "TaskSummary",
    "TaskStatus",
    "ValkyrieAPIError",
    "ValkyrieClient",
    "ValkyrieConfig",
    "ValkyrieConfigError",
    "ValkyrieContractError",
    "ValkyrieInputError",
    "ValkyrieRunError",
    "ValkyrieSDKError",
    "ValkyrieStreamError",
    "ValkyrieTransportError",
    "device_login",
]

EXPECTED_SIGNATURES = {
    ValkyrieClient: "config=None, *, timeout=120, transport=None, auth=None",
    device_login: "tracker_url, announce, *, timeout=30, transport=None",
    SessionAuth: "session",
    LoginSession.load: "tracker_url",
    LoginSession.forget: "tracker_url",
    RunsResource.start: (
        "self, agent, benchmark, *, model=None, concurrency=5, priority=None, task_ids=None, "
        "slice_str=None, dataset=None, dataset_version=None, "
        "label=None, lambda_function=None, provider=None, agent_kwargs=None, secrets=None, service_headers=None, "
        "ignore_custom_services=False, project=None, monitoring=None"
    ),
    RunsResource.retag: "self, run_id, project",
    RunsResource.filter_options: "self",
    RunsResource.fetch: "self, run_id",
    RunsResource.list: "self, request=None",
    RunsResource.stream: "self, run_id",
    RunsResource.results: "self, run_id, *, task_ids=None, upload_to_s3=False",
    RunsResource.preview: "self, run_id, *, task_ids=None",
    RunsResource.metadata: "self, run_id",
    RunsResource.results_exist: "self, run_id",
    RunsResource.analyze: "self, run_id, *, no_cache=False, lambda_function=None",
    RunsResource.stream_outputs: "self, run_id, *, task_ids=None",
    RunsResource.stop: "self, run_id, *, force=False, task_ids=None",
    RunsResource.resume: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False, "
        "update_agent=False, benchmark_url=None, lambda_function=None"
    ),
    RunsResource.retry: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False, "
        "update_agent=False, benchmark_url=None, lambda_function=None"
    ),
    RunsResource.regrade: "self, run_id, *, dataset, dataset_version=None, concurrency=None, service_headers=None",
    RunsResource.events: "self, run_id",
    LogsResource.page_task: (
        "self, run_id, task_id, *, query=None, start_time=None, end_time=None, cursor=None, limit=1000"
    ),
    LogsResource.fetch_task: "self, run_id, task_id, *, query=None, start_time=None, end_time=None",
    LogsResource.page_run: "self, run_id, *, query=None, start_time=None, end_time=None, cursor=None, limit=1000",
    LogsResource.fetch_run: "self, run_id, *, query=None, start_time=None, end_time=None",
    LogsResource.stream_task: "self, run_id, task_id, *, query=None, start_time=None, end_time=None",
    BenchmarksResource.fetch: "self, run_id",
    BenchmarksResource.statuses: "self, run_ids",
    BenchmarksResource.tasks: "self, run_id, request=None",
    BenchmarksResource.task: "self, run_id, task_id",
    BenchmarksResource.artifacts: "self, run_id, task_id",
    SchedulerResource.overview: (
        "self, *, waiting_limit=100, active_limit=100, waiting_offset=0, active_offset=0, include_capacity=False"
    ),
    AgentsResource.list: "self",
    AgentsResource.download_url: "self, name",
    SecretsResource.list: "self",
    SecretsResource.set: "self, name, value",
    SecretsResource.remove: "self, name",
    ProvidersResource.list: "self",
    ProvidersResource.set: "self, provider, secret_name",
    ProvidersResource.remove: "self, provider",
    SettingsResource.get: "self",
    SettingsResource.set_default_provider: "self, provider",
    SettingsResource.set_benchmark_service: "self, benchmark, url, *, auth_secret=None, auth_header=None",
    SettingsResource.remove_benchmark_service: "self, benchmark",
    BenchmarkServicesResource.catalog: "self",
    BenchmarkServicesResource.list: "self",
    BenchmarkServicesResource.check: "self, services",
    BenchmarkServicesResource.task_ids: (
        "self, benchmark, *, dataset=None, service_headers=None, ignore_custom_services=False"
    ),
}


def signature_text(callable_object: Callable[..., object]) -> str:
    parts: list[str] = []
    keyword_only = False
    parameters = list(inspect.signature(callable_object).parameters.values())
    for index, parameter in enumerate(parameters):
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and not keyword_only:
            parts.append("*")
            keyword_only = True
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            part = f"*{parameter.name}"
            keyword_only = True
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            part = f"**{parameter.name}"
        else:
            part = parameter.name
        if parameter.default is not inspect.Parameter.empty:
            part += f"={parameter.default!r}"
        parts.append(part)
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY and (
            index + 1 == len(parameters) or parameters[index + 1].kind is not inspect.Parameter.POSITIONAL_ONLY
        ):
            parts.append("/")
    return ", ".join(parts)


@pytest.mark.description("`valkyrie.sdk` exports the same names and constants as the recorded public API.")
def test_public_exports_and_constants_are_stable() -> None:
    sdk = import_module("valkyrie.sdk")

    assert getattr(sdk, "__all__") == EXPECTED_ALL
    assert getattr(sdk, "ValkyrieClient") is ValkyrieClient
    assert getattr(sdk, "ValkyrieConfig") is ValkyrieConfig
    assert getattr(sdk, "LoginSession") is LoginSession
    assert getattr(sdk, "SessionAuth") is SessionAuth
    assert getattr(sdk, "device_login") is device_login


@pytest.mark.description("Public SDK functions and methods keep their recorded signatures.")
def test_public_signatures_are_stable() -> None:
    for callable_object, expected in EXPECTED_SIGNATURES.items():
        assert signature_text(callable_object) == expected


@pytest.mark.description("Signature text keeps parameter kinds.")
def test_signature_text_preserves_parameter_kinds() -> None:
    def sample(value: object, /, other: object, *, flag: bool = False) -> None:
        pass

    assert signature_text(sample) == "value, /, other, *, flag=False"


@pytest.mark.description("The client has a typed attribute for each v2 resource such as `settings`.")
async def test_client_exposes_v2_resource_namespaces(make_client) -> None:
    async with make_client(lambda _request: None) as client:
        assert isinstance(client.runs, RunsResource)
        assert isinstance(client.logs, LogsResource)
        assert isinstance(client.benchmarks, BenchmarksResource)
        assert isinstance(client.agents, AgentsResource)
        assert isinstance(client.services, BenchmarkServicesResource)
        assert isinstance(client.scheduler, SchedulerResource)
        assert isinstance(client.secrets, SecretsResource)
        assert isinstance(client.providers, ProvidersResource)
        assert isinstance(client.settings, SettingsResource)


RUN_ID = UUID("00000000-0000-0000-0000-000000000001")

INVALID_INPUT_CALLS: dict[str, Callable[[ValkyrieClient], Awaitable[object]]] = {
    "runs.start": lambda client: client.runs.start("agent", "bench", concurrency=0),
    "runs.iter": lambda client: anext(client.runs.iter(FetchBenchmarksRequest(offset=5))),
    "runs.download_outputs": lambda client: client.runs.download_outputs(RUN_ID, "outputs", max_entries=0),
    "logs.page_run": lambda client: client.logs.page_run(RUN_ID, query=" "),
    "artifacts.list": lambda client: client.artifacts.list(RUN_ID, limit=0),
    "artifacts.download_url": lambda client: client.artifacts.download_url(RUN_ID, "../secret"),
    "benchmarks.task": lambda client: client.benchmarks.task(RUN_ID, "a/b"),
    "benchmark_definitions.get": lambda client: client.benchmark_definitions.get("Bad Name"),
    "datasets.get": lambda client: client.datasets.get("bench", "Bad Name"),
    "services.task_ids": lambda client: client.services.task_ids(" "),
    "scheduler.overview": lambda client: client.scheduler.overview(waiting_limit=0),
    "secrets.remove": lambda client: client.secrets.remove("a/b"),
    "agents.download": lambda client: client.agents.download("agent", max_entries=0),
    "agents.install": lambda client: client.agents.install("https://gitlab.com/a/b"),
    "providers.remove": lambda client: client.providers.remove(".."),
}


@pytest.mark.description(
    "Every resource rejects an invalid argument with `ValkyrieInputError` before sending a request."
)
@pytest.mark.parametrize("call", INVALID_INPUT_CALLS.values(), ids=INVALID_INPUT_CALLS.keys())
async def test_invalid_input_raises_valkyrie_input_error(make_client, call) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request to {request.url}")

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieInputError) as raised:
            await call(client)

    assert isinstance(raised.value, ValkyrieSDKError)
    assert isinstance(raised.value, ValueError)


@pytest.mark.description("Pushing a bad local agent directory raises `ValkyrieInputError` before sending a request.")
@pytest.mark.parametrize("problem", ["non_string_name", "symlink_path", "symlink_member"])
async def test_agent_push_rejects_bad_directory_with_valkyrie_input_error(make_client, tmp_path, problem) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request to {request.url}")

    agent = tmp_path / "agent"
    agent.mkdir()
    (agent / "contract.yaml").write_text("name: 5\n" if problem == "non_string_name" else "name: demo\n")
    path = agent
    if problem == "symlink_path":
        path = tmp_path / "link"
        path.symlink_to(agent, target_is_directory=True)
    if problem == "symlink_member":
        (agent / "link").symlink_to(tmp_path)

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieInputError):
            await client.agents.push(path)
