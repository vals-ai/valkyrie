import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple

from benchmark_service.client import (
    BenchmarkServiceError,
    BenchmarkServiceStreamError,
    BenchmarkServiceUnauthenticatedError,
)
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tracker.clients.dataset_tasks import InvalidTaskError
from tracker.services.datasets.grading import VerifierResultError
from tracker.services.datasets.healthcheck import HealthcheckError
from tracker.services.datasets.separate_verifier import SeparateVerifierError
from tracker.services.datasets.steps import StepSetupError
from tracker.domain import FailureCategory, FailurePhase
from tracker.exceptions import (
    AgentRunFailedError,
    CloudWatchError,
    EpisodeOrchestratorFailedError,
    LambdaError,
    OutputArtifactError,
    S3Error,
    SandboxDNSResolutionError,
    SandboxError,
    SandboxImageBuildError,
    SandboxSetupError,
    SecretsError,
)

PRE_AGENT_PHASES = frozenset({FailurePhase.ADMISSION, FailurePhase.SANDBOX, FailurePhase.SETUP})


_DOCKER_NOT_READY_SIGNATURES = (
    "docker daemon is not ready inside the sandbox",
    "Docker daemon did not become ready inside the compose sandbox",
)

_INFRASTRUCTURE_ERRORS = (
    SandboxError,
    ProviderSandboxError,
    OutputArtifactError,
    S3Error,
    CloudWatchError,
    LambdaError,
    SecretsError,
)


class Disposition(str, Enum):
    RECREATE_SANDBOX = "recreate_sandbox"
    RESUME_EVALUATION = "resume_evaluation"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class Failure:
    exc: BaseException
    message: str
    category: FailureCategory
    producer: str
    operation: str
    phase: FailurePhase | None = None
    cause_code: str | None = None
    transient: bool | None = None

    @property
    def error_type(self) -> str:
        return type(self.exc).__name__


class BenchmarkServiceWebSocketDNSResolutionError(BenchmarkServiceError):
    pass


def is_agent_outcome(exc: BaseException) -> bool:
    return isinstance(exc, AgentRunFailedError) and not isinstance(exc, EpisodeOrchestratorFailedError)


def classify_failure(exc: BaseException) -> FailureCategory:
    if isinstance(exc, asyncio.CancelledError):
        return FailureCategory.CANCELLED
    if isinstance(exc, AgentRunFailedError):
        return FailureCategory.AGENT
    if isinstance(exc, _INFRASTRUCTURE_ERRORS):
        return FailureCategory.INFRASTRUCTURE
    if isinstance(exc, BenchmarkServiceError):
        return FailureCategory.BENCHMARK_SERVICE
    return FailureCategory.UNKNOWN


class _Labels(NamedTuple):
    producer: str
    operation: str
    transient: bool | None = None
    cause_code: str | None = None
    message: str = "{message}"
    category: FailureCategory | None = None


_LABELS: dict[type[BaseException], _Labels] = {
    SandboxSetupError: _Labels("sandbox_provider", "setup", transient=True),
    SandboxDNSResolutionError: _Labels("sandbox_provider", "dns", transient=True, cause_code="sandbox_dns_resolution"),
    SandboxImageBuildError: _Labels("sandbox_provider", "build", cause_code="sandbox_image_build"),
    VerifierResultError: _Labels(
        "dataset_task", "verify", False, "verifier_result", category=FailureCategory.BENCHMARK_SERVICE
    ),
    StepSetupError: _Labels(
        "dataset_task", "step_setup", False, "step_setup", category=FailureCategory.BENCHMARK_SERVICE
    ),
    HealthcheckError: _Labels(
        "dataset_task", "healthcheck", False, "healthcheck", category=FailureCategory.BENCHMARK_SERVICE
    ),
    SeparateVerifierError: _Labels(
        "dataset_task", "separate_verifier", False, "separate_verifier", category=FailureCategory.BENCHMARK_SERVICE
    ),
    InvalidTaskError: _Labels(
        "dataset_task", "load_task", False, "invalid_task", category=FailureCategory.BENCHMARK_SERVICE
    ),
    OutputArtifactError: _Labels("output_artifact", "upload_output_artifacts", transient=False),
    AgentRunFailedError: _Labels("tracker", "process_task", transient=False),
    SandboxError: _Labels("sandbox_provider", "sandbox"),
    BenchmarkServiceWebSocketDNSResolutionError: _Labels(
        "benchmark_service",
        "websocket_connect",
        True,
        "websocket_dns_resolution",
        "Benchmark service WebSocket connection failed during DNS resolution: {message}",
    ),
    ConnectionClosedError: _Labels(
        "benchmark_service",
        "websocket",
        True,
        "websocket_connection_closed",
        "Benchmark service WebSocket disconnected: {message}; last application message received {idle_seconds}s ago",
        FailureCategory.BENCHMARK_SERVICE,
    ),
    BenchmarkServiceStreamError: _Labels(
        "benchmark_service",
        "websocket",
        True,
        "websocket_connection_closed",
        "Benchmark service WebSocket stream failed: {message}",
    ),
}


def describe_failure(
    exc: BaseException,
    message: str,
    *,
    phase: FailurePhase | None = None,
    idle_seconds: int | None = None,
) -> Failure:
    category = classify_failure(exc)
    transient = getattr(exc, "transient", None)
    labels = next((_LABELS[cls] for cls in type(exc).__mro__ if cls in _LABELS), None)
    if labels is not None:
        return Failure(
            exc,
            labels.message.format(message=message, idle_seconds=idle_seconds),
            labels.category or category,
            labels.producer,
            labels.operation,
            phase,
            cause_code=getattr(exc, "cause_code", None) or labels.cause_code,
            transient=transient if labels.transient is None else labels.transient,
        )
    if isinstance(exc, ValidationError):
        return _incompatible_response(exc, phase)
    if isinstance(exc, InvalidStatus):
        status = exc.response.status_code
        return Failure(
            exc,
            f"Benchmark service rejected the WebSocket connection (HTTP {status}): {message}",
            FailureCategory.BENCHMARK_SERVICE,
            "benchmark_service",
            "websocket_connect",
            phase,
            cause_code="websocket_http_rejected",
            transient=status >= 500,
        )
    if isinstance(exc, BenchmarkServiceError):
        return _benchmark_service_request(exc, message, category, phase)
    return Failure(exc, message, category, "tracker", "process_task", phase, transient=transient)


def _incompatible_response(exc: ValidationError, phase: FailurePhase | None) -> Failure:
    field_errors = ", ".join(
        f"{'.'.join(str(loc) for loc in err['loc'])} ({err['type']})"
        for err in exc.errors(include_url=False, include_context=False, include_input=False)
    )
    return Failure(
        exc,
        f"Benchmark service returned an incompatible task response. Missing or invalid fields: ValidationError: {field_errors}",
        FailureCategory.BENCHMARK_SERVICE,
        "benchmark_service",
        "decode_task_response",
        phase,
        cause_code="incompatible_response",
        transient=False,
    )


def is_rejected_request(status: int | None) -> bool:
    return status is not None and 400 <= status < 500 and status not in (408, 429)


def _benchmark_service_request(
    exc: BenchmarkServiceError, message: str, category: FailureCategory, phase: FailurePhase | None
) -> Failure:
    if isinstance(exc, BenchmarkServiceUnauthenticatedError) or is_rejected_request(exc.status_code):
        return Failure(exc, message, category, "benchmark_service", "request", phase, transient=False)
    if any(signature in message for signature in _DOCKER_NOT_READY_SIGNATURES):
        return Failure(
            exc, message, category, "sandbox_provider", "setup", phase, cause_code="docker_not_ready", transient=True
        )
    return Failure(exc, message, category, "benchmark_service", "request", phase)


def decide(failure: Failure) -> Disposition:
    if isinstance(failure.exc, (SandboxSetupError, SandboxDNSResolutionError)):
        return Disposition.RECREATE_SANDBOX
    if failure.phase in PRE_AGENT_PHASES and (
        failure.transient or (failure.transient is None and failure.category is FailureCategory.BENCHMARK_SERVICE)
    ):
        return Disposition.RECREATE_SANDBOX
    if not failure.transient:
        return Disposition.TERMINAL
    if failure.phase is FailurePhase.EVALUATION:
        return Disposition.RESUME_EVALUATION
    return Disposition.TERMINAL
