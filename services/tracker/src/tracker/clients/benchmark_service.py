import asyncio
import socket
from collections.abc import Coroutine
from typing import Any

import httpx
from benchmark_service import SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import DatasetVersion

from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.outbound_security import validate_service_headers, validate_service_url_syntax
from tracker.runtime.secrets import SecretStore, sandbox_provider_config_from_secret
from tracker.services.failures import BenchmarkServiceWebSocketDNSResolutionError


def _exception_message(exc: BaseException) -> str:
    detail = str(exc).strip()
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


async def run_benchmark_service_websocket(operation: Coroutine[Any, Any, Any]) -> Any:
    try:
        return await operation
    except socket.gaierror as exc:
        raise BenchmarkServiceWebSocketDNSResolutionError(_exception_message(exc)) from exc


logger = get_logger(__name__)

_WAKE_TIMEOUT_SECONDS = 15 * 60.0
_WAKE_POLL_SECONDS = 15.0


async def fetch_sandbox_provider_config(
    secret_name: str,
    secret_store: SecretStore,
    provider_type: str,
) -> SandboxProviderConfig:
    return sandbox_provider_config_from_secret(await secret_store.get(secret_name), provider_type)


SANDBOX_PROVIDER_HEADER = "X-Vals-Sandbox-Provider"


def create_benchmark_service_client(
    url: str,
    service_headers: dict[str, str] | None = None,
    dataset_version: DatasetVersion | None = None,
    sandbox_provider: str | None = None,
) -> BenchmarkServiceClient:
    url = validate_service_url_syntax(url)
    headers = {
        name: value
        for name, value in (service_headers or {}).items()
        if sandbox_provider is None or name.lower() != SANDBOX_PROVIDER_HEADER.lower()
    }
    if sandbox_provider is not None:
        headers[SANDBOX_PROVIDER_HEADER] = sandbox_provider
    validate_service_headers(headers)

    return BenchmarkServiceClient(
        url=url, headers=headers, dataset_version=dataset_version.id if dataset_version is not None else None
    )


async def wait_for_benchmark_service(
    client: BenchmarkServiceClient,
    *,
    timeout_seconds: float = _WAKE_TIMEOUT_SECONDS,
    poll_seconds: float = _WAKE_POLL_SECONDS,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TrackerServiceError("Benchmark service did not become ready before the wake deadline")
        try:
            await asyncio.wait_for(client.health_check(), remaining)
            return
        except TimeoutError as error:
            raise TrackerServiceError("Benchmark service did not become ready before the wake deadline") from error
        except (httpx.TransportError, BenchmarkServiceError) as error:
            if isinstance(error, BenchmarkServiceError) and (error.status_code or 500) < 500:
                raise
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TrackerServiceError(f"Benchmark service is not ready: {error}") from error
            logger.info("Waiting for benchmark service to start", extra={"error": str(error)})
            await asyncio.sleep(min(poll_seconds, remaining))
