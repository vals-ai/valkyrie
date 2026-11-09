from benchmark_service import SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import DatasetVersion
from tracker.outbound_security import validate_service_headers, validate_service_url_syntax
from tracker.runtime.secrets import SecretStore, sandbox_provider_config_from_secret


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
