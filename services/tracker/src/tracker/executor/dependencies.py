from asyncio import to_thread
from typing import Literal, cast
from uuid import UUID

from tracker.aws.runtime import AWSResources
from tracker.aws.services import CloudRuntimeFactory
from tracker.exceptions import SecretsError, TrackerServiceError
from tracker.local.resources import LocalResources, reads_secrets_file
from tracker.local.secrets import InMemorySecretStore, load_execution_secrets
from tracker.runtime.services import RuntimeServices
from tracker.types import OrgContext, StartBenchmarkRequest


async def get_execution_runtime(
    request: StartBenchmarkRequest,
    *,
    benchmark_id: UUID,
    org_id: UUID,
    org_name: str,
    org_storage_key: str | None,
    saved_environment: Literal["aws", "local"],
    saved_properties: AWSResources | LocalResources | None,
    saved_sandbox_provider: str,
    saved_sandbox_provider_secret_name: str | None,
    context_version: int | None,
) -> RuntimeServices:
    if request.environment != saved_environment:
        raise SecretsError("Queued runtime environment does not match the saved run")
    if saved_environment == "local":
        from tracker.local.runtime import LocalRuntimeFactory

        properties = cast(LocalResources, saved_properties)
        secrets = None
        if reads_secrets_file(properties):
            values = await to_thread(load_execution_secrets, properties.secrets_file, request.contract.secrets)
            secrets = InMemorySecretStore(request.contract.secrets, values)
        runtime = LocalRuntimeFactory.create_runtime(
            properties,
            org_id,
            secrets=secrets,
            sandbox_provider=saved_sandbox_provider,
            sandbox_provider_secret_name=saved_sandbox_provider_secret_name,
        )
        await runtime.prepare_execution(request, benchmark_id)
        return runtime

    assert request.properties is None or isinstance(request.properties, AWSResources)
    stored = cast(AWSResources | None, saved_properties)
    queued = request.properties
    if context_version == 3 and stored is None:
        raise TrackerServiceError("Managed execution has no saved AWS resources")

    if stored is not None and (queued is not None or context_version == 3) and queued != stored:
        raise TrackerServiceError("Queued AWS resources differ from the saved run")

    return await CloudRuntimeFactory.create_execution_runtime(
        request,
        OrgContext(id=org_id, name=org_name, storage_key=org_storage_key),
        benchmark_id,
        properties=stored,
    )
