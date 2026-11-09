from fastapi import APIRouter, Depends, HTTPException

from tracker.api.secrets import ADMINS_ONLY, require_own_secret, require_secret_writer
from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.repositories.provider_secret import ProviderSecretRepositoryDependency
from tracker.types import ProviderName, ProviderSecretRequest, ProvidersResponse

router = APIRouter(prefix="/sandbox-providers")


@router.get(
    "",
    response_model=ProvidersResponse,
    description="List which secret holds each sandbox provider's configuration, by name.",
)
def list_providers(
    repository: ProviderSecretRepositoryDependency, org: Org = Depends(get_current_org)
) -> ProvidersResponse:
    return ProvidersResponse(providers=repository.list_all(org.id))


@router.put(
    "/{provider}",
    response_model=ProvidersResponse,
    responses={403: ADMINS_ONLY, 404: {"description": "Secret does not exist"}},
    description="Use the named secret for this sandbox provider's runs.",
)
async def set_provider(
    provider: ProviderName,
    body: ProviderSecretRequest,
    repository: ProviderSecretRepositoryDependency,
    org: Org = Depends(require_secret_writer),
) -> ProvidersResponse:
    await require_own_secret(body.secret_name, org)
    repository.set(org.id, provider, body.secret_name)
    return ProvidersResponse(providers=repository.list_all(org.id))


@router.delete(
    "/{provider}",
    response_model=ProvidersResponse,
    responses={403: ADMINS_ONLY, 404: {"description": "Provider is not mapped"}},
    description="Stop using a secret for this sandbox provider.",
)
def remove_provider(
    provider: ProviderName, repository: ProviderSecretRepositoryDependency, org: Org = Depends(require_secret_writer)
) -> ProvidersResponse:
    if not repository.delete(org.id, provider):
        raise HTTPException(status_code=404, detail=f"Provider '{provider}' has no secret")
    return ProvidersResponse(providers=repository.list_all(org.id))
