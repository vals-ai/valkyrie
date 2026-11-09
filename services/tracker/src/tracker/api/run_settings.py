from fastapi import APIRouter, Depends, HTTPException

from tracker.api.secrets import require_own_secret
from tracker.auth import RequestIdentity, get_current_identity
from tracker.config import AUTH_REQUIRED
from tracker.database.models import UserBenchmarkService, UserDefaultProvider
from tracker.repositories.run_settings import RunSettingsRepository, RunSettingsRepositoryDependency
from tracker.outbound_security import validate_custom_service_destination
from tracker.types import BenchmarkName, BenchmarkServiceRequest, DefaultProviderRequest, RunSettingsResponse

router = APIRouter(prefix="/settings")
_LOGIN_ONLY = {"description": "Only a signed-in user can change run settings"}


def _user_id(identity: RequestIdentity) -> str | None:
    if identity.user_id is None and not AUTH_REQUIRED:
        return "local"
    return identity.user_id


def _require_user_id(identity: RequestIdentity) -> str:
    user_id = _user_id(identity)
    if user_id is None or identity.access_key_id is not None:
        raise HTTPException(
            status_code=403,
            detail="Only a signed-in user can change run settings; sign in with `valkyrie login` "
            "and unset VALKYRIE_API_KEY",
        )
    return user_id


def user_run_settings(repository: RunSettingsRepository, identity: RequestIdentity) -> RunSettingsResponse:
    user_id = _user_id(identity)
    if user_id is None:
        return RunSettingsResponse(default_provider=None, benchmark_services={})
    default = repository.get_default_provider(identity.org.id, user_id)
    services = repository.list_benchmark_services(identity.org.id, user_id)
    return RunSettingsResponse.model_validate(
        {
            "default_provider": default.provider if default else None,
            "benchmark_services": {row.benchmark: row for row in services},
        }
    )


@router.get(
    "",
    response_model=RunSettingsResponse,
    description="Show the caller's default sandbox provider and benchmark services. Keys without a user get an empty result.",
)
def get_run_settings(
    repository: RunSettingsRepositoryDependency, identity: RequestIdentity = Depends(get_current_identity)
) -> RunSettingsResponse:
    return user_run_settings(repository, identity)


@router.put(
    "/default-provider",
    response_model=RunSettingsResponse,
    responses={403: _LOGIN_ONLY},
    description="Run on this sandbox provider unless a run names another.",
)
def set_default_provider(
    repository: RunSettingsRepositoryDependency,
    body: DefaultProviderRequest,
    identity: RequestIdentity = Depends(get_current_identity),
) -> RunSettingsResponse:
    repository.save(
        UserDefaultProvider(org_id=identity.org.id, user_id=_require_user_id(identity), provider=body.provider)
    )
    return user_run_settings(repository, identity)


@router.put(
    "/benchmark-services/{benchmark}",
    response_model=RunSettingsResponse,
    responses={
        403: {"description": "Not a signed-in user, or the URL is not an allowed destination"},
        404: {"description": "Secret does not exist"},
    },
    description="Use this benchmark service URL for the benchmark's runs, sending the named secret's value as a header.",
)
async def set_benchmark_service(
    repository: RunSettingsRepositoryDependency,
    benchmark: BenchmarkName,
    body: BenchmarkServiceRequest,
    identity: RequestIdentity = Depends(get_current_identity),
) -> RunSettingsResponse:
    user_id = _require_user_id(identity)
    try:
        validate_custom_service_destination(body.url, org_name=identity.org.name, auth_required=AUTH_REQUIRED)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if body.auth_secret_name is not None:
        await require_own_secret(body.auth_secret_name, identity.org)
    repository.save(
        UserBenchmarkService(org_id=identity.org.id, user_id=user_id, benchmark=benchmark, **body.model_dump())
    )
    return user_run_settings(repository, identity)


@router.delete(
    "/benchmark-services/{benchmark}",
    response_model=RunSettingsResponse,
    responses={403: _LOGIN_ONLY, 404: {"description": "No URL is saved for the benchmark"}},
    description="Stop using a saved benchmark service URL.",
)
def remove_benchmark_service(
    repository: RunSettingsRepositoryDependency,
    benchmark: BenchmarkName,
    identity: RequestIdentity = Depends(get_current_identity),
) -> RunSettingsResponse:
    if not repository.delete_benchmark_service(identity.org.id, _require_user_id(identity), benchmark):
        raise HTTPException(status_code=404, detail=f"Benchmark '{benchmark}' has no saved URL")
    return user_run_settings(repository, identity)
