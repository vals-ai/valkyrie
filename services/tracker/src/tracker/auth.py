from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from dataclasses import dataclass, replace
import hashlib
import hmac
import json
import math
import random
import secrets
from threading import Condition
import time
from typing import Any, TypeVar, cast
from weakref import WeakKeyDictionary, ref, ReferenceType

from cachetools import TLRUCache, cached
from descope import DescopeClient
from descope.exceptions import AuthException, RateLimitException
from descope.management.access_key import AccessKey
from descope.mgmt import MGMT
from fastapi import Depends, HTTPException, Request
from jwt import PyJWTError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from sqlmodel import Session
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tracker import config
from tracker.config import (
    AUTH_REQUIRED,
    DESCOPE_CLI_CLIENT_ID,
    DESCOPE_MANAGEMENT_KEY,
    DESCOPE_PROJECT_ID,
    PASSKEY_ORGS,
    BenchmarkServiceDestination,
)
from tracker.aws.tenant_storage import TenantBucketError, ensure_managed_org_bucket
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.repositories.organization import OrganizationRepository
from tracker.database.session import database_call, get_session, server_engine
from tracker.exceptions import MembershipUnavailableError
from tracker.services.memberships import MembershipCache
from tracker.logging import get_logger
from tracker.outbound_security import validate_service_headers

logger = get_logger(__name__)

BENCHMARK_SERVICE_API_KEY_HEADER = "X-Descope-Api-Key"
RUN_KEY_CLAIM = "valkyrie_run_key"
USER_KEY_CLAIM = "valkyrie_user_key"
READ_ONLY_KEY_CLAIM = "valkyrie_read_only"
READ_METHODS = frozenset({"GET", "HEAD"})
TENANT_HEADER = "x-valkyrie-tenant"
NOT_A_MEMBER = "This login is not a member of the requested organization"
SEVERAL_ORGANIZATIONS = "This login belongs to several organizations. Choose one with `valkyrie org use NAME`."
NO_ORGANIZATION = (
    "This login is not in an organization yet. Ask an organization admin to run `valkyrie org invite EMAIL`."
)
LOGIN_REQUIRED = "This needs a `valkyrie login` session; unset VALKYRIE_API_KEY if it is set."
_ACCESS_KEY_CACHE_MAX_SIZE = 1024
_ACCESS_KEY_CACHE_TTL_SECONDS = 60.0
_ACCESS_KEY_EXPIRY_SKEW_SECONDS = 1.0
_USER_LOOKUP_TIMEOUT_SECONDS = 2.0
_USER_LOOKUP_RETRY_BUDGET_SECONDS = 15.0
_USER_LOOKUP_ATTEMPTS = 5
_USER_LOOKUP_CACHE_TTL_SECONDS = 5.0
_USER_LOOKUP_CACHE_MAX_SIZE = 1024
_DESCOPE_CONCURRENCY = 8
_descope_executor = ThreadPoolExecutor(max_workers=_DESCOPE_CONCURRENCY, thread_name_prefix="tracker-auth-user")
_credential_executor = ThreadPoolExecutor(max_workers=_DESCOPE_CONCURRENCY, thread_name_prefix="tracker-auth-key")
_descope_slots: WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[ThreadPoolExecutor, ReferenceType[asyncio.Semaphore]]
] = WeakKeyDictionary()
_SDKResult = TypeVar("_SDKResult")


@dataclass(frozen=True)
class RequestIdentity:
    org: Org
    access_key_id: str | None = None
    email: str | None = None
    name: str | None = None
    user_id: str | None = None
    passkey: bool = False

    @property
    def tenant_name(self) -> str:
        return self.org.name


@dataclass(frozen=True)
class _Credentials:
    tenant_name: str
    access_key_id: str | None = None
    email: str | None = None
    name: str | None = None
    user_id: str | None = None
    passkey: bool = False


_Identity = TypeVar("_Identity", RequestIdentity, _Credentials)


@dataclass(frozen=True)
class LoginIdentity:
    user_id: str
    login_id: str
    email: str | None
    organizations: dict[str, tuple[str, ...]]
    organization: str | None
    permissions: tuple[str, ...] = ()

    @property
    def current_organization(self) -> str:
        return require_organization(self.organizations, self.organization)


@dataclass(frozen=True)
class DescopeUserProfile:
    email: str | None
    name: str | None


@dataclass(frozen=True)
class CachedAccessKeyClaims:
    tenant_name: str
    access_key_id: str
    email: str | None
    name: str | None
    user_id: str | None
    read_only: bool
    cache_deadline_monotonic: float
    requires_membership: bool = False


_access_key_digest_secret = secrets.token_bytes(32)
_access_key_cache_condition = Condition()
_access_key_cache: TLRUCache[bytes, CachedAccessKeyClaims, float] = TLRUCache(
    maxsize=_ACCESS_KEY_CACHE_MAX_SIZE,
    ttu=lambda _key, claims, _now: claims.cache_deadline_monotonic,
    timer=time.monotonic,
)
_descope_client: DescopeClient | None = (
    DescopeClient(
        project_id=DESCOPE_PROJECT_ID,
        management_key=DESCOPE_MANAGEMENT_KEY or None,
    )
    if AUTH_REQUIRED and DESCOPE_PROJECT_ID
    else None
)
_descope_user_client: DescopeClient | None = (
    DescopeClient(
        project_id=DESCOPE_PROJECT_ID,
        management_key=DESCOPE_MANAGEMENT_KEY or None,
        timeout_seconds=_USER_LOOKUP_TIMEOUT_SECONDS,
    )
    if AUTH_REQUIRED and DESCOPE_PROJECT_ID
    else None
)
_live_user_cache_condition = Condition()
_live_user_cache: TLRUCache[tuple[str, DescopeClient | None, str], dict[str, Any] | None, float] = TLRUCache(
    maxsize=_USER_LOOKUP_CACHE_MAX_SIZE,
    ttu=lambda _key, user, now: now + _USER_LOOKUP_CACHE_TTL_SECONDS if user is not None else now,
    timer=lambda: time.monotonic(),
)
_live_user_pending: dict[
    tuple[asyncio.AbstractEventLoop, str, DescopeClient | None, str], asyncio.Task[dict[str, Any] | None]
] = {}
_live_user_tasks: set[asyncio.Task[dict[str, Any] | None]] = set()


async def _descope_call(
    work: Callable[..., _SDKResult], *args: Any, executor: ThreadPoolExecutor = _descope_executor
) -> _SDKResult:
    loop = asyncio.get_running_loop()
    with _live_user_cache_condition:
        references = _descope_slots.setdefault(loop, {})
        reference = references.get(executor)
        slots = reference() if reference is not None else None
        if slots is None:
            slots = asyncio.Semaphore(_DESCOPE_CONCURRENCY)
            references[executor] = ref(slots)
    await slots.acquire()
    try:
        context = copy_context()
        future = loop.run_in_executor(executor, lambda: context.run(work, *args))
    except BaseException:
        slots.release()
        raise

    def finished(completed: asyncio.Future[_SDKResult]) -> None:
        slots.release()
        if not completed.cancelled():
            completed.exception()

    future.add_done_callback(finished)
    return await asyncio.shield(future)


def _clean_string(value: object, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str) or not (value := value.strip()):
        return None
    return value.lower() if lowercase else value


def _string_claim(claims: Mapping[str, object], name: str, *, lowercase: bool = False) -> str | None:
    return _clean_string(claims.get(name), lowercase=lowercase)


def _load_descope_user_profile(user_id: str) -> DescopeUserProfile:
    if not _descope_client:
        return DescopeUserProfile(email=None, name=None)

    try:
        user_response = _descope_client.mgmt.user.load_by_user_id(user_id)
    except Exception:
        logger.warning("Failed to load Descope user profile", exc_info=True)
        return DescopeUserProfile(email=None, name=None)

    user = user_response.get("user")
    if not isinstance(user, Mapping):
        logger.warning("Descope user profile response did not include a user object")
        return DescopeUserProfile(email=None, name=None)

    return DescopeUserProfile(
        email=_string_claim(user, "email", lowercase=True),
        name=_clean_string(user.get("name") or user.get("displayName")),
    )


def get_default_org(session: Session) -> Org:
    org = OrganizationRepository(session).get_by_name(DEFAULT_ORG_NAME)
    if not org:
        raise RuntimeError("Default org not found — run the migration")
    return org


@retry(
    retry=retry_if_exception_type((ReadTimeout, RequestsConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=2),
    reraise=True,
)
def _exchange_access_key(api_key: str, descope_client: DescopeClient) -> dict[str, Any]:
    return cast(dict[str, Any], descope_client.exchange_access_key(api_key))


def _access_key_digest(api_key: str) -> bytes:
    return hmac.digest(_access_key_digest_secret, api_key.encode(), hashlib.sha256)


def _cache_deadline(jwt_expires_at: float, *, wall_now: float, monotonic_now: float) -> float:
    remaining = jwt_expires_at - wall_now - _ACCESS_KEY_EXPIRY_SKEW_SECONDS
    cache_seconds = min(_ACCESS_KEY_CACHE_TTL_SECONDS, max(0.0, remaining))
    return monotonic_now + cache_seconds


def _session_failure_reason(exc: AuthException | PyJWTError) -> str:
    if isinstance(exc, AuthException):
        reason = exc.error_message or exc.error_type or type(exc).__name__
    else:
        reason = type(exc).__name__
    return reason[:200]


def _has_claim(claims: Mapping[str, Any], name: str) -> bool:
    return any(
        isinstance(nested, Mapping) and cast(Mapping[str, object], nested).get(name)
        for nested in (claims, claims.get("customClaims"), claims.get("nsec"))
    )


def _normalize_access_key_claims(jwt_response: Mapping[str, object]) -> CachedAccessKeyClaims:
    session_token = jwt_response.get("sessionToken")
    claims_from_token: Mapping[str, object] = (
        cast(Mapping[str, object], session_token) if isinstance(session_token, Mapping) else {}
    )
    claims: dict[str, object] = {**jwt_response, **claims_from_token}

    tenants = jwt_response.get("tenants")
    tenant_names: list[object] = list(cast(Mapping[object, object], tenants)) if isinstance(tenants, Mapping) else []
    if len(tenant_names) != 1:
        raise HTTPException(
            status_code=400,
            detail=f"Access key must be scoped to exactly one tenant, got {len(tenant_names)}",
        )

    access_key_id = _string_claim(claims, "keyId") or _string_claim(claims, "sub")
    if not access_key_id:
        raise HTTPException(status_code=400, detail="Descope JWT missing access key id")

    if _has_claim(claims, RUN_KEY_CLAIM):
        raise HTTPException(status_code=401, detail="Invalid API key")
    custom = claims.get("customClaims")
    user_id = _string_claim(claims, "user_id") or (
        _string_claim(cast(Mapping[str, object], custom), "user_id") if isinstance(custom, Mapping) else None
    )
    expires_at = claims.get("exp")
    try:
        jwt_expires_at = float(expires_at) if isinstance(expires_at, (int, float)) else 0.0
    except OverflowError:
        jwt_expires_at = 0.0
    return CachedAccessKeyClaims(
        tenant_name=str(tenant_names[0]),
        access_key_id=access_key_id,
        email=_string_claim(claims, "email", lowercase=True),
        name=_string_claim(claims, "name"),
        user_id=user_id,
        read_only=_has_claim(jwt_response, READ_ONLY_KEY_CLAIM) or _has_claim(claims_from_token, READ_ONLY_KEY_CLAIM),
        requires_membership=_has_claim(claims, USER_KEY_CLAIM),
        cache_deadline_monotonic=_cache_deadline(
            jwt_expires_at if math.isfinite(jwt_expires_at) else 0.0,
            wall_now=time.time(),
            monotonic_now=time.monotonic(),
        ),
    )


@cached(cache=_access_key_cache, key=_access_key_digest, condition=_access_key_cache_condition)
def _exchange_and_normalize_access_key(api_key: str) -> CachedAccessKeyClaims:
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    try:
        jwt_response = _exchange_access_key(api_key, _descope_client)
    except AuthException as exc:
        logger.warning("Descope API key validation failed")
        raise HTTPException(status_code=401, detail="Invalid API key") from exc
    except Exception as exc:
        logger.warning("Descope API key validation failed because the provider is unavailable", exc_info=True)
        raise HTTPException(status_code=503, detail="Auth service unavailable") from exc

    return _normalize_access_key_claims(jwt_response)


def _create_org(tenant_name: str, session: Session) -> Org:
    repository = OrganizationRepository(session)
    repository.create(tenant_name)
    org = repository.get_by_name(tenant_name)
    if org is None:
        raise HTTPException(status_code=500, detail="Internal error during org creation")
    return org


def _require_org(tenant_name: str, session: Session) -> Org:
    org = OrganizationRepository(session).get_by_name(tenant_name) or _create_org(tenant_name, session)
    prepare_org_storage(session, org)
    return org


def prepare_org_storage(session: Session, org: Org) -> None:
    try:
        ensure_managed_org_bucket(session, org)
    except TenantBucketError as exc:
        logger.exception("Failed to prepare organization storage", extra={"org_id": str(org.id)})
        raise HTTPException(
            status_code=503, detail="Organization storage is not ready. Try again later or contact Vals support."
        ) from exc


def forward_tracker_api_key(
    service_headers: Mapping[str, str] | None,
    tracker_api_key: str | None,
    *,
    destination: BenchmarkServiceDestination,
) -> dict[str, str]:
    forwarded_headers = dict(service_headers or {})
    try:
        validate_service_headers(forwarded_headers)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Unsupported benchmark service header") from exc
    if not tracker_api_key or destination is not BenchmarkServiceDestination.HOSTED:
        return forwarded_headers

    if BENCHMARK_SERVICE_API_KEY_HEADER.lower() not in (key.lower() for key in forwarded_headers):
        forwarded_headers[BENCHMARK_SERVICE_API_KEY_HEADER] = tracker_api_key

    return forwarded_headers


def _validate_bearer(jwt: str) -> dict[str, Any]:
    if not _descope_client:
        raise RuntimeError("Descope client not initialized — check DESCOPE_PROJECT_ID and AUTH_REQUIRED")

    failure_reasons: list[str] = []
    for audience in (None, DESCOPE_CLI_CLIENT_ID) if DESCOPE_CLI_CLIENT_ID else (None,):
        try:
            claims = cast(dict[str, Any], _descope_client.validate_session(jwt, audience=audience))
        except (AuthException, PyJWTError) as exc:
            failure_reasons.append(_session_failure_reason(exc))
            continue
        if _has_claim(claims, RUN_KEY_CLAIM):
            raise HTTPException(status_code=401, detail="Invalid session")
        return claims
    logger.warning("Descope session validation failed: %s", ", ".join(dict.fromkeys(failure_reasons)))
    raise HTTPException(status_code=401, detail="Invalid session")


def _chosen_organization(organizations: list[str], requested: str | None, claims: Mapping[str, Any]) -> str | None:
    if requested:
        return requested
    if (current := _string_claim(claims, "dct")) in organizations:
        return current
    return organizations[0] if len(organizations) == 1 else None


def require_organization(organizations: Collection[str], chosen: str | None) -> str:
    if chosen is None:
        if organizations:
            detail = SEVERAL_ORGANIZATIONS
        elif config.SELF_SERVE_ORGS_ENABLED:
            detail = (
                "This login is not in an organization yet. Run `valkyrie org create ORGANIZATION` to create one, "
                "or ask an organization admin to run `valkyrie org invite EMAIL`."
            )
        else:
            detail = NO_ORGANIZATION
        raise HTTPException(status_code=400, detail=detail)
    if chosen not in organizations:
        raise HTTPException(status_code=403, detail=NOT_A_MEMBER)
    return chosen


def _request_credentials(request: Request) -> tuple[str, bool]:
    scheme, _, bearer = request.headers.get("authorization", "").partition(" ")
    is_session = scheme.lower() == "bearer" and bool(bearer)
    api_key = request.headers.get("x-api-key", "")
    if is_session and api_key:
        raise HTTPException(status_code=401, detail="Send Authorization OR x-api-key, not both")
    if not is_session and not api_key:
        raise HTTPException(status_code=401, detail="Missing Authorization or x-api-key header")
    return (bearer, True) if is_session else (api_key, False)


def _require_read_method(request: Request, read_only: bool) -> None:
    if read_only and request.method not in READ_METHODS:
        raise HTTPException(status_code=403, detail="This access key is read-only")


async def _get_credentials(request: Request) -> _Credentials | None:
    if not AUTH_REQUIRED:
        return None

    token, is_session = _request_credentials(request)
    if is_session:
        claims = await _descope_call(_validate_bearer, token, executor=_credential_executor)
        tenants_claim = claims.get("tenants")
        tenants = (
            [str(name) for name in cast(Mapping[object, object], tenants_claim)]
            if isinstance(tenants_claim, Mapping)
            else []
        )
        tenant = require_organization(
            tenants, _chosen_organization(tenants, request.headers.get(TENANT_HEADER), claims)
        )
        bound_user_id = _string_claim(claims, "user_id")
        is_access_key = bound_user_id is not None
        _require_read_method(request, _has_claim(claims, READ_ONLY_KEY_CLAIM))
        user_id = bound_user_id if is_access_key else _string_claim(claims, "sub")
        return _Credentials(
            tenant_name=tenant,
            access_key_id=_string_claim(claims, "sub") if is_access_key else None,
            email=_string_claim(claims, "email", lowercase=True),
            name=_string_claim(claims, "name"),
            user_id=user_id,
            passkey=not is_access_key and "webauthn" in (claims.get("amr") or ()),
        )

    key = await _descope_call(_exchange_and_normalize_access_key, token, executor=_credential_executor)
    if key.requires_membership and not await _is_member(key.user_id, key.tenant_name):
        raise HTTPException(status_code=401, detail="Invalid API key")
    _require_read_method(request, key.read_only)
    return _Credentials(
        tenant_name=key.tenant_name,
        access_key_id=key.access_key_id,
        email=key.email,
        name=key.name,
        user_id=key.user_id,
    )


async def _identity_from_credentials(credentials: _Credentials | None, session: Session) -> RequestIdentity:
    if credentials is None:
        return RequestIdentity(org=await database_call(lambda: get_default_org(session)))
    org = await database_call(lambda: _require_org(credentials.tenant_name, session))
    return RequestIdentity(
        org=org,
        access_key_id=credentials.access_key_id,
        email=credentials.email,
        name=credentials.name,
        user_id=credentials.user_id,
        passkey=credentials.passkey,
    )


async def get_current_identity(
    credentials: _Credentials | None = Depends(_get_credentials), session: Session = Depends(get_session)
) -> RequestIdentity:
    return await _identity_from_credentials(credentials, session)


def get_current_org(identity: RequestIdentity = Depends(get_current_identity)) -> Org:
    return identity.org


def descope_management() -> MGMT | None:
    return _descope_client.mgmt if _descope_client is not None and DESCOPE_MANAGEMENT_KEY else None


def descope_access_keys() -> AccessKey | None:
    return management.access_key if (management := descope_management()) else None


def _user_lookup_retry_after(exc: Exception) -> float:
    parameters = getattr(exc, "rate_limit_parameters", None)
    if not isinstance(parameters, Mapping):
        return 0.0
    value = cast(Mapping[str, object], parameters).get("Retry-After")
    if not isinstance(value, (str, int, float)):
        return 0.0
    try:
        delay = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return delay if math.isfinite(delay) and delay > 0 else 0.0


def invalidate_user_cache(user_id: str) -> None:
    try:
        _membership_cache.invalidate(DESCOPE_PROJECT_ID, user_id)
    finally:
        with _live_user_cache_condition:
            for key in list(_live_user_cache):
                if key[2] == user_id:
                    _live_user_cache.pop(key, None)
            for key in list(_live_user_pending):
                if key[-1] == user_id:
                    _live_user_pending.pop(key, None)


def _user_lookup_finished(task: asyncio.Task[dict[str, Any] | None]) -> None:
    _live_user_tasks.discard(task)
    if not task.cancelled():
        task.exception()


async def _cached_run_owner_user(user_id: str) -> dict[str, Any] | None:
    key = (DESCOPE_PROJECT_ID, _descope_user_client, user_id)
    pending_key = (asyncio.get_running_loop(), *key)
    with _live_user_cache_condition:
        try:
            return _live_user_cache[key]
        except KeyError:
            pass
        task = _live_user_pending.get(pending_key)
        if task is None:
            task = asyncio.create_task(_refresh_run_owner_user(pending_key))
            _live_user_pending[pending_key] = task
            _live_user_tasks.add(task)
            task.add_done_callback(_user_lookup_finished)
    return await asyncio.shield(task)


async def _refresh_run_owner_user(
    pending_key: tuple[asyncio.AbstractEventLoop, str, DescopeClient | None, str],
) -> dict[str, Any] | None:
    key = pending_key[1:]
    task = asyncio.current_task()
    try:
        user = await _live_user(key[2])
        with _live_user_cache_condition:
            if user is not None and _live_user_pending.get(pending_key) is task:
                _live_user_cache[key] = user
        return user
    finally:
        with _live_user_cache_condition:
            if _live_user_pending.get(pending_key) is task:
                del _live_user_pending[pending_key]


async def _live_user(user_id: str) -> dict[str, Any] | None:
    deadline = time.monotonic() + _USER_LOOKUP_RETRY_BUDGET_SECONDS
    attempt = 0
    while True:
        attempt += 1
        try:
            if _descope_user_client is None:
                raise RuntimeError("Descope client not initialized")
            async with asyncio.timeout(max(0.0, deadline - time.monotonic())):
                response = await _descope_call(
                    cast(Callable[[str], dict[str, Any]], _descope_user_client.mgmt.user.load_by_user_id), user_id
                )
            return cast(dict[str, Any], response["user"])
        except Exception as exc:
            if isinstance(exc, AuthException) and exc.status_code in (400, 404):
                return None
            rate_limited = isinstance(exc, RateLimitException) or (
                isinstance(exc, AuthException) and str(exc.status_code) in ("429", "E130429")
            )
            if rate_limited and attempt < _USER_LOOKUP_ATTEMPTS:
                delay = max(_user_lookup_retry_after(exc), random.uniform(0, min(4.0, 0.5 * 2 ** (attempt - 1))))
                if time.monotonic() + delay + 2 * _USER_LOOKUP_TIMEOUT_SECONDS < deadline:
                    await asyncio.sleep(delay)
                    if time.monotonic() + 2 * _USER_LOOKUP_TIMEOUT_SECONDS < deadline:
                        continue
            logger.warning("Descope user lookup failed: %s", type(exc).__name__, exc_info=True)
            raise HTTPException(status_code=503, detail="Auth service unavailable") from exc


def _descope_failure(exc: Exception) -> str:
    parts = [type(exc).__name__, str(getattr(exc, "status_code", "") or "")]
    if isinstance(exc, AuthException):
        try:
            details: object = json.loads(exc.error_message or "{}")
        except (TypeError, ValueError):
            details = None
        if isinstance(details, dict):
            fields = cast(dict[str, object], details)
            parts.extend(str(fields[key]) for key in ("errorCode", "errorDescription") if fields.get(key))
    return " ".join(part for part in parts if part)


@contextmanager
def descope_call(action: str) -> Generator[None]:
    try:
        yield
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Descope failed to %s: %s", action, _descope_failure(exc), exc_info=True)
        raise HTTPException(status_code=503, detail=f"Could not {action}; try again") from exc


def user_tenant_roles(user: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    return {t["tenantId"]: tuple(t.get("roleNames") or ()) for t in user.get("userTenants") or ()}


async def _load_membership_roles(user_id: str) -> dict[str, tuple[str, ...]]:
    if _descope_user_client is None:
        raise RuntimeError("Descope client not initialized")
    try:
        response = await _descope_call(
            cast(Callable[[str], dict[str, Any]], _descope_user_client.mgmt.user.load_by_user_id), user_id
        )
    except AuthException as error:
        if error.status_code in (400, 404):
            return {}
        raise
    user = response.get("user")
    return user_tenant_roles(user) if isinstance(user, dict) else {}


_membership_cache = MembershipCache(server_engine, _load_membership_roles)


async def _cached_membership_roles(user_id: str) -> dict[str, tuple[str, ...]]:
    return await _membership_cache.get(DESCOPE_PROJECT_ID, user_id)


async def _is_member(user_id: str | None, tenant: str) -> bool:
    management = descope_management()
    if management is None or user_id is None:
        return False
    try:
        return tenant in await _cached_membership_roles(user_id)
    except MembershipUnavailableError as error:
        raise HTTPException(status_code=503, detail="Auth service unavailable") from error


def primary_login_id(user: Mapping[str, Any]) -> str | None:
    return next(iter(user.get("loginIds") or []), None)


async def get_current_login(request: Request) -> LoginIdentity:
    management = descope_management()
    if management is None:
        raise HTTPException(status_code=404, detail="This Tracker cannot look up logins")
    token, is_session = _request_credentials(request)
    claims = await _descope_call(_validate_bearer, token, executor=_credential_executor) if is_session else {}
    if (user_id := _string_claim(claims, "sub")) is None:
        raise HTTPException(status_code=403, detail=LOGIN_REQUIRED)
    if (user := await _live_user(user_id)) is None:
        raise HTTPException(status_code=403, detail=LOGIN_REQUIRED)
    organizations = user_tenant_roles(user)
    return LoginIdentity(
        user_id=user_id,
        login_id=primary_login_id(user) or user_id,
        email=_string_claim(user, "email", lowercase=True) if user.get("verifiedEmail") is True else None,
        organizations=organizations,
        organization=_chosen_organization(list(organizations), request.headers.get(TENANT_HEADER), claims),
        permissions=tuple(user.get("permissions") or ()),
    )


def get_current_starter(identity: RequestIdentity = Depends(get_current_identity)) -> RequestIdentity:
    if identity.email is None and identity.user_id is not None:
        profile = _load_descope_user_profile(identity.user_id)
        identity = replace(identity, email=profile.email, name=identity.name or profile.name)
    return identity


async def _resolve_run_owner(
    management: MGMT, identity: _Identity, subject_id: str
) -> tuple[_Identity, dict[str, Any] | None]:
    if identity.user_id is not None:
        user = await _cached_run_owner_user(identity.user_id)
        if user is not None:
            return identity, user
        if identity.access_key_id is not None:
            raise HTTPException(status_code=403, detail="This user cannot start or retry benchmarks")

    with descope_call("load the access key"):
        try:
            response = await _descope_call(
                cast(Callable[[str], dict[str, Any]], management.access_key.load),
                subject_id,
                executor=_credential_executor,
            )
            key = response["key"]
        except AuthException as exc:
            if exc.status_code in (400, 404):
                raise HTTPException(status_code=403, detail="This user cannot start or retry benchmarks") from exc
            raise
    bound_user_id = _string_claim(key, "boundUserId")
    identity = replace(identity, access_key_id=subject_id, user_id=bound_user_id)
    if bound_user_id is None:
        return identity, None
    user = await _cached_run_owner_user(bound_user_id)
    if user is None:
        raise HTTPException(status_code=403, detail="This user cannot start or retry benchmarks")
    return identity, user


async def _authorize_runner(identity: _Identity) -> _Identity:
    if identity.tenant_name in PASSKEY_ORGS and not identity.passkey:
        raise HTTPException(status_code=403, detail=f"Runs in {identity.tenant_name} need a passkey `valkyrie login`")
    subject_id = identity.user_id or identity.access_key_id
    if subject_id is None:
        return identity
    management = descope_management()
    if management is None:
        raise HTTPException(status_code=503, detail="Auth service unavailable")
    identity, user = await _resolve_run_owner(management, identity, subject_id)
    if user is None:
        return identity
    if "valkyrie-no-start" in (user.get("roleNames") or ()) or "valkyrie-no-start" in user_tenant_roles(user).get(
        identity.tenant_name, ()
    ):
        raise HTTPException(status_code=403, detail="This user cannot start or retry benchmarks")
    if identity.email is None:
        identity = replace(
            identity,
            email=_string_claim(user, "email", lowercase=True),
            name=identity.name or _clean_string(user.get("name") or user.get("displayName")),
        )
    return identity


async def get_current_runner(
    credentials: _Credentials | None = Depends(_get_credentials), session: Session = Depends(get_session)
) -> RequestIdentity:
    if credentials is None:
        return await _authorize_runner(await _identity_from_credentials(None, session))
    credentials = await _authorize_runner(credentials)
    return await _identity_from_credentials(credentials, session)
