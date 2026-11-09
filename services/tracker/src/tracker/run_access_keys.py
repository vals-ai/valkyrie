from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any, cast
from zoneinfo import ZoneInfo

from descope.exceptions import AuthException
from descope.management.access_key import AccessKey
from descope.management.common import AssociatedTenant
from fastapi import Depends, HTTPException, Request
from sqlalchemy import Connection, Engine
from sqlmodel import Session

from tracker.auth import BENCHMARK_SERVICE_API_KEY_HEADER, RUN_KEY_CLAIM, descope_access_keys
from tracker.config import AUTH_REQUIRED, RUN_ACCESS_KEY_TTL_SECONDS, BenchmarkServiceDestination
from tracker.database.models import Org
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.repositories.run_access_key import RunAccessKeyRepository

logger = get_logger(__name__)

_SWEEP_BATCH_SIZE = 20
_CALL_KEY_TTL_SECONDS = 600


@dataclass(frozen=True)
class IssuedRunKey:
    key_id: str
    secret: str = field(repr=False)


async def issued_run_keys(session: Session = Depends(get_session)) -> AsyncGenerator[list[IssuedRunKey], None]:
    bind = session.get_bind()
    issued: list[IssuedRunKey] = []
    try:
        yield issued
    finally:
        if issued and (access_keys := descope_access_keys()) is not None:
            await asyncio.shield(asyncio.to_thread(_discard_unrecorded, access_keys, bind, list(issued)))


IssuedKeys = Annotated[list[IssuedRunKey], Depends(issued_run_keys)]


async def run_service_key(
    http_request: Request,
    org: Org,
    destination: BenchmarkServiceDestination,
    service_headers: Mapping[str, str] | None,
    issued: list[IssuedRunKey],
    *,
    for_run: bool = True,
) -> str | None:
    if destination is not BenchmarkServiceDestination.HOSTED or BENCHMARK_SERVICE_API_KEY_HEADER.lower() in (
        key.lower() for key in service_headers or {}
    ):
        return None
    caller_key = http_request.headers.get("x-api-key") or None
    if (access_keys := descope_access_keys()) is None or (caller_key and not for_run):
        if AUTH_REQUIRED and caller_key is None:
            raise HTTPException(
                status_code=503, detail="This Tracker cannot create benchmark-service credentials for login sessions"
            )
        return caller_key
    ttl_seconds = RUN_ACCESS_KEY_TTL_SECONDS if for_run else _CALL_KEY_TTL_SECONDS
    creation_task = asyncio.create_task(asyncio.to_thread(_create, access_keys, org, issued, ttl_seconds=ttl_seconds))
    try:
        return (await asyncio.shield(creation_task)).secret
    except asyncio.CancelledError:
        while not creation_task.done():
            try:
                await asyncio.shield(creation_task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break

        try:
            creation_task.result()
        except Exception:
            pass
        raise


def _create(access_keys: AccessKey, org: Org, issued: list[IssuedRunKey], *, ttl_seconds: int) -> IssuedRunKey:
    try:
        response = cast(
            dict[str, Any],
            access_keys.create(
                name="Valkyrie run",
                expire_time=int(time.time()) + ttl_seconds,
                key_tenants=[AssociatedTenant(org.name)],
                custom_claims={RUN_KEY_CLAIM: True},
                description="Lets hosted benchmark services accept one Valkyrie run",
            ),
        )
        issued_key = IssuedRunKey(key_id=str(response["key"]["id"]), secret=str(response["cleartext"]))
    except Exception as exc:
        logger.warning("Failed to create a run access key", extra={"org_id": str(org.id)}, exc_info=True)
        raise HTTPException(status_code=503, detail="Could not create credentials for this run; try again") from exc
    issued.append(issued_key)
    return issued_key


def _delete(access_keys: AccessKey, key_id: str) -> bool:
    try:
        access_keys.delete(key_id)
    except Exception as exc:
        if isinstance(exc, AuthException) and exc.status_code == 404:
            return True
        logger.warning("Failed to delete a run access key", extra={"key_id": key_id})
        return False
    return True


def _discard_unrecorded(access_keys: AccessKey, bind: Engine | Connection, issued: Sequence[IssuedRunKey]) -> None:
    with Session(bind) as session:
        recorded = RunAccessKeyRepository(session).recorded_key_ids([key.key_id for key in issued])
    for key in issued:
        if key.key_id not in recorded:
            _delete(access_keys, key.key_id)


def delete_finished_run_keys(bind: Engine | Connection) -> None:
    access_keys = descope_access_keys()
    if access_keys is None:
        return
    now = datetime.now(ZoneInfo("UTC"))
    with Session(bind) as session:
        repository = RunAccessKeyRepository(session)
        for key in repository.lock_finished(_SWEEP_BATCH_SIZE):
            if _delete(access_keys, key.key_id):
                repository.delete(key)
            else:
                key.last_attempt_at = now
                repository.save(key)
        session.commit()
