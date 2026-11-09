import asyncio
from collections.abc import AsyncGenerator
from uuid import UUID
from benchmark_service import (
    Sandbox,
    SandboxProvider,
    SandboxNotFoundError,
    SandboxQuery,
)
import sentry_sdk
from sqlmodel import Session
from tracker.repositories.cost import CostRepository
from tracker.database.models import (
    Benchmark,
    Org,
)
from tracker.database.session import database_call, engine
from tracker.logging import get_logger
from tracker.runtime.services import RuntimeServices
from tracker.sandbox import delete_sandbox

logger = get_logger(__name__)


class ForceStopError(Exception):
    pass


FORCE_STOP_DELETE_ATTEMPTS = 3


FORCE_STOP_RETRY_DELAY_S = 1.0


async def stop_sandbox(sandbox: Sandbox, provider: SandboxProvider, org: Org, *, strict: bool = False) -> None:
    error: Exception | None = None
    for attempt in range(FORCE_STOP_DELETE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(FORCE_STOP_RETRY_DELAY_S * 2 ** (attempt - 1))
        try:
            if await delete_sandbox(sandbox, provider, initiated_by="force_stop", org_id=str(org.id)):
                return
            error = None
        except Exception as exc:
            error = exc
    logger.error(
        "Force stop could not delete sandbox %s after %d attempts",
        sandbox.name,
        FORCE_STOP_DELETE_ATTEMPTS,
        exc_info=error,
    )
    labels = sandbox.labels or {}
    with sentry_sdk.new_scope() as scope:
        scope.fingerprint = ["force-stop-sandbox-delete-failed"]
        scope.set_tag("sandbox_id", sandbox.id)
        scope.set_tag("benchmark_id", labels.get("Id"))
        scope.set_tag("task_id", labels.get("Task"))
        scope.set_tag("org_id", str(org.id))
        sentry_sdk.capture_message(f"Force stop could not delete sandbox {sandbox.id}", level="error")
    if strict:
        raise ForceStopError("Unable to send force-stop signal")


async def sandbox_generator(
    benchmark_row: Benchmark,
    provider: SandboxProvider,
    task_ids: list[str] | None = None,
) -> AsyncGenerator[Sandbox, None]:
    labels = {"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id)}
    queries = (
        [SandboxQuery(labels={**labels, "Task": task_id}) for task_id in task_ids]
        if task_ids
        else [SandboxQuery(labels=labels)]
    )
    seen_sandbox_ids: set[str] = set()
    for query in queries:
        async for sandbox in provider.list_sandboxes(query):
            if sandbox.id in seen_sandbox_ids:
                continue
            seen_sandbox_ids.add(sandbox.id)
            yield sandbox


async def capture_stop_sandboxes(
    benchmark_row: Benchmark,
    runtime: RuntimeServices,
    task_ids: list[str],
    *,
    strict: bool = False,
) -> list[Sandbox]:

    async def capture() -> list[Sandbox]:
        config = await runtime.get_sandbox_provider_config()
        async with config.create_provider() as provider:
            return [sandbox async for sandbox in sandbox_generator(benchmark_row, provider, task_ids=task_ids)]

    try:
        return await asyncio.wait_for(capture(), timeout=15)
    except Exception:
        logger.exception("Unable to capture force-stop targets for benchmark %s", benchmark_row.id)
        if strict:
            raise ForceStopError("Unable to capture force-stop targets") from None
        return []


RECORDED_SANDBOX_LOOKUPS = 16


def recorded_sandbox_ids(benchmark_id: UUID, provider_type: str) -> list[str]:
    with Session(engine) as session:
        return CostRepository(session).sandbox_ids(benchmark_id, provider_type)


async def recorded_sandboxes(
    benchmark_id: UUID, provider_type: str, provider: SandboxProvider, seen: set[str]
) -> list[Sandbox]:
    lookups = asyncio.Semaphore(RECORDED_SANDBOX_LOOKUPS)

    async def lookup(sandbox_id: str) -> Sandbox | None:
        async with lookups:
            try:
                return await provider.get_sandbox(sandbox_id)
            except SandboxNotFoundError:
                return None
            except Exception:
                logger.exception("Force stop could not look up recorded sandbox %s", sandbox_id)
                with sentry_sdk.new_scope() as scope:
                    scope.fingerprint = ["force-stop-sandbox-lookup-failed"]
                    scope.set_tag("sandbox_id", sandbox_id)
                    scope.set_tag("benchmark_id", str(benchmark_id))
                    sentry_sdk.capture_message(f"Force stop could not look up sandbox {sandbox_id}", level="error")
                return None

    ids = await database_call(lambda: recorded_sandbox_ids(benchmark_id, provider_type))
    found = await asyncio.gather(*(lookup(i) for i in ids if i not in seen))
    return [sandbox for sandbox in found if sandbox is not None]


async def force_stop_sandboxes(
    benchmark_row: Benchmark,
    runtime: RuntimeServices,
    org: Org,
    task_ids: list[str] | None = None,
    *,
    sandboxes: list[Sandbox] | None = None,
    strict: bool = False,
) -> None:
    try:
        config = await runtime.get_sandbox_provider_config()
        async with config.create_provider() as provider:
            if sandboxes is None:
                sandboxes = []
                try:
                    async for sandbox in sandbox_generator(benchmark_row, provider, task_ids=task_ids):
                        sandboxes.append(sandbox)
                except Exception:
                    if task_ids:
                        raise
                    logger.exception("Force stop could not list sandboxes for benchmark %s", benchmark_row.id)
                    with sentry_sdk.new_scope() as scope:
                        scope.fingerprint = ["force-stop-sandbox-list-failed"]
                        scope.set_tag("benchmark_id", str(benchmark_row.id))
                        scope.set_tag("provider", config.type)
                        sentry_sdk.capture_message(
                            f"Force stop could not list sandboxes for benchmark {benchmark_row.id}", level="error"
                        )
                if not task_ids:
                    sandboxes += await recorded_sandboxes(
                        benchmark_row.id, config.type, provider, {s.id for s in sandboxes}
                    )
            if strict:
                results = await asyncio.gather(
                    *(stop_sandbox(sandbox, provider, org, strict=True) for sandbox in sandboxes),
                    return_exceptions=True,
                )
                if any(isinstance(result, BaseException) for result in results):
                    raise ForceStopError("Unable to send force-stop signals")
            else:
                await asyncio.gather(*(stop_sandbox(sandbox, provider, org) for sandbox in sandboxes))
    except Exception:
        logger.exception("Unable to send force-stop signals for benchmark %s", benchmark_row.id)
        if strict:
            raise ForceStopError("Unable to send force-stop signals") from None
