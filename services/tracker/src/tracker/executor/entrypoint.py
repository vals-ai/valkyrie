from __future__ import annotations

import argparse
import asyncio
import logging
import os
import secrets
import signal
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any, TypeVar
from uuid import UUID

import httpx
import logfire
import sentry_sdk
from executor_protocol import (
    DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS,
    DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
    DEFAULT_EXECUTOR_DISPATCH_LEASE_TICK_SECONDS,
    executor_payload_benchmark_id,
    normalize_executor_telemetry_context,
)

from tracker.config import ENVIRONMENT
from tracker.executor.control_client import CONTROL_TIMEOUT, ControlClient
from tracker.executor.control_protocol import (
    AckDispatchRequest,
    AckDispatchResponse,
    ClaimDispatchRequest,
    ClaimDispatchResponse,
    DispatchRef,
    FailedFinishDispatchRequest,
    FinishDispatchRequest,
    FinishDispatchResponse,
    RenewDispatchRequest,
    RenewDispatchResponse,
)
from tracker.executor.control_queue import QueueControl
from tracker.executor.control_runs import RunControl
from tracker.executor.control_tasks import TaskControl
from tracker.executor.runner_observability import (
    capture_dispatch_error,
    capture_runner_failure,
    dispatch_observability_context,
    record_dispatch_cancellation,
    record_dispatch_completion,
)
from tracker.executor.watchdog import DeadlineChannel
from tracker.logging import configure_logging
from tracker.observability import configure_observability
from tracker.services.runs.start import process_benchmark
from tracker.services.failures import classify_failure

logger = logging.getLogger(__name__)
_Response = TypeVar("_Response")


async def _retry(operation: Callable[[], Awaitable[_Response]], cutoff: float) -> _Response:
    delay = 0.1
    while True:
        try:
            async with asyncio.timeout_at(cutoff):
                return await operation()
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code not in (502, 503, 504):
                raise
            remaining = cutoff - time.monotonic()
            if remaining <= 0:
                raise
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 5.0)


class LocalLease:
    def __init__(self, client: ControlClient, ref: DispatchRef, confirmed_at: float) -> None:
        self.client = client
        self.ref = ref
        self.lost = asyncio.Event()
        self.expiry = confirmed_at + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS
        self.timer = asyncio.get_running_loop().call_at(self.expiry, self.lost.set)

    def set_expiry(self, expiry: float) -> None:
        self.expiry = expiry
        self.timer.cancel()
        if expiry <= time.monotonic():
            self.lost.set()
        else:
            self.timer = asyncio.get_running_loop().call_at(expiry, self.lost.set)

    async def renew(self) -> None:
        while True:
            await asyncio.sleep(DEFAULT_EXECUTOR_DISPATCH_LEASE_TICK_SECONDS)
            started = time.monotonic()
            try:
                response = await self.client.call(
                    "renew_dispatch", RenewDispatchRequest(ref=self.ref), RenewDispatchResponse
                )
            except (httpx.TransportError, httpx.HTTPStatusError):
                logger.exception("Failed to renew local executor dispatch %s", self.ref.dispatch_id)
                continue
            if response.live is False or response.stopped is True:
                self.lost.set()
                return
            if response.renewed and started < self.expiry:
                self.set_expiry(started + DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS)

    def close(self) -> None:
        self.timer.cancel()


async def _claim(client: ControlClient, dispatch_id: UUID) -> tuple[dict[str, Any], float] | None:
    cutoff = time.monotonic() + DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS
    request = ClaimDispatchRequest(dispatch_id=dispatch_id)
    started = time.monotonic()
    response = await _retry(lambda: client.send("claim_dispatch", request, ClaimDispatchResponse), cutoff)
    if not response.claimed:
        return None
    assert response.payload is not None
    return response.payload, started


async def _run_dispatch(dispatch_id: UUID, watchdog: DeadlineChannel, shutdown_started: asyncio.Event) -> None:
    client = ControlClient(
        http=httpx.AsyncClient(base_url=os.environ["EXECUTOR_CONTROL_URL"], timeout=CONTROL_TIMEOUT),
        dispatch_id=dispatch_id,
        invocation_key=secrets.token_urlsafe(32),
        bootstrap_key=os.environ["EXECUTOR_BOOTSTRAP_KEY"],
    )
    try:
        claimed = await _claim(client, dispatch_id)
        if claimed is None:
            logger.info("Executor dispatch %s is no longer claimable", dispatch_id)
            return
        payload, claim_started = claimed
        ref = DispatchRef(benchmark_id=UUID(executor_payload_benchmark_id(payload)), dispatch_id=dispatch_id)
        await asyncio.to_thread(watchdog.identify, ref.benchmark_id, client.invocation_key)
        telemetry = normalize_executor_telemetry_context(payload.get("telemetry_context_json"))
        lease = LocalLease(client, ref, claim_started) if os.environ["EXECUTOR_LAUNCHER"] == "local" else None
        with dispatch_observability_context(
            str(ref.benchmark_id), str(dispatch_id), payload["executor_release_id"], telemetry
        ) as context:
            try:
                receipt = await _retry(
                    lambda: client.send("ack_dispatch", AckDispatchRequest(ref=ref), AckDispatchResponse),
                    lease.expiry if lease is not None else float("inf"),
                )
                if not receipt.acknowledged:
                    raise RuntimeError("Executor ciphertext receipt was not acknowledged")
                task_control = TaskControl(client, ref)
                run_control = RunControl(client, ref)
                queue_control = QueueControl(client, ref)
                work = asyncio.create_task(
                    process_benchmark(
                        start_benchmark_request_json=payload.get("start_benchmark_request_json"),
                        benchmark_id_str=payload.get("benchmark_id_str"),
                        verified_task_ids=payload.get("verified_task_ids"),
                        execution_context_json=payload.get("execution_context_json"),
                        executor_dispatch_id=str(dispatch_id),
                        run_control=run_control,
                        task_control=task_control,
                        queue_control=queue_control,
                    )
                )
                renewal = asyncio.create_task(lease.renew()) if lease is not None else None
                loss = asyncio.create_task(lease.lost.wait()) if lease is not None else None
                finish_task: asyncio.Task[FinishDispatchResponse] | None = None
                try:
                    while True:
                        pending = {finish_task or work}
                        if loss is not None:
                            pending.add(loss)
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                        if loss is not None and loss in done:
                            raise RuntimeError("Executor local dispatch lease expired")
                        if finish_task is not None and finish_task in done:
                            finish = await finish_task
                            if not finish.finished:
                                raise RuntimeError("Executor dispatch finish was rejected")
                            record_dispatch_completion(context)
                            return
                        if work in done:
                            await work
                            finish_task = asyncio.create_task(
                                client.call(
                                    "finish_dispatch",
                                    FinishDispatchRequest(ref=ref, succeeded=True),
                                    FinishDispatchResponse,
                                )
                            )
                finally:
                    shutdown_started.set()
                    if finish_task is not None and not finish_task.done():
                        finish_task.cancel()
                        await asyncio.gather(finish_task, return_exceptions=True)
                    if renewal is not None:
                        renewal.cancel()
                    if loss is not None:
                        loss.cancel()
                    await asyncio.gather(
                        *(task for task in (renewal, loss) if task is not None), return_exceptions=True
                    )
                    if not work.done():
                        work.cancel()
                        await asyncio.gather(work, return_exceptions=True)
            except asyncio.CancelledError:
                record_dispatch_cancellation(context)
                raise
            except BaseException as error:
                capture_dispatch_error(error, context)
                if lease is None or not lease.lost.is_set():
                    with suppress(Exception):
                        await client.call(
                            "finish_dispatch",
                            FailedFinishDispatchRequest(
                                ref=ref,
                                succeeded=False,
                                error_message=f"Executor dispatch failed: {error}",
                                error_type=type(error).__name__,
                                category=classify_failure(error),
                            ),
                            FinishDispatchResponse,
                        )
                raise
            finally:
                if lease is not None:
                    lease.close()
    finally:
        await client.close()


def _flush_observability() -> None:
    try:
        logfire.force_flush(timeout_millis=3000)
    except Exception as error:
        logger.warning("Failed to flush executor traces: %s: %s", type(error).__name__, error)
    try:
        sentry_sdk.flush(timeout=3)
    except Exception as error:
        logger.warning("Failed to flush executor Sentry events: %s: %s", type(error).__name__, error)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dispatch-id", required=True, type=UUID, help="Dispatch ID to claim and execute.")
    dispatch_id = parser.parse_args().dispatch_id
    configure_logging()
    configure_observability("valkyrie-executor", environment=ENVIRONMENT)
    channel = DeadlineChannel(int(os.environ["TRACKER_WATCHDOG_FD"]))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown_started = asyncio.Event()
    task = loop.create_task(_run_dispatch(dispatch_id, channel, shutdown_started))

    def request_shutdown() -> None:
        if not shutdown_started.is_set():
            shutdown_started.set()
            task.cancel()

    loop.add_signal_handler(signal.SIGTERM, request_shutdown)
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    except Exception as error:
        capture_runner_failure(error, str(dispatch_id))
        raise
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        loop.close()
        channel.close()
        _flush_observability()


if __name__ == "__main__":
    main()
