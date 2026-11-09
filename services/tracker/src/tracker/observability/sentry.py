from collections.abc import Iterator
from contextlib import contextmanager
import logging
import os
from typing import Any, cast

import sentry_sdk
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.otlp import OTLPIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sentry_sdk.types import Event, Hint, Log

from tracker.exceptions import SSLConnectionError
from tracker.logging import task_id_var
from tracker.logging.context import (
    attempt_started_at_var,
    get_context_tags,
    get_sandbox_fields,
    get_trace_fields,
    sandbox_id_var,
)

logger = logging.getLogger(__name__)

_REPORTED_HTTP_STATUS_CODES = {500}


@contextmanager
def task_scope(task_id: str, *, attempt_started_at: str) -> Iterator[None]:
    with sentry_sdk.isolation_scope():
        token = task_id_var.set(task_id)
        attempt_token = attempt_started_at_var.set(attempt_started_at)
        try:
            yield
        finally:
            attempt_started_at_var.reset(attempt_token)
            task_id_var.reset(token)


def _before_send(
    event: Event,
    hint: Hint,
) -> Event | None:
    exc_info = hint.get("exc_info")
    if exc_info:
        exc = exc_info[1]
        if isinstance(exc, SSLConnectionError):
            event["fingerprint"] = ["{{ default }}", "SSLConnectionError"]

    tags = event.setdefault("tags", {})
    for key, value in get_context_tags().items():
        if value:
            tags[key] = value
    return event


def _before_send_transaction(event: Event, _hint: Hint) -> Event | None:
    attributes = event.get("contexts", {}).get("otel", {}).get("attributes", {})
    for key in get_context_tags():
        if key in attributes:
            event.setdefault("tags", {})[key] = attributes[key]
    return event


def _apply_current_otel_trace_context(telemetry: dict[str, Any]) -> None:
    trace_fields = get_trace_fields()
    if trace_fields["trace_id"]:
        telemetry.update(trace_fields)


def _before_send_log(log: Log, _hint: Hint) -> Log | None:
    attributes = log.setdefault("attributes", {})
    for key, value in (get_context_tags() | get_sandbox_fields()).items():
        if value:
            attributes[key] = value
    _apply_current_otel_trace_context(cast(dict[str, Any], log))
    return log


def init_sentry(service_name: str, environment: str) -> None:
    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.environ.get("SENTRY_ENVIRONMENT", environment),
            release=os.environ.get("SENTRY_RELEASE", ""),
            server_name=service_name,
            traces_sample_rate=1.0,
            instrumenter=INSTRUMENTER.OTEL,
            enable_logs=True,
            send_default_pii=False,
            max_request_body_size="never",
            include_local_variables=False,
            before_send=_before_send,
            before_send_transaction=_before_send_transaction,
            before_send_log=_before_send_log,
            integrations=[
                LoggingIntegration(
                    level=logging.INFO,
                    event_level=None,
                    sentry_logs_level=logging.INFO,
                ),
                OTLPIntegration(
                    setup_otlp_traces_exporter=False,
                    setup_propagator=False,
                ),
                StarletteIntegration(failed_request_status_codes=_REPORTED_HTTP_STATUS_CODES),
                FastApiIntegration(failed_request_status_codes=_REPORTED_HTTP_STATUS_CODES),
            ],
        )
    except Exception as e:
        logger.warning("Failed to initialize Sentry: %s: %s", type(e).__name__, e)


def capture_exception(error: BaseException) -> None:
    try:
        sentry_sdk.capture_exception(error)
    except Exception as telemetry_error:
        logger.warning("Failed to capture exception: %s: %s", type(telemetry_error).__name__, telemetry_error)


def clear_sandbox_context() -> None:
    sandbox_id_var.set("")
    try:
        scope = sentry_sdk.get_isolation_scope()
        scope.remove_tag("sandbox_id")
        scope.remove_tag("sandbox_name")
        scope.remove_context("sandbox")
    except Exception as e:
        logger.warning("clear_sandbox_context failed: %s: %s", type(e).__name__, e)


def set_sandbox_context(sandbox: Any, *, image: str | None = None) -> None:
    sandbox_id_var.set(sandbox.id)
    try:
        sentry_sdk.set_tag("sandbox_id", sandbox.id)
        sentry_sdk.set_tag("sandbox_name", sandbox.name)

        state = getattr(sandbox, "state", None)
        context = {
            "id": sandbox.id,
            "name": sandbox.name,
            "state": str(state) if state is not None else None,
        }
        if image is not None:
            context["image"] = image

        sentry_sdk.set_context("sandbox", context)
    except Exception as e:
        logger.warning("set_sandbox_context failed: %s: %s", type(e).__name__, e)
