import contextvars
import logging
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import trace

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")
benchmark_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("benchmark_id", default="")
task_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="")
executor_dispatch_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("executor_dispatch_id", default="")
attempt_started_at_var: contextvars.ContextVar[str] = contextvars.ContextVar("attempt_started_at", default="")
sandbox_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("sandbox_id", default="")
sandbox_provider_var: contextvars.ContextVar[str] = contextvars.ContextVar("sandbox_provider", default="")


def get_context_tags() -> dict[str, str]:
    return {
        "request_id": request_id_var.get(""),
        "benchmark_id": benchmark_id_var.get(""),
        "task_id": task_id_var.get(""),
        "executor_dispatch_id": executor_dispatch_id_var.get(""),
        "attempt_started_at": attempt_started_at_var.get(""),
    }


def get_sandbox_fields() -> dict[str, str]:
    return {
        "sandbox_id": sandbox_id_var.get(""),
        "sandbox_provider": sandbox_provider_var.get(""),
    }


def get_trace_fields() -> dict[str, str]:
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return {"trace_id": "", "span_id": ""}
    return {
        "trace_id": trace.format_trace_id(span_context.trace_id),
        "span_id": trace.format_span_id(span_context.span_id),
    }


@contextmanager
def sandbox_provider_scope(provider: str) -> Iterator[None]:
    token = sandbox_provider_var.set(provider)
    try:
        yield
    finally:
        sandbox_provider_var.reset(token)


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in (get_context_tags() | get_sandbox_fields() | get_trace_fields()).items():
            if not getattr(record, key, None):
                setattr(record, key, value)
        return True
