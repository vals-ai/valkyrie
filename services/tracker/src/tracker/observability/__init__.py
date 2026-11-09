import logging
import time

from tracker.observability.metrics import TaskOutcome, distribution, gauge, incr, record_task_outcome
from tracker.observability.retry import retry_callback
from tracker.observability.sentry import init_sentry, set_sandbox_context
from tracker.observability.tracing import configure_tracing, error_span

logger = logging.getLogger(__name__)


def configure_observability(service_name: str, environment: str) -> None:
    try:
        init_sentry(service_name, environment=environment)
    except Exception as error:
        logger.warning("Failed to initialize Sentry: %s: %s", type(error).__name__, error)

    try:
        configure_tracing(service_name, environment=environment)
    except Exception as error:
        logger.warning("Failed to initialize tracing: %s: %s", type(error).__name__, error)


def elapsed_ms(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 2)


__all__ = [
    "configure_observability",
    "configure_tracing",
    "distribution",
    "elapsed_ms",
    "error_span",
    "gauge",
    "incr",
    "init_sentry",
    "record_task_outcome",
    "retry_callback",
    "set_sandbox_context",
    "TaskOutcome",
]
