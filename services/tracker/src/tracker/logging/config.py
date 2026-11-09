import logging
import logging.config
import os
from collections.abc import Mapping
from types import MappingProxyType
from typing import ClassVar


class DevFormatter(logging.Formatter):
    COLORS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "DEBUG": "\033[36m",
            "INFO": "\033[32m",
            "WARNING": "\033[33m",
            "ERROR": "\033[31m",
            "CRITICAL": "\033[41m\033[97m",
        }
    )
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        level = f"{color}{record.levelname:<8}{self.RESET}"
        context_parts: list[str] = []
        for field in (
            "event",
            "request_id",
            "benchmark_id",
            "task_id",
            "sandbox_id",
            "sandbox_provider",
            "failure_category",
        ):
            value = getattr(record, field, "")
            if value:
                context_parts.append(f"{field}={value}")
        context = " ".join(context_parts)
        context_str = f" [{context}]" if context else ""
        message = super().format(record)
        return f"{level} {record.name}:{context_str} {message}"


_JSON_FORMATTER = {
    "()": "pythonjsonlogger.json.JsonFormatter",
    "fmt": (
        "%(asctime)s %(levelname)s %(name)s %(module)s %(funcName)s %(message)s "
        "%(request_id)s %(benchmark_id)s %(task_id)s %(trace_id)s %(span_id)s %(sandbox_id)s %(sandbox_provider)s"
    ),
    "rename_fields": {
        "asctime": "timestamp",
        "levelname": "level",
        "name": "logger",
    },
}

_DEVELOPMENT_FORMATTER = {
    "()": "tracker.logging.config.DevFormatter",
    "fmt": "%(message)s",
}

_FORMATTERS = {
    "production": _JSON_FORMATTER,
    "prod": _JSON_FORMATTER,
    "dev": _JSON_FORMATTER,
    "development": _DEVELOPMENT_FORMATTER,
    "release-test": _DEVELOPMENT_FORMATTER,
}


def configure_logging() -> None:
    environment = os.getenv("ENVIRONMENT", "development")
    log_level = os.getenv("LOG_LEVEL", "INFO")

    if environment not in _FORMATTERS:
        raise ValueError(f"Unknown ENVIRONMENT={environment!r}. Must be one of: {', '.join(_FORMATTERS)}")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {
                "context": {
                    "()": "tracker.logging.context.ContextFilter",
                },
                "access_noise": {
                    "()": "tracker.logging.filters.AccessLogNoiseFilter",
                },
                "engineio_noise": {
                    "()": "tracker.logging.filters.EngineioNoiseFilter",
                },
            },
            "formatters": {
                "default": _FORMATTERS[environment],
            },
            "handlers": {
                "console": {
                    "level": "DEBUG",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "default",
                    "filters": ["context"],
                },
            },
            "root": {"handlers": ["console"], "level": log_level},
            "loggers": {
                "tracker": {"handlers": ["console"], "level": log_level, "propagate": False},
                "uvicorn": {"handlers": ["console"], "level": "INFO", "propagate": False},
                "uvicorn.access": {
                    "handlers": ["console"],
                    "level": "INFO",
                    "propagate": False,
                    "filters": ["access_noise"],
                },
                "uvicorn.error": {"handlers": ["console"], "level": "INFO", "propagate": False},
                "httpx": {"level": "WARNING"},
                "httpcore": {"level": "WARNING"},
                "engineio.client": {"filters": ["engineio_noise"]},
            },
        }
    )
