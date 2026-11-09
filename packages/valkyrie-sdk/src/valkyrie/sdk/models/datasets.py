"""Benchmarks an organization owns and the datasets that belong to them."""

import re

from valkyrie.sdk.models._generated.datasets import (
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    DatasetResponse,
    DatasetsResponse,
    DatasetTaskDownloadResponse,
    DatasetTaskResponse,
    DatasetTasksResponse,
    ResourceResponse,
)

__all__ = [
    "BenchmarkDefinitionResponse",
    "BenchmarkDefinitionsResponse",
    "DatasetResponse",
    "DatasetTaskDownloadResponse",
    "DatasetTaskResponse",
    "DatasetTasksResponse",
    "DatasetsResponse",
    "ResourceResponse",
    "validate_name",
    "validate_task_id",
]

NAME_PATTERN = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*")
NAME_MAX_LENGTH = 64
TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
TASK_ID_MAX_LENGTH = 128


def validate_name(name: str) -> str:
    if len(name) > NAME_MAX_LENGTH or not NAME_PATTERN.fullmatch(name):
        raise ValueError(
            "must be lowercase letters and digits with single '.', '_' or '-' separators, "
            f"at most {NAME_MAX_LENGTH} characters, e.g. swe-bench"
        )
    return name


def validate_task_id(task_id: str) -> str:
    if len(task_id) > TASK_ID_MAX_LENGTH or not TASK_ID_PATTERN.fullmatch(task_id):
        raise ValueError(
            "must start with a letter or digit and use only letters, digits, '.', '_' and '-', "
            f"at most {TASK_ID_MAX_LENGTH} characters, e.g. django__django-11099"
        )
    return task_id
