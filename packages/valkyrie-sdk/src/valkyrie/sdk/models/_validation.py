from pathlib import Path, PurePosixPath
from typing import Any, Protocol, Self
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from valkyrie.sdk.models._base import ResponseModel

MAX_OUTPUT_ARTIFACT_COUNT = 10


class _OutputArtifactLike(Protocol):
    path: str

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False) -> Self: ...


def _glob_root(source: str) -> str | None:
    first_glob = next((index for index, char in enumerate(source) if char in "*?["), None)
    if first_glob is None:
        return None
    return source[:first_glob].rsplit("/", 1)[0] or "/"


def normalize_output_artifacts[T: _OutputArtifactLike](value: list[str | T]) -> list[str | T]:
    if len(value) > MAX_OUTPUT_ARTIFACT_COUNT:
        raise ValueError(f"output_artifacts cannot contain more than {MAX_OUTPUT_ARTIFACT_COUNT} entries")

    normalized_artifacts: list[str | T] = []
    normalized_paths: set[str] = set()
    for artifact in value:
        artifact_path = artifact if isinstance(artifact, str) else artifact.path
        path = PurePosixPath(artifact_path)
        if path.is_absolute():
            raise ValueError("output_artifacts paths must be relative paths")
        if not path.parts or ".." in path.parts or "." in path.parts:
            raise ValueError("output_artifacts paths cannot contain empty, '.', or '..' path parts")
        normalized_path = str(path)
        if normalized_path in normalized_paths:
            raise ValueError(f"output_artifacts cannot contain duplicate paths: {normalized_path}")
        normalized_paths.add(normalized_path)
        normalized_artifacts.append(
            normalized_path if isinstance(artifact, str) else artifact.model_copy(update={"path": normalized_path})
        )
    return normalized_artifacts


def default_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {"environment": "aws", **value}
    return value


def reject_obsolete_egress(value: Any) -> Any:
    if isinstance(value, dict) and "egress" in value:
        raise ValueError("'egress' is no longer supported; use 'install_egress'")
    return value


class OutputArtifactBase(BaseModel):
    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if data.get("required", True):
            data.pop("required", None)
        return data

    @field_validator("source", check_fields=False)
    @classmethod
    def validate_source(cls, value: str | None) -> str | None:
        if not value:
            return None
        if not value.startswith("/"):
            raise ValueError("output_artifacts source paths must be absolute sandbox paths")

        path = PurePosixPath(value)
        if not path.parts or ".." in path.parts or "." in path.parts:
            raise ValueError("output_artifacts source paths cannot contain empty, '.', or '..' path parts")
        if _glob_root(value) == "/":
            raise ValueError("output_artifacts glob sources must include a non-root directory prefix")
        return value


class AgentContractRequestBase(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        return reject_obsolete_egress(value)

    @field_validator("output_artifacts", check_fields=False)
    @classmethod
    def normalize_artifacts(cls, value: list[Any]) -> list[Any]:
        return normalize_output_artifacts(value)


class StartBenchmarkRequestBase(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)


class LocalResourcesBase(BaseModel):
    @field_validator("data_root", "secrets_file", check_fields=False)
    @classmethod
    def validate_absolute_path(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Local resource paths must be absolute")
        return value


class BenchmarkServiceEntryBase(ResponseModel):
    @field_validator("url", check_fields=False)
    @classmethod
    def normalize_url(cls, value: str) -> str:
        return value.rstrip("/")


class StoredArgumentsBase(ResponseModel):
    @field_validator("benchmark_arguments", mode="before", check_fields=False)
    @classmethod
    def default_environment(cls, value: Any) -> Any:
        return default_environment(value)


class FinalEvaluationBase(ResponseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
