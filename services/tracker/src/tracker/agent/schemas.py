import re
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ValidationError, create_model, field_validator, model_validator

from tracker.egress import EgressPolicy
from tracker.exceptions import ContractValidationError
from tracker.types import AgentTurns, OutputArtifact, OutputArtifactSpec


__all__ = [
    "AgentConfig",
    "AgentContract",
    "OutputArtifact",
    "OutputArtifactSpec",
    "Parameter",
    "validate_agent_name",
]


_AGENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def validate_agent_name(name: str) -> str:
    if not name or not _AGENT_NAME_PATTERN.fullmatch(name) or name in {".", ".."}:
        raise ValueError(f"Invalid agent name {name!r}: use only letters, digits, dots, dashes, or underscores.")
    return name


class Defaults(str, Enum):
    PROBLEM_STATEMENT_PATH = "problem_statement_path"
    TASK_ID = "task_id"


class AgentConfig(BaseModel):
    model: str | None = None

    kwargs: dict[str, Any] = {}


ParameterType = Literal["float", "bool", "dict", "int", "str"]


class Parameter(BaseModel):
    type: ParameterType
    required: bool
    default: float | bool | dict[str, Any] | int | str | None = None
    choices: list[str] | None = None
    description: str | None = None


class AgentContract(BaseModel):
    name: str
    install_cmd: str
    final_output: Path | None = None
    output_artifacts: list[OutputArtifactSpec] = []
    egress_allowlist: list[str] = []
    install_egress: EgressPolicy | None = None
    allow_provider_tools: bool = False
    isolate_agent_uid: bool = False
    secrets: dict[str, str] = {}
    ingest_lambda: str | None = None
    defaults: dict[str, Parameter] = {}
    kwargs: dict[str, Parameter] = {}
    run_cmd: str
    turns: AgentTurns | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        if isinstance(value, dict) and "egress" in value:
            raise ValueError("'egress' is no longer supported; use 'install_egress'")
        return value

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return validate_agent_name(v)

    @field_validator("run_cmd")
    @classmethod
    def validate_run_cmd(cls, v: str) -> str:
        if f"{{{Defaults.PROBLEM_STATEMENT_PATH.value}}}" not in v:
            raise ValueError(f"run_cmd must contain {{{Defaults.PROBLEM_STATEMENT_PATH.value}}}")

        return v

    def validate_kwargs(self, schema: dict[str, Parameter], values: dict[str, Any]) -> dict[str, Any]:
        if not schema:
            return {}

        type_map = {"float": float, "int": int, "bool": bool, "str": str, "dict": dict}

        fields: dict[Any, Any] = {}
        for name, param in schema.items():
            if param.choices:
                python_type = Literal[tuple(param.choices)]  # type: ignore[valid-type]
            else:
                python_type = type_map[param.type]
            default = param.default if not param.required else ...
            fields[name] = (python_type, default)

        DynamicModel = create_model("KwargsModel", **fields)

        try:
            validated = DynamicModel(**values)
        except ValidationError as e:
            raise ContractValidationError(e) from e

        return validated.model_dump(exclude_none=True)
