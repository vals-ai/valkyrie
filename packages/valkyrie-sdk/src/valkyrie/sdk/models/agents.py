"""Agent contract models used by SDK run requests."""

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import AfterValidator, BaseModel, Field, ValidationError, create_model, field_validator, model_validator

from valkyrie.sdk.agent_bundle import validate_agent_name
from valkyrie.sdk.errors import ValkyrieContractError
from valkyrie.sdk.models._generated.agents import (
    AgentContractRequest,
    AgentDownloadURLResponse,
    AgentEntry,
    AgentsResponse,
    AgentTurns,
    OutputArtifact,
)
from valkyrie.sdk.models._validation import (
    MAX_OUTPUT_ARTIFACT_COUNT,
    normalize_output_artifacts,
    reject_obsolete_egress,
)

__all__ = [
    "MAX_OUTPUT_ARTIFACT_COUNT",
    "AgentContract",
    "AgentContractRequest",
    "AgentDownloadURLResponse",
    "AgentEntry",
    "AgentTurns",
    "AgentsResponse",
    "OutputArtifact",
    "OutputArtifactSpec",
    "Parameter",
]

OutputArtifactSpec = str | OutputArtifact
OutputArtifacts = Annotated[list[OutputArtifactSpec], AfterValidator(normalize_output_artifacts)]
EgressPolicy = Literal["*"] | list[str]


_PARAMETER_TYPES: dict[str, type] = {"float": float, "int": int, "bool": bool, "str": str, "dict": dict}


class Parameter(BaseModel):
    """Represents one parameter an agent accepts, declared under `defaults` or `kwargs` in its contract."""

    type: Literal["float", "bool", "dict", "int", "str"] = Field(
        description='Type a passed value is converted to, so a temperature passed as "0.5" becomes the float 0.5.'
    )
    required: bool = Field(description="Whether a run must pass a value. A required parameter ignores its default.")
    default: float | bool | dict[str, Any] | int | str | None = Field(
        default=None, description="Value used when a run doesn't pass one."
    )
    choices: list[str] | None = Field(
        default=None, description="When set, the value must be one of these strings, and type isn't applied."
    )
    description: str | None = None


class AgentContract(BaseModel):
    """Represents an agent's `contract.yaml`, which says how to install and run the agent and which parameters it accepts.

    Use `from_directory()` to load and check one before publishing, and `validate_kwargs()` to check a
    run's `-k` values. Tracker checks the published contract again when a run starts.
    """

    name: str
    install_cmd: str
    final_output: Path | None = None
    output_artifacts: OutputArtifacts = Field(default_factory=list)
    egress_allowlist: list[str] = Field(default_factory=list)
    install_egress: EgressPolicy | None = None
    allow_provider_tools: bool = False
    secrets: dict[str, str] = Field(default_factory=dict)
    ingest_lambda: str | None = None
    defaults: dict[str, Parameter] = Field(
        default_factory=dict, description="Parameters Valkyrie fills itself, such as the model from --model."
    )
    kwargs: dict[str, Parameter] = Field(
        default_factory=dict,
        description="The agent's own parameters, passed with -k. A name here overrides the same name in defaults.",
    )
    run_cmd: str
    turns: AgentTurns | None = None

    @classmethod
    def from_directory(cls, agent_path: Path) -> "AgentContract":
        """Read `contract.yaml`, or else `contract.yml`, from an agent directory and validate it.

        Raises ValkyrieContractError when neither file exists, the YAML doesn't parse to a mapping,
        or a field is invalid (one line per field, prefixed with the contract's path).
        """
        contract_path = next(
            (agent_path / name for name in ("contract.yaml", "contract.yml") if (agent_path / name).is_file()),
            None,
        )
        if contract_path is None:
            raise ValkyrieContractError(f"No contract.yaml or contract.yml in {agent_path}")

        display_name = "/".join(contract_path.parts[-2:])
        try:
            data: object = yaml.safe_load(contract_path.read_text())
        except yaml.YAMLError as error:
            raise ValkyrieContractError(f"Failed to parse YAML contract from `{display_name}`:\n{error}") from error
        if not isinstance(data, dict):
            raise ValkyrieContractError(f"Invalid contract `{display_name}`: expected a mapping")

        try:
            return cls.model_validate(data)
        except ValidationError as error:
            raise ValkyrieContractError.from_validation_error(
                error, context=f"Invalid contract `{display_name}`"
            ) from error

    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        return reject_obsolete_egress(value)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return validate_agent_name(value)

    @field_validator("run_cmd")
    @classmethod
    def validate_run_cmd(cls, value: str) -> str:
        if "{problem_statement_path}" not in value:
            raise ValueError("run_cmd must contain {problem_statement_path}")
        return value

    def validate_kwargs(self, kwargs: Mapping[str, Any], *, model: str | None = None) -> dict[str, Any]:
        """Validate a run's kwargs against the contract and return them with defaults filled in.

        `model` is used for a `model` parameter the kwargs leave out, the way `--model` is at run
        start. Raises ValkyrieContractError listing each missing, mistyped or disallowed value.
        """
        schema = {**self.defaults, **self.kwargs}
        if not schema:
            return {}

        values = dict(kwargs)
        if model and "model" not in values:
            values["model"] = model

        fields: dict[str, Any] = {
            name: (
                Literal[tuple(parameter.choices)] if parameter.choices else _PARAMETER_TYPES[parameter.type],
                ... if parameter.required else parameter.default,
            )
            for name, parameter in schema.items()
        }
        try:
            validated = create_model("KwargsModel", **fields)(**values)
        except ValidationError as error:
            raise ValkyrieContractError.from_validation_error(error) from error

        return validated.model_dump(exclude_none=True)
