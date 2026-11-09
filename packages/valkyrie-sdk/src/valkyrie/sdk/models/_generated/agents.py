from __future__ import annotations
from valkyrie.sdk.models._base import ResponseModel
from pydantic import BaseModel, Field
from valkyrie.sdk.models._validation import AgentContractRequestBase, OutputArtifactBase
from typing import Annotated, Literal


class AgentDownloadURLResponse(ResponseModel):
    """Temporary URL for downloading an uploaded agent archive."""

    name: str
    download_url: str
    expires_in: int


class AgentEntry(ResponseModel):
    """One uploaded agent visible to the configured tenant."""

    name: str
    last_modified: str | None = None


class AgentTurns(BaseModel):
    continue_cmd: str | None = None
    interrupt_grace_seconds: float | None = None
    container_name: str | None = None


class AgentsResponse(ResponseModel):
    """Uploaded agents visible to the configured tenant."""

    agents: list[AgentEntry]


class OutputArtifact(OutputArtifactBase):
    """One artifact copied from a sandbox after an agent run."""

    path: str
    source: str | None = None
    required: bool = True


class AgentContractRequest(AgentContractRequestBase):
    """Agent definition submitted when starting a run."""

    name: str
    model: str | None = None
    install_cmd: str = ""
    run_cmd: str = ""
    turns: AgentTurns | None = None
    final_output: str | None = None
    output_artifacts: Annotated[list[str | OutputArtifact], Field(validate_default=True)] = []
    egress_allowlist: list[str] = []
    install_egress: Literal["*"] | list[str] | None = None
    allow_provider_tools: bool = False
    secrets: dict[str, str] = {}
    kwargs: dict[str, str] = {}
    inference_settings_attested: bool = False
