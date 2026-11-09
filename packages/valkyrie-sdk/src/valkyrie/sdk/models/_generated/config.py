from __future__ import annotations
from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field
from pathlib import Path
from valkyrie.sdk.models._validation import LocalResourcesBase


class AWSResources(BaseModel):
    model_config = ConfigDict(
        frozen=True,
    )
    region: Annotated[str, Field(min_length=1)]
    s3_bucket: Annotated[str, Field(min_length=1)]
    log_group: Annotated[str, Field(min_length=1)]
    log_retention_days: Annotated[int, Field(gt=0)]


class LocalResources(LocalResourcesBase):
    model_config = ConfigDict(
        frozen=True,
    )
    data_root: Path
    secrets_file: Path | None = None
    secrets_manager_region: str | None = None
