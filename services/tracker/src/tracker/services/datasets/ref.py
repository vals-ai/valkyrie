from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class HostedDataset(BaseModel):
    kind: Literal["hosted"] = "hosted"
    benchmark: str
    dataset: str
    ref: str = "latest"
    dataset_id: UUID | None = None
    revision: int | None = None


class LocalDataset(BaseModel):
    kind: Literal["folder"] = "folder"
    path: str


DatasetRef = Annotated[HostedDataset | LocalDataset, Field(discriminator="kind")]
