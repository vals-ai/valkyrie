from datetime import datetime, timezone
from typing import Annotated

from pydantic import BaseModel, ConfigDict, PlainSerializer


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


def _serialize_utc(value: datetime) -> str:
    return (value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)).isoformat()


UTCDateTime = Annotated[datetime, PlainSerializer(_serialize_utc)]
