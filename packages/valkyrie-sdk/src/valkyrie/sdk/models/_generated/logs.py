from __future__ import annotations
from datetime import datetime
from valkyrie.sdk.models._base import ResponseModel


class LogEvent(ResponseModel):
    """One log message returned by the tracker."""

    timestamp: datetime
    message: str
    task_id: str | None = None
    ingestion_time: datetime | None = None
    event_id: str | None = None


class LogPage(ResponseModel):
    """A bounded page of log events and its continuation cursor."""

    events: list[LogEvent]
    next_cursor: str | None = None
