from typing import TypeVar

from fastapi import HTTPException
from sqlmodel import SQLModel

from tracker.database.models import Org

T = TypeVar("T", bound=SQLModel)


def assert_org(row: T | None, org: Org) -> T:
    if row is None or row.org_id != org.id:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Not found")
    return row
