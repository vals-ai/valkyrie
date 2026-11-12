import pytest
from fastapi import FastAPI
from sqlmodel import Session
from tracker.auth import get_current_org
from tracker.database.session import get_session


@pytest.mark.description(
    "Each local tracker request reads through its own database session, so concurrent CLI requests never share one."
)
def test_local_tracker_requests_use_their_own_session(local_tracker_app: FastAPI, database_session: Session) -> None:
    overrides = local_tracker_app.dependency_overrides
    requests = [overrides[get_session]() for _ in range(2)]
    sessions = [next(request) for request in requests]

    assert sessions[0] is not sessions[1]
    assert database_session not in sessions
    org = overrides[get_current_org](sessions[0])
    assert org in sessions[0]
    assert org not in database_session

    for request in requests:
        request.close()
