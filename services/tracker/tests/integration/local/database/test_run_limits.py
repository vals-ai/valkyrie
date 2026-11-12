import json
import threading
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session

import tracker.services.runs.limits as run_limits_module
from tests.factories import make_benchmark
from tracker.database.models import Org
from tracker.exceptions import RunLimitExceededError
from tracker.executor.release_control import get_executor_admission
from tracker.services.runs.limits import OrgRunLimitsCache, ensure_run_capacity


@pytest.mark.description(
    "Under the executor admission lock, a second admission waits for the first to commit, then sees its run and is refused."
)
def test_run_capacity_serializes_admissions_under_admission_lock(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = OrgRunLimitsCache(read=lambda: json.dumps({"default": 1}))
    cache.refresh()
    monkeypatch.setattr(run_limits_module, "run_limits", cache)
    org = Org(id=uuid4(), name=f"run-limit-{uuid4()}")
    postgres_session.add(org)
    postgres_session.commit()
    first, second = make_benchmark(org_id=org.id), make_benchmark(org_id=org.id)
    outcome: list[BaseException | None] = []
    second_started = threading.Event()

    def admit_second() -> None:
        with Session(postgres_engine) as session:
            second_started.set()
            try:
                get_executor_admission(session, for_update=True)
                ensure_run_capacity(session, org.id, second.id)
                session.add(second)
                session.commit()
                outcome.append(None)
            except RunLimitExceededError as exc:
                outcome.append(exc)

    with Session(postgres_engine) as session:
        get_executor_admission(session, for_update=True)
        ensure_run_capacity(session, org.id, first.id)
        session.add(first)
        session.flush()
        contender = threading.Thread(target=admit_second)
        contender.start()
        assert second_started.wait(timeout=5)
        contender.join(timeout=0.5)
        assert contender.is_alive()
        session.commit()
    contender.join(timeout=5)

    assert not contender.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], RunLimitExceededError)
    assert (outcome[0].limit, outcome[0].active) == (1, 1)
