import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import ANY

import pytest
from sqlmodel import Session

from tests.factories import make_benchmark, make_error_result
from tests.utils import TEST_ORG_ID
from tracker.database.models import BenchmarkStatus, FailureCategory, Org, Task, TaskStatus
from tracker.local.storage import FilesystemObjectStore
from tracker.services.runs.attempts import PriorAttempt, record_prior_attempts
from tracker.services.runs.retry import prepare_retry_state
from tracker.services.tasks.attempts import keep_replaced_attempts
from tracker.services.tasks.execute import task_attempt_id

_STARTED_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


@pytest.mark.description(
    "A retry records the replaced attempt's error, and the claim copies that attempt's files, late uploads included, but not the retried attempt's."
)
async def test_retry_keeps_replaced_attempt_artifacts_and_error(database_session: Session, tmp_path: Path) -> None:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    benchmark = make_benchmark(status=BenchmarkStatus.STOPPED)
    failed = Task(
        org_id=TEST_ORG_ID,
        task_id="task_0",
        benchmark=benchmark.id,
        status=TaskStatus.ERROR,
        started_at=_STARTED_AT,
    )
    stopped = Task(org_id=TEST_ORG_ID, task_id="task_1", benchmark=benchmark.id, status=TaskStatus.STOPPED)
    database_session.add_all([benchmark, failed, stopped])
    database_session.flush()
    database_session.add_all(
        [
            make_error_result(failed, "earlier attempt error", _STARTED_AT - timedelta(minutes=1)),
            make_error_result(
                failed,
                "setup retried",
                _STARTED_AT + timedelta(minutes=1),
                retry_scheduled=True,
                category=FailureCategory.INFRASTRUCTURE,
            ),
            make_error_result(
                failed, "agent crashed", _STARTED_AT + timedelta(minutes=4), category=FailureCategory.AGENT
            ),
        ]
    )
    database_session.commit()
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    result_key = f"benchmarks/{benchmark.id}/task_0/vals_format/result.json"
    await store.put_bytes(result_key, b'{"attempt": 1}')
    stale_key = f"benchmarks/{benchmark.id}/task_0/trajectory.json"
    await store.put_bytes(stale_key, b"older attempt")
    stale_time = (_STARTED_AT - timedelta(hours=1)).timestamp()
    os.utime(store.root / stale_key, (stale_time, stale_time))

    assert failed.finished_at is not None
    finished_at = failed.finished_at.isoformat()
    state = prepare_retry_state(benchmark, database_session, True, ["task_0", "task_1"], org)
    await record_prior_attempts(store, str(benchmark.id), state.attempts)
    kept = f"benchmarks/attempts/{benchmark.id}/task_0/{task_attempt_id(failed.id, failed.started_at)}/"
    assert [stored.key async for stored in store.list_objects(f"benchmarks/attempts/{benchmark.id}/task_0/")] == [
        kept + "attempt.json"
    ]
    late_key = f"benchmarks/{benchmark.id}/task_0/late.json"
    await store.put_bytes(late_key, b"written after the retry")
    late_time = (datetime.now(UTC) + timedelta(seconds=5)).timestamp()
    os.utime(store.root / late_key, (late_time, late_time))

    claimed_at = datetime.now(UTC) + timedelta(minutes=1)
    await keep_replaced_attempts(store, str(benchmark.id), "task_0", claimed_at)
    new_key = f"benchmarks/{benchmark.id}/task_0/new.json"
    new_time = (claimed_at + timedelta(seconds=30)).timestamp()
    for key, content in [(result_key, b'{"attempt": 2}'), (new_key, b"retried attempt")]:
        await store.put_bytes(key, content)
        os.utime(store.root / key, (new_time, new_time))
    await keep_replaced_attempts(store, str(benchmark.id), "task_0", claimed_at + timedelta(minutes=1))

    assert await store.get_bytes(kept + "vals_format/result.json") == b'{"attempt": 1}'
    record = json.loads(await store.get_bytes(kept + "attempt.json"))
    assert record == {
        "task_id": "task_0",
        "attempt_id": task_attempt_id(failed.id, failed.started_at),
        "status": "ERROR",
        "started_at": _STARTED_AT.isoformat(),
        "finished_at": finished_at,
        "replaced_at": ANY,
        "kept_until": claimed_at.isoformat(),
        "error": "agent crashed",
        "failure_category": "agent",
    }
    assert [stored.key async for stored in store.list_objects(f"benchmarks/attempts/{benchmark.id}/task_0/")] == [
        kept + "attempt.json",
        kept + "late.json",
        kept + "vals_format/result.json",
    ]


@pytest.mark.description("A failed or stopped attempt that wrote no artifacts still keeps its error after a retry.")
@pytest.mark.parametrize("status", [TaskStatus.ERROR, TaskStatus.STOPPED])
async def test_retry_keeps_error_of_attempt_without_artifacts(
    status: TaskStatus, database_session: Session, tmp_path: Path
) -> None:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    benchmark = make_benchmark(status=BenchmarkStatus.STOPPED)
    failed = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark.id, status=status, started_at=_STARTED_AT)
    database_session.add_all([benchmark, failed])
    database_session.flush()
    database_session.add(make_error_result(failed, "sandbox lost", _STARTED_AT + timedelta(seconds=30)))
    database_session.commit()
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")

    state = prepare_retry_state(benchmark, database_session, True, [], org)
    await record_prior_attempts(store, str(benchmark.id), state.attempts)

    record_key = (
        f"benchmarks/attempts/{benchmark.id}/task_0/{task_attempt_id(failed.id, failed.started_at)}/attempt.json"
    )
    record = json.loads(await store.get_bytes(record_key))
    assert (record["status"], record["error"], record["failure_category"]) == (status.value, "sandbox lost", None)


@pytest.mark.description("A resume keeps no attempt, since the stopped tasks it restarts were not retried.")
async def test_resume_keeps_no_attempt(database_session: Session) -> None:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    benchmark = make_benchmark(status=BenchmarkStatus.STOPPED)
    stopped = Task(
        org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark.id, status=TaskStatus.STOPPED, started_at=_STARTED_AT
    )
    database_session.add_all([benchmark, stopped])
    database_session.commit()

    state = prepare_retry_state(benchmark, database_session, False, [], org)

    assert (state.task_ids, state.attempts) == (("task_0",), ())


@pytest.mark.description("Recording an already recorded attempt again keeps the claim's copy window.")
async def test_rerecorded_attempt_keeps_its_window(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    attempt = PriorAttempt(
        task_id="task_0",
        attempt_id="first",
        status=TaskStatus.ERROR,
        started_at=_STARTED_AT,
        finished_at=None,
        error=None,
        category=None,
    )
    claimed_at = _STARTED_AT + timedelta(minutes=5)
    assert await record_prior_attempts(store, "run", [attempt]) == ["benchmarks/attempts/run/task_0/first/attempt.json"]
    await keep_replaced_attempts(store, "run", "task_0", claimed_at)

    assert await record_prior_attempts(store, "run", [attempt]) == []

    record = json.loads(await store.get_bytes("benchmarks/attempts/run/task_0/first/attempt.json"))
    assert record["kept_until"] == claimed_at.isoformat()


@pytest.mark.description("A task with no replaced attempt starts without copying or listing its artifacts.")
async def test_first_attempt_copies_nothing(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    await store.put_bytes("benchmarks/run/task_0/output.txt", b"current")

    await keep_replaced_attempts(store, "run", "task_0", datetime.now(UTC))

    assert [stored.key async for stored in store.list_objects("benchmarks/attempts/")] == []


@pytest.mark.description(
    "A task retried again before it starts still keeps the first attempt's files under that attempt."
)
async def test_second_retry_keeps_each_replaced_attempt(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    root = "benchmarks/attempts/run/task_0/"
    for attempt_id, started in [("first", 0), ("second", 10)]:
        record = {
            "started_at": (_STARTED_AT + timedelta(minutes=started)).isoformat(),
        }
        await store.put_bytes(f"{root}{attempt_id}/attempt.json", json.dumps(record).encode())
    for name, minute in [("first.txt", 5), ("second.txt", 15)]:
        key = f"benchmarks/run/task_0/{name}"
        await store.put_bytes(key, name.encode())
        written = (_STARTED_AT + timedelta(minutes=minute)).timestamp()
        os.utime(store.root / key, (written, written))

    await keep_replaced_attempts(store, "run", "task_0", _STARTED_AT + timedelta(minutes=30))

    assert [stored.key async for stored in store.list_objects(root)] == [
        f"{root}first/attempt.json",
        f"{root}first/first.txt",
        f"{root}second/attempt.json",
        f"{root}second/second.txt",
    ]
    assert await store.get_bytes(f"{root}first/first.txt") == b"first.txt"
