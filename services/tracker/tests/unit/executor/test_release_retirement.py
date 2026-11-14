import pytest
from collections.abc import Callable
from threading import Event
from unittest.mock import Mock

from pytest import MonkeyPatch
from sqlmodel import Session

import main as main_module
from tracker.executor import release_retirement
from tracker.database.models import ExecutorRelease
from tracker.domain import ExecutorReleaseStatus
from tracker.executor.release_control import promote_release, register_release


@pytest.mark.description("The retirement loop retires a release with nothing blocking it.")
def test_retirement_loop_retires_a_blocker_free_release(
    database_session: Session,
    monkeypatch: MonkeyPatch,
) -> None:
    old_release = register_release(
        database_session,
        ExecutorRelease(
            id="old",
            artifact_uri="source:///artifacts/old/src",
            artifact_digest="a" * 64,
            protocol_version="1",
        ),
    )
    register_release(
        database_session,
        ExecutorRelease(
            id="active",
            artifact_uri="source:///artifacts/active/src",
            artifact_digest="b" * 64,
            protocol_version="1",
        ),
    )
    promote_release(database_session, old_release.id)
    promote_release(database_session, "active")
    database_session.commit()
    stop_event = Event()
    reconcile_once = release_retirement.retire_drained_releases_once

    def reconcile_then_stop() -> list[str]:
        try:
            return reconcile_once()
        finally:
            stop_event.set()

    monkeypatch.setattr(release_retirement, "engine", database_session.get_bind())
    monkeypatch.setattr(release_retirement, "retire_drained_releases_once", reconcile_then_stop)

    release_retirement.run_release_retirement_loop(stop_event, interval_seconds=60)

    database_session.expire_all()
    stored_release = database_session.get(ExecutorRelease, old_release.id)
    assert stored_release is not None
    assert stored_release.status == ExecutorReleaseStatus.RETIRED


@pytest.mark.description("The retirement loop tries again after a failed pass.")
def test_retirement_loop_retries_after_a_failed_pass(monkeypatch: MonkeyPatch) -> None:
    stop_event = Event()
    attempts = 0

    def reconcile() -> list[str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database unavailable")
        stop_event.set()
        return []

    logger = Mock()
    monkeypatch.setattr(release_retirement, "retire_drained_releases_once", reconcile)
    monkeypatch.setattr(release_retirement, "_logger", logger)

    release_retirement.run_release_retirement_loop(stop_event, interval_seconds=0)

    assert attempts == 2
    logger.exception.assert_called_once_with(
        "executor_release_retirement_failed",
        extra={"event": "automatic_retirement_failed"},
    )


@pytest.mark.description("Automatic retirement starts and stops its own thread.")
def test_automatic_retirement_starts_and_stops_owned_thread(monkeypatch: MonkeyPatch) -> None:
    events: list[str] = []

    class FakeThread:
        def __init__(
            self,
            *,
            target: Callable[..., None],
            args: tuple[Event],
            kwargs: dict[str, float],
            name: str,
            daemon: bool,
        ) -> None:
            assert target is release_retirement.run_release_retirement_loop
            assert kwargs == {"interval_seconds": 60}
            assert name == "executor-release-retirement"
            assert daemon
            self.stop_event = args[0]

        def start(self) -> None:
            events.append("started")

        def join(self, timeout: float) -> None:
            assert timeout == 5
            assert self.stop_event.is_set()
            events.append("joined")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(release_retirement, "Thread", FakeThread)
    retirement = release_retirement.AutomaticReleaseRetirement(interval_seconds=60)

    retirement.start()
    retirement.stop()

    assert events == ["started", "joined"]


@pytest.mark.description(
    "The Tracker lifespan runs release retirement while the app is up and stops it after recovery stops."
)
async def test_tracker_lifespan_starts_and_stops_automatic_retirement(monkeypatch: MonkeyPatch) -> None:
    events: list[str] = []
    migration = Mock(return_value=None)
    monkeypatch.setattr(main_module, "migrate_projects", migration)

    class FakeAutomaticReleaseRetirement:
        def start(self) -> None:
            migration.assert_called_once_with()
            events.append("started")

        def stop(self) -> None:
            events.append("stopped")

    class FakeAutomaticDispatchRecovery:
        def start(self) -> None:
            events.append("recovery started")

        def stop(self) -> None:
            events.append("recovery stopped")

    class FakeSharedPoolScheduler:
        def start(self) -> None:
            events.append("scheduler started")

        async def stop(self) -> None:
            events.append("scheduler stopped")

    monkeypatch.setattr(main_module, "AutomaticReleaseRetirement", FakeAutomaticReleaseRetirement)

    class FakeAutomaticSandboxCost:
        def start(self) -> None:
            events.append("cost started")

        def stop(self) -> None:
            events.append("cost stopped")

    monkeypatch.setattr(main_module, "AutomaticDispatchRecovery", FakeAutomaticDispatchRecovery)
    monkeypatch.setattr(main_module, "SharedPoolScheduler", FakeSharedPoolScheduler)
    monkeypatch.setattr(main_module, "AutomaticSandboxCost", FakeAutomaticSandboxCost)

    async with main_module.tracker_lifespan(main_module.app):
        assert events == ["started", "recovery started", "cost started", "scheduler started"]

    assert events == [
        "started",
        "recovery started",
        "cost started",
        "scheduler started",
        "scheduler stopped",
        "cost stopped",
        "recovery stopped",
        "stopped",
    ]


@pytest.mark.description("The retirement loop leaves the admission fence alone when no release can retire.")
def test_retirement_skips_the_admission_fence_without_retirable_releases(
    database_session: Session,
    monkeypatch: MonkeyPatch,
) -> None:
    register_release(
        database_session,
        ExecutorRelease(
            id="active",
            artifact_uri="source:///artifacts/active/src",
            artifact_digest="b" * 64,
            protocol_version="1",
        ),
    )
    promote_release(database_session, "active")
    database_session.commit()
    locked = Mock(side_effect=AssertionError("took the admission fence"))
    monkeypatch.setattr(release_retirement, "engine", database_session.get_bind())
    monkeypatch.setattr(release_retirement, "retire_drained_releases", locked)

    assert release_retirement.retire_drained_releases_once() == []
    locked.assert_not_called()
