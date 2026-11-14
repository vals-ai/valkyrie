import asyncio
import threading
from typing import cast
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from tracker.api.projects import PreliminaryProjectCache
from tracker.clients.projects import ProjectServiceUnavailableError
from tracker.database.models import Project, SpendType
from tracker.repositories.project import ProjectRepository


class TestPreliminaryProjectCache:
    @pytest.mark.description("Concurrent starts share one project lookup and receive independent project records.")
    async def test_concurrent_lookups(self) -> None:
        cache = PreliminaryProjectCache()
        org_id = uuid4()
        project = Project(org_id=org_id, handle="smoke", name="Smoke", spend_type=SpendType.DEVELOPMENT)
        entered = threading.Event()
        release = threading.Event()

        def lookup(_org_id: object, _handle: str) -> Project:
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Lookup was not released")
            return project

        repository = Mock(spec=ProjectRepository)
        repository.find.side_effect = lookup
        tasks = [
            asyncio.create_task(cache.get(cast(ProjectRepository, repository), org_id, "smoke")) for _ in range(20)
        ]
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            await asyncio.sleep(0)
            assert repository.find.call_count == 1
        finally:
            release.set()
        results = await asyncio.gather(*tasks)

        assert all(result.id == project.id for result in results)
        results[0].name = "Changed"
        assert results[1].name == "Smoke"
        assert repository.find.call_count == 1

    @pytest.mark.description("Completed lookups are read again and distinct organizations and handles remain separate.")
    async def test_completed_lookups_are_not_cached(self) -> None:
        cache = PreliminaryProjectCache()
        org_id = uuid4()
        repository = Mock(spec=ProjectRepository)

        def lookup(org: UUID, handle: str) -> Project:
            return Project(org_id=org, handle=handle, name=handle, spend_type=SpendType.DEVELOPMENT)

        repository.find.side_effect = lookup
        typed = cast(ProjectRepository, repository)

        first = await cache.get(typed, org_id, "smoke")
        assert (await cache.get(typed, org_id, "smoke")).id != first.id
        assert (await cache.get(typed, uuid4(), "smoke")).id != first.id
        assert (await cache.get(typed, org_id, "other")).id != first.id
        assert repository.find.call_count == 4

    @pytest.mark.description("Missing projects and service failures reach every waiter and a later lookup can recover.")
    @pytest.mark.parametrize("failure", [None, ProjectServiceUnavailableError("unavailable")])
    async def test_errors_propagate_to_all_waiters(self, failure: Exception | None) -> None:
        cache = PreliminaryProjectCache()
        org_id = uuid4()
        project = Project(org_id=org_id, handle="smoke", name="Smoke", spend_type=SpendType.DEVELOPMENT)
        repository = Mock(spec=ProjectRepository)
        entered = threading.Event()
        release = threading.Event()

        def lookup(_org_id: UUID, _handle: str) -> Project | None:
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Lookup was not released")
            if failure is not None:
                raise failure
            return None

        repository.find.side_effect = lookup
        repository.list_all.return_value = []
        tasks = [
            asyncio.create_task(cache.get(cast(ProjectRepository, repository), org_id, "smoke")) for _ in range(10)
        ]
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            await asyncio.sleep(0)
        finally:
            release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        error_type = HTTPException if failure is None else ProjectServiceUnavailableError
        assert all(isinstance(result, error_type) for result in results)
        assert repository.find.call_count == 1

        repository.find.side_effect = None
        repository.find.return_value = project
        assert (await cache.get(cast(ProjectRepository, repository), org_id, "smoke")).id == project.id
        assert repository.find.call_count == 2

    @pytest.mark.description("Cancelling one start leaves the shared project lookup available to another start.")
    async def test_cancelled_waiter(self) -> None:
        cache = PreliminaryProjectCache()
        org_id = uuid4()
        project = Project(org_id=org_id, handle="smoke", name="Smoke", spend_type=SpendType.DEVELOPMENT)
        entered = threading.Event()
        release = threading.Event()

        def lookup(_org_id: object, _handle: str) -> Project:
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Lookup was not released")
            return project

        repository = Mock(spec=ProjectRepository)
        repository.find.side_effect = lookup
        first = asyncio.create_task(cache.get(cast(ProjectRepository, repository), org_id, "smoke"))
        second = asyncio.create_task(cache.get(cast(ProjectRepository, repository), org_id, "smoke"))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        finally:
            release.set()

        assert (await second).id == project.id
        assert repository.find.call_count == 1
