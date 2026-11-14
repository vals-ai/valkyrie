import asyncio
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker import project_gateway
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import DEFAULT_ORG_NAME, Org, Project, SpendType
from tracker.repositories import project as project_repository


async def _second_run_waits(engine: Engine) -> None:
    for _ in range(50):
        with engine.connect() as connection:
            waiting = connection.execute(
                text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted")
            )
            if waiting.scalar_one():
                return
        await asyncio.sleep(0.1)
    pytest.fail("The second run never waited on the Gateway key lock")


@pytest.mark.description("Two first runs of a project collect its Gateway key once and both start on that key.")
def test_first_runs_share_one_gateway_key(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    routes = MockProjectRoutes()
    reads: list[str | None] = []

    def handle(request: httpx.Request) -> httpx.Response:
        response = routes.handle(request)
        if request.method == "GET":
            with postgres_engine.connect() as connection:
                assert connection.execute(
                    text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted")
                ).scalar_one()
            reads.append(response.json()["gateway_key_id"])
        return response

    client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(handle)))
    monkeypatch.setattr(project_repository, "project_service", lambda: client)
    org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    project = Project(
        org_id=TEST_ORG_ID,
        handle="router-bench",
        name="Router Bench",
        spend_type=SpendType.DEVELOPMENT,
        gateway_request_id=uuid4(),
    )
    routes.seed(project)
    key_id = uuid4()
    collects: list[UUID] = []

    async def collect(*_: object) -> UUID:
        collects.append(key_id)
        if len(collects) > 1:
            response = httpx.Response(409, request=httpx.Request("POST", "http://gateway/collect"))
            raise httpx.HTTPStatusError("Key state or revision changed", request=response.request, response=response)
        await _second_run_waits(postgres_engine)
        return key_id

    def start(_: int) -> dict[str, str]:
        return project_gateway._prepare_project_gateway_key(  # pyright: ignore[reportPrivateUsage]
            postgres_engine, project, org, MagicMock()
        )

    monkeypatch.setattr(project_gateway, "_collect_project_gateway_key", collect)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(start, range(2))

    assert first == second
    assert str(key_id) in first["MODEL_GATEWAY_API_KEY"]
    assert collects == [key_id]
    assert routes.rows[str(project.id)]["gateway_key_id"] == str(key_id)
    assert reads == [None, str(key_id)]
    client._client.close()
