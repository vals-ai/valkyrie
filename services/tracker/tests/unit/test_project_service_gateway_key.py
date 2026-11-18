from uuid import uuid4

import httpx
import pytest

from tests.utils import TEST_ORG_ID
from tracker.clients.projects import GatewayKeyConflictError, ProjectServiceClient


def _client(handler: httpx.MockTransport) -> ProjectServiceClient:
    return ProjectServiceClient(httpx.Client(base_url="http://projects", transport=handler))


@pytest.mark.description("A mint returns the project's key ID, and a 409 raises the conflict with its detail.")
def test_mint_returns_the_key_id_or_raises_the_conflict() -> None:
    key_id, project_id = uuid4(), uuid4()
    answers = [
        httpx.Response(200, json={"gateway_key_id": str(key_id)}),
        httpx.Response(409, json={"detail": "Gateway key was collected elsewhere"}),
    ]
    paths: list[str] = []

    def answer(request: httpx.Request) -> httpx.Response:
        paths.append(f"{request.method} {request.url.path}")
        return answers.pop(0)

    service = _client(httpx.MockTransport(answer))

    assert service.mint_gateway_key(TEST_ORG_ID, project_id) == key_id
    with pytest.raises(GatewayKeyConflictError, match="collected elsewhere"):
        _ = service.mint_gateway_key(TEST_ORG_ID, project_id)
    assert paths == [f"POST /orgs/{TEST_ORG_ID}/projects/{project_id}/gateway-key"] * 2


@pytest.mark.description("A hand-over sends the key ID and secret, and a 409 raises the conflict with its detail.")
def test_hand_over_sends_the_secret_or_raises_the_conflict() -> None:
    key_id, project_id = uuid4(), uuid4()
    answers = [httpx.Response(204), httpx.Response(409, json={"detail": "Secret is not for the project's Gateway key"})]
    bodies: list[bytes] = []

    def answer(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return answers.pop(0)

    service = _client(httpx.MockTransport(answer))

    service.hand_over_gateway_key(TEST_ORG_ID, project_id, key_id, "mgwk_held")
    with pytest.raises(GatewayKeyConflictError, match="not for the project"):
        service.hand_over_gateway_key(TEST_ORG_ID, project_id, key_id, "mgwk_held")
    assert httpx.Response(200, content=bodies[0]).json() == {"gateway_key_id": str(key_id), "secret": "mgwk_held"}
