import json
from collections.abc import Callable

import httpx
import pytest

from valkyrie.sdk import IdentityResponse, OrganizationEntry, OrganizationsResponse, ValkyrieClient
from valkyrie.sdk.login import ApiKeyAuth

_RESPONSES = {
    ("GET", "/auth/me"): {"organization": "acme", "email": "a@example.com", "name": None, "credential": "login"},
    ("POST", "/access-keys"): {"key_id": "K1", "key": "secret", "expires_at": "2026-10-15T00:00:00Z"},
    ("GET", "/orgs"): {
        "email": "a@example.com",
        "organizations": [{"id": "acme", "admin": True, "current": True}],
        "invitations": ["beta"],
    },
    ("POST", "/orgs"): {"id": "acme-labs", "name": "acme-labs"},
    ("GET", "/orgs/members"): {"organization": "acme", "members": []},
    ("POST", "/orgs/members"): {"organization": "acme", "email": "b@example.com"},
}


@pytest.mark.description(
    "Account methods call the account routes, and an empty or dot segment member email raises `ValueError`."
)
async def test_requests_hit_the_account_routes_and_reject_dot_segments(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    requests: list[tuple[str, str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.raw_path.decode(), body))
        reply = _RESPONSES.get((request.method, request.url.path))
        return httpx.Response(200, json=reply) if reply else httpx.Response(204)

    async with make_client(handler) as client:
        identity = await client.account.identity()
        created = await client.account.create_access_key("ci", expires_in_days=7)
        await client.account.revoke_access_key("../K1")
        listing = await client.account.organizations()
        await client.account.create_organization("acme-labs")
        await client.account.accept_invitation("beta")
        await client.account.decline_invitation("gamma")
        await client.account.members()
        await client.account.invite("b@example.com")
        await client.account.remove_member("b@example.com")
        for bad in ("", ".", ".."):
            with pytest.raises(ValueError, match="Invalid email"):
                await client.account.remove_member(bad)

    assert identity == IdentityResponse(organization="acme", email="a@example.com", name=None, credential="login")
    assert (created.key_id, created.key, created.expires_at.isoformat()) == (
        "K1",
        "secret",
        "2026-10-15T00:00:00+00:00",
    )
    assert "secret" not in repr(created)
    assert listing == OrganizationsResponse(
        email="a@example.com",
        organizations=[OrganizationEntry(id="acme", admin=True, current=True)],
        invitations=["beta"],
    )
    assert requests == [
        ("GET", "/auth/me", None),
        ("POST", "/access-keys", {"name": "ci", "expires_in_days": 7}),
        ("DELETE", "/access-keys/..%2FK1", None),
        ("GET", "/orgs", None),
        ("POST", "/orgs", {"id": "acme-labs", "name": None}),
        ("POST", "/orgs/invitations/beta/accept", None),
        ("DELETE", "/orgs/invitations/gamma", None),
        ("GET", "/orgs/members", None),
        ("POST", "/orgs/members", {"email": "b@example.com"}),
        ("DELETE", "/orgs/members/b%40example.com", None),
    ]


@pytest.mark.description("An explicit `auth` replaces the credential the config would pick for requests.")
async def test_auth_replaces_the_credential_the_config_picks(make_client: Callable[..., ValkyrieClient]) -> None:
    headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        headers.append(request.headers)
        return httpx.Response(200, json=_RESPONSES[("GET", "/auth/me")])

    async with make_client(handler) as client:
        await client.account.identity()
    async with ValkyrieClient(
        client.config, transport=httpx.MockTransport(handler), auth=ApiKeyAuth("other")
    ) as client:
        await client.account.identity()

    assert [request_headers["x-api-key"] for request_headers in headers] == ["vals-key", "other"]
