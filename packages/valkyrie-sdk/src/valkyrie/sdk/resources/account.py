"""Your identity, access keys, and organizations.

Tracker accepts only a login, not an access key, for access keys and organizations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from valkyrie.sdk.models import (
    AccessKeyResponse,
    IdentityResponse,
    InviteResponse,
    MembersResponse,
    OrganizationResponse,
    OrganizationsResponse,
)
from valkyrie.sdk.resources._path import encode_path_segment as _segment

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class AccountResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def identity(self) -> IdentityResponse:
        """Show the organization and user your requests act as, and which kind of credential they use."""
        return await self._sdk.request_model("GET", "/auth/me", IdentityResponse)

    async def create_access_key(
        self, name: str, *, expires_in_days: int = 30, read_only: bool = False
    ) -> AccessKeyResponse:
        """Create an access key for the current organization that expires after 1 to 90 days.

        A `read_only` key is refused for anything but GET and HEAD requests.
        The returned `key` is shown only once. Needs a login, not an access key.
        """
        return await self._sdk.request_model(
            "POST",
            "/access-keys",
            AccessKeyResponse,
            json={"name": name, "expires_in_days": expires_in_days, **({"read_only": True} if read_only else {})},
        )

    async def revoke_access_key(self, key_id: str) -> None:
        """Delete an access key you created; Tracker may accept it for up to one more minute, and tokens exchanged from it until they expire."""
        await self._sdk.request_no_content("DELETE", f"/access-keys/{_segment(key_id, label='key ID')}")

    async def organizations(self) -> OrganizationsResponse:
        """List the organizations your login belongs to and the ones that invited it."""
        return await self._sdk.request_model("GET", "/orgs", OrganizationsResponse)

    async def create_organization(self, organization: str, *, name: str | None = None) -> OrganizationResponse:
        """Create an organization with this ID and make yourself its admin.

        The ID is 3 to 63 lowercase letters, digits, dots, or hyphens; `name` defaults to it.
        """
        return await self._sdk.request_model(
            "POST", "/orgs", OrganizationResponse, json={"id": organization, "name": name}
        )

    async def accept_invitation(self, organization: str) -> None:
        """Join an organization that invited you."""
        await self._sdk.request_no_content(
            "POST", f"/orgs/invitations/{_segment(organization, label='organization')}/accept"
        )

    async def decline_invitation(self, organization: str) -> None:
        """Decline an invitation to an organization."""
        await self._sdk.request_no_content(
            "DELETE", f"/orgs/invitations/{_segment(organization, label='organization')}"
        )

    async def members(self) -> MembersResponse:
        """List the current organization's members and pending invitations."""
        return await self._sdk.request_model("GET", "/orgs/members", MembersResponse)

    async def invite(self, email: str) -> InviteResponse:
        """Invite someone to the current organization. Only admins can invite."""
        return await self._sdk.request_model("POST", "/orgs/members", InviteResponse, json={"email": email})

    async def remove_member(self, email: str) -> None:
        """Remove someone from the current organization, or cancel their invitation. Only admins can remove."""
        await self._sdk.request_no_content("DELETE", f"/orgs/members/{_segment(email, label='email')}")
