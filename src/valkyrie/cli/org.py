from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table
from valkyrie.cli.login import show_organizations
from valkyrie.sdk import LoginSession, SessionAuth, ValkyrieClient, ValkyrieConfig

Result = TypeVar("Result")


def _saved_login() -> LoginSession:
    session = LoginSession.load(ValkyrieConfig().url)
    if session is None:
        raise click.ClickException("Organizations are managed with your login. Run `valkyrie login`.")
    return session


def _as_login(operation: Callable[[ValkyrieClient], Awaitable[Result]]) -> Result:
    return call(operation, auth=SessionAuth(_saved_login()))


def _switch_organization(organization: str) -> None:
    session = _saved_login()
    switched = session.model_copy(update={"tenant": organization, "expires_at": 0.0})
    switched.save()
    try:
        call(lambda client: client.account.identity(), auth=SessionAuth(switched))
    except click.ClickException:
        current = LoginSession.load(session.tracker_url)
        if current is None or current.access_token == session.access_token:
            current = session
        current.model_copy(update={"tenant": session.tenant}).save()
        raise


@click.group(help="Create, join and switch organizations, and manage their members.")
def org() -> None:
    pass


@org.command(
    "list", help="List the organizations your login belongs to. The one marked * is the one your requests act for."
)
def org_list() -> None:
    show_organizations(_as_login(lambda client: client.account.organizations()))


@org.command(
    "create",
    help=(
        "Create an organization, make yourself its admin, and switch to it.\n"
        "\n"
        "ORGANIZATION is its ID: 3 to 63 lowercase letters, digits, dots or hyphens."
    ),
)
@click.argument("organization")
@click.option("--name", "display_name", help="Display name. Defaults to the organization ID.")
def org_create(organization: str, display_name: str | None) -> None:
    created = _as_login(lambda client: client.account.create_organization(organization, name=display_name))
    _switch_organization(created.id)
    click.echo(f"Created {created.id} and switched to it. You're its admin.")


@org.command("use", help="Act for another organization your login belongs to.")
@click.argument("organization")
def org_use(organization: str) -> None:
    _switch_organization(organization)
    click.echo(f"Now acting for {organization}.")


@org.command("invite", help="Invite someone to your current organization. Only admins can invite.")
@click.argument("email")
def org_invite(email: str) -> None:
    invited = _as_login(lambda client: client.account.invite(email))
    click.echo(
        f"Invited {invited.email}. They can join by signing in with `valkyrie login` using that address "
        f"and running `valkyrie org accept {invited.organization}`."
    )


@org.command("accept", help="Join an organization that invited you, and switch to it.")
@click.argument("organization")
def org_accept(organization: str) -> None:
    _as_login(lambda client: client.account.accept_invitation(organization))
    _switch_organization(organization)
    click.echo(f"Joined {organization} and switched to it.")


@org.command("decline", help="Decline an invitation to an organization.")
@click.argument("organization")
def org_decline(organization: str) -> None:
    _as_login(lambda client: client.account.decline_invitation(organization))
    click.echo(f"Declined the invitation to {organization}.")


@org.command("members", help="List the members and pending invitations of your current organization.")
def org_members() -> None:
    rows = [
        {
            "Email": member.email or "",
            "Name": member.name or "",
            "Role": "admin" if member.admin else "member",
            "Status": member.status,
        }
        for member in _as_login(lambda client: client.account.members()).members
    ]
    format_table(rows, ["Email", "Name", "Role", "Status"], item_name="member")


@org.command(
    "remove", help="Remove someone from your current organization, or cancel their invitation. Only admins can remove."
)
@click.argument("email")
def org_remove(email: str) -> None:
    _as_login(lambda client: client.account.remove_member(email))
    click.echo(f"Removed {email}.")
