from __future__ import annotations

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import utc_isoformat
from valkyrie.sdk import (
    IdentityResponse,
    LoginSession,
    OrganizationsResponse,
    SessionAuth,
    ValkyrieAPIError,
    ValkyrieClient,
    ValkyrieConfig,
    ValkyrieTransportError,
    device_login,
)


def show_organizations(listing: OrganizationsResponse) -> None:
    if not listing.organizations and not listing.invitations:
        click.echo(
            "You're not in an organization yet. Ask Vals AI to add you, "
            "or ask an organization admin to run `valkyrie org invite EMAIL`."
        )
    for entry in listing.organizations:
        click.echo(f"{'*' if entry.current else ' '} {entry.id}{' (admin)' if entry.admin else ''}")
    for organization in listing.invitations:
        click.echo(f"  {organization} (invited; run `valkyrie org accept {organization}`)")


async def _organizations_or_identity(client: ValkyrieClient) -> OrganizationsResponse | IdentityResponse:
    try:
        return await client.account.organizations()
    except ValkyrieAPIError as error:
        if error.status_code != 404:
            raise
    return await client.account.identity()


@click.command(help="Sign in with your browser, save the login, and use this Tracker by default.")
@click.option("--no-browser", is_flag=True, help="Print the sign-in link without opening a browser.")
def login(no_browser: bool) -> None:
    tracker_url = ValkyrieConfig().url

    def announce(link: str, user_code: str) -> None:
        click.echo(f"Open {link} and confirm the code {user_code}.", err=True)
        if not no_browser:
            click.launch(link)

    try:
        session = device_login(tracker_url, announce)
    except ValkyrieTransportError as error:
        raise click.ClickException(str(error)) from error
    session.save()
    session.remember()
    if ValkyrieConfig().api_key:
        click.echo("VALKYRIE_API_KEY is set and takes precedence over this login; unset it to use the login.", err=True)
    listing = call(_organizations_or_identity, auth=SessionAuth(session))
    if isinstance(listing, IdentityResponse):
        who = listing.email or listing.name or "your account"
        click.echo(f"Logged in to {session.tracker_url} as {who} for {listing.organization}.")
        return
    click.echo(f"Logged in to {session.tracker_url} as {listing.email or 'your account'}.")
    show_organizations(listing)


@click.command(help="Forget the saved login for this Tracker, and stop using it by default.")
def logout() -> None:
    tracker_url = ValkyrieConfig().url
    if LoginSession.forget(tracker_url):
        click.echo(f"Logged out of {tracker_url}.")
    else:
        click.echo(f"You were not logged in to {tracker_url}.")


@click.command(help="Show which Tracker, organization and account your requests use.")
def whoami() -> None:
    identity = call(lambda client: client.account.identity())
    credential = {"login": "a login", "access_key": "an access key"}.get(identity.credential, "no credential")
    who = identity.email or identity.name or "unknown user"
    click.echo(f"{who} in {identity.organization} on {ValkyrieConfig().url} (using {credential})")


@click.group(help="Create and revoke access keys for CI and other headless use.")
def token() -> None:
    pass


@token.command(
    "create", help="Create an access key for your organization. Set it as VALKYRIE_API_KEY where you need it."
)
@click.argument("name")
@click.option(
    "--expires-in-days",
    type=click.IntRange(1, 90),
    default=30,
    show_default=True,
    help="Days until the key stops working.",
)
def token_create(name: str, expires_in_days: int) -> None:
    created = call(lambda client: client.account.create_access_key(name, expires_in_days=expires_in_days))
    click.echo(f"Created access key {created.key_id}, expiring {utc_isoformat(created.expires_at)}.", err=True)
    click.echo("It is shown only once:", err=True)
    click.echo(created.key)


@token.command(
    "revoke",
    help="Delete an access key you created. The Tracker may accept it for up to one more minute, and tokens exchanged from it until they expire.",
)
@click.argument("key_id")
def token_revoke(key_id: str) -> None:
    call(lambda client: client.account.revoke_access_key(key_id))
    click.echo(f"Revoked access key {key_id}.")
