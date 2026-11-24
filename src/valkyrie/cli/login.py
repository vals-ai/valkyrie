from __future__ import annotations

from typing import TYPE_CHECKING

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, utc_isoformat, json_option
from valkyrie.cli.tracker_profiles import TrackerProfiles
from valkyrie.sdk import (
    IdentityResponse,
    OrganizationsResponse,
    ValkyrieAPIError,
    ValkyrieTransportError,
)

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient


def show_organizations(listing: OrganizationsResponse) -> None:
    if not listing.organizations and not listing.invitations:
        click.echo(
            "You're not in an organization yet. Run `valkyrie org create my-team` to create one, "
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


@click.command(help="Sign in to the active Tracker with your browser and save the login for it.")
@click.option("--no-browser", is_flag=True, help="Print the sign-in link without opening a browser.")
@click.option("--yes", "-y", is_flag=True, help="Allow the browser sign-in flow with --json.")
@json_option()
def login(no_browser: bool, yes: bool, output_format: str) -> None:
    from valkyrie.sdk import SessionAuth, ValkyrieConfig, device_login

    if output_format != "text" and not yes:
        raise click.UsageError("Use --yes to allow browser sign-in with --json.")
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
    if emit(listing, output_format):
        return
    if isinstance(listing, IdentityResponse):
        who = listing.email or listing.name or "your account"
        click.echo(
            f"Logged in to {TrackerProfiles.load().describe(session.tracker_url)} as {who} for {listing.organization}.",
            err=True,
        )
        return
    click.echo(
        f"Logged in to {TrackerProfiles.load().describe(session.tracker_url)} as {listing.email or 'your account'}.",
        err=True,
    )
    show_organizations(listing)


@click.command(help="Forget the saved login for the active Tracker.")
@json_option()
def logout(output_format: str) -> None:
    from valkyrie.sdk import LoginSession, ValkyrieConfig

    tracker_url = ValkyrieConfig().url
    forgotten = LoginSession.forget(tracker_url)
    if emit({"tracker": tracker_url, "logged_out": forgotten}, output_format):
        return
    if forgotten:
        click.echo(f"Logged out of {TrackerProfiles.load().describe(tracker_url)}.", err=True)
    else:
        click.echo(f"You were not logged in to {TrackerProfiles.load().describe(tracker_url)}.", err=True)


@click.command(help="Show which Tracker, organization and account your requests use.")
@json_option()
def whoami(output_format: str) -> None:
    from valkyrie.sdk import ValkyrieConfig

    identity = call(lambda client: client.account.identity())
    tracker = ValkyrieConfig().url
    if emit({**identity.model_dump(mode="json"), "tracker": tracker}, output_format):
        return
    where = f"{identity.organization} on {TrackerProfiles.load().describe(tracker)}"
    credential = {"login": "a login", "access_key": "an access key"}.get(identity.credential)
    if credential is None:
        click.echo(f"{where} (no sign-in needed)")
        return
    who = identity.email or identity.name or "unknown user"
    click.echo(f"{who} in {where} (using {credential})")


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
@json_option()
def token_create(name: str, expires_in_days: int, output_format: str) -> None:
    created = call(lambda client: client.account.create_access_key(name, expires_in_days=expires_in_days))
    if emit(created, output_format):
        return
    click.echo(f"Created access key {created.key_id}, expiring {utc_isoformat(created.expires_at)}.", err=True)
    click.echo("It is shown only once:", err=True)
    click.echo(created.key)


@token.command(
    "revoke",
    help="Delete an access key you created. The Tracker may accept it for up to one more minute, and tokens exchanged from it until they expire.",
)
@click.argument("key_id")
@json_option()
def token_revoke(key_id: str, output_format: str) -> None:
    call(lambda client: client.account.revoke_access_key(key_id))
    if emit({"revoked": key_id}, output_format):
        return
    click.echo(f"Revoked access key {key_id}.", err=True)
