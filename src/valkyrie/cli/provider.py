import click

from valkyrie.cli.client import call
from valkyrie.cli.display import json_option, emit


@click.group(help="Choose the secret that configures each sandbox provider, and your default provider.")
def provider() -> None:
    pass


@provider.command(
    "set",
    help=(
        "Configure a sandbox provider with a stored secret.\n"
        "\n"
        "Replaces the secret the provider had. SECRET is the name or reference that `valkyrie secret set` printed.\n"
        "\n"
        "Example:\n"
        "    valkyrie provider set daytona daytona-credentials"
    ),
)
@click.argument("name")
@click.argument("secret")
@json_option()
def provider_set(name: str, secret: str, output_format: str) -> None:
    saved = call(lambda client: client.providers.set(name, secret))
    if emit(saved, output_format):
        return
    click.echo(f"Provider {name} now uses the secret {saved.providers[name]}.", err=True)


@provider.command(
    "remove",
    help=(
        "Stop configuring a sandbox provider.\n"
        "\n"
        "Runs on it fail until it has a secret again.\n"
        "\n"
        "Example:\n"
        "    valkyrie provider remove modal"
    ),
)
@click.argument("name")
@json_option()
def provider_remove(name: str, output_format: str) -> None:
    call(lambda client: client.providers.remove(name))
    if emit({"removed": name}, output_format):
        return
    click.echo(f"Provider {name} no longer has a secret.", err=True)


@provider.command("list", help="List the secret behind each sandbox provider. Your default provider is marked *.")
@json_option()
def provider_list(output_format: str) -> None:
    providers = call(lambda client: client.providers.list()).providers
    default = call(lambda client: client.settings.get()).default_provider
    if emit({"providers": providers, "default_provider": default}, output_format):
        return
    if not providers and default is None:
        click.echo("No provider has a secret yet. Map one with `valkyrie provider set NAME SECRET`.")
    for name in sorted({*providers, default} if default else providers):
        if name in providers:
            click.echo(f"{'*' if name == default else ' '} {name} → {providers[name]}")
        else:
            click.echo(f"* {name} (default; no secret set — run `valkyrie provider set {name} SECRET`)")


@provider.command(
    "default",
    help=(
        "Run on this sandbox provider unless a run passes --provider.\n\nExample:\n    valkyrie provider default modal"
    ),
)
@click.argument("name")
@json_option()
def provider_default(name: str, output_format: str) -> None:
    settings = call(lambda client: client.settings.set_default_provider(name))
    if emit(settings, output_format):
        return
    click.echo(f"Your default provider is now {settings.default_provider}.", err=True)
