import click

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, format_table


@click.group(help="Store secrets that your runs reference.")
def secret() -> None:
    pass


@secret.command(
    name="set",
    help=(
        "Store a secret, replacing its value if it already exists.\n"
        "\n"
        "Reads one line from a hidden prompt, or all of stdin when piped, for multi-line values like JSON. The value is\n"
        "never taken from the command line. Prints the reference to use as the secret name.\n"
        "\n"
        "Example:\n"
        "    valkyrie secret set openai-key\n"
        "    valkyrie secret set daytona < daytona.json"
    ),
)
@click.argument("name", type=str)
def set_secret(name: str) -> None:
    stdin = click.get_text_stream("stdin")
    value = (click.prompt("Value", hide_input=True) if stdin.isatty() else stdin.read()).strip()
    if not value:
        raise click.ClickException("The secret value is empty")
    entry = call(lambda client: client.secrets.set(name, value))
    click.echo(click.style(f"✓ Secret '{entry.name}' saved.", fg="green", bold=True))
    click.echo(f"Use this reference as the secret name: {entry.reference}")


@secret.command(
    name="list",
    help=(
        "List stored secrets by name, without their values.\n"
        "\n"
        "Secrets set or removed in the last few minutes may not show up yet."
    ),
)
@format_option()
def list_secrets(output_format: str) -> None:
    response = call(lambda client: client.secrets.list())
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    format_table(
        [{"Secret": entry.name, "Reference": entry.reference} for entry in response.secrets],
        ["Secret", "Reference"],
        item_name="secret",
    )


@secret.command(name="remove", help="Delete a stored secret. It cannot be recovered.")
@click.argument("name", type=str)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def remove_secret(name: str, yes: bool) -> None:
    if not yes and not click.confirm(f"Delete secret '{name}'? Runs that reference it will fail."):
        click.echo("Cancelled.")
        return
    call(lambda client: client.secrets.remove(name))
    click.echo(click.style(f"✓ Secret '{name}' removed.", fg="green", bold=True))
