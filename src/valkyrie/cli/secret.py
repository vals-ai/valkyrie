import click

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, json_option, format_table, confirm, done, styled


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
        "    valkyrie secret set daytona-credentials < daytona.json"
    ),
)
@click.argument("name", type=str)
@click.option("--yes", "-y", is_flag=True, help="Allow the hidden value prompt with --json.")
@json_option()
def set_secret(name: str, yes: bool, output_format: str) -> None:
    stdin = click.get_text_stream("stdin")
    if output_format != "text" and stdin.isatty() and not yes:
        raise click.UsageError("Pipe the secret value on stdin or use --yes to allow the prompt with --json.")
    value = (
        click.prompt(styled(("? ", "accent"), ("Value", "bold"), err=True), hide_input=True, err=True)
        if stdin.isatty()
        else stdin.read()
    ).strip()
    if not value:
        raise click.ClickException("The secret value is empty")
    entry = call(lambda client: client.secrets.set(name, value))
    if emit(entry, output_format):
        return
    done(f"Secret '{entry.name}' saved.")
    click.echo(f"Use this reference as the secret name: {entry.reference}")


@secret.command(
    name="list",
    help=(
        "List stored secrets by name, without their values.\n"
        "\n"
        "Secrets set or removed in the last few minutes may not show up yet."
    ),
)
@json_option()
def list_secrets(output_format: str) -> None:
    response = call(lambda client: client.secrets.list())
    if output_format == "json":
        emit(response)
        return
    format_table(
        [{"Secret": entry.name, "Reference": entry.reference} for entry in response.secrets],
        ["Secret", "Reference"],
        item_name="secret",
    )


@secret.command(name="remove", help="Delete a stored secret. It cannot be recovered.")
@click.argument("name", type=str)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@json_option()
def remove_secret(name: str, yes: bool, output_format: str) -> None:
    if not confirm(f"Delete secret '{name}'? Runs that reference it will fail.", yes=yes):
        click.echo("Cancelled.", err=True)
        return
    call(lambda client: client.secrets.remove(name))
    if emit({"removed": name}, output_format):
        return
    done(f"Secret '{name}' removed.")
