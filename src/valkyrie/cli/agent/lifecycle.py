from datetime import datetime
from pathlib import Path

import click

from valkyrie.display import terminal_safe

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, json_option, format_table, local_time, paginate_cli_pages, confirm, done, empty


@click.command(name="install", help="Install a GitHub agent through Tracker, replacing an existing alias.")
@click.argument("github_url", type=str)
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to the contract name).",
)
@json_option()
def install(github_url: str, name: str | None, output_format: str):
    entry = call(lambda client: client.agents.install(github_url, name=name))
    if emit(entry, output_format):
        return
    done(f"Agent '{entry.name}' installed successfully!")


@click.command(name="push", help="Upload a local agent through Tracker, replacing an existing alias.")
@click.argument("agent_path", type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True))
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to the contract name).",
)
@json_option()
def push(agent_path: Path, name: str | None, output_format: str):
    entry = call(lambda client: client.agents.push(agent_path, name=name or None))
    if emit(entry, output_format):
        return
    done(f"Agent '{entry.name}' pushed successfully!")


@click.command(name="remove", help="Remove an installed agent.")
@click.argument("agent_name", type=str)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@json_option()
def agent_remove(agent_name: str, yes: bool, output_format: str):
    if not confirm(f"Are you sure you want to remove agent '{agent_name}'?", yes=yes):
        click.echo("Cancelled.", err=True)
        return

    call(lambda client: client.agents.remove(agent_name))
    if emit({"removed": agent_name}, output_format):
        return
    done(f"Agent '{agent_name}' removed successfully!")


@click.command(name="download", help="Download an installed agent.")
@click.argument("agent_name", type=str)
@click.option(
    "--output",
    "-o",
    "output_dir",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=None,
    required=False,
    help="Output directory for downloaded agent (default: current directory).",
)
@click.option("--overwrite", is_flag=True, help="Replace an existing agent directory after validating the download.")
@json_option()
def download(agent_name: str, output_dir: Path | None, overwrite: bool, output_format: str):
    destination = call(lambda client: client.agents.download(agent_name, output_dir, overwrite=overwrite))
    if emit({"agent": agent_name, "path": destination}, output_format):
        return
    done(f"Agent '{agent_name}' downloaded successfully!")


@click.command(name="list", help="List installed agents.")
@click.option("--all", "all_agents", is_flag=True, help="Show every agent without interactive paging.")
@json_option()
def list_installed_agents(all_agents: bool, output_format: str):

    if output_format == "json":
        emit(call(lambda client: client.agents.list()))
        return

    if not all_agents:
        paginate_agents()
        return

    agents = _installed_agents()
    if not agents:
        empty("No agents found.")
        return
    _format_agents_response(agents, 1, 1, len(agents))


def _installed_agents() -> list[tuple[str, datetime | None]]:
    response = call(lambda client: client.agents.list())
    try:
        return [
            (agent.name, datetime.fromisoformat(agent.last_modified) if agent.last_modified else None)
            for agent in response.agents
        ]
    except ValueError as error:
        raise click.ClickException(terminal_safe(str(error), preserve_newlines=True)) from error


def _format_agents_response(
    agents: list[tuple[str, datetime | None]],
    current_page: int,
    total_pages: int,
    total_count: int,
) -> None:
    rows = [
        {"Agent": name, "Last Modified": local_time(last_modified) if last_modified else "-"}
        for name, last_modified in agents
    ]

    format_table(rows, ["Agent", "Last Modified"], current_page, total_pages, total_count, "agent")


def paginate_agents(limit: int = 10) -> None:
    def load_page(offset: int, page_limit: int) -> tuple[int, list[tuple[str, datetime | None]]]:
        agents = _installed_agents()
        return len(agents), agents[offset : offset + page_limit]

    paginate_cli_pages(
        load_page,
        _format_agents_response,
        limit=limit,
        render_empty=lambda: empty("No agents found."),
    )
