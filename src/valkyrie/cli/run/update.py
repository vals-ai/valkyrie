from uuid import UUID

import click

from valkyrie.cli.client import call


@click.command(
    help=(
        "Change an active run's concurrency.\n"
        "\n"
        "Example:\n"
        "    valkyrie run update 123e4567-e89b-12d3-a456-426614174000 --concurrency 10"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    required=True,
    help="New maximum number of concurrent tasks.",
)
def update(run_id: UUID, concurrency: int) -> None:
    response = call(lambda client: client.runs.update_concurrency(run_id, concurrency=concurrency))
    click.echo(click.style(f"✓ Run concurrency updated to {response.concurrency}.", fg="green", bold=True))
