from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.project import project_option
from valkyrie.cli.display import one_line


@click.command(
    help=(
        "Put a run in another project, in any run state. The earlier project links are kept as history.\n"
        "\n"
        "Example:\n"
        "    valkyrie run retag 123e4567-e89b-12d3-a456-426614174000 --project google-report-xyz"
    )
)
@click.argument("run_id", type=UUID)
@project_option(required=True)
def retag(run_id: UUID, project: str) -> None:
    response = call(lambda client: client.runs.retag(run_id, project))
    click.echo(
        click.style(
            f"✓ Run {run_id} is now in project {one_line(response.handle)}.",
            fg="green",
            bold=True,
        )
    )
