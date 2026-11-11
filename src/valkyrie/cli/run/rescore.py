from typing import cast
from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import terminal_safe


@click.command(
    help=(
        "Score a finished dataset run again with its metric script. \n\n"
        "The script runs again and the new score replaces the stored one. \n\n"
        "Example:\nvalkyrie run rescore 123e4567-e89b-12d3-a456-426614174000"
    )
)
@click.argument("run_id", type=UUID)
def rescore(run_id: UUID) -> None:
    response = call(lambda client: client.runs.rescore(run_id))
    scorer = cast(dict[str, object], response.properties.get("scorer") or {})
    if error := scorer.get("error"):
        click.echo(
            click.style(f"✗ The metric script failed: {terminal_safe(str(error), preserve_newlines=True)}", fg="red"),
            err=True,
        )
        raise click.ClickException(f"Run {run_id} has no score")
    score = "no single headline score" if response.final_score is None else f"score {response.final_score:.2f}"
    click.echo(click.style(f"✓ Run {run_id} rescored: {score}.", fg="green", bold=True))
