from typing import cast
from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, terminal_safe, json_option, done, fail


@click.command(
    help=(
        "Score a finished dataset run again with its metric script. \n\n"
        "The script runs again and the new score replaces the stored one. \n\n"
        "Example:\nvalkyrie run rescore 123e4567-e89b-12d3-a456-426614174000"
    )
)
@click.argument("run_id", type=UUID)
@json_option()
def rescore(run_id: UUID, output_format: str) -> None:
    response = call(lambda client: client.runs.rescore(run_id))
    scorer = cast(dict[str, object], response.properties.get("scorer") or {})
    if error := scorer.get("error"):
        fail(f"The metric script failed: {terminal_safe(str(error), preserve_newlines=True)}")
        raise click.ClickException(f"Run {run_id} has no score")
    if emit(response, output_format):
        return
    score = "no single headline score" if response.final_score is None else f"score {response.final_score:.2f}"
    done(f"Run {run_id} rescored: {score}.")
