from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

import click
from valkyrie.sdk.models import FinalViewResponse, RetrieveResultsResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, terminal_safe, json_option, confirm, done, echo, fail, warn
from valkyrie.cli.run.task_ids import resolve_task_ids

_PRIVATE_CONFIG = {"benchmark_arguments": {"contract": {"secrets", "kwargs"}}}

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient


def _format_expiration(seconds: int) -> str:
    return {3600: "1 hour", 86400: "1 day"}.get(seconds, f"{seconds} seconds")


@click.command(
    name="results",
    help=(
        "Save a run's results by its ID.\n"
        "\n"
        "Example:\n"
        "    valkyrie run results 123e4567-e89b-12d3-a456-426614174000 -o ./results.json"
    ),
)
@click.argument("run_id", type=UUID)
@click.option(
    "-o",
    "--output",
    "path",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    default=None,
    required=False,
    help="Path to save the results (default: ./results-<run_id>.json).",
)
@click.option(
    "--s3",
    is_flag=True,
    default=False,
    required=False,
    help="Saves results to s3 instead of downloading them locally. Can be found at bucket://benchmarks/run_id/<benchmark>.json.",
)
@click.option(
    "--preview",
    is_flag=True,
    default=False,
    help="Archive the current S3 results, replace them with a fresh snapshot, and run the completion callback.",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated task IDs, @FILE, or an http(s) URL with one task ID per line.",
)
@click.option("--yes", "-y", is_flag=True, help="Answer yes to confirmation prompts.")
@click.option("--overwrite", is_flag=True, help="Replace an existing results file.")
@json_option()
def results(
    run_id: UUID,
    path: Path | None,
    s3: bool,
    preview: bool,
    task_ids: str | None,
    yes: bool,
    overwrite: bool,
    output_format: str,
):
    if preview and path:
        raise click.UsageError("--preview cannot be combined with --output")

    subset_task_ids = resolve_task_ids(task_ids)

    click.echo(f"Retrieving results for run: {run_id}", err=True)

    async def retrieve(client: ValkyrieClient) -> RetrieveResultsResponse:
        if preview:
            return await client.runs.preview(run_id, task_ids=subset_task_ids)
        if s3 and (await client.runs.results_exist(run_id)).exists:
            if not confirm("Results already exist in S3. Overwrite?", yes=yes):
                raise click.Abort()
        return await client.runs.results(run_id, task_ids=subset_task_ids, upload_to_s3=s3)

    results_response = call(retrieve)
    if isinstance(results_response, FinalViewResponse):
        if subset_task_ids:
            scored = len(results_response.evaluation_results or {}) + len(results_response.task_errors or {})
            report = warn if scored < len(subset_task_ids) else done
            report(f"Scored over {scored} of {len(subset_task_ids)} subset task ids.")
        download_final_view(path or Path(f"./results-{run_id}.json"), results_response, overwrite=overwrite)
        if emit(results_response.model_dump(mode="json", exclude=_PRIVATE_CONFIG), output_format):
            return
        evaluation = results_response.final_evaluation
        if evaluation is not None and evaluation.final_score is None:
            scorer = evaluation.properties.get("scorer")
            error = cast(dict[str, object], scorer).get("error") if isinstance(scorer, dict) else None
            if error:
                fail(
                    f"The dataset's metric script failed, so the run has no score: "
                    f"{terminal_safe(str(error), preserve_newlines=True)}\n"
                    f"Fix it and run `valkyrie run rescore {run_id}`."
                )
            else:
                warn(
                    "The run has no single headline score: its tasks report several rewards and none is named "
                    "'reward'. Every mean is in the results file under final_evaluation.properties.metrics."
                )
    else:
        if emit(results_response, output_format):
            return
        echo((f"Download (expires in {_format_expiration(results_response.expires_in)}):", "bold"))
        click.echo(f"  {results_response.presigned_url}")
        click.echo(err=True)
        echo(("AWS Console:", "bold"))
        click.echo(f"  {results_response.console_url}")


def download_final_view(path: Path, final_view: FinalViewResponse, overwrite: bool) -> None:
    if not path.parent.exists():
        raise click.ClickException(f"'{path.parent}' directory does not exist! Please create it first.")

    if path.exists() and not overwrite:
        raise click.ClickException(f"File '{path}' already exists. Use --overwrite to replace it.")

    with open(path, "w") as output_file:
        output_file.write(
            final_view.model_dump_json(
                indent=4,
                exclude_none=True,
                exclude=_PRIVATE_CONFIG,
            )
        )

    done(f"Results saved to '{path}'")
