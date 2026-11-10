from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import echo_next_steps


@click.command(
    help=(
        "Regrade a finished run's saved task states into a new run on another dataset.\n"
        "\n"
        "The source run, its tasks and its evaluation stay unchanged. Tasks without saved evaluation state are skipped.\n"
        "\n"
        "Example:\n"
        "    valkyrie run regrade 123e4567-e89b-12d3-a456-426614174000 --dataset terminal-bench-4.1 --concurrency 16"
    ),
    short_help="Regrade a finished run into a new run on another dataset.",
)
@click.argument("run_id", type=UUID)
@click.option("--dataset", type=str, required=True, help="Dataset whose verifiers grade the saved task states.")
@click.option(
    "--dataset-version", type=str, default=None, help="Dataset version to pin, when the service has versions."
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=None,
    help="Tasks to grade at once. Defaults to the source run's concurrency.",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential).",
)
def regrade(
    run_id: UUID,
    dataset: str,
    dataset_version: str | None,
    concurrency: int | None,
    headers: tuple[tuple[str, str], ...],
) -> None:
    response = call(
        lambda client: client.runs.regrade(
            run_id,
            dataset=dataset,
            dataset_version=dataset_version,
            concurrency=concurrency,
            service_headers=dict(headers),
        )
    )
    click.echo(click.style(f"✓ Run {response.benchmark_id} regrades run {run_id}.", fg="green", bold=True))
    click.echo(f"Regrading {len(response.task_ids)} task(s) on {dataset}.")
    if response.skipped_task_ids:
        click.echo(
            click.style(
                f"Skipped {len(response.skipped_task_ids)} task(s) without saved evaluation state: "
                + ", ".join(response.skipped_task_ids),
                fg="yellow",
            )
        )
    echo_next_steps(response.benchmark_id)
