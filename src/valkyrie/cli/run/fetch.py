from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import follow_run_status, format_benchmark_status, run_snapshot_data
from valkyrie.cli.project import describe_run_spend
from valkyrie.cli.display import emit, json_option
from valkyrie.cli.run.status import show_statuses


@click.command(help="Show one run's details or several runs' status. Use --follow to stream one run.")
@click.argument("run_ids", type=UUID, nargs=-1, required=True)
@click.option(
    "--follow",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates.",
)
@json_option(stream=True)
def show(run_ids: tuple[UUID, ...], follow: bool, output_format: str):
    run_ids = tuple(dict.fromkeys(run_ids))
    if len(run_ids) > 1:
        if follow or output_format == "jsonl":
            raise click.UsageError("--follow and --jsonl require one run.")
        show_statuses(list(run_ids), output_format)
        return
    run_id = run_ids[0]

    if follow and output_format == "json":
        raise click.UsageError("--json cannot be used with --follow; use --jsonl.")
    if not follow and output_format == "jsonl":
        raise click.UsageError("--jsonl requires --follow.")

    if follow and output_format == "jsonl":
        follow_run_status(run_id, output_format="jsonl")
        return
    if follow:
        follow_run_status(run_id, show_identity=True)
        return

    snapshot = call(lambda client: client.runs.snapshot(run_id))
    if output_format == "json":
        emit(run_snapshot_data(snapshot))
    else:
        format_benchmark_status(snapshot.run, describe_run_spend(snapshot.metadata), snapshot.metadata)
