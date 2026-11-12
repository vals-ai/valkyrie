from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import follow_run_status, format_benchmark_status, format_run_snapshot_json
from valkyrie.cli.project import describe_run_spend
from valkyrie.cli.display import format_option


@click.command(
    help="Show a run by its ID.\n\nExample:\n    valkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates.",
)
@format_option("Output format. Use json for a snapshot or jsonl with --connect.", choices=("text", "json", "jsonl"))
def fetch(run_id: UUID, connect: bool, output_format: str):

    if connect and output_format == "json":
        raise click.UsageError("--format json cannot be used with --connect; use --format jsonl.")
    if not connect and output_format == "jsonl":
        raise click.UsageError("--format jsonl requires --connect.")

    if connect and output_format == "jsonl":
        follow_run_status(run_id, output_format="jsonl")
        return
    if connect:
        follow_run_status(run_id, show_identity=True)
        return

    snapshot = call(lambda client: client.runs.snapshot(run_id))
    if output_format == "json":
        click.echo(format_run_snapshot_json(snapshot))
    else:
        format_benchmark_status(snapshot.run, describe_run_spend(snapshot.metadata), snapshot.metadata)
