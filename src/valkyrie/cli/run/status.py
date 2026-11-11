from uuid import UUID

import click
from valkyrie.sdk.models import BenchmarkStatusEntry, BenchmarkStatusResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, format_table, progress_percent, short_local_time
from valkyrie.cli.run.progress import styled_status


@click.command(
    name="status",
    help=(
        "Show the status of several runs at once.\n"
        "\n"
        "Example:\n"
        "    valkyrie run status --ids 123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001 --format json"
    ),
)
@click.option(
    "--ids",
    "run_ids_value",
    required=True,
    help="Comma-separated run UUIDs. Duplicates are ignored while preserving order.",
)
@format_option()
def status_runs(run_ids_value: str, output_format: str) -> None:
    run_ids = parse_run_ids(run_ids_value)

    response = call(lambda client: client.benchmarks.statuses(run_ids))

    entries_by_id = {entry.id: entry for entry in response.entries}
    ordered_entries = [entries_by_id[run_id] for run_id in run_ids if run_id in entries_by_id]
    missing_run_ids = [run_id for run_id in run_ids if run_id not in entries_by_id]

    if output_format == "json":
        click.echo(BenchmarkStatusResponse(entries=ordered_entries).model_dump_json())
    else:
        format_status_table(ordered_entries)

    if missing_run_ids:
        raise click.ClickException(f"{len(missing_run_ids)} requested run ID(s) were not found or are not accessible.")


def parse_run_ids(value: str) -> list[UUID]:
    raw_ids = [part.strip() for part in value.split(",") if part.strip()]
    if not raw_ids:
        raise click.BadParameter("provide at least one run UUID", param_hint="--ids")

    run_ids: list[UUID] = []
    for raw_id in raw_ids:
        try:
            run_ids.append(UUID(raw_id))
        except ValueError as e:
            raise click.BadParameter(f"invalid run UUID: {raw_id}", param_hint="--ids") from e
    return list(dict.fromkeys(run_ids))


def format_status_table(entries: list[BenchmarkStatusEntry]) -> None:
    rows: list[dict[str, str]] = []
    for entry in entries:
        percent = progress_percent(entry.finished_tasks, entry.total_tasks)
        rows.append(
            {
                "ID": str(entry.id),
                "Status": styled_status(entry.status),
                "Progress": f"{entry.finished_tasks}/{entry.total_tasks} ({percent:.1f}%)",
                "Finished": short_local_time(entry.finished_at) if entry.finished_at else "-",
            }
        )
    format_table(rows, ["ID", "Status", "Progress", "Finished"], total_count=len(entries), item_name="run")
