from uuid import UUID

from valkyrie.sdk.models import BenchmarkStatusEntry, BenchmarkStatusResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import Failure, command, emit, format_table, progress_percent, short_local_time
from valkyrie.cli.run.progress import styled_status


def show_statuses(run_ids: list[UUID], output_format: str) -> None:
    response = call(lambda client: client.benchmarks.statuses(run_ids))

    entries_by_id = {entry.id: entry for entry in response.entries}
    ordered_entries = [entries_by_id[run_id] for run_id in run_ids if run_id in entries_by_id]
    missing_run_ids = [run_id for run_id in run_ids if run_id not in entries_by_id]

    if output_format == "json":
        emit(BenchmarkStatusResponse(entries=ordered_entries))
    else:
        format_status_table(ordered_entries)

    if missing_run_ids:
        raise Failure(
            f"{len(missing_run_ids)} requested run ID(s) were not found or are not accessible.",
            hint=command("run", "list"),
        )


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
