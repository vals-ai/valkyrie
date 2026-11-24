from uuid import UUID

import click
from valkyrie.sdk.models import ShedBenchmarkResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import one_line, confirm, done


def shed(run_id: UUID, concurrency: int, dry_run: bool, yes: bool) -> ShedBenchmarkResponse | None:
    planned = call(lambda client: client.runs.shed(run_id, concurrency=concurrency, dry_run=True))
    _print_tasks(planned.task_ids, "Would force stop")
    if dry_run:
        return planned
    if not confirm(f"Lower run {run_id} concurrency to {concurrency} and force stop the tasks above it?", yes=yes):
        click.echo("Cancelled.", err=True)
        return None

    response = call(lambda client: client.runs.shed(run_id, concurrency=concurrency))
    done(f"Run concurrency updated to {response.concurrency}.")
    _print_tasks(response.task_ids, "Force stopped")
    return response


def _print_tasks(task_ids: list[str], action: str) -> None:
    if not task_ids:
        click.echo("No building or in-progress tasks are above the limit.", err=True)
        return
    click.echo(f"{action} {len(task_ids)} task(s):", err=True)
    for task_id in task_ids:
        click.echo(f"  {one_line(task_id)}", err=True)
