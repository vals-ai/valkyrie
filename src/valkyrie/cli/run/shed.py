from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import one_line


@click.command(
    help=(
        "Lower concurrency for an active run, then force stop the newest building or in-progress tasks until the run is back at the new limit. Evaluating tasks are never stopped.\n"
        "\n"
        "Example:\n"
        "    valkyrie run shed 123e4567-e89b-12d3-a456-426614174000 --concurrency 10 --dry-run"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    required=True,
    help="New maximum number of concurrent tasks. Must be below the run's current limit.",
)
@click.option("--dry-run", is_flag=True, help="Show the tasks that would be stopped without changing the run.")
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def shed(run_id: UUID, concurrency: int, dry_run: bool, yes: bool) -> None:
    planned = call(lambda client: client.runs.shed(run_id, concurrency=concurrency, dry_run=True))
    _print_tasks(planned.task_ids, "Would force stop")
    if dry_run:
        return
    if not yes and not click.confirm(
        f"Lower run {run_id} concurrency to {concurrency} and force stop the tasks above it?"
    ):
        click.echo("Cancelled.")
        return

    response = call(lambda client: client.runs.shed(run_id, concurrency=concurrency))
    click.echo(click.style(f"✓ Run concurrency updated to {response.concurrency}.", fg="green", bold=True))
    _print_tasks(response.task_ids, "Force stopped")


def _print_tasks(task_ids: list[str], action: str) -> None:
    if not task_ids:
        click.echo("No building or in-progress tasks are above the limit.")
        return
    click.echo(f"{action} {len(task_ids)} task(s):")
    for task_id in task_ids:
        click.echo(f"  {one_line(task_id)}")
