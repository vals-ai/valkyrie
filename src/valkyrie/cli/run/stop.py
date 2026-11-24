from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import json_option, emit, confirm, done, warn
from valkyrie.cli.run.progress import echo_next_steps
from valkyrie.cli.run.task_ids import resolve_task_ids


@click.command(
    help="Stop a run by its ID.\n\nExample:\n    valkyrie run stop 123e4567-e89b-12d3-a456-426614174000 --force"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated task IDs, @FILE, or an http(s) URL with one task ID per line.",
)
@click.option(
    "--force",
    is_flag=True,
    required=False,
    default=False,
    help="Force stop the benchmark run.",
)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@json_option()
def stop(run_id: UUID, task_ids: str | None, force: bool, yes: bool, output_format: str):
    selected_task_ids = resolve_task_ids(task_ids)
    scope = f"{len(selected_task_ids)} selected task(s) in run {run_id}" if selected_task_ids else f"run {run_id}"
    if not confirm(f"{'Force stop' if force else 'Stop'} {scope}?", yes=yes):
        click.echo("Cancelled.", err=True)
        return

    call(lambda client: client.runs.stop(run_id, force=force, task_ids=selected_task_ids))
    if emit({"run_id": run_id, "stopped": True, "force": force, "task_ids": selected_task_ids}, output_format):
        return

    subject = "Selected tasks" if selected_task_ids else "Run"
    if force:
        done(f"{subject} force stopped.")
    else:
        detail = (
            "agent tasks already running may finish"
            if selected_task_ids
            else "will finish after in-flight tasks complete"
        )
        warn(f"{subject} {'are' if selected_task_ids else 'is'} stopping — {detail}.")
    echo_next_steps(run_id)
