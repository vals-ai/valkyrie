from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import json_option, emit, done
from valkyrie.cli.project import project_option
from valkyrie.cli.run.retag import retag
from valkyrie.cli.run.shed import shed as shed_run


@click.command(
    help=(
        "Change a run's concurrency or project. Add --shed to force stop excess tasks.\n"
        "\n"
        "Example:\n"
        "    valkyrie run update 123e4567-e89b-12d3-a456-426614174000 --concurrency 10"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=None,
    help="New maximum number of concurrent tasks.",
)
@project_option(required=False, help="Move the run to this project's handle, keeping earlier links.")
@click.option("--shed", is_flag=True, help="Force stop the newest excess tasks after lowering concurrency.")
@click.option("--dry-run", is_flag=True, help="Show the tasks that would be stopped without changing the run.")
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@json_option()
def update(
    run_id: UUID, concurrency: int | None, project: str | None, shed: bool, dry_run: bool, yes: bool, output_format: str
) -> None:
    if shed and concurrency is None:
        raise click.UsageError("--shed requires --concurrency.")
    if (dry_run or yes) and not shed:
        raise click.UsageError("--dry-run and --yes require --shed.")
    if concurrency is None and project is None:
        raise click.UsageError("Provide --concurrency or --project.")
    if dry_run and project is not None:
        raise click.UsageError("--dry-run cannot be combined with --project.")
    changes = {}
    if shed:
        assert concurrency is not None
        response = shed_run(run_id, concurrency, dry_run, yes)
        if response is None:
            return
        changes["shed"] = response.model_dump(mode="json")
    elif concurrency is not None:
        response = call(lambda client: client.runs.update_concurrency(run_id, concurrency=concurrency))
        changes["concurrency"] = response.concurrency
        done(f"Run concurrency updated to {response.concurrency}.")
    if project is not None:
        changes["project"] = retag(run_id, project).model_dump(mode="json")
    emit({"run_id": run_id, **changes}, output_format)
