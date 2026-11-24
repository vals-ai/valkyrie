import json
from datetime import UTC, datetime
from uuid import UUID

import click
from pydantic import BaseModel
from valkyrie.sdk.models import FetchTasksRequest, Order, TaskStatus

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, json_option, format_table, one_line, short_local_time
from valkyrie.display import ago


@click.command(name="tasks", help="List tasks with their status and errors.")
@click.argument("run_id", type=UUID)
@click.option(
    "--status",
    multiple=True,
    type=click.Choice([status.value for status in TaskStatus], case_sensitive=False),
    help="Filter by task status. Repeat for multiple statuses.",
)
@click.option("--search", help="Match part of a task ID.")
@click.option(
    "--sort",
    type=click.Choice(["task_id", "started_at", "duration", "status", "last_log_at"]),
    default="started_at",
    show_default=True,
    help="Field to sort tasks by.",
)
@click.option(
    "--order",
    "order_by",
    type=click.Choice(["asc", "desc"], case_sensitive=False),
    default="desc",
    show_default=True,
    help="Sort direction.",
)
@click.option("--limit", type=click.IntRange(1, 500), default=50, show_default=True, help="Most tasks to list.")
@click.option(
    "--offset", type=click.IntRange(min=0), default=0, show_default=True, help="Tasks to skip before listing."
)
@json_option()
def tasks(
    run_id: UUID,
    status: tuple[str, ...],
    search: str | None,
    sort: str,
    order_by: str,
    limit: int,
    offset: int,
    output_format: str,
) -> None:
    request = FetchTasksRequest.model_validate(
        {
            "status": list(status) or None,
            "task_id_search": search,
            "sort": sort,
            "sort_dir": Order(order_by),
            "limit": limit,
            "offset": offset,
        }
    )
    response = call(lambda client: client.benchmarks.tasks(run_id, request))
    if output_format == "json":
        emit(response)
        return
    now = datetime.now(UTC)
    rows = [
        {
            "Task": one_line(task.task_id),
            "Status": task.status.value,
            "Category": task.failure_category.value if task.failure_category else "",
            "Error": one_line(task.error_message or ""),
            "Started": short_local_time(task.started_at),
            "Last log": f"{ago(task.last_log_at, now, compact=True)} ago" if task.last_log_at else "-",
            "Finished": short_local_time(task.finished_at) if task.finished_at else "-",
        }
        for task in response.tasks
    ]
    format_table(
        rows,
        ["Task", "Status", "Category", "Error", "Started", "Last log", "Finished"],
        total_count=response.total_count,
        item_name="task",
    )


@click.command(name="task", help="Inspect a task or get its file links.")
@click.argument("run_id", type=UUID)
@click.argument("task_id")
@click.option("--links", is_flag=True, help="Print temporary download and log links for the task.")
@json_option()
def task(run_id: UUID, task_id: str, links: bool, output_format: str) -> None:
    _print_detail(
        call(lambda client: (client.benchmarks.artifacts if links else client.benchmarks.task)(run_id, task_id)),
        output_format,
    )


def _print_detail(response: BaseModel, output_format: str) -> None:
    if output_format == "json":
        emit(response)
        return
    for key, value in response.model_dump(mode="json").items():
        rendered = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        click.echo(f"{key}: {one_line(rendered)}")
