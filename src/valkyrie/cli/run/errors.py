from uuid import UUID

import click
from valkyrie.sdk.models import FinalViewResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, one_line, status_label
from valkyrie.display import group_task_errors, terminal_safe

_TASK_ID_PREVIEW_LIMIT = 5


def _display_error_message(message: str) -> str:
    if message == "":
        return "(empty error message)"
    return terminal_safe(message, preserve_newlines=True)


def _indent_message(message: str) -> str:
    return "\n".join(f"  {line}" for line in message.split("\n"))


def _count_label(count: int, singular: str) -> str:
    return f"{count} {singular if count == 1 else f'{singular}s'}"


def _format_task_id_preview(task_ids: tuple[str, ...]) -> str:
    visible_ids = task_ids[:_TASK_ID_PREVIEW_LIMIT]
    preview = ", ".join(one_line(task_id) for task_id in visible_ids)
    omitted_count = len(task_ids) - len(visible_ids)
    return f"{preview} (+{omitted_count} more)" if omitted_count else preview


def format_run_errors_text(response: FinalViewResponse) -> None:
    task_errors = response.task_errors or {}
    groups = group_task_errors(task_errors, response.task_failure_categories)

    click.echo(click.style("Run Errors", bold=True))
    click.echo(f"{'Run ID:':<12}{response.benchmark_id}")
    click.echo(f"{'Benchmark:':<12}{one_line(response.benchmark_name)}")
    click.echo(f"{'Status:':<12}{status_label(response.status.value)}")

    if response.error_message is not None:
        click.echo()
        click.echo(click.style("Stored run error", bold=True))
        click.echo(_indent_message(_display_error_message(response.error_message)))

    if groups:
        click.echo()
        click.echo(
            click.style(
                f"Task errors ({_count_label(len(task_errors), 'task')}, "
                f"{_count_label(len(groups), 'distinct message')})",
                bold=True,
            )
        )

        for category, message, task_ids in groups:
            click.echo()
            label = _count_label(len(task_ids), "task")
            if category is not None:
                label = f"{label}, {category.value}"
            click.echo(f"[{label}] {_format_task_id_preview(task_ids)}")
            click.echo(_indent_message(_display_error_message(message)))
    elif response.error_message is None:
        click.echo()
        click.echo("No current error messages recorded.")


@click.command(
    help=(
        "Show stored run and current task error messages.\n"
        "\n"
        "Example:\n"
        "    valkyrie run errors 123e4567-e89b-12d3-a456-426614174000"
    ),
)
@click.argument("run_id", type=UUID)
@format_option()
def errors(run_id: UUID, output_format: str) -> None:
    response = call(lambda client: client.runs.results(run_id))
    if output_format == "json":
        click.echo(response.model_dump_json())
    else:
        format_run_errors_text(response)
