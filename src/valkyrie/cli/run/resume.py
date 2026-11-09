from collections.abc import Callable
from typing import Any
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import echo_next_steps, follow_run_status
from valkyrie.cli.run.task_ids import resolve_task_ids

_RESTART_OPTIONS: tuple[Callable[[Callable[..., Any]], Callable[..., Any]], ...] = (
    click.option(
        "--concurrency",
        type=click.IntRange(min=1),
        required=False,
        default=None,
        help="Override concurrency level (e.g., 20).",
    ),
    click.option(
        "--task-ids",
        type=str,
        required=False,
        default=None,
        help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908).",
    ),
    click.option(
        "--task-ids-file",
        type=str,
        required=False,
        default=None,
        help="Path or http(s) URL to a text file with one task ID per line.",
    ),
    click.option(
        "--secret",
        "-s",
        "secrets",
        multiple=True,
        nargs=2,
        type=(str, str),
        help="Secret as ENV_VAR aws_secret_name (e.g., -s ANTHROPIC_API_KEY YourAnthropicKeySecret).",
    ),
    click.option(
        "--header",
        "-H",
        "headers",
        multiple=True,
        nargs=2,
        type=(str, str),
        help="Custom header for benchmark service requests (e.g., -H Authorization my-credential).",
    ),
    click.option(
        "--update-agent",
        "-u",
        is_flag=True,
        default=False,
        help="Refresh the frozen agent copy from the current published agent bundle before resuming.",
    ),
    click.option(
        "--from-scratch",
        is_flag=True,
        default=False,
        help="Clear durable eval state and rerun generation.",
    ),
    click.option(
        "--benchmark-url",
        type=str,
        default=None,
        help="Replace the benchmark service URL for this run.",
    ),
    click.option(
        "--connect",
        is_flag=True,
        required=False,
        help="Connect to the tracker service to stream run updates after resuming.",
    ),
    click.option(
        "--lambda",
        "lambda_function",
        type=str,
        default=None,
        help="Replace the post-run Lambda for this retry or resume.",
    ),
)


def _restart_options(command: Callable[..., Any]) -> Callable[..., Any]:
    for option in reversed(_RESTART_OPTIONS):
        command = option(command)
    return command


def _restart(
    run_id: UUID,
    *,
    retry: bool,
    concurrency: int | None,
    task_ids: str | None,
    task_ids_file: str | None,
    secrets: tuple[tuple[str, str], ...],
    headers: tuple[tuple[str, str], ...],
    update_agent: bool,
    from_scratch: bool,
    benchmark_url: str | None,
    lambda_function: str | None,
    connect: bool,
) -> None:
    retry_task_ids = resolve_task_ids(task_ids, task_ids_file)

    async def restart(client: ValkyrieClient) -> None:
        await (client.runs.retry if retry else client.runs.resume)(
            run_id,
            concurrency=concurrency,
            task_ids=retry_task_ids,
            secrets=dict(secrets),
            service_headers=dict(headers),
            from_scratch=from_scratch,
            update_agent=update_agent,
            benchmark_url=benchmark_url,
            lambda_function=lambda_function,
        )
        click.echo(click.style(f"✓ Run {'retried' if retry else 'resumed'} successfully!", fg="green", bold=True))
        echo_next_steps(run_id, track=not connect)

    call(restart)
    if connect:
        follow_run_status(run_id)


@click.command(
    help=(
        "Resume a run's stopped tasks by its ID.\n"
        "\n"
        "Example:\n"
        "    valkyrie run resume 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--retry",
    is_flag=True,
    required=False,
    default=False,
    help="Retry tasks with the status error.",
)
@_restart_options
def resume(**options: Any) -> None:
    _restart(**options)


@click.command(
    name="retry",
    help=(
        "Retry a run's errored tasks by its ID.\n"
        "\n"
        "Example:\n"
        "    valkyrie run retry 123e4567-e89b-12d3-a456-426614174000 --concurrency 20"
    ),
    short_help="Retry a run's errored tasks by its ID.",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--retry", is_flag=True, hidden=True, help="Accepted for compatibility. `retry` always retries errored tasks."
)
@_restart_options
def retry_command(**options: Any) -> None:
    options["retry"] = True
    _restart(**options)
