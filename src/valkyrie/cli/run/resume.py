from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID

import click
from valkyrie.sdk import AgentReleaseSelection

from valkyrie.cli.client import call, retry_unavailable
from valkyrie.cli.display import json_option, emit, done
from valkyrie.cli.run.progress import echo_next_steps, follow_run_status
from valkyrie.cli.run.task_ids import resolve_task_ids

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient

_RESTART_OPTIONS: tuple[Callable[[Callable[..., Any]], Callable[..., Any]], ...] = (
    click.option("--agent-release", default=None, help="Saved replacement agent version."),
    click.option("--agent-sha256", default=None, help="Optional expected SHA-256 of the replacement ZIP."),
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
        help="Comma-separated task IDs, @FILE, or an http(s) URL with one task ID per line.",
    ),
    click.option(
        "--secret",
        "-s",
        "secrets",
        multiple=True,
        nargs=2,
        type=(str, str),
        help="Secret as ENV_VAR SECRET_NAME. Adds to or replaces the agent contract's mapping for ENV_VAR.",
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
        "--follow",
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
    secrets: tuple[tuple[str, str], ...],
    headers: tuple[tuple[str, str], ...],
    update_agent: bool,
    agent_release: str | None,
    agent_sha256: str | None,
    from_scratch: bool,
    benchmark_url: str | None,
    lambda_function: str | None,
    follow: bool,
    output_format: str,
) -> None:
    if output_format == "jsonl" and not follow:
        raise click.UsageError("--jsonl requires --follow.")
    retry_task_ids = resolve_task_ids(task_ids)
    if agent_sha256 and not agent_release:
        raise click.UsageError("--agent-sha256 requires --agent-release")
    if agent_release and not (update_agent and from_scratch):
        raise click.UsageError("An exact agent update requires --update-agent --from-scratch")
    selection = AgentReleaseSelection(reference=agent_release, archive_sha256=agent_sha256) if agent_release else None

    async def restart(client: ValkyrieClient) -> None:
        await retry_unavailable(
            lambda: (client.runs.retry if retry else client.runs.resume)(
                run_id,
                concurrency=concurrency,
                task_ids=retry_task_ids,
                secrets=dict(secrets),
                service_headers=dict(headers),
                from_scratch=from_scratch,
                update_agent=update_agent,
                agent_release=selection,
                benchmark_url=benchmark_url,
                lambda_function=lambda_function,
            )
        )
        done(f"Run {'retried' if retry else 'resumed'} successfully!")
        echo_next_steps(run_id, track=not follow)

    call(restart)
    emit({"run_id": run_id, "retried": retry, "resumed": not retry}, output_format)
    if follow:
        follow_run_status(run_id, output_format="jsonl" if output_format == "jsonl" else "text")


@click.command(
    help=(
        "Resume a run's stopped tasks by its ID.\n"
        "\n"
        "Example:\n"
        "    valkyrie run resume 123e4567-e89b-12d3-a456-426614174000 --concurrency 20"
    )
)
@click.argument("run_id", type=UUID)
@_restart_options
@json_option(stream=True)
def resume(**options: Any) -> None:
    _restart(retry=False, **options)


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
@_restart_options
@json_option(stream=True)
def retry_command(**options: Any) -> None:
    options["retry"] = True
    _restart(**options)
