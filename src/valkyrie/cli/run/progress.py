import asyncio
from collections.abc import Callable, Iterable
from typing import Literal
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.models import (
    BenchmarkDetails,
    BenchmarkStatus,
    DocentReadingStatus,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    TaskStatus,
    RunSnapshot,
)

from valkyrie.cli.client import call
from valkyrie.cli.display import local_time, progress_percent, status_label
from valkyrie.display import terminal_safe
from valkyrie.cli.project import describe_run_spend

STATUS_COLORS = {
    "PENDING": "yellow",
    "BUILDING": "cyan",
    "IN_PROGRESS": "blue",
    "EVALUATING": "magenta",
    "STOPPED": "cyan",
    "STOPPING": "magenta",
    "FINISHED": "green",
    "ERROR": "red",
}

TASK_STATUS_ORDER = (
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
    TaskStatus.ERROR,
    TaskStatus.STOPPED,
    TaskStatus.FINISHED,
)

TERMINAL_STATUSES = {BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED}

OUTCOME_MESSAGES = {
    "complete": click.style("✓ Run completed!", fg="green", bold=True),
    "error": click.style("✗ Run errored.", fg="red", bold=True),
    "stopped": click.style("Run stopped.", fg="cyan", bold=True),
}


def styled_status(status: BenchmarkStatus, *, bold: bool = False) -> str:
    return click.style(status_label(status.value), fg=STATUS_COLORS[status.value], bold=bold)


def _progress_lines(details: BenchmarkDetails, bar_width: int = 30) -> tuple[str, str]:
    percent = progress_percent(details.finished_tasks, details.total_tasks)
    filled = int(bar_width * percent / 100)
    bar = "█" * filled + "░" * (bar_width - filled)
    progress = (
        f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({percent:.1f}%) • "
        f"{styled_status(details.status, bold=True)}"
    )
    breakdown = " │ ".join(
        click.style(f"{status_label(status.value)}: {count}", fg=STATUS_COLORS[status.value])
        for status in TASK_STATUS_ORDER
        if (count := details.task_breakdown.get(status, 0))
    )
    return progress, breakdown


def _echo_rows(rows: Iterable[tuple[str, str | None]], label_width: int) -> None:
    for label, value in rows:
        if value:
            click.echo(f"│ {label:<{label_width}} {value}")


def _display_run_error(benchmark_response: FetchBenchmarkResponse) -> None:
    if benchmark_response.error_message:
        safe_error = terminal_safe(benchmark_response.error_message, preserve_newlines=True)
        click.secho(f"Error: {safe_error}", fg="red")


def format_benchmark_status(
    benchmark_response: FetchBenchmarkResponse,
    spend: str | None = None,
    metadata: FetchBenchmarkMetadataResponse | None = None,
) -> None:
    details = benchmark_response.details
    arguments = metadata.benchmark_arguments if metadata is not None else None
    progress_line, breakdown_line = _progress_lines(details)

    click.echo("┌─ Run Status " + "─" * 66)
    _echo_rows(
        [
            ("Benchmark:", benchmark_response.benchmark_name),
            ("Run ID:", str(benchmark_response.benchmark_id)),
            ("Label:", benchmark_response.label),
            ("Agent:", arguments.contract.name if arguments else None),
            ("Model:", (arguments.contract.model or "-") if arguments else None),
            ("Dataset:", (arguments.dataset or "default") if arguments else None),
            ("Started by:", metadata.started_by_email if metadata else None),
            ("Concurrency:", str(arguments.concurrency) if arguments else None),
            (
                "Regraded:",
                f"from run {benchmark_response.regraded_from_run_id}"
                if benchmark_response.regraded_from_run_id
                else None,
            ),
            (
                "Skipped:",
                f"{len(benchmark_response.regrade_skipped_task_ids)} task(s) without saved evaluation state"
                if benchmark_response.regrade_skipped_task_ids
                else None,
            ),
            ("Spend:", spend),
            ("Started at:", local_time(details.started_at)),
            (
                "Final score:",
                f"{benchmark_response.final_score:.1f}%" if benchmark_response.final_score is not None else None,
            ),
            ("Artifacts:", benchmark_response.s3_bucket_url),
            ("Analysis:", _format_docent_analysis(details, benchmark_response.benchmark_id)),
        ],
        12,
    )
    click.echo("├" + "─" * 79)
    click.echo(f"│ {progress_line}")
    if breakdown_line:
        click.echo(f"│ {breakdown_line}")
    click.echo("└" + "─" * 79)
    if details.status == BenchmarkStatus.ERROR:
        _display_run_error(benchmark_response)


def format_run_identity(
    benchmark_response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
) -> None:
    rows: list[tuple[str, str | None]] = [("Benchmark:", benchmark_response.benchmark_name)]
    if metadata is None:
        rows += [("Run ID:", str(benchmark_response.benchmark_id)), ("Label:", benchmark_response.label)]
        rows.append(("Metadata:", "unavailable"))
    else:
        arguments = metadata.benchmark_arguments
        version = arguments.dataset_version
        rows += [
            ("Agent:", arguments.contract.name),
            (
                "Agent release:",
                arguments.contract.agent_release.reference if arguments.contract.agent_release else "unversioned",
            ),
            (
                "Agent SHA-256:",
                arguments.contract.agent_release.archive_sha256 if arguments.contract.agent_release else "-",
            ),
            ("Model:", arguments.contract.model or "-"),
            ("Dataset:", arguments.dataset or "default"),
            ("Dataset version:", version.label or version.id if version else "not pinned"),
            ("Run ID:", str(benchmark_response.benchmark_id)),
            ("Label:", benchmark_response.label),
            ("Started by:", metadata.started_by_email),
            ("Max concurrency:", str(arguments.concurrency)),
            ("Spend:", describe_run_spend(metadata)),
        ]
    rows.append(("Started at:", local_time(benchmark_response.details.started_at)))

    click.echo("┌─ Run Details " + "─" * 65)
    _echo_rows(rows, 17)
    click.echo("└" + "─" * 79)


def _format_docent_analysis(details: BenchmarkDetails, run_id: UUID) -> str | None:
    status = details.docent_reading_status
    if status == DocentReadingStatus.DONE and details.docent_reading_url:
        return details.docent_reading_url
    if status == DocentReadingStatus.RUNNING:
        return "running..."
    if status == DocentReadingStatus.ERROR:
        return f"failed (re-run with `valk run analyze {run_id} --no-cache`)"
    return None


def echo_next_steps(run_id: UUID, *, track: bool = False, outputs: bool = False, s3_url: str | None = None) -> None:
    steps = {
        "Track progress:": f"valkyrie run fetch {run_id} --connect" if track else None,
        "Get results:": f"valkyrie run results {run_id} --path ./results-{run_id}.json",
        "Run outputs:": f"valkyrie run outputs {run_id} --output-dir ." if outputs else None,
        "S3 view:": s3_url,
    }
    click.echo("┌─ Next Steps " + "─" * 66)
    _echo_rows(((label, click.style(command, fg="cyan")) for label, command in steps.items() if command), 17)
    click.echo("└" + "─" * 79)


def _completion_event(response: FetchBenchmarkResponse) -> Literal["complete", "error", "stopped"]:
    if response.details.status == BenchmarkStatus.ERROR:
        return "error"
    if response.details.status == BenchmarkStatus.STOPPED:
        return "stopped"
    return "complete"


async def stream_benchmark_status(
    client: ValkyrieClient,
    benchmark_id: UUID,
    *,
    show_identity: bool = False,
    output_format: Literal["text", "jsonl"] = "text",
    on_interrupt: Callable[[], None] = lambda: None,
) -> None:
    if output_format == "jsonl":
        await _stream_jsonl(client, benchmark_id, on_interrupt)
        return
    snapshot = await client.runs.snapshot(benchmark_id) if show_identity else None
    initial = snapshot.run if snapshot is not None else await client.runs.fetch(benchmark_id)
    metadata = snapshot.metadata if snapshot is not None else None
    if initial.details.status in TERMINAL_STATUSES:
        format_benchmark_status(initial, describe_run_spend(metadata) if show_identity else None, metadata)
        return

    if show_identity:
        format_run_identity(initial, metadata)
    click.echo(click.style("Streaming run updates (Ctrl+C to stop)...", fg="cyan"))
    progress_line, breakdown_line = _progress_lines(initial.details)
    click.echo(f"{progress_line}\n{breakdown_line}", nl=False)

    latest = initial

    def finish(message: str, *, error: bool = False, next_steps: bool = True) -> None:
        click.echo("\n")
        click.echo(message)
        if error:
            _display_run_error(latest)
        if next_steps:
            echo_next_steps(benchmark_id, outputs=True, s3_url=initial.s3_bucket_url)

    disconnected = click.style("Disconnected from stream.", fg="yellow")
    try:
        async for event in client.runs.events(benchmark_id):
            if event.run is not None:
                latest = event.run
                progress_line, breakdown_line = _progress_lines(latest.details)
                click.echo(f"\033[F\033[K{progress_line}\n\033[K{breakdown_line}", nl=False)
            elif event.event == "complete":
                outcome = _completion_event(latest)
                finish(OUTCOME_MESSAGES[outcome], error=outcome == "error")
                return
            elif event.event == "error":
                finish(OUTCOME_MESSAGES["error"], error=True)
                return
            else:
                finish(disconnected, next_steps=False)
                return
    except (KeyboardInterrupt, asyncio.CancelledError):
        finish(click.style("Streaming stopped.", fg="yellow"))
        on_interrupt()
        raise

    finish(disconnected, next_steps=False)
    raise click.ClickException("Run stream ended without a terminal event.")


async def _stream_jsonl(client: ValkyrieClient, benchmark_id: UUID, on_interrupt: Callable[[], None]) -> None:
    click.echo(format_run_snapshot_json(await client.runs.snapshot(benchmark_id)))
    try:
        async for event in client.runs.events(benchmark_id):
            click.echo(event.model_dump_json())
            if event.event != "update":
                return
    except (KeyboardInterrupt, asyncio.CancelledError):
        on_interrupt()
        raise
    raise click.ClickException("Run stream ended without a terminal event.")


def follow_run_status(
    benchmark_id: UUID, *, show_identity: bool = False, output_format: Literal["text", "jsonl"] = "text"
) -> None:
    interrupted = False

    def mark_interrupted() -> None:
        nonlocal interrupted
        interrupted = True

    try:
        call(
            lambda client: stream_benchmark_status(
                client,
                benchmark_id,
                show_identity=show_identity,
                output_format=output_format,
                on_interrupt=mark_interrupted,
            )
        )
    except KeyboardInterrupt:
        if not interrupted:
            raise


def format_run_snapshot_json(snapshot: RunSnapshot) -> str:
    return snapshot.model_dump_json(
        exclude={
            "metadata": {
                "benchmark_arguments": {
                    "contract": {
                        "kwargs": True,
                        "secrets": True,
                        "agent_release": {"prescribed_configuration", "overrides"},
                    },
                    "agent_attempts": {
                        "__all__": {
                            "effective_configuration": True,
                            "agent_release": {"prescribed_configuration", "overrides"},
                            "results": True,
                            "final_evaluation": True,
                        }
                    },
                }
            }
        }
    )
