from __future__ import annotations

import asyncio
import sys
from contextlib import nullcontext, redirect_stdout
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import click
from valkyrie.sdk.models import (
    BenchmarkDetails,
    BenchmarkStatus,
    DocentReadingStatus,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    TaskStatus,
    RunSnapshot,
    RunStreamEvent,
)

from valkyrie.cli.client import call
from valkyrie.cli.display import (
    command,
    console,
    details,
    done,
    echo,
    emit,
    fail,
    local_time,
    progress_percent,
    status_text,
    styled,
    warn,
)
from valkyrie.palette import STATUSES, status_label
from valkyrie.display import terminal_safe
from valkyrie.cli.project import describe_run_spend

if TYPE_CHECKING:
    from rich.console import Group

    from valkyrie.sdk import ValkyrieClient

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

OUTCOME_MESSAGES: dict[str, Callable[[], None]] = {
    "complete": lambda: done("Run completed!"),
    "error": lambda: fail("Run errored."),
    "stopped": lambda: echo((f"{STATUSES['STOPPED'][0]} ", "stopped"), "Run stopped."),
    "disconnected": lambda: warn("Disconnected from stream."),
    "interrupted": lambda: warn("Streaming stopped."),
}


def styled_status(status: BenchmarkStatus) -> str:
    return styled(status_text(status.value))


def _progress_lines(details: BenchmarkDetails) -> tuple[str, str]:
    percent = progress_percent(details.finished_tasks, details.total_tasks)
    progress = f"{details.finished_tasks}/{details.total_tasks} ({percent:.1f}%) • {styled_status(details.status)}"
    breakdown = " · ".join(
        styled((f"{status_label(status.value)}: {count}", STATUSES[status.value][1]))
        for status in TASK_STATUS_ORDER
        if (count := details.task_breakdown.get(status, 0))
    )
    return progress, breakdown


def _progress_view(details: BenchmarkDetails) -> "Group":
    from rich.console import Group
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text

    role = STATUSES[details.status.value][1]
    progress, breakdown = _progress_lines(details)
    bar = Table.grid(padding=(0, 1))
    bar.add_row(
        ProgressBar(
            total=max(details.total_tasks, 1),
            completed=details.finished_tasks,
            width=30,
            style="faint",
            complete_style=role,
            finished_style=role,
        ),
        Text.from_ansi(progress),
    )
    return Group(bar, Text.from_ansi(breakdown))


def _display_run_error(benchmark_response: FetchBenchmarkResponse) -> None:
    if benchmark_response.error_message:
        safe_error = terminal_safe(benchmark_response.error_message, preserve_newlines=True)
        echo(("Error: ", "error"), safe_error)


def format_benchmark_status(
    benchmark_response: FetchBenchmarkResponse,
    spend: str | None = None,
    metadata: FetchBenchmarkMetadataResponse | None = None,
) -> None:
    state = benchmark_response.details
    arguments = metadata.benchmark_arguments if metadata is not None else None
    progress_line, breakdown_line = _progress_lines(state)

    details(
        "Run status",
        [
            ("Benchmark", benchmark_response.benchmark_name),
            ("Run ID", str(benchmark_response.benchmark_id)),
            ("Label", benchmark_response.label),
            ("Agent", arguments.contract.name if arguments else None),
            ("Model", (arguments.contract.model or "-") if arguments else None),
            ("Dataset", (arguments.dataset or "default") if arguments else None),
            ("Started by", metadata.started_by_email if metadata else None),
            ("Concurrency", str(arguments.concurrency) if arguments else None),
            (
                "Regraded",
                f"from run {benchmark_response.regraded_from_run_id}"
                if benchmark_response.regraded_from_run_id
                else None,
            ),
            (
                "Skipped",
                f"{len(benchmark_response.regrade_skipped_task_ids)} task(s) without saved evaluation state"
                if benchmark_response.regrade_skipped_task_ids
                else None,
            ),
            ("Spend", spend),
            ("Started at", local_time(state.started_at)),
            (
                "Final score",
                f"{benchmark_response.final_score:.1f}%" if benchmark_response.final_score is not None else None,
            ),
            ("Artifacts", benchmark_response.s3_bucket_url),
            ("Analysis", _format_docent_analysis(state, benchmark_response.benchmark_id)),
            ("Progress", progress_line),
            ("Tasks", breakdown_line),
        ],
    )
    if state.status == BenchmarkStatus.ERROR:
        _display_run_error(benchmark_response)


def format_run_identity(
    benchmark_response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
) -> None:
    rows: list[tuple[str, str | None]] = [("Benchmark", benchmark_response.benchmark_name)]
    if metadata is None:
        rows += [("Run ID", str(benchmark_response.benchmark_id)), ("Label", benchmark_response.label)]
        rows.append(("Metadata", "unavailable"))
    else:
        arguments = metadata.benchmark_arguments
        version = arguments.dataset_version
        rows += [
            ("Agent", arguments.contract.name),
            (
                "Agent release",
                arguments.contract.agent_release.reference if arguments.contract.agent_release else "unversioned",
            ),
            (
                "Agent SHA-256",
                arguments.contract.agent_release.archive_sha256 if arguments.contract.agent_release else "-",
            ),
            ("Model", arguments.contract.model or "-"),
            ("Dataset", arguments.dataset or "default"),
            ("Dataset version", version.label or version.id if version else "not pinned"),
            ("Run ID", str(benchmark_response.benchmark_id)),
            ("Label", benchmark_response.label),
            ("Started by", metadata.started_by_email),
            ("Max concurrency", str(arguments.concurrency)),
            ("Spend", describe_run_spend(metadata)),
        ]
    rows.append(("Started at", local_time(benchmark_response.details.started_at)))

    details("Run details", rows)


def _format_docent_analysis(details: BenchmarkDetails, run_id: UUID) -> str | None:
    status = details.docent_reading_status
    if status == DocentReadingStatus.DONE and details.docent_reading_url:
        return details.docent_reading_url
    if status == DocentReadingStatus.RUNNING:
        return "running..."
    if status == DocentReadingStatus.ERROR:
        return f"failed (re-run with `{command('run', 'analyze', str(run_id), '--no-cache')}`)"
    return None


def echo_next_steps(
    run_id: UUID, *, track: bool = False, outputs: bool = False, artifacts_url: str | None = None
) -> None:
    run = str(run_id)
    details(
        "",
        [],
        [
            ("Track progress", command("run", "show", run, "--follow") if track else None),
            ("Get results", command("run", "results", run, "--output", f"./results-{run}.json")),
            ("Run outputs", command("run", "download", run, "--output", ".") if outputs else None),
            ("Artifacts", artifacts_url),
        ],
        err=True,
    )


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
        if initial.details.status == BenchmarkStatus.ERROR:
            raise click.exceptions.Exit(1)
        return

    if show_identity:
        format_run_identity(initial, metadata)
    echo(err=True)
    echo(("Streaming run updates (Ctrl+C to stop)...", "muted"), err=True)
    from rich.live import Live

    out = console(err=True)
    live = Live(_progress_view(initial.details), console=out, auto_refresh=False)
    latest = initial

    def show(details: BenchmarkDetails) -> None:
        if out.is_terminal:
            live.update(_progress_view(details), refresh=True)
        else:
            out.print(_progress_view(details))

    def finish(outcome: str, *, error: bool = False, next_steps: bool = True) -> None:
        live.stop()
        click.echo()
        OUTCOME_MESSAGES[outcome]()
        if error:
            _display_run_error(latest)
        if next_steps:
            echo_next_steps(benchmark_id, outputs=True, artifacts_url=initial.s3_bucket_url)

    if out.is_terminal:
        live.start(refresh=True)
    else:
        show(initial.details)
    try:
        async for event in client.runs.events(benchmark_id):
            if event.run is not None:
                latest = event.run
                show(latest.details)
            elif event.event == "complete":
                outcome = _completion_event(latest)
                finish(outcome, error=outcome == "error")
                if outcome == "error":
                    raise click.exceptions.Exit(1)
                return
            elif event.event == "error":
                finish("error", error=True)
                raise click.exceptions.Exit(1)
            else:
                finish("disconnected", next_steps=False)
                return
    except (KeyboardInterrupt, asyncio.CancelledError):
        finish("interrupted")
        on_interrupt()
        raise
    finally:
        live.stop()

    finish("disconnected", next_steps=False)
    raise click.ClickException("Run stream ended without a terminal event.")


async def _stream_jsonl(client: ValkyrieClient, benchmark_id: UUID, on_interrupt: Callable[[], None]) -> None:
    snapshot = await client.runs.snapshot(benchmark_id)
    emit(run_snapshot_data(snapshot))
    latest = snapshot.run
    if latest.details.status in TERMINAL_STATUSES:
        emit(RunStreamEvent(event="complete"))
        if latest.details.status == BenchmarkStatus.ERROR:
            raise click.exceptions.Exit(1)
        return
    try:
        async for event in client.runs.events(benchmark_id):
            emit(event)
            if event.run is not None:
                latest = event.run
            if event.event != "update":
                if event.event == "error" or (
                    event.event == "complete" and latest.details.status == BenchmarkStatus.ERROR
                ):
                    raise click.exceptions.Exit(1)
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
        with redirect_stdout(sys.stderr) if output_format == "text" else nullcontext():
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


def run_snapshot_data(snapshot: RunSnapshot) -> dict[str, object]:
    return snapshot.model_dump(
        mode="json",
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
        },
    )
