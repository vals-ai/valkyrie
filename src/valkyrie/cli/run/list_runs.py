import shutil
from collections.abc import AsyncIterator

import click
from valkyrie.sdk.models import (
    BenchmarkStatus,
    BenchmarkTableRow,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    Order,
)

from valkyrie.cli.client import call
from valkyrie.cli.display import (
    console,
    emit,
    json_option,
    format_table,
    paginate_cli_pages,
    progress_percent,
    short_local_time,
    empty,
)
from valkyrie.cli.run.progress import styled_status

_MACHINE_PAGE_LIMIT = 500
_WIDE_HEADERS = {"Label", "By", "Dataset", "Finished"}
_NARROW_HEADERS = {"Started", "Progress"}


@click.command(
    name="list",
    help=(
        "List runs that match the filters.\n"
        "\n"
        "Example:\n"
        "    valkyrie run list --agent claude_code --benchmark swebench --status IN_PROGRESS --order DESC"
    ),
)
@click.option(
    "--agent",
    "agent_name",
    type=str,
    required=False,
    help="Agent name (e.g., claude_code).",
)
@click.option(
    "--benchmark",
    "benchmark_name",
    type=str,
    required=False,
    help="Benchmark name (e.g., swebench).",
)
@click.option(
    "--model",
    type=str,
    required=False,
    help="Model name (e.g., anthropic/claude-sonnet-4-20250514).",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    help="Dataset name (e.g., default, terminal-bench-2.1).",
)
@click.option(
    "--label",
    "-l",
    type=str,
    required=False,
    help="Run label.",
)
@click.option(
    "--status",
    type=click.Choice([option.value for option in BenchmarkStatus], case_sensitive=False),
    required=False,
    default=None,
    help="Status of the benchmarks to fetch (e.g., in_progress, finished, error).",
)
@click.option(
    "--order",
    "order_by",
    type=click.Choice([option.value for option in Order], case_sensitive=False),
    required=False,
    default=Order.DESC.value,
    help="Order by the benchmarks to fetch (e.g., desc, asc).",
)
@click.option(
    "--started-by",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of starter emails (e.g., alice@vals.ai,bob@vals.ai). Case-insensitive.",
)
@json_option()
@click.option("--limit", type=click.IntRange(min=1), default=None, help="Maximum number of matching runs to print.")
@click.option(
    "--page-size",
    type=click.IntRange(min=1, max=_MACHINE_PAGE_LIMIT),
    default=None,
    help="Runs to show per page. Defaults to as many as fit in the terminal.",
)
@click.option("--wide", is_flag=True, help="Also show the label, starter, dataset and finish time of each run.")
@click.option(
    "--all",
    "all_runs",
    is_flag=True,
    help="Fetch matching runs without interactive paging. JSON does this automatically. Requires --json.",
)
def list_runs(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str,
    started_by: str | None,
    output_format: str,
    all_runs: bool,
    limit: int | None,
    page_size: int | None,
    wide: bool,
) -> None:
    if all_runs and output_format != "json":
        raise click.UsageError("--all requires --json.")

    request = FetchBenchmarksRequest(
        agent_name=[agent_name] if agent_name else None,
        benchmark_name=[benchmark_name] if benchmark_name else None,
        model=model,
        dataset=dataset,
        label=label,
        status=[BenchmarkStatus(status)] if status else None,
        started_by=[email.strip() for email in (started_by or "").split(",") if email.strip()] or None,
        order_by=Order(order_by),
    )

    if output_format == "json":
        machine_request = request.model_copy(update={"limit": min(limit or _MACHINE_PAGE_LIMIT, _MACHINE_PAGE_LIMIT)})
        runs = call(lambda client: _collect(client.runs.iter(machine_request), limit))
        emit(FetchBenchmarksResponse(benchmarks=runs))
    else:
        paginate_benchmarks(
            request,
            page_size or min(max(5, shutil.get_terminal_size().lines - 5), _MACHINE_PAGE_LIMIT),
            wide,
            max_runs=limit,
        )


def format_fetch_benchmarks_response(
    fetch_benchmarks_response: FetchBenchmarksResponse,
    current_page: int,
    total_pages: int,
    wide: bool,
) -> None:
    benchmarks = fetch_benchmarks_response.benchmarks

    if not benchmarks:
        empty("No runs found.")
        return

    def format_score(score: float | None) -> str:
        return f"{score:.1f}%" if score is not None else "-"

    rows: list[dict[str, str]] = []
    for benchmark in benchmarks:
        progress_percentage = progress_percent(benchmark.finished_tasks, benchmark.total_tasks)

        rows.append(
            {
                "ID": str(benchmark.id),
                "Benchmark": benchmark.name,
                "Agent": benchmark.agent_name,
                "Label": benchmark.label or "-",
                "By": benchmark.started_by_email or "—",
                "Model": benchmark.model or "-",
                "Dataset": benchmark.dataset or "default",
                "Status": styled_status(benchmark.status),
                "Score": format_score(benchmark.final_score),
                "Started": short_local_time(benchmark.started_at),
                "Finished": short_local_time(benchmark.finished_at, include_date=False)
                if benchmark.finished_at
                else "-",
                "Progress": f"{progress_percentage:.1f}%",
            }
        )

    headers = [
        "ID",
        "Benchmark",
        "Agent",
        "Label",
        "By",
        "Model",
        "Dataset",
        "Status",
        "Score",
        "Started",
        "Finished",
        "Progress",
    ]
    hidden = (set() if wide else _WIDE_HEADERS) | (_NARROW_HEADERS if console().width < 100 else set())
    format_table(
        rows,
        [header for header in headers if header not in hidden],
        current_page,
        total_pages,
        fetch_benchmarks_response.total_count,
        "run",
    )


def format_no_benchmarks_found(request: FetchBenchmarksRequest) -> None:
    filters = {
        "Agent": request.agent_name,
        "Benchmark": request.benchmark_name,
        "Model": request.model,
        "Dataset": request.dataset,
        "Label": request.label,
        "Status": request.status and [status.value for status in request.status],
        "Started By": request.started_by,
    }
    click.echo()
    empty("No runs found matching the specified filters.")
    click.echo()
    if active := {name: value for name, value in filters.items() if value}:
        click.echo("Filters applied:")
        for name, value in active.items():
            click.echo(f"  • {name}: {value if isinstance(value, str) else ', '.join(value)}")


async def _collect(rows: AsyncIterator[BenchmarkTableRow], limit: int | None) -> list[BenchmarkTableRow]:
    collected: list[BenchmarkTableRow] = []
    async for row in rows:
        collected.append(row)
        if limit is not None and len(collected) >= limit:
            break
    return collected


def paginate_benchmarks(request: FetchBenchmarksRequest, limit: int, wide: bool, max_runs: int | None = None) -> None:

    def load_page(offset: int, page_limit: int) -> tuple[int, FetchBenchmarksResponse]:
        if max_runs is not None:
            page_limit = min(page_limit, max_runs - offset)
        page_request = request.model_copy(update={"offset": offset, "limit": page_limit})
        response = call(lambda client: client.runs.list(page_request))
        total = response.total_count or 0
        return min(total, max_runs) if max_runs is not None else total, response

    paginate_cli_pages(
        load_page,
        lambda response, current_page, total_pages, _total_count: format_fetch_benchmarks_response(
            response, current_page, total_pages, wide
        ),
        limit=limit,
        render_empty=lambda: format_no_benchmarks_found(request),
    )
