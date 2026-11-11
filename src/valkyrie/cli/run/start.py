from collections.abc import Iterable
from pathlib import Path
from uuid import UUID

from typing import Any

import click
from valkyrie.sdk import (
    AgentContract,
    RunMonitoring,
    StartBenchmarkResponse,
    ValkyrieAPIError,
    ValkyrieClient,
    ValkyrieSDKError,
)
from valkyrie.sdk.models import HostedDataset, LocalDataset, folder_run_name

from valkyrie.cli.client import call
from valkyrie.cli.display import local_time, one_line
from valkyrie.cli.project import describe_project, project_option
from valkyrie.cli.resource import validate_versioned_dataset_reference
from valkyrie.cli.run.progress import follow_run_status
from valkyrie.cli.run.task_ids import resolve_task_ids


BOX_WIDTH = 80
COLUMN_WIDTH = 14
RUN_COLUMN_WIDTH = 17
_UNKNOWN_START_OUTCOME = "The latest start request's outcome may be unknown. Verify with `valkyrie run list`."
_CLEAR_LINE = "\r\033[K"


def row(label: str, value: str, width: int = COLUMN_WIDTH) -> str:
    return f"│ {label:<{width}} {value}"


def format_run_start_details(
    benchmark: str,
    dataset: str | None,
    concurrency: int,
    slice_str: str | None,
    task_ids: str | None,
    priority: int | None,
) -> None:
    click.echo("┌─ Benchmark ".ljust(BOX_WIDTH, "─"))
    click.echo(row("Name:", benchmark))
    click.echo(row("Dataset:", dataset or "default"))
    click.echo(row("Concurrency:", str(concurrency)))
    if priority is not None:
        click.echo(row("Priority override:", str(priority)))
    if slice_str:
        click.echo(row("Slice:", slice_str))
    click.echo(
        row("Task IDs:", (task_ids[:60] + "..." if len(task_ids) > 60 else task_ids) if task_ids else "all tasks")
    )
    click.echo("└".ljust(BOX_WIDTH, "─"))
    click.echo()


def format_agent_start_details(
    agent: str,
    model: str | None,
    secrets: tuple[tuple[str, str], ...],
    kwargs: tuple[tuple[str, str], ...],
    header_names: Iterable[str],
) -> None:
    click.echo("┌─ Agent ".ljust(BOX_WIDTH, "─"))
    click.echo(row("Agent:", agent))
    if model:
        click.echo(row("Model:", model))
    for index, (env_var, secret_name) in enumerate(secrets):
        click.echo(row("Secrets:" if index == 0 else "", f"{env_var} → {secret_name}"))
    if kwargs:
        click.echo(row("Kwargs:", ", ".join(f"{key}={value}" for key, value in kwargs)))
    if header_names := list(header_names):
        click.echo(row("Headers:", ", ".join(header_names)))
    click.echo("└".ljust(BOX_WIDTH, "─"))
    click.echo()


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse, connect: bool = False) -> None:
    run_id = start_benchmark_response.benchmark_id
    version = start_benchmark_response.dataset_version
    details = [
        ("Benchmark:", start_benchmark_response.benchmark_name),
        ("Agent:", start_benchmark_response.agent_name),
        ("Run ID:", run_id),
        ("Started at:", local_time(start_benchmark_response.started_at)),
        ("Max concurrency:", start_benchmark_response.concurrency),
        ("Total tasks:", start_benchmark_response.task_count),
        ("Dataset version:", version.label or version.id if version else "not pinned"),
    ]
    project = start_benchmark_response.project
    details.append(("Spend:", describe_project(project) if project is not None else "untagged"))
    commands = [
        ("Get results:", f"valkyrie run results {run_id} --path ./results-{run_id}.json"),
        ("Stop:", f"valkyrie run stop {run_id}"),
        ("Resume:", f"valkyrie run resume {run_id}"),
        ("Retry:", f"valkyrie run retry {run_id}"),
        ("Run outputs:", f"valkyrie run outputs {run_id} --output-dir ."),
    ]
    if not connect:
        commands.insert(0, ("Track progress:", f"valkyrie run fetch {run_id} --connect"))

    click.echo()
    click.echo("┌─ Run Details ".ljust(BOX_WIDTH, "─"))
    for label, value in details:
        click.echo(row(label, str(value), RUN_COLUMN_WIDTH))
    if start_benchmark_response.dataset_version_warning:
        click.secho(start_benchmark_response.dataset_version_warning, fg="yellow")
    if start_benchmark_response.cloudwatch_url:
        click.echo(row("Logs:", start_benchmark_response.cloudwatch_url, RUN_COLUMN_WIDTH))
    if start_benchmark_response.s3_bucket_url:
        click.echo(row("Artifacts:", start_benchmark_response.s3_bucket_url, RUN_COLUMN_WIDTH))
    click.echo("├".ljust(BOX_WIDTH, "─"))
    for label, command in commands:
        click.echo(row(label, click.style(command, fg="cyan"), RUN_COLUMN_WIDTH))
    click.echo("└".ljust(BOX_WIDTH, "─"))


def format_confirmed_start_summary(run_ids: list[UUID], count: int, outcome_unknown: bool = False) -> None:
    if count > 1:
        click.echo()
        click.echo(f"{len(run_ids)} / {count} requested runs successfully started.")
        if run_ids:
            click.echo(f"Track progress: valkyrie run status --ids {','.join(str(run_id) for run_id in run_ids)}")
    if outcome_unknown:
        click.echo(click.style(_UNKNOWN_START_OUTCOME, fg="yellow"))


def _is_folder(path: str) -> bool:
    try:
        return Path(path).expanduser().is_dir()
    except (RuntimeError, OSError):
        return False


def _local(path: str) -> LocalDataset:
    return LocalDataset(path=str(Path(path).expanduser().resolve()))


def _hosted(dataset: str) -> HostedDataset | None:
    try:
        owner, name, version = validate_versioned_dataset_reference(dataset)
    except ValueError:
        return None
    return HostedDataset(benchmark=owner, dataset=name, ref=version)


def _dataset_reference(dataset: str | None, benchmark: str | None) -> HostedDataset | LocalDataset | None:
    if not dataset:
        return None
    if dataset.startswith((".", "/", "~")):
        if not _is_folder(dataset):
            raise click.UsageError(f"--dataset {dataset} is not a folder")
        return _local(dataset)
    if "/" not in dataset or benchmark not in (None, dataset.partition("/")[0]):
        return None
    hosted = _hosted(dataset)
    if hosted is None:
        raise click.UsageError(
            "--dataset must be BENCHMARK/DATASET[@VERSION], such as swe-bench/verified@2, or a folder such as ./tasks"
        )
    return hosted


@click.command(
    help=(
        "Start a run of an agent on a benchmark.\n"
        "\n"
        "Example:\n"
        "    valkyrie run start --agent agents/claude_code --benchmark swebench --project router-bench --concurrency 5"
    )
)
@click.option(
    "--agent",
    type=str,
    required=True,
    help="Published agent name or local agent path (e.g., claude_code or ./claude_code), or oracle to run each task's solution.",
)
@click.option(
    "--model",
    type=str,
    required=False,
    help="Model key (e.g., openai/gpt-4o).",
)
@click.option(
    "--benchmark",
    type=str,
    help="Name of the benchmark to run (e.g., swebench, finance).",
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=5,
    required=False,
    help="Maximum concurrent tasks. Defaults to 5.",
)
@click.option(
    "--lambda",
    "lambda_function",
    type=str,
    default=None,
    required=False,
    help="Lambda function to invoke at the end of the benchmark run.",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908).",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line.",
)
@click.option(
    "--slice",
    "slice_str",
    type=str,
    required=False,
    default=None,
    help="Dataset slice in Python slice syntax start:stop[:step] (e.g., 0:10).",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Service dataset name, a hosted BENCHMARK/DATASET[@VERSION], or a folder starting with ./, / or ~.",
)
@click.option("--dataset-version", type=str, default=None, help="Fixed dataset version to use for this run.")
@click.option(
    "--provider",
    type=str,
    required=False,
    default=None,
    help="Sandbox provider (e.g., daytona, modal). Defaults to your default provider, then daytona.",
)
@click.option(
    "--priority",
    type=click.IntRange(0, 4),
    required=False,
    default=None,
    help="Override configured queue priority: 0 (highest) through 4 (lowest). Default is 3.",
)
@click.option(
    "--label",
    "-l",
    type=str,
    required=False,
    default=None,
    help="Label to attach to the run.",
)
@project_option(required=False)
@click.option(
    "--kwarg",
    "-k",
    "kwargs",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Kwargs as key value (e.g., -k temperature 7 -k max_tokens 1000).",
)
@click.option(
    "--secret",
    "-s",
    "secrets",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Secret as ENV_VAR aws_secret_name (e.g., -s ANTHROPIC_API_KEY YourAnthropicKeySecret).",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential).",
)
@click.option(
    "--ignore-custom-services",
    "--ics",
    is_flag=True,
    help="Use the hosted benchmark service instead of the URL saved with `valkyrie benchmark service set`.",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after starting.",
)
@click.option(
    "--monitor", is_flag=True, help="Enroll the run in the external run monitor, including its network requests."
)
@click.option(
    "--count",
    "-n",
    type=click.IntRange(1, 10),
    default=1,
    show_default=True,
    help="Number of independent runs to start.",
)
def start(
    agent: str,
    model: str | None,
    benchmark: str | None,
    concurrency: int,
    lambda_function: str | None,
    task_ids: str | None,
    task_ids_file: str | None,
    slice_str: str | None,
    dataset: str | None,
    dataset_version: str | None,
    provider: str | None,
    priority: int | None,
    label: str | None,
    project: str | None,
    kwargs: tuple[tuple[str, str]],
    secrets: tuple[tuple[str, str]],
    headers: tuple[tuple[str, str]],
    ignore_custom_services: bool,
    connect: bool,
    monitor: bool,
    count: int,
) -> None:
    if count > 1 and connect:
        raise click.UsageError("--connect cannot be used when --count is greater than 1")

    monitoring = RunMonitoring() if monitor else None
    reference = _dataset_reference(dataset, benchmark)
    if reference is not None and (dataset_version or headers or ignore_custom_services):
        raise click.UsageError(
            "--dataset-version, --header and --ignore-custom-services belong to benchmark services "
            "and cannot be used with a pushed dataset or a folder"
        )
    if reference is not None and monitoring is not None:
        raise click.UsageError("--monitor cannot be used with a pushed dataset or a folder")
    formatted_task_ids = resolve_task_ids(task_ids, task_ids_file)
    agent_dir = _local_agent_dir(agent)
    if reference is None and benchmark is None:
        raise click.UsageError("Missing option '--benchmark'.")
    shared_options: dict[str, Any] = {
        "model": model,
        "concurrency": concurrency,
        "priority": priority,
        "task_ids": formatted_task_ids,
        "slice_str": slice_str,
        "label": label,
        "lambda_function": lambda_function,
        "provider": provider,
        "agent_kwargs": dict(kwargs),
        "secrets": dict(secrets),
        "project": project,
    }

    async def begin(client: ValkyrieClient, agent_name: str) -> StartBenchmarkResponse:
        if reference is not None:
            return await client.datasets.start_run(agent_name, reference, benchmark=benchmark, **shared_options)
        assert benchmark is not None
        return await client.runs.start(
            agent_name,
            benchmark,
            dataset=dataset,
            dataset_version=dataset_version,
            service_headers=dict(headers),
            ignore_custom_services=ignore_custom_services,
            monitoring=monitoring,
            **shared_options,
        )

    async def start_runs(client: ValkyrieClient) -> list[UUID]:
        task_ids_display = ",".join(formatted_task_ids) if formatted_task_ids else None
        shown_benchmark, shown_dataset = benchmark or "", dataset
        if isinstance(reference, HostedDataset):
            shown_benchmark, shown_dataset = reference.benchmark, reference.dataset
        elif isinstance(reference, LocalDataset):
            shown_benchmark, shown_dataset = benchmark or folder_run_name(reference.path), reference.path
        format_run_start_details(shown_benchmark, shown_dataset, concurrency, slice_str, task_ids_display, priority)
        format_agent_start_details(agent, model, secrets, kwargs, dict(headers))

        agent_name = await _publish_local_agent(client, agent_dir, model, kwargs) if agent_dir else agent
        confirmed_run_ids: list[UUID] = []
        for _ in range(count):
            click.echo(f"{_CLEAR_LINE}Starting run for: {agent_name}...", nl=False)
            try:
                response = await begin(client, agent_name)
            except ValkyrieAPIError as error:
                click.echo(_CLEAR_LINE, nl=False)
                click.echo(click.style("Run failed to start!", fg="red", bold=True))
                format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=error.status_code >= 500)
                raise click.ClickException(one_line(str(error.detail))) from error
            except (ValkyrieSDKError, ValueError):
                click.echo(_CLEAR_LINE, nl=False)
                format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=True)
                raise

            click.echo(_CLEAR_LINE, nl=False)
            confirmed_run_ids.append(response.benchmark_id)
            format_start_benchmark_response(response, connect)

        format_confirmed_start_summary(confirmed_run_ids, count)
        return confirmed_run_ids

    run_ids = call(start_runs)
    if connect:
        follow_run_status(run_ids[0])


def _local_agent_dir(agent: str) -> Path | None:
    path = Path(agent)
    if agent == path.name and agent != "..":
        if path.is_dir():
            click.echo(
                click.style(
                    f"Running the published agent '{agent}'. Use --agent ./{agent} to run the local directory.",
                    fg="yellow",
                ),
                err=True,
            )
        return None
    if not path.is_dir():
        raise click.BadParameter(f"Agent directory not found: {agent}", param_hint="'--agent'")
    return path


async def _publish_local_agent(
    client: ValkyrieClient, agent_path: Path, model: str | None, kwargs: tuple[tuple[str, str], ...]
) -> str:
    contract = AgentContract.from_directory(agent_path)
    contract.validate_kwargs(dict(kwargs), model=model)
    try:
        await client.agents.push(agent_path, name=contract.name, overwrite=False)
    except ValkyrieAPIError as error:
        if error.status_code != 409:
            raise
        raise click.UsageError(
            f"A published agent named '{contract.name}' already exists. "
            "Refusing to overwrite it as a side effect of run start. "
            f"Use --agent {contract.name} to run the published release, or publish the local copy under "
            f"another name with `valkyrie agent push {agent_path} --name <new-name>` and run --agent <new-name>. "
            f"Only `valkyrie agent push {agent_path}` without --name replaces the published release."
        ) from error

    return contract.name
