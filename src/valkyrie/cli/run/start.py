from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from uuid import UUID

from typing import TYPE_CHECKING, Any

import click
from valkyrie.sdk import (
    AgentContract,
    AgentContractRequest,
    AgentReleaseSelection,
    RunMonitoring,
    StartBenchmarkResponse,
    ValkyrieAPIError,
    ValkyrieConfig,
    ValkyrieSDKError,
)
from valkyrie.sdk.models import HostedDataset, LocalDataset, folder_run_name

from valkyrie.cli.client import call, retry_unavailable
from valkyrie.cli.display import command, details, emit, fail, local_time, one_line, json_option, warn
from valkyrie.cli.project import describe_project, project_option
from valkyrie.cli.resource import validate_versioned_dataset_reference
from valkyrie.cli.run.progress import follow_run_status
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.tracker_profiles import TrackerProfiles

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient


_CLEAR_LINE = "\r\033[K"


def format_run_start_details(
    benchmark: str,
    dataset: str | None,
    concurrency: int,
    slice_str: str | None,
    task_ids: str | None,
    priority: int | None,
) -> None:
    details(
        "Benchmark",
        [
            ("Name", benchmark),
            ("Dataset", dataset or "default"),
            ("Concurrency", str(concurrency)),
            ("Priority override", str(priority) if priority is not None else None),
            ("Slice", slice_str),
            ("Task IDs", (task_ids[:60] + "..." if len(task_ids) > 60 else task_ids) if task_ids else "all tasks"),
        ],
        err=True,
    )
    click.echo(err=True)


def format_agent_start_details(
    agent: str,
    model: str | None,
    secrets: tuple[tuple[str, str], ...],
    kwargs: tuple[tuple[str, str], ...],
    header_names: Iterable[str],
) -> None:
    details(
        "Agent",
        [
            ("Agent", agent),
            ("Model", model),
            *(
                ("Secrets" if index == 0 else "", f"{env_var} → {secret_name}")
                for index, (env_var, secret_name) in enumerate(secrets)
            ),
            ("Kwargs", ", ".join(f"{key}={value}" for key, value in kwargs)),
            ("Headers", ", ".join(header_names)),
        ],
        err=True,
    )
    click.echo(err=True)


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse, follow: bool = False) -> None:
    run_id = str(start_benchmark_response.benchmark_id)
    version = start_benchmark_response.dataset_version
    project = start_benchmark_response.project
    details(
        "Run details",
        [
            ("Run ID", run_id),
            ("Tracker", TrackerProfiles.load().describe(ValkyrieConfig().url)),
            ("Started at", local_time(start_benchmark_response.started_at)),
            ("Max concurrency", str(start_benchmark_response.concurrency)),
            ("Total tasks", str(start_benchmark_response.task_count)),
            ("Dataset version", version.label or version.id if version else "not pinned"),
            ("Spend", describe_project(project) if project is not None else "untagged"),
            ("Logs", start_benchmark_response.cloudwatch_url),
            ("Artifacts", start_benchmark_response.s3_bucket_url),
        ],
        [
            ("Track progress", None if follow else command("run", "show", run_id, "--follow")),
            ("Get results", command("run", "results", run_id, "--output", f"./results-{run_id}.json")),
            ("Stop", command("run", "stop", run_id)),
            ("Resume", command("run", "resume", run_id)),
            ("Retry", command("run", "retry", run_id)),
            ("Run outputs", command("run", "download", run_id, "--output", ".")),
        ],
        err=True,
    )
    if start_benchmark_response.dataset_version_warning:
        warn(start_benchmark_response.dataset_version_warning)


def format_confirmed_start_summary(run_ids: list[UUID], count: int, outcome_unknown: bool = False) -> None:
    if count > 1:
        click.echo(err=True)
        click.echo(f"{len(run_ids)} / {count} requested runs successfully started.", err=True)
        if run_ids:
            click.echo(f"Track progress: {command('run', 'show', *map(str, run_ids))}", err=True)
    if outcome_unknown:
        warn(f"The latest start request's outcome may be unknown. Verify with `{command('run', 'list')}`.")


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
@click.option("--agent-release", default=None, help="Saved agent version. Valkyrie verifies and records its checksum.")
@click.option("--agent-sha256", default=None, help="Optional expected SHA-256 of the saved agent ZIP.")
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
    help="Comma-separated task IDs, @FILE, or an http(s) URL with one task ID per line.",
)
@click.option(
    "--task-range",
    "slice_str",
    type=str,
    required=False,
    default=None,
    help="Tasks by 0-based position as start:stop[:step], stop excluded, such as 0:10.",
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
    help="Secret as ENV_VAR SECRET_NAME. Adds to or replaces the agent contract's mapping for ENV_VAR.",
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
    "--follow",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after starting.",
)
@click.option(
    "--monitor", is_flag=True, help="Enroll the run in the external run monitor, including its network requests."
)
@click.option(
    "--count",
    type=click.IntRange(1, 10),
    default=1,
    show_default=True,
    help="Number of independent runs to start.",
)
@json_option(stream=True)
def start(
    agent: str,
    agent_release: str | None,
    agent_sha256: str | None,
    model: str | None,
    benchmark: str | None,
    concurrency: int,
    lambda_function: str | None,
    task_ids: str | None,
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
    follow: bool,
    monitor: bool,
    count: int,
    output_format: str,
) -> None:
    if count > 1 and follow:
        raise click.UsageError("--follow cannot be used when --count is greater than 1")
    if output_format == "jsonl" and not follow:
        raise click.UsageError("--jsonl requires --follow.")

    monitoring = RunMonitoring() if monitor else None
    reference = _dataset_reference(dataset, benchmark)
    if reference is not None and (dataset_version or headers or ignore_custom_services):
        raise click.UsageError(
            "--dataset-version, --header and --ignore-custom-services belong to benchmark services "
            "and cannot be used with a pushed dataset or a folder"
        )
    if reference is not None and monitoring is not None:
        raise click.UsageError("--monitor cannot be used with a pushed dataset or a folder")
    formatted_task_ids = resolve_task_ids(task_ids)
    agent_dir = _local_agent_dir(agent)
    if agent_sha256 and not agent_release:
        raise click.UsageError("--agent-sha256 requires --agent-release")
    if agent_release and agent_dir is not None:
        raise click.UsageError("Exact releases require a published agent name")
    selection = (
        AgentReleaseSelection(reference=agent_release, archive_sha256=agent_sha256, overrides=dict(kwargs))
        if agent_release
        else None
    )
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
        "agent_kwargs": {} if selection else dict(kwargs),
        "secrets": dict(secrets),
        "project": project,
    }

    async def begin(client: ValkyrieClient, agent_name: str) -> StartBenchmarkResponse:
        selected_agent = AgentContractRequest(name=agent_name, agent_release=selection) if selection else agent_name
        if reference is not None:
            return await client.datasets.start_run(selected_agent, reference, benchmark=benchmark, **shared_options)
        assert benchmark is not None
        return await client.runs.start(
            selected_agent,
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
            click.echo(f"{_CLEAR_LINE}Starting run for: {agent_name}...", nl=False, err=True)
            try:
                response = await retry_unavailable(lambda: begin(client, agent_name))
            except ValkyrieAPIError as error:
                click.echo(_CLEAR_LINE, nl=False, err=True)
                fail("Run failed to start!")
                format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=error.status_code >= 500)
                raise click.ClickException(one_line(str(error.detail))) from error
            except (ValkyrieSDKError, ValueError):
                click.echo(_CLEAR_LINE, nl=False, err=True)
                format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=True)
                raise

            click.echo(_CLEAR_LINE, nl=False, err=True)
            confirmed_run_ids.append(response.benchmark_id)
            format_start_benchmark_response(response, follow)

        format_confirmed_start_summary(confirmed_run_ids, count)
        return confirmed_run_ids

    run_ids = call(start_runs)
    emit({"run_ids": run_ids}, output_format)
    if follow:
        follow_run_status(run_ids[0], output_format="jsonl" if output_format == "jsonl" else "text")


def _local_agent_dir(agent: str) -> Path | None:
    path = Path(agent)
    if agent == path.name and agent != "..":
        if path.is_dir():
            warn(f"Running the published agent '{agent}'. Use --agent ./{agent} to run the local directory.")
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
