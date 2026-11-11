import json
from pathlib import Path
from typing import get_args

import click
from pydantic import TypeAdapter
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieDatasetPushError
from valkyrie.sdk.models import (
    BlobResponse,
    DatasetLocation,
    DatasetResponse,
    DatasetTaskResponse,
    DatasetVersionDetailResponse,
)

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, format_table, local_time, one_line
from valkyrie.cli.resource import (
    DATASET_REFERENCE,
    INCLUDE_ARCHIVED,
    RESOURCE_NAME,
    TASK_ID,
    VERSIONED_DATASET_REFERENCE,
    list_resources,
    show_resource,
)


@click.group(help="Create, list, show, update, push, install and remove the datasets that belong to your benchmarks.")
def dataset() -> None:
    pass


@dataset.command(
    name="create",
    help=(
        "Create a dataset in a benchmark.\n\n"
        "BENCHMARK is a benchmark you created with `valkyrie benchmark create`. NAME is lowercase letters and "
        "digits with single '.', '_' or '-' separators, such as verified.\n\n"
        "Example:\n"
        '    valkyrie dataset create swe-bench verified --description "Human-verified issues"'
    ),
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--description", default="", help="What the dataset holds.")
def create_dataset(benchmark: str, name: str, description: str) -> None:
    created = call(lambda client: client.datasets.create(benchmark, name, description=description))
    click.echo(
        click.style(f"✓ Dataset created: {one_line(created.benchmark)}/{one_line(created.name)}", fg="green", bold=True)
    )


@dataset.command(name="list", help="List the datasets in a benchmark.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@INCLUDE_ARCHIVED
@format_option()
def list_datasets(benchmark: str, include_archived: bool, output_format: str) -> None:
    response = call(lambda client: client.datasets.list(benchmark, include_archived=include_archived))
    list_resources(response, response.datasets, output_format, "dataset")


@dataset.command(
    name="show",
    help="Show one dataset, archived or not, with its latest version and the dataset.toml it was published with.",
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@format_option()
def show_dataset(benchmark: str, name: str, output_format: str) -> None:
    async def fetch(client: ValkyrieClient) -> tuple[DatasetResponse, DatasetVersionDetailResponse | None]:
        found = await client.datasets.get(benchmark, name)
        if output_format == "json":
            return found, None
        try:
            return found, await client.datasets.versions.get(benchmark, name)
        except ValkyrieAPIError as error:
            if error.status_code != 404:
                raise
            return found, None

    found, latest = call(fetch)
    show_resource(found, output_format, f"Benchmark:   {one_line(found.benchmark)}")
    if output_format == "json":
        return
    if latest is None:
        click.echo("Version:     none published")
        return
    click.echo(
        f"Version:     @{latest.revision} ({latest.digest.removeprefix('sha256:')[:12]}), {latest.task_count} task(s), "
        f"tags {one_line(', '.join(latest.tags))}"
    )
    if (manifest := latest.manifest) is None:
        return
    click.echo(
        f"Source:      {one_line(manifest.name)}" + (f" {one_line(manifest.version)}" if manifest.version else "")
    )
    if manifest.authors:
        click.echo(
            "Authors:     "
            + one_line(", ".join(f"{a.name} <{a.email}>" if a.email else a.name for a in manifest.authors))
        )
    if manifest.keywords:
        click.echo(f"Keywords:    {one_line(', '.join(manifest.keywords))}")
    if manifest.metadata:
        click.echo(f"Metadata:    {one_line(json.dumps(manifest.metadata, sort_keys=True))}")


@dataset.command(
    name="update",
    help=(
        "Change a dataset's description. Its name never changes.\n\n"
        "Example:\n"
        '    valkyrie dataset update swe-bench verified --description "SWE-bench Verified"'
    ),
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--description", required=True, help="The description the dataset now has.")
def update_dataset(benchmark: str, name: str, description: str) -> None:
    updated = call(lambda client: client.datasets.update(benchmark, name, description=description))
    click.echo(
        click.style(f"✓ Dataset updated: {one_line(updated.benchmark)}/{one_line(updated.name)}", fg="green", bold=True)
    )


@dataset.command(
    name="remove",
    help="Archive a dataset. It stays readable, keeps its name, and can be restored.",
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def remove_dataset(benchmark: str, name: str, yes: bool) -> None:
    if not yes and not click.confirm(f"Archive dataset '{benchmark}/{name}'?"):
        click.echo("Cancelled.")
        return
    call(lambda client: client.datasets.remove(benchmark, name))
    click.echo(click.style(f"✓ Dataset '{benchmark}/{name}' archived.", fg="green", bold=True))


@dataset.command(name="restore", help="Restore an archived dataset so its tasks can change again.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
def restore_dataset(benchmark: str, name: str) -> None:
    restored = call(lambda client: client.datasets.restore(benchmark, name))
    click.echo(
        click.style(
            f"✓ Dataset restored: {one_line(restored.benchmark)}/{one_line(restored.name)}", fg="green", bold=True
        )
    )


def _publish(
    dataset_reference: str,
    tags: tuple[str, ...],
    concurrency: int,
    source: Path | tuple[DatasetLocation, str],
) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    uploaded: list[str] = []

    def report(task_id: str, blob: BlobResponse) -> None:
        uploaded.append(task_id)
        click.echo(f"✓ Uploaded {one_line(task_id)} ({blob.size:,} bytes)")
        for warning in blob.warnings:
            click.secho(f"! {one_line(task_id)}: {one_line(warning)}", fg="yellow")

    def announce(created: str) -> None:
        click.echo(f"✓ Created {one_line(created)}")

    def note(message: str) -> None:
        click.secho(f"! {one_line(message)}", fg="yellow")

    def warn(task_id: str, entries: list[str]) -> None:
        names = ", ".join(one_line(entry) for entry in entries)
        click.secho(f"! {one_line(task_id)}: not stored because they are not part of a task: {names}", fg="yellow")

    async def push(client: ValkyrieClient) -> DatasetVersionDetailResponse:
        try:
            if isinstance(source, Path):
                return await client.datasets.push(
                    benchmark,
                    dataset_name,
                    source,
                    tags=tags,
                    concurrency=concurrency,
                    on_stored=report,
                    on_created=announce,
                    on_left_out=warn,
                    on_warning=note,
                )
            location, name = source
            click.echo(f"Downloading {one_line(name)} from {location.replace('_', ' ')}...")
            return await client.datasets.install(
                benchmark,
                dataset_name,
                name,
                location=location,
                tags=tags,
                concurrency=concurrency,
                on_stored=report,
                on_created=announce,
                on_left_out=warn,
                on_warning=note,
            )
        except ValkyrieDatasetPushError as error:
            for task_id, message in error.failed.items():
                click.echo(click.style(f"✗ {one_line(task_id)}: {one_line(message)}", fg="red"), err=True)
            raise click.ClickException(
                f"{len(error.failed)} of {len(error.stored) + len(error.failed)} uploads failed and nothing was "
                "published; run the command again to retry them"
            ) from error

    version = call(push)
    click.echo(
        click.style(
            f"✓ Published {benchmark}/{dataset_name}@{version.revision}: {version.task_count} task(s), "
            f"{len(uploaded)} uploaded{', with metric.py' if version.metric_digest else ''}. Tags: {', '.join(version.tags)}.",
            fg="green",
            bold=True,
        )
    )


_TAGS = click.option("--tag", "tags", multiple=True, help="Also tag the version. Repeatable.")
_CONCURRENCY = click.option(
    "--concurrency", type=click.IntRange(min=1), default=4, show_default=True, help="Tasks to upload at once"
)


@dataset.command(
    name="push",
    help=("Publish the task folders under FOLDER as a new version of a dataset. Only new tasks are uploaded."),
)
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@click.argument("folder", metavar="FOLDER", type=click.Path(exists=True, file_okay=False, path_type=Path))
@_TAGS
@_CONCURRENCY
def push_dataset(dataset_reference: str, folder: Path, tags: tuple[str, ...], concurrency: int) -> None:
    _publish(dataset_reference, tags, concurrency, folder)


@dataset.command(
    name="install",
    help=(
        "Download a dataset from Harbor, GitHub or Hugging Face and publish it as a new version.\n\n"
        "Example:\n"
        "    valkyrie dataset install tb/tb2 terminal-bench/terminal-bench-2 --location harbor"
    ),
)
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@click.argument("source", metavar="SOURCE")
@click.option(
    "--location",
    type=click.Choice(get_args(DatasetLocation)),
    required=True,
    help="Where SOURCE is.",
)
@_TAGS
@_CONCURRENCY
def install_dataset(
    dataset_reference: str, source: str, location: DatasetLocation, tags: tuple[str, ...], concurrency: int
) -> None:
    _publish(dataset_reference, tags, concurrency, (location, source))


@dataset.command(name="versions", help="List the versions of a dataset, newest first, with the tags on each.")
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@format_option()
def list_versions(dataset_reference: str, output_format: str) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    response = call(lambda client: client.datasets.versions.list(benchmark, dataset_name))
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    format_table(
        [
            {
                "Version": str(entry.revision),
                "Digest": entry.digest.removeprefix("sha256:")[:12],
                "Tasks": str(entry.task_count),
                "Tags": one_line(", ".join(entry.tags)),
                "Published": local_time(entry.created_at),
            }
            for entry in response.versions
        ],
        ["Version", "Digest", "Tasks", "Tags", "Published"],
        item_name="version",
    )


@dataset.command(
    name="tag",
    help=(
        "Point a tag at a version of a dataset. The version is the latest one unless "
        "the reference ends in @ and a revision number, a tag or a digest.\n\n"
        "A tag that points at another version moves only with --force. The latest tag always moves.\n\n"
        "A tag is lowercase letters, digits, hyphens and dots, starts with a letter or digit, and can't be a whole "
        "number or start with sha256:.\n\n"
        "Example:\n"
        "    valkyrie dataset tag swe-bench/verified@2 stable"
    ),
)
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET[@VERSION]", type=VERSIONED_DATASET_REFERENCE)
@click.argument("tag_name", metavar="TAG")
@click.option("--force", is_flag=True, help="Move the tag when it already points at another version.")
def tag_version(dataset_reference: tuple[str, str, str], tag_name: str, force: bool) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    tagged = call(lambda client: client.datasets.versions.tag(benchmark, dataset_name, tag_name, version_ref, force))
    click.echo(
        click.style(
            f"✓ Tagged {benchmark}/{dataset_name}@{tagged.revision} as {one_line(tag_name)}", fg="green", bold=True
        )
    )


@dataset.command(name="untag", help="Remove a tag from a dataset. The latest tag can't be removed.")
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@click.argument("tag_name", metavar="TAG")
def untag_version(dataset_reference: str, tag_name: str) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    removed = call(lambda client: client.datasets.versions.untag(benchmark, dataset_name, tag_name))
    click.echo(
        click.style(
            f"✓ Removed tag {one_line(tag_name)} from {benchmark}/{dataset_name}@{removed.revision}",
            fg="green",
            bold=True,
        )
    )


@dataset.group(name="task", help="List and download the tasks of a dataset version")
def task() -> None:
    pass


@task.command(
    name="list",
    help=(
        "List the tasks of a dataset version: the latest one, or the version after @, which is a revision number, "
        "a tag or a digest.\n\n"
        "Example:\n"
        "    valkyrie dataset task list swe-bench/verified@2"
    ),
)
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET[@VERSION]", type=VERSIONED_DATASET_REFERENCE)
@format_option()
def list_tasks(dataset_reference: tuple[str, str, str], output_format: str) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    tasks = call(lambda client: client.datasets.versions.tasks(benchmark, dataset_name, version_ref))
    if output_format == "json":
        click.echo(TypeAdapter(list[DatasetTaskResponse]).dump_json(tasks).decode())
        return
    format_table(
        [
            {
                "Task": one_line(entry.task_id),
                "Digest": entry.digest.removeprefix("sha256:")[:12],
                "Bytes": f"{entry.size:,}",
            }
            for entry in tasks
        ],
        ["Task", "Digest", "Bytes"],
        item_name="task",
    )


@task.command(
    name="download",
    help=(
        "Download a task's archive from the latest version of a dataset, or from the version after @. It is saved as "
        "TASK_ID.zip unless you pass --output."
    ),
)
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET[@VERSION]", type=VERSIONED_DATASET_REFERENCE)
@click.argument("task_id", metavar="TASK_ID", type=TASK_ID)
@click.option("--output", "-o", type=click.Path(dir_okay=False, path_type=Path), help="Where to save the archive.")
@click.option("--overwrite", is_flag=True, help="Replace the file if it exists.")
def download_task(dataset_reference: tuple[str, str, str], task_id: str, output: Path | None, overwrite: bool) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    destination = output or Path(f"{task_id}.zip")
    saved = call(
        lambda client: client.datasets.versions.download(
            benchmark, dataset_name, task_id, destination, ref=version_ref, overwrite=overwrite
        )
    )
    click.echo(click.style(f"✓ Task downloaded: {saved}", fg="green", bold=True))
