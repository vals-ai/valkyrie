from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, get_args

import click
from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieDatasetPushError
from valkyrie.sdk.models import (
    BlobResponse,
    DatasetLocation,
    DatasetResponse,
    DatasetVersionDetailResponse,
)

from valkyrie.cli.client import call
from valkyrie.cli.display import (
    emit,
    json_option,
    format_table,
    local_time,
    one_line,
    confirm,
    console,
    done,
    fail,
    warn,
)
from valkyrie.cli.resource import (
    DATASET_REFERENCE,
    INCLUDE_ARCHIVED,
    RESOURCE_NAME,
    TASK_ID,
    VERSIONED_DATASET_REFERENCE,
    list_resources,
    show_resource,
)

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient


@click.group(help="Publish, version and manage the datasets in your benchmarks.")
def dataset() -> None:
    pass


@dataset.command(
    name="create",
    help=(
        "Create a dataset in a benchmark.\n\n"
        "BENCHMARK must already exist. `valkyrie dataset push` creates a missing one. NAME is lowercase letters and "
        "digits with single '.', '_' or '-' separators, such as verified.\n\n"
        "Example:\n"
        '    valkyrie dataset create swe-bench verified --description "Human-verified issues"'
    ),
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--description", default="", help="What the dataset holds.")
@json_option()
def create_dataset(benchmark: str, name: str, description: str, output_format: str) -> None:
    created = call(lambda client: client.datasets.create(benchmark, name, description=description))
    if emit(created, output_format):
        return
    done(f"Dataset created: {one_line(created.benchmark)}/{one_line(created.name)}")


@dataset.command(name="list", help="List the datasets in a benchmark.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@INCLUDE_ARCHIVED
@json_option()
def list_datasets(benchmark: str, include_archived: bool, output_format: str) -> None:
    response = call(lambda client: client.datasets.list(benchmark, include_archived=include_archived))
    list_resources(response, response.datasets, output_format, "dataset")


@dataset.command(
    name="show",
    help="Show one dataset, archived or not, with its latest version and the dataset.toml it was published with.",
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@json_option()
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
    manifest = latest.manifest if latest is not None else None
    show_resource(
        found,
        output_format,
        "Dataset",
        before=[("Benchmark", one_line(found.benchmark))],
        after=[
            (
                "Version",
                f"@{latest.revision} ({latest.digest.removeprefix('sha256:')[:12]}), {latest.task_count} task(s), "
                f"tags {one_line(', '.join(latest.tags))}"
                if latest is not None
                else "none published",
            ),
            *(
                [
                    (
                        "Source",
                        one_line(manifest.name) + (f" {one_line(manifest.version)}" if manifest.version else ""),
                    ),
                    (
                        "Authors",
                        one_line(", ".join(f"{a.name} <{a.email}>" if a.email else a.name for a in manifest.authors)),
                    ),
                    ("Keywords", one_line(", ".join(manifest.keywords))),
                    (
                        "Metadata",
                        one_line(json.dumps(manifest.metadata, sort_keys=True)) if manifest.metadata else None,
                    ),
                ]
                if manifest is not None
                else []
            ),
        ],
    )


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
@json_option()
def update_dataset(benchmark: str, name: str, description: str, output_format: str) -> None:
    updated = call(lambda client: client.datasets.update(benchmark, name, description=description))
    if emit(updated, output_format):
        return
    done(f"Dataset updated: {one_line(updated.benchmark)}/{one_line(updated.name)}")


@dataset.command(
    name="remove",
    help="Archive a dataset. It stays readable, keeps its name, and can be restored.",
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
@json_option()
def remove_dataset(benchmark: str, name: str, yes: bool, output_format: str) -> None:
    if not confirm(f"Archive dataset '{benchmark}/{name}'?", yes=yes):
        click.echo("Cancelled.", err=True)
        return
    call(lambda client: client.datasets.remove(benchmark, name))
    if emit({"archived": f"{benchmark}/{name}"}, output_format):
        return
    done(f"Dataset '{benchmark}/{name}' archived.")


@dataset.command(name="restore", help="Restore an archived dataset so its tasks can change again.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@json_option()
def restore_dataset(benchmark: str, name: str, output_format: str) -> None:
    restored = call(lambda client: client.datasets.restore(benchmark, name))
    if emit(restored, output_format):
        return
    done(f"Dataset restored: {one_line(restored.benchmark)}/{one_line(restored.name)}")


def _publish(
    dataset_reference: str,
    tags: tuple[str, ...],
    concurrency: int,
    source: Path | tuple[DatasetLocation, str],
    output_format: str,
) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    uploaded: list[str] = []
    progress = console(err=True).status("Preparing tasks")

    def report(task_id: str, blob: BlobResponse) -> None:
        uploaded.append(task_id)
        progress.update(f"Uploaded {len(uploaded)} task(s)")
        for warning in blob.warnings:
            warn(f"{one_line(task_id)}: {one_line(warning)}")

    def announce(created: str) -> None:
        done(f"Created {one_line(created)}")

    def note(message: str) -> None:
        warn(one_line(message))

    def left_out(task_id: str, entries: list[str]) -> None:
        names = ", ".join(one_line(entry) for entry in entries)
        warn(f"{one_line(task_id)}: not stored because they are not part of a task: {names}")

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
                    on_left_out=left_out,
                    on_warning=note,
                )
            location, name = source
            click.echo(f"Downloading {one_line(name)} from {location.replace('_', ' ')}...", err=True)
            return await client.datasets.install(
                benchmark,
                dataset_name,
                name,
                location=location,
                tags=tags,
                concurrency=concurrency,
                on_stored=report,
                on_created=announce,
                on_left_out=left_out,
                on_warning=note,
            )
        except ValkyrieDatasetPushError as error:
            for task_id, message in error.failed.items():
                fail(f"{one_line(task_id)}: {one_line(message)}")
            raise click.ClickException(
                f"{len(error.failed)} of {len(error.stored) + len(error.failed)} uploads failed and nothing was "
                "published; run the command again to retry them"
            ) from error

    with progress:
        version = call(push)
    if emit(version, output_format):
        return
    done(
        f"Published {benchmark}/{dataset_name}@{version.revision}: {version.task_count} task(s), "
        f"{len(uploaded)} uploaded{', with metric.py' if version.metric_digest else ''}. Tags: {', '.join(version.tags)}."
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
@json_option()
def push_dataset(
    dataset_reference: str, folder: Path, tags: tuple[str, ...], concurrency: int, output_format: str
) -> None:
    _publish(dataset_reference, tags, concurrency, folder, output_format)


@dataset.command(
    name="check",
    help=(
        "List every way the Harbor dataset under FOLDER departs from the porting standard, without uploading it.\n\n"
        "Example:\n"
        "    valkyrie dataset check out/a2j --details sample/details.json"
    ),
)
@click.argument("folder", metavar="FOLDER", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--details",
    "details",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="A `details.json` a grader wrote, checked against the score types. Repeatable.",
)
@json_option()
def check_dataset(folder: Path, details: tuple[Path, ...], output_format: str) -> None:
    from valkyrie.sdk.harbor_dataset import dataset_problems

    problems = dataset_problems(folder, details)
    if emit({"folder": str(folder), "problems": problems}, output_format):
        if problems:
            raise SystemExit(1)
        return
    for problem in problems:
        fail(one_line(problem))
    if problems:
        raise click.ClickException(f"{len(problems)} problem(s) found in {folder}")
    done(f"{folder} follows the Harbor dataset standard")


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
@json_option()
def install_dataset(
    dataset_reference: str,
    source: str,
    location: DatasetLocation,
    tags: tuple[str, ...],
    concurrency: int,
    output_format: str,
) -> None:
    _publish(dataset_reference, tags, concurrency, (location, source), output_format)


@dataset.command(name="versions", help="List the versions of a dataset, newest first, with the tags on each.")
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@json_option()
def list_versions(dataset_reference: str, output_format: str) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    response = call(lambda client: client.datasets.versions.list(benchmark, dataset_name))
    if output_format == "json":
        emit(response)
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
@json_option()
def tag_version(dataset_reference: tuple[str, str, str], tag_name: str, force: bool, output_format: str) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    tagged = call(lambda client: client.datasets.versions.tag(benchmark, dataset_name, tag_name, version_ref, force))
    if emit(tagged, output_format):
        return
    done(f"Tagged {benchmark}/{dataset_name}@{tagged.revision} as {one_line(tag_name)}")


@dataset.command(name="untag", help="Remove a tag from a dataset. The latest tag can't be removed.")
@click.argument("dataset_reference", metavar="BENCHMARK/DATASET", type=DATASET_REFERENCE)
@click.argument("tag_name", metavar="TAG")
@json_option()
def untag_version(dataset_reference: str, tag_name: str, output_format: str) -> None:
    benchmark, _, dataset_name = dataset_reference.partition("/")
    removed = call(lambda client: client.datasets.versions.untag(benchmark, dataset_name, tag_name))
    if emit(removed, output_format):
        return
    done(f"Removed tag {one_line(tag_name)} from {benchmark}/{dataset_name}@{removed.revision}")


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
@json_option()
def list_tasks(dataset_reference: tuple[str, str, str], output_format: str) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    tasks = call(lambda client: client.datasets.versions.tasks(benchmark, dataset_name, version_ref))
    if output_format == "json":
        emit(tasks)
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
@json_option()
def download_task(
    dataset_reference: tuple[str, str, str], task_id: str, output: Path | None, overwrite: bool, output_format: str
) -> None:
    benchmark, dataset_name, version_ref = dataset_reference
    destination = output or Path(f"{task_id}.zip")
    saved = call(
        lambda client: client.datasets.versions.download(
            benchmark, dataset_name, task_id, destination, ref=version_ref, overwrite=overwrite
        )
    )
    if emit({"path": saved}, output_format):
        return
    done(f"Task downloaded: {saved}")
