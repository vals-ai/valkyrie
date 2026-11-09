from pathlib import Path

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, format_table, local_time, one_line
from valkyrie.cli.resource import (
    INCLUDE_ARCHIVED,
    RESOURCE_NAME,
    TASK_ID,
    list_resources,
    show_resource,
)


@click.group(help="Create, list, show, update and remove the datasets that belong to your benchmarks.")
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


@dataset.command(name="show", help="Show one dataset, archived or not.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@format_option()
def show_dataset(benchmark: str, name: str, output_format: str) -> None:
    found = call(lambda client: client.datasets.get(benchmark, name))
    show_resource(found, output_format, f"Benchmark:   {one_line(found.benchmark)}")


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


@dataset.group(name="task", help="Store, list, download and remove the task archives of a dataset.")
def task() -> None:
    pass


@task.command(
    name="put",
    help=(
        "Store a task's .zip archive in a dataset, replacing the task if it exists.\n\n"
        "TASK_ID starts with a letter or digit and uses letters, digits, '.', '_' and '-'.\n\n"
        "Example:\n"
        "    valkyrie dataset task put swe-bench verified django__django-11099 ./django__django-11099.zip"
    ),
)
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("dataset_name", metavar="DATASET", type=RESOURCE_NAME)
@click.argument("task_id", metavar="TASK_ID", type=TASK_ID)
@click.argument("archive", metavar="ARCHIVE", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def put_task(benchmark: str, dataset_name: str, task_id: str, archive: Path) -> None:
    stored = call(lambda client: client.datasets.tasks.put(benchmark, dataset_name, task_id, archive))
    click.echo(
        click.style(
            f"✓ Task stored: {benchmark}/{dataset_name}/{one_line(stored.task_id)} ({stored.size:,} bytes)",
            fg="green",
            bold=True,
        )
    )


@task.command(name="list", help="List the tasks stored in a dataset.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("dataset_name", metavar="DATASET", type=RESOURCE_NAME)
@format_option()
def list_tasks(benchmark: str, dataset_name: str, output_format: str) -> None:
    response = call(lambda client: client.datasets.tasks.list_all(benchmark, dataset_name))
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    format_table(
        [
            {
                "Task": one_line(entry.task_id),
                "Bytes": f"{entry.size:,}",
                "Updated": local_time(entry.last_modified) if entry.last_modified else "",
            }
            for entry in response.tasks
        ],
        ["Task", "Bytes", "Updated"],
        item_name="task",
    )


@task.command(name="download", help="Download a task's archive. It is saved as TASK_ID.zip unless you pass --output.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("dataset_name", metavar="DATASET", type=RESOURCE_NAME)
@click.argument("task_id", metavar="TASK_ID", type=TASK_ID)
@click.option("--output", "-o", type=click.Path(dir_okay=False, path_type=Path), help="Where to save the archive.")
@click.option("--overwrite", is_flag=True, help="Replace the file if it exists.")
def download_task(benchmark: str, dataset_name: str, task_id: str, output: Path | None, overwrite: bool) -> None:
    destination = output or Path(f"{task_id}.zip")
    saved = call(
        lambda client: client.datasets.tasks.download(
            benchmark, dataset_name, task_id, destination, overwrite=overwrite
        )
    )
    click.echo(click.style(f"✓ Task downloaded: {saved}", fg="green", bold=True))


@task.command(name="remove", help="Delete a task's archive from a dataset.")
@click.argument("benchmark", metavar="BENCHMARK", type=RESOURCE_NAME)
@click.argument("dataset_name", metavar="DATASET", type=RESOURCE_NAME)
@click.argument("task_id", metavar="TASK_ID", type=TASK_ID)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def remove_task(benchmark: str, dataset_name: str, task_id: str, yes: bool) -> None:
    if not yes and not click.confirm(f"Delete task '{benchmark}/{dataset_name}/{task_id}'?"):
        click.echo("Cancelled.")
        return
    call(lambda client: client.datasets.tasks.remove(benchmark, dataset_name, task_id))
    click.echo(click.style(f"✓ Task '{benchmark}/{dataset_name}/{task_id}' removed.", fg="green", bold=True))
