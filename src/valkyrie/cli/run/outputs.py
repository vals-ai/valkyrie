from __future__ import annotations

import tarfile
import tempfile
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO
from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import emit, json_option, format_table, one_line, short_local_time, done
from valkyrie.cli.run.task_ids import resolve_task_ids

if TYPE_CHECKING:
    from valkyrie.sdk import ValkyrieClient


@click.command(
    name="download",
    help=(
        "Download a run's outputs or a file or folder by its path.\n"
        "\n"
        "Example:\n"
        "    valkyrie run download 123e4567-e89b-12d3-a456-426614174000 -o ./run_outputs"
    ),
)
@click.argument("run_id", type=UUID)
@click.argument("path", required=False)
@click.option(
    "-o",
    "--output",
    "output_dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save run outputs (defaults to ./<benchmark>_<agent>_<run-id>).",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated task IDs, @FILE, or an http(s) URL with one task ID per line.",
)
@click.option("--attempts", is_flag=True, help="Use artifacts kept from task attempts replaced by a retry.")
@json_option()
def download(
    run_id: UUID, path: str | None, output_dir: Path | None, task_ids: str | None, attempts: bool, output_format: str
):
    selected_task_ids = resolve_task_ids(task_ids)
    if path is not None or attempts:
        destination = output_dir or Path(str(run_id))

        async def download_artifacts(client: ValkyrieClient) -> Path:
            if not selected_task_ids:
                return await client.artifacts.download(run_id, destination, path=path or "", attempts=attempts)
            if destination.exists():
                raise click.ClickException(f"Output directory already exists: {destination}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
                staging = Path(temporary) / "selected"
                for index, task_id in enumerate(selected_task_ids):
                    prefix = path or ""
                    if prefix and not (prefix == task_id or prefix.startswith(task_id + "/")):
                        continue
                    downloaded = await client.artifacts.download(
                        run_id, Path(temporary) / str(index), path=prefix or task_id, attempts=attempts
                    )
                    shutil.copytree(downloaded, staging, dirs_exist_ok=True)
                if not staging.exists():
                    raise click.ClickException("No artifacts found for the selected task IDs and path.")
                staging.rename(destination)
            return destination

        result = call(download_artifacts)
        if emit({"run_id": run_id, "path": result}, output_format):
            return
        done(f"Run artifacts downloaded to: {result}")
        return

    async def download(client: ValkyrieClient) -> Path:
        metadata = await client.runs.metadata(run_id)
        contract_name = metadata.benchmark_arguments.contract.name
        destination = output_dir or Path(f"{metadata.benchmark_name}_{contract_name}_{metadata.benchmark_id}")
        click.echo(f"\r\033[KFetching run outputs for run {run_id}...", nl=False, err=True)
        with tempfile.TemporaryFile() as archive:
            click.echo("\r\033[KDownloading...", nl=False, err=True)
            async for chunk in client.runs.stream_outputs(run_id, task_ids=selected_task_ids):
                archive.write(chunk)
            archive.seek(0)
            extract_run_outputs(archive, destination)
        return destination

    destination = call(download)
    if emit({"run_id": run_id, "path": destination}, output_format):
        return
    done(f"Run outputs extracted to: {destination}")


@click.command(name="files", help="List one page of artifact paths available within a run.")
@click.argument("run_id", type=UUID)
@click.argument("prefix", default="", required=False)
@click.option("--cursor", default=None, help="Continue from a previous next_cursor.")
@click.option(
    "--limit", type=click.IntRange(1, 1000), default=100, show_default=True, help="Most artifact paths to list."
)
@click.option("--attempts", is_flag=True, help="Use artifacts kept from task attempts replaced by a retry.")
@json_option()
def files(run_id: UUID, prefix: str, cursor: str | None, limit: int, attempts: bool, output_format: str) -> None:
    response = call(
        lambda client: client.artifacts.list(run_id, prefix=prefix, cursor=cursor, limit=limit, attempts=attempts)
    )
    if output_format == "json":
        emit(response)
        return
    format_table(
        [
            {
                "Path": one_line(entry.path),
                "Bytes": str(entry.size),
                "Modified": short_local_time(entry.last_modified) if entry.last_modified else "-",
            }
            for entry in response.artifacts
        ],
        ["Path", "Bytes", "Modified"],
        item_name="artifact",
    )
    if response.next_cursor is not None:
        click.echo(f"Next cursor: {one_line(response.next_cursor)}")


def extract_run_outputs(archive: BinaryIO, output_dir: Path) -> None:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"\r\033[KExtracting archives to {output_dir}...", nl=False, err=True)
    with tarfile.open(fileobj=archive, mode="r") as tar:
        tar.extractall(output_dir, filter="data")

    nested_tars = list(output_dir.rglob("*.tar.gz"))
    if nested_tars:
        click.echo(f"\r\033[KUnpacking {len(nested_tars)} nested tar.gz files...", nl=False, err=True)
    for nested_tar in nested_tars:
        extract_dir = nested_tar.parent / nested_tar.stem.replace(".tar", "")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(nested_tar, "r:gz") as tar:
            tar.extractall(extract_dir, filter="data")
        nested_tar.unlink()
