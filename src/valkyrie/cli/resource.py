from collections.abc import Callable, Sequence
from typing import Generic, TypeVar

import click
from pydantic import BaseModel
from valkyrie.sdk.models import ResourceResponse, validate_name, validate_task_id

from valkyrie.cli.display import format_table, local_time, one_line
from valkyrie.display import terminal_safe


T = TypeVar("T")


class _Validated(click.types.StringParamType, Generic[T]):
    def __init__(self, validate: Callable[[str], T]) -> None:
        super().__init__()
        self._validate = validate

    def convert(self, value: object, param: click.Parameter | None, ctx: click.Context | None) -> T:
        text = super().convert(value, param, ctx).strip()
        try:
            return self._validate(text)
        except ValueError as error:
            self.fail(str(error), param, ctx)


def validate_dataset_reference(reference: str) -> str:
    benchmark, separator, dataset = reference.partition("/")
    if not separator:
        raise ValueError("must be BENCHMARK/DATASET, such as swe-bench/verified")
    for label, name in (("benchmark", benchmark), ("dataset", dataset)):
        try:
            validate_name(name)
        except ValueError as error:
            raise ValueError(f"the {label} {error}") from error
    return reference


def validate_versioned_dataset_reference(reference: str) -> tuple[str, str, str]:
    dataset, separator, version = reference.partition("@")
    validate_dataset_reference(dataset)
    if separator and not version:
        raise ValueError("write a version after @, such as swe-bench/verified@2")
    benchmark, _, name = dataset.partition("/")
    return benchmark, name, version or "latest"


INCLUDE_ARCHIVED = click.option(
    "--include-archived", is_flag=True, help="Also list archived ones, with when each was archived."
)
RESOURCE_NAME = _Validated(validate_name)
DATASET_REFERENCE = _Validated(validate_dataset_reference)
VERSIONED_DATASET_REFERENCE = _Validated(validate_versioned_dataset_reference)
TASK_ID = _Validated(validate_task_id)


def list_resources(
    response: BaseModel, entries: Sequence[ResourceResponse], output_format: str, item_name: str
) -> None:
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    columns = ["Name", "Description", "Updated"]
    rows = [
        {
            "Name": one_line(entry.name),
            "Description": one_line(entry.description),
            "Updated": local_time(entry.updated_at),
        }
        for entry in entries
    ]
    if any(entry.archived_at for entry in entries):
        columns.append("Archived")
        for row, entry in zip(rows, entries, strict=True):
            row["Archived"] = local_time(entry.archived_at) if entry.archived_at else ""
    format_table(rows, columns, item_name=item_name)


def show_resource(resource: ResourceResponse, output_format: str, *heading: str) -> None:
    if output_format == "json":
        click.echo(resource.model_dump_json())
        return
    created_by = f" by {one_line(resource.created_by_email)}" if resource.created_by_email else ""
    for line in heading:
        click.echo(line)
    click.echo(f"Name:        {one_line(resource.name)}")
    click.echo(f"Description: {terminal_safe(resource.description, preserve_newlines=True)}")
    click.echo(f"Created:     {local_time(resource.created_at)}{created_by}")
    click.echo(f"Updated:     {local_time(resource.updated_at)}")
    if resource.archived_at:
        click.echo(f"Archived:    {local_time(resource.archived_at)}")
