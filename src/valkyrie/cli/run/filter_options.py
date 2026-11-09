import click

from valkyrie.cli.client import call
from valkyrie.cli.display import format_option, one_line


@click.command(
    name="filter-options", help="List benchmark, agent, model, dataset, and starter values from your run history."
)
@format_option("Print as text or JSON.")
def filter_options(output_format: str) -> None:
    response = call(lambda client: client.runs.filter_options())
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    for field in type(response).model_fields:
        values: list[str] = getattr(response, field)
        click.echo(f"{field}: {one_line(', '.join(values))}")
