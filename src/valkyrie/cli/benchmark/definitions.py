import click

from valkyrie.cli.client import call
from valkyrie.cli.resource import INCLUDE_ARCHIVED, RESOURCE_NAME, list_resources, show_resource
from valkyrie.cli.display import format_option, one_line


@click.command(
    name="create",
    help=(
        "Create a benchmark that datasets can belong to.\n\n"
        "NAME is lowercase letters and digits with single '.', '_' or '-' separators, such as swe-bench.\n\n"
        "Example:\n"
        '    valkyrie benchmark create swe-bench --description "SWE-bench tasks"'
    ),
)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--description", default="", help="What the benchmark covers.")
def create_benchmark(name: str, description: str) -> None:
    created = call(lambda client: client.benchmark_definitions.create(name, description=description))
    click.echo(click.style(f"✓ Benchmark created: {one_line(created.name)}", fg="green", bold=True))


@click.command(name="list", help="List your organization's benchmarks.")
@INCLUDE_ARCHIVED
@format_option()
def list_benchmarks(include_archived: bool, output_format: str) -> None:
    response = call(lambda client: client.benchmark_definitions.list(include_archived=include_archived))
    list_resources(response, response.benchmarks, output_format, "benchmark")


@click.command(name="show", help="Show one benchmark, archived or not.")
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@format_option()
def show_benchmark(name: str, output_format: str) -> None:
    found = call(lambda client: client.benchmark_definitions.get(name))
    show_resource(found, output_format)


@click.command(
    name="update",
    help=(
        "Change a benchmark's description. Its name never changes.\n\n"
        "Example:\n"
        '    valkyrie benchmark update swe-bench --description "SWE-bench family"'
    ),
)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--description", required=True, help="The description the benchmark now has.")
def update_benchmark(name: str, description: str) -> None:
    updated = call(lambda client: client.benchmark_definitions.update(name, description=description))
    click.echo(click.style(f"✓ Benchmark updated: {one_line(updated.name)}", fg="green", bold=True))


@click.command(
    name="remove",
    help="Archive a benchmark once it has no active datasets. It stays readable, keeps its name, and can be restored.",
)
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def remove_benchmark(name: str, yes: bool) -> None:
    if not yes and not click.confirm(f"Archive benchmark '{name}'?"):
        click.echo("Cancelled.")
        return
    call(lambda client: client.benchmark_definitions.remove(name))
    click.echo(click.style(f"✓ Benchmark '{name}' archived.", fg="green", bold=True))


@click.command(name="restore", help="Restore an archived benchmark so it can change again.")
@click.argument("name", metavar="NAME", type=RESOURCE_NAME)
def restore_benchmark(name: str) -> None:
    restored = call(lambda client: client.benchmark_definitions.restore(name))
    click.echo(click.style(f"✓ Benchmark restored: {one_line(restored.name)}", fg="green", bold=True))
