import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    lazy_commands={
        "tasks": ("valkyrie.cli.benchmark.tasks:tasks", "Export the task IDs of a benchmark dataset."),
        "service": (
            "valkyrie.cli.benchmark.services:service",
            "Save benchmark service URLs that your runs use instead of the hosted services.",
        ),
    },
    hidden_commands={
        "create": "valkyrie.cli.benchmark.definitions:create_benchmark",
        "list": "valkyrie.cli.benchmark.definitions:list_benchmarks",
        "show": "valkyrie.cli.benchmark.definitions:show_benchmark",
        "update": "valkyrie.cli.benchmark.definitions:update_benchmark",
        "remove": "valkyrie.cli.benchmark.definitions:remove_benchmark",
        "restore": "valkyrie.cli.benchmark.definitions:restore_benchmark",
    },
    help="Export the task IDs in a benchmark dataset, and save benchmark services.",
)
def benchmark():
    pass


__all__ = ["benchmark"]
