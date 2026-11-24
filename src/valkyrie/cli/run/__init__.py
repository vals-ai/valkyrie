import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    removed_commands={"filter-options": None},
    lazy_commands={
        "analyze": (
            "valkyrie.cli.run.analyze:analyze",
            "Analyze a finished run's errors with Docent.",
        ),
        "errors": ("valkyrie.cli.run.errors:errors", "Show stored run and current task error messages, one line each."),
        "show": (
            "valkyrie.cli.run.fetch:show",
            "Show one run's details or several runs' status. Use --follow to stream one run.",
        ),
        "list": ("valkyrie.cli.run.list_runs:list_runs", "List runs that match the filters."),
        "logs": ("valkyrie.cli.run.logs:logs", "Fetch or follow logs for a run or one task."),
        "files": ("valkyrie.cli.run.outputs:files", "List one page of artifact paths available within a run."),
        "download": ("valkyrie.cli.run.outputs:download", "Download a run's outputs or a file or folder by its path."),
        "results": ("valkyrie.cli.run.results:results", "Save a run's results by its ID."),
        "rescore": ("valkyrie.cli.run.rescore:rescore", "Score a finished dataset run again with its metric script."),
        "regrade": (
            "valkyrie.cli.run.regrade:regrade",
            "Regrade a finished run into a new run on another dataset.",
        ),
        "resume": ("valkyrie.cli.run.resume:resume", "Resume a run's stopped tasks by its ID."),
        "retry": ("valkyrie.cli.run.resume:retry_command", "Retry a run's errored tasks by its ID."),
        "start": ("valkyrie.cli.run.start:start", "Start a run of an agent on a benchmark."),
        "stop": ("valkyrie.cli.run.stop:stop", "Stop a run by its ID."),
        "update": (
            "valkyrie.cli.run.update:update",
            "Change a run's concurrency or project. Add --shed to force stop excess tasks.",
        ),
        "tasks": ("valkyrie.cli.run.tasks:tasks", "List tasks with their status and errors."),
        "task": ("valkyrie.cli.run.tasks:task", "Inspect a task or get its file links."),
    },
    help="Start, monitor, manage, and retrieve runs.",
)
def run():
    pass


__all__ = ["run"]
