import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    lazy_commands={
        "analyze": (
            "valkyrie.cli.run.analyze:analyze",
            "Analyze a finished run's errors with Docent.",
        ),
        "errors": ("valkyrie.cli.run.errors:errors", "Show stored run and current task error messages."),
        "fetch": ("valkyrie.cli.run.fetch:fetch", "Show a run by its ID."),
        "filter-options": (
            "valkyrie.cli.run.filter_options:filter_options",
            "List benchmark, agent, model, dataset, and starter values from your run history.",
        ),
        "list": ("valkyrie.cli.run.list_runs:list_runs", "List runs that match the filters."),
        "logs": ("valkyrie.cli.run.logs:logs", "Fetch or follow logs for a run or one task."),
        "artifacts": ("valkyrie.cli.run.outputs:artifacts", "List one page of artifact paths available within a run."),
        "output": ("valkyrie.cli.run.outputs:output_path", "Download files from a run by its ID."),
        "outputs": ("valkyrie.cli.run.outputs:outputs", "Download a run's outputs by its ID."),
        "results": ("valkyrie.cli.run.results:results", "Save a run's results by its ID."),
        "retag": ("valkyrie.cli.run.retag:retag", "Put a run in another project, in any run state."),
        "resume": ("valkyrie.cli.run.resume:resume", "Resume a run's stopped tasks by its ID."),
        "retry": ("valkyrie.cli.run.resume:retry_command", "Retry a run's errored tasks by its ID."),
        "shed": (
            "valkyrie.cli.run.shed:shed",
            "Lower concurrency for an active run, then force stop the newest building or in-progress tasks until the "
            "run is back at the new limit.",
        ),
        "start": ("valkyrie.cli.run.start:start", "Start a run of an agent on a benchmark."),
        "status": ("valkyrie.cli.run.status:status_runs", "Show the status of several runs at once."),
        "stop": ("valkyrie.cli.run.stop:stop", "Stop a run by its ID."),
        "update": ("valkyrie.cli.run.update:update", "Change an active run's concurrency."),
        "tasks": ("valkyrie.cli.run.tasks:tasks", "List one page of tasks, including their status and error messages."),
        "task": ("valkyrie.cli.run.tasks:task", "Inspect one task's status, evaluation result, and failure reason."),
        "task-artifacts": (
            "valkyrie.cli.run.tasks:task_artifacts",
            "Get temporary download and log links for one task.",
        ),
    },
    help="Start, monitor, manage, and retrieve runs.",
)
def run():
    pass


__all__ = ["run"]
