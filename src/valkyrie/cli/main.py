import os
import traceback
from typing import Any

import click

from valkyrie.cli.entry import main
from valkyrie.cli.lazy import LazyGroup
from valkyrie.cli.logging import load_cli_environment
from valkyrie.cli.tracker_profiles import TrackerProfiles, checked_url


class _ValkyrieCLI(LazyGroup):
    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        sections = {
            "Runs": ("run", "agent", "dataset", "benchmark"),
            "Organization": ("project", "secret", "provider", "org", "queue"),
            "Account": ("tracker", "login", "logout", "whoami", "token"),
            "Browse": ("tui", "completion"),
        }
        for section, names in sections.items():
            with formatter.section(section):
                formatter.write_dl([(name, self.lazy_commands[name][1]) for name in names])

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except Exception as error:
            if isinstance(error, (click.exceptions.Exit, click.Abort)):
                raise
            if ctx.params.get("verbose"):
                traceback.print_exc()
            from valkyrie.cli.display import Failure, command, subcommand_path

            if isinstance(error, Failure):
                raise
            if isinstance(error, click.UsageError):
                hint = command(*subcommand_path(error.ctx), "--help")
                raise Failure(error.format_message(), hint=hint, exit_code=error.exit_code) from error
            message = " ".join(str(error).splitlines())
            exit_code = error.exit_code if isinstance(error, click.ClickException) else 1
            raise Failure(message, exit_code=exit_code) from error


def _tracker(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    if value is None:
        return None
    if "://" in value:
        value = checked_url(value)
    ctx.meta["tracker"] = value
    return value if "://" in value else TrackerProfiles.load().resolve(value)


def _version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    from importlib.metadata import version

    click.echo(f"valkyrie CLI {version('valkyrie-cli')}\nvalkyrie SDK {version('valkyrie-sdk')}")
    ctx.exit()


@click.group(
    cls=_ValkyrieCLI,
    lazy_commands={
        "run": ("valkyrie.cli.run:run", "Start, monitor, manage, and retrieve runs."),
        "agent": ("valkyrie.cli.agent:agent", "Manage agents and releases."),
        "benchmark": (
            "valkyrie.cli.benchmark:benchmark",
            "Manage benchmarks and services.",
        ),
        "dataset": ("valkyrie.cli.dataset:dataset", "Publish and manage datasets."),
        "queue": ("valkyrie.cli.queue:queue", "Inspect your organization's sandbox scheduler."),
        "project": ("valkyrie.cli.project:project", "Manage the projects runs are started in."),
        "secret": ("valkyrie.cli.secret:secret", "Store secrets that your runs reference."),
        "provider": (
            "valkyrie.cli.provider:provider",
            "Configure sandbox providers.",
        ),
        "login": (
            "valkyrie.cli.login:login",
            "Sign in to a Tracker.",
        ),
        "logout": (
            "valkyrie.cli.login:logout",
            "Forget a saved login.",
        ),
        "tracker": ("valkyrie.cli.tracker:tracker", "Save Trackers by name and choose the default."),
        "whoami": ("valkyrie.cli.login:whoami", "Show your Tracker and identity."),
        "token": ("valkyrie.cli.login:token", "Manage access keys."),
        "org": ("valkyrie.cli.org:org", "Manage organizations and members."),
        "tui": ("valkyrie.cli.tui:tui", "Browse runs, tasks and logs in a terminal UI."),
        "completion": ("valkyrie.cli.completion:completion", "Print a shell completion script."),
    },
    help="Run agents against benchmarks and manage their runs.",
)
@click.option(
    "--version", is_flag=True, is_eager=True, expose_value=False, callback=_version, help="Show CLI and SDK versions."
)
@click.option(
    "--tracker",
    callback=_tracker,
    metavar="NAME|URL",
    help="Use this saved Tracker name or URL for this call. Defaults to `VALKYRIE_URL`, then `tracker use`.",
)
@click.option("-v", "--verbose", is_flag=True, help="Show SDK logs and full tracebacks.")
@click.option("--no-color", is_flag=True, help="Disable colors. Also honors NO_COLOR.")
@click.pass_context
def cli(ctx: click.Context, tracker: str | None, verbose: bool, no_color: bool) -> None:
    def override(name: str, value: str) -> None:
        previous = os.environ.get(name)

        def restore() -> None:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous

        ctx.call_on_close(restore)
        os.environ[name] = value

    if verbose:
        override("VALKYRIE_CLI_LOGS", "true")
    if no_color or "NO_COLOR" in os.environ:
        ctx.color = False
    load_cli_environment()
    override(
        "VALKYRIE_URL",
        tracker or os.environ.get("VALKYRIE_URL", "").strip().rstrip("/") or TrackerProfiles.load().resolve(),
    )


__all__ = ["cli"]


if __name__ == "__main__":
    main()
