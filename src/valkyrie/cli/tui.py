import click


@click.command(help="Browse runs, tasks and logs in a terminal UI.")
@click.option("--fake", is_flag=True, help="Use built-in sample data instead of the tracker.")
def tui(fake: bool) -> None:
    from valkyrie.sdk.errors import ValkyrieConfigError
    from valkyrie.tui.app import ValkyrieApp
    from valkyrie.tui.fake import FakeSource
    from valkyrie.tui.source import RunSource, TrackerSource, TrackerSwitcher

    source: RunSource
    if fake:
        source = FakeSource()
    else:
        try:
            source = TrackerSource.from_cli_config()
        except ValkyrieConfigError as error:
            from valkyrie.cli.display import Failure, command

            raise Failure(str(error), hint=command("tui", "--fake")) from error
    ValkyrieApp(source, switcher=None if fake else TrackerSwitcher()).run()
