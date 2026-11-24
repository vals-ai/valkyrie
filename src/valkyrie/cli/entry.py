import os
import sys


def main():
    os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "__all__")
    try:
        from valkyrie.cli.main import cli

        complete_var = f"_{os.path.basename(sys.argv[0]).replace('-', '_').upper()}_COMPLETE"
        if os.environ.get(complete_var):
            from valkyrie.cli.completion import completion_tree

            completion_tree()()
        else:
            from valkyrie.cli.legacy import upgrade_args

            args, notes = upgrade_args(sys.argv[1:])
            if notes:
                import click

                from valkyrie.cli.display import warn

                with click.Context(cli, color=False if "--no-color" in args else None):
                    for note in notes:
                        warn(f"{note}. The old form will be removed in a later release.")
            cli(args)
    except KeyboardInterrupt:
        sys.exit(130)
