import os
import sys


def main():
    os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "__all__")
    try:
        from valkyrie.cli.main import cli

        cli()
    except KeyboardInterrupt:
        sys.exit(130)
