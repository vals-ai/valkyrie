from collections.abc import Mapping
from importlib import import_module
from typing import Any

import click
from click.utils import make_default_short_help


class LazyGroup(click.Group):
    def __init__(
        self,
        *args: Any,
        lazy_commands: Mapping[str, tuple[str, str]],
        hidden_commands: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lazy_commands = dict(lazy_commands)
        self.hidden_commands = dict(hidden_commands or {})

    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted(self.lazy_commands)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        if cmd_name in self.lazy_commands:
            target = self.lazy_commands[cmd_name][0]
        elif cmd_name in self.hidden_commands:
            target = self.hidden_commands[cmd_name]
        else:
            return None
        module, attribute = target.split(":")
        return getattr(import_module(module), attribute)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        names = self.list_commands(ctx)
        limit = formatter.width - 6 - max(map(len, names))
        with formatter.section("Commands"):
            formatter.write_dl([(name, make_default_short_help(self.lazy_commands[name][1], limit)) for name in names])
