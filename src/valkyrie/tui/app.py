from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import Any

from textual import work
from textual.app import App, SystemCommand
from textual.binding import Binding
from textual.notifications import SeverityLevel
from textual.screen import Screen

from valkyrie.tui.run import RunCache
from valkyrie.tui.runs import RunsScreen
from valkyrie.sdk import ValkyrieConfigError
from valkyrie.tui.source import RunSource, Switcher
from valkyrie.tui import theme
from valkyrie.tui.browse import BrowseScreen
from valkyrie.tui.widgets import shown

SYSTEM_CLIPBOARDS = (["pbcopy"], ["wl-copy"], ["xclip", "-selection", "clipboard"])


class ValkyrieApp(App[None]):
    CSS_PATH = "valkyrie.tcss"
    TITLE = "valkyrie"
    BINDINGS = [Binding("q", "quit", "quit"), Binding("t", "toggle_theme", "theme")]
    HORIZONTAL_BREAKPOINTS = [(0, "-narrow"), (120, "-wide")]

    def __init__(
        self, source: RunSource, *, now: Callable[[], datetime] | None = None, switcher: Switcher | None = None
    ) -> None:
        super().__init__()
        self.source = source
        self.launch_url = source.environment.url
        self.switcher = switcher
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.cache = RunCache()

    def on_mount(self) -> None:
        for palette in theme.PALETTES.values():
            self.register_theme(palette.theme())
        self.theme = theme.DARK.name
        self.push_screen(RunsScreen(self.source))

    def watch_theme(self, name: str) -> None:
        theme.use(name)
        for screen in self.screen_stack:
            if isinstance(screen, BrowseScreen):
                screen.restyle()

    def action_toggle_theme(self) -> None:
        self.theme = theme.LIGHT.name if self.theme == theme.DARK.name else theme.DARK.name

    def get_system_commands(self, screen: Screen[Any]) -> Iterable[SystemCommand]:
        return (command for command in super().get_system_commands(screen) if command.title != "Theme")

    @work(exclusive=True, group="environment")
    async def switch_environment(self, url: str) -> None:
        assert self.switcher is not None
        try:
            source = self.switcher.open(url)
        except ValkyrieConfigError as error:
            self.notify(str(error), severity="error")
            return
        previous, self.source = self.source, source
        self.cache = RunCache()
        await self.switch_screen(RunsScreen(source))
        await previous.close()

    async def on_unmount(self) -> None:
        await self.source.close()

    def notify(
        self,
        message: str,
        *,
        title: str = "",
        severity: SeverityLevel = "information",
        timeout: float | None = None,
        markup: bool = False,
    ) -> None:
        super().notify(shown(message), title=shown(title), severity=severity, timeout=timeout, markup=markup)

    def copy(self, text: str) -> None:
        self.copy_to_clipboard(text)
        if self.is_headless:
            return
        command = next((command for command in SYSTEM_CLIPBOARDS if shutil.which(command[0])), None)
        if command:
            self.run_worker(
                lambda: subprocess.run(command, input=text.encode(), check=False, timeout=5),
                thread=True,
                exit_on_error=False,
            )
