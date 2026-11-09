from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, ClassVar, TypeVar, cast

from rich.text import Text
from textual import events, on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.messages import Prune
from textual.screen import ModalScreen, Screen
from textual.widgets import Input, Static
from textual.worker import Worker, WorkType

from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieSDKError
from valkyrie.tui.theme import palette
from valkyrie.tui.widgets import CommandLine, KeyBar, Table, TopBar

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

ResultT = TypeVar("ResultT")
WorkResultT = TypeVar("WorkResultT")

SHARED_KEYS = [("t", "light or dark theme"), ("?", "these keys"), ("q", "quit")]


def matches(query: str, *fields: str | None) -> bool:
    haystack = " ".join(field for field in fields if field).lower()
    return all(term in haystack for term in query.lower().split())


class FilterInput(Input):
    BINDINGS = [Binding("escape", "screen.clear_filter", show=False)]

    def __init__(self) -> None:
        super().__init__(placeholder="type to filter · enter keeps it · esc clears", id="filter")


class BrowseScreen(Screen[ResultT]):
    BINDINGS = [
        Binding("slash", "open_filter", "filter"),
        Binding("question_mark", "keys", "keys"),
        Binding("y", "copy_command", "copy command"),
        Binding("escape", "back", "back"),
    ]
    KEYS: ClassVar[list[tuple[str, str]]] = []
    CAN_GO_BACK: ClassVar[bool] = False

    filter = ""

    def footer(self) -> ComposeResult:
        with Horizontal(id="filter-bar"):
            yield Static("/", id="filter-prompt")
            yield FilterInput()
        yield KeyBar()
        yield CommandLine()

    def refilter(self) -> None:
        raise NotImplementedError

    def restyle(self) -> None:
        self.refilter()
        for widget in self.walk_children():
            widget.refresh()

    def filter_count(self, visible: int, total: int) -> str:
        return f"{visible}/{total}" if self.filter else str(total)

    def filter_label(self) -> str:
        return f" · /{self.filter}" if self.filter else ""

    def action_open_filter(self) -> None:
        self.add_class("-filtering")
        self.query_one(FilterInput).focus()

    def action_clear_filter(self) -> None:
        self.query_one(FilterInput).value = ""
        self._close_filter()

    def action_back(self) -> None:
        if self.filter:
            self.action_clear_filter()
        elif self.CAN_GO_BACK:
            self.app.pop_screen()

    @on(Input.Changed, "#filter")
    def _filter_changed(self, event: Input.Changed) -> None:
        self.filter = event.value
        self.refilter()

    @on(Input.Submitted, "#filter")
    def _close_filter(self) -> None:
        self.remove_class("-filtering")
        self.query_one(Table).focus()

    def action_keys(self) -> None:
        self.app.push_screen(KeysScreen([*self.KEYS, *SHARED_KEYS]))

    def action_copy_command(self) -> None:
        command_line = self.query_one(CommandLine)
        cast("ValkyrieApp", self.app).copy(command_line.command)
        command_line.flash_copied()

    @on(Prune)
    def _cancel_workers(self) -> None:
        self.workers.cancel_node(self)

    def run_worker(
        self,
        work: WorkType[WorkResultT],
        name: str | None = "",
        group: str = "default",
        description: str = "",
        exit_on_error: bool = True,
        start: bool = True,
        exclusive: bool = False,
        thread: bool = False,
    ) -> Worker[WorkResultT]:
        worker = super().run_worker(work, name, group, description, exit_on_error, start, exclusive, thread)
        if self._closing:
            worker.cancel()
        return worker

    def busy(self, group: str) -> bool:
        return any(worker.node is self and worker.group == group and not worker.is_finished for worker in self.workers)

    def fresh(self) -> None:
        self.query_one(TopBar).status = ("● live", "accent")

    def report(self, error: ValkyrieSDKError) -> None:
        top_bar = self.query_one(TopBar)
        if isinstance(error, ValkyrieAPIError) and error.status_code == 401:
            url = shlex.quote(top_bar.environment.url)
            top_bar.status = ("⚠ not logged in", "warning")
            message = f"{error}\nLog in with `VALKYRIE_URL={url} valk login`, or set VALKYRIE_API_KEY."
            self.notify(message, title="Not logged in", severity="error")
            return
        top_bar.status = ("⚠ tracker unreachable, retrying", "warning")
        self.notify(str(error), title="Tracker error", severity="error")


class KeysScreen(ModalScreen[None]):
    def __init__(self, keys: list[tuple[str, str]]) -> None:
        super().__init__()
        self.keys = keys

    def compose(self) -> ComposeResult:
        width = max(len(key) for key, _ in self.keys)
        lines = Text("\n").join(
            Text.assemble((f"{key:>{width}}", f"bold {palette().accent}"), "   ", (description, palette().text))
            for key, description in self.keys
        )
        yield Static(lines, id="keys", classes="panel")

    def on_mount(self) -> None:
        keys = self.query_one("#keys")
        keys.border_title = "Keys"
        keys.border_subtitle = "any key closes"

    def on_key(self, event: events.Key) -> None:
        event.stop()
        self.dismiss()

    def on_click(self) -> None:
        self.dismiss()
