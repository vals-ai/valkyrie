from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, ClassVar, cast

from rich.console import Group, RenderableType
from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.markup import escape
from textual.reactive import reactive
from textual.widget import Widget

from valkyrie.sdk.errors import ValkyrieSDKError
from valkyrie.sdk.models import DatasetResponse, DatasetTaskResponse
from valkyrie.tui.browse import BrowseScreen, matches
from valkyrie.tui.source import RunSource
from valkyrie.tui.theme import palette
from valkyrie.tui.widgets import CommandLine, KeyBar, Table, TopBar, ago, shown

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

DESCRIPTION_WIDTH = 30


def _label(dataset: DatasetResponse) -> str:
    return f"{dataset.benchmark}/{dataset.name}"


class DatasetPreview(Widget):
    dataset: reactive[DatasetResponse | None] = reactive(None)

    def render(self) -> RenderableType:
        dataset = self.dataset
        if dataset is None:
            return Text("No datasets yet.", style=palette().muted)
        now = cast("ValkyrieApp", self.app).now()
        created = " · ".join(
            part for part in (f"created {ago(dataset.created_at, now)} ago", dataset.created_by_email) if part
        )
        return Group(
            Text(shown(_label(dataset)), style=f"bold {palette().text}", no_wrap=True, overflow="ellipsis"),
            Text(shown(created), style=palette().faint, no_wrap=True, overflow="ellipsis"),
            Text(f"updated {ago(dataset.updated_at, now)} ago", style=palette().faint),
            Text(),
            Text(shown(dataset.description) or "No description.", style=palette().muted),
        )


class DatasetsScreen(BrowseScreen[None]):
    BINDINGS = [Binding("r", "reload", "reload")]
    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move"),
        ("g G", "top, bottom"),
        ("enter", "open the dataset's tasks"),
        ("/", "filter by benchmark, name, description or who created it"),
        ("esc", "clear the filter, then go back"),
        ("y", "copy the command on the bottom line"),
        ("r", "reload the list"),
    ]
    CAN_GO_BACK = True

    def __init__(self, source: RunSource) -> None:
        super().__init__()
        self.source = source
        self.rows: dict[str, DatasetResponse] = {}

    def compose(self) -> ComposeResult:
        yield TopBar(self.source.environment)
        with Horizontal(id="body"):
            yield Table(id="datasets")
            yield DatasetPreview(id="preview", classes="panel")
        yield from self.footer()

    def on_mount(self) -> None:
        table = self.query_one(Table)
        table.border_title = "Datasets"
        table.add_column("benchmark", key="benchmark")
        table.add_column("dataset", key="name")
        table.add_column("description", key="description", width=DESCRIPTION_WIDTH)
        table.add_column("updated", key="updated")
        self.query_one(KeyBar).show(
            ("enter", "tasks"), ("/", "filter"), ("y", "copy cmd"), ("esc", "back"), ("?", "keys"), ("q", "quit")
        )
        self.query_one(CommandLine).show("valk benchmark list")
        self.load_datasets()

    @work(exclusive=True, group="datasets")
    async def load_datasets(self) -> None:
        try:
            datasets = await self.source.datasets()
        except ValkyrieSDKError as error:
            self.report(error)
            return
        self.rows = {_label(dataset): dataset for dataset in datasets}
        self.refilter()
        self.fresh()

    def refilter(self) -> None:
        table = self.query_one(Table)
        visible = {name: row for name, row in self.rows.items() if self._matches(row)}
        table.show_rows({name: self._cells(row) for name, row in visible.items()})
        count = self.filter_count(len(visible), len(self.rows))
        table.border_title = f"Datasets · {count}{self.filter_label()}"
        self._show_highlighted()

    def _matches(self, dataset: DatasetResponse) -> bool:
        return matches(self.filter, dataset.benchmark, dataset.name, dataset.description, dataset.created_by_email)

    def _cells(self, dataset: DatasetResponse) -> list[Text]:
        now = cast("ValkyrieApp", self.app).now()
        summary = dataset.description.splitlines()[0] if dataset.description else ""
        return [
            Text(shown(dataset.benchmark), style=palette().muted),
            Text(shown(dataset.name), style=palette().text),
            Text(shown(summary), style=palette().muted, no_wrap=True, overflow="ellipsis"),
            Text(ago(dataset.updated_at, now, compact=True), style=palette().faint, justify="right"),
        ]

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        self._show_highlighted()

    def on_data_table_row_selected(self, event: Table.RowSelected) -> None:
        self.app.push_screen(DatasetTasksScreen(self.source, self.rows[str(event.row_key.value)]))

    def action_reload(self) -> None:
        self.load_datasets()

    def _show_highlighted(self) -> None:
        key = self.query_one(Table).highlighted_key()
        dataset = self.rows.get(key) if key else None
        preview = self.query_one(DatasetPreview)
        preview.dataset = dataset
        preview.border_title = escape(shown(_label(dataset))) if dataset else ""
        command = f"valk dataset show {dataset.benchmark} {dataset.name}" if dataset else "valk benchmark list"
        self.query_one(CommandLine).show(command)


class DatasetTasksScreen(BrowseScreen[None]):
    BINDINGS = [Binding("r", "reload", "reload")]
    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move"),
        ("g G", "top, bottom"),
        ("/", "filter by task ID"),
        ("esc", "clear the filter, then go back"),
        ("y", "copy the command on the bottom line"),
        ("r", "reload the list"),
    ]
    CAN_GO_BACK = True

    def __init__(self, source: RunSource, dataset: DatasetResponse) -> None:
        super().__init__()
        self.source = source
        self.dataset = dataset
        self.title = escape(shown(_label(dataset)))
        self.rows: dict[str, DatasetTaskResponse] = {}

    def compose(self) -> ComposeResult:
        yield TopBar(self.source.environment)
        with Horizontal(id="body"):
            yield Table(id="dataset-tasks")
        yield from self.footer()

    def on_mount(self) -> None:
        table = self.query_one(Table)
        table.border_title = f"{self.title} · tasks"
        table.add_column("task", key="task")
        table.add_column("digest", key="digest")
        table.add_column("bytes", key="size")
        self.query_one(KeyBar).show(("/", "filter"), ("y", "copy cmd"), ("esc", "back"), ("?", "keys"), ("q", "quit"))
        self.query_one(CommandLine).show(f"valk dataset task list {self.dataset.benchmark}/{self.dataset.name}")
        self.load_tasks()

    @work(exclusive=True, group="dataset-tasks")
    async def load_tasks(self) -> None:
        try:
            tasks = await self.source.dataset_tasks(self.dataset.benchmark, self.dataset.name)
        except ValkyrieSDKError as error:
            self.report(error)
            return
        self.rows = {task.task_id: task for task in tasks}
        self.refilter()
        self.fresh()

    def refilter(self) -> None:
        table = self.query_one(Table)
        visible = {key: row for key, row in self.rows.items() if matches(self.filter, row.task_id)}
        table.show_rows({key: self._cells(row) for key, row in visible.items()})
        count = self.filter_count(len(visible), len(self.rows))
        table.border_title = f"{self.title} · tasks · {count}{self.filter_label()}"
        self._show_highlighted()

    def _cells(self, task: DatasetTaskResponse) -> list[Text]:
        return [
            Text(shown(task.task_id), style=palette().text),
            Text(task.digest.removeprefix("sha256:")[:12], style=palette().faint),
            Text(f"{task.size:,}", style=palette().muted, justify="right"),
        ]

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        self._show_highlighted()

    def action_reload(self) -> None:
        self.load_tasks()

    def _show_highlighted(self) -> None:
        key = self.query_one(Table).highlighted_key()
        dataset = self.dataset
        command = (
            f"valk dataset task download {dataset.benchmark}/{dataset.name} {shlex.quote(key)}"
            if key
            else f"valk dataset task list {dataset.benchmark}/{dataset.name}"
        )
        self.query_one(CommandLine).show(command)
