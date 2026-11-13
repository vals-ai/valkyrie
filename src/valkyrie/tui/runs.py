from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, cast

from rich.console import Group, RenderableType
from rich.text import Text
from textual import events, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.screen import ModalScreen
from textual.widgets import OptionList
from textual.widgets.option_list import Option
from textual.reactive import reactive
from textual.markup import escape
from textual.widget import Widget

from valkyrie.sdk.errors import ValkyrieSDKError
from valkyrie.sdk.models import BenchmarkStatusEntry, BenchmarkTableRow, TaskStatus
from valkyrie.tui.theme import palette
from valkyrie.tui.browse import BrowseScreen, matches
from valkyrie.tui.datasets import DatasetsScreen
from valkyrie.tui.run import RunScreen
from valkyrie.tui.source import FINAL_STATUSES, RunSource
from valkyrie.tui.widgets import (
    CommandLine,
    KeyBar,
    Table,
    TopBar,
    ago,
    progress_bar,
    run_glyph,
    run_status,
    short_id,
    shown,
    status_label,
    task_glyph,
)

if TYPE_CHECKING:
    from valkyrie.tui.app import ValkyrieApp

WINDOW_DAYS = (2, 7, 30)
RUN_LIMIT = 1000
STATUS_POLL_SECONDS = 3
LIST_REFRESH_SECONDS = 30
COUNT_ORDER = [
    TaskStatus.FINISHED,
    TaskStatus.ERROR,
    TaskStatus.EVALUATING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.BUILDING,
    TaskStatus.STOPPED,
    TaskStatus.PENDING,
]


class RunPreview(Widget):
    run: reactive[BenchmarkTableRow | None] = reactive(None)

    def render(self) -> RenderableType:
        run = self.run
        if run is None:
            return Text("No runs yet.", style=palette().muted)
        now = cast("ValkyrieApp", self.app).now()
        title = _line((shown(run.name), f"bold {palette().text}"))
        if run.label:
            title.append(f"  {shown(run.label)}", style=palette().accent)
        started = shown(" · ".join(part for part in (f"{ago(run.started_at, now)} ago", run.started_by_email) if part))
        glyph, color = run_glyph(run.status)
        status = _line((f"{glyph} {status_label(run.status)}", f"bold {color}"))
        if run.final_score is not None:
            status.append(f"   score {run.final_score:.3f}", style=palette().text)

        lines: list[RenderableType] = [
            title,
            _line((shown(" · ".join(part for part in (run.agent_name, run.model) if part)), palette().muted)),
            _line((started, palette().faint)),
            Text(),
            status,
        ]
        if run.total_tasks:
            percent = round(100 * run.finished_tasks / run.total_tasks)
            summary = f"  {run.finished_tasks}/{run.total_tasks}  {percent}%"
            width = max(self.content_size.width - len(summary), 8)
            lines += [
                progress_bar(run.task_state_counts, run.total_tasks, width) + Text(summary, style=palette().muted)
            ]
            lines += [Text(), *_counts(run)]
        if run.error_message:
            lines += [Text(), Text(shown(run.error_message), style=palette().error)]
        return Group(*lines)


def _line(*parts: tuple[str, str]) -> Text:
    return Text.assemble(*parts, no_wrap=True, overflow="ellipsis")


def _counts(run: BenchmarkTableRow) -> list[Text]:
    lines = []
    for status in COUNT_ORDER:
        count = run.task_state_counts.get(status.value, 0)
        if count:
            glyph, color = task_glyph(status)
            lines.append(
                Text.assemble((f"{glyph} ", color), (f"{status_label(status):<12}", palette().muted), f"{count:>5}")
            )
    return lines


class MenuScreen(ModalScreen[str | None]):
    BINDINGS = [Binding("escape,q", "dismiss", show=False)]

    def __init__(self, heading: str, choices: dict[str, str], current: str, key: str) -> None:
        super().__init__()
        self.heading = heading
        self.choices = choices
        self.current = current
        self.close_key = key

    def compose(self) -> ComposeResult:
        colors = palette()
        options = [
            Option(
                Text.assemble(
                    ("● " if choice == self.current else "  ", colors.accent), (label, f"bold {colors.text}")
                ),
                id=choice,
            )
            for choice, label in self.choices.items()
        ]
        yield OptionList(*options, id="menu")

    def on_mount(self) -> None:
        menu = self.query_one(OptionList)
        menu.border_title = self.heading
        menu.border_subtitle = "enter picks · esc closes"
        menu.highlighted = list(self.choices).index(self.current)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(event.option.id)

    def on_key(self, event: events.Key) -> None:
        if event.key == self.close_key:
            event.stop()
            self.dismiss(None)

    def on_click(self, event: events.Click) -> None:
        if event.widget is self:
            self.dismiss(None)


class RunsScreen(BrowseScreen[None]):
    BINDINGS = [
        Binding("r", "reload", "reload"),
        Binding("s", "pick_window", "since"),
        Binding("c", "switch_environment", "tracker"),
        Binding("d", "open_datasets", show=False),
    ]
    KEYS: ClassVar[list[tuple[str, str]]] = [
        ("j k ↑ ↓", "move"),
        ("g G", "top, bottom"),
        ("enter", "open the run"),
        ("/", "filter by benchmark, agent, model, label, status or who started it"),
        ("esc", "clear the filter"),
        ("s", "pick how far back the list goes, or click it in the title. Runs still going always show"),
        ("y", "copy the command on the bottom line"),
        ("r", "reload the list"),
        ("c", "switch to another Tracker you've logged in to; clicking its name does too"),
    ]

    def __init__(self, source: RunSource) -> None:
        super().__init__()
        self.source = source
        self.rows: dict[str, BenchmarkTableRow] = {}
        self.window_days = WINDOW_DAYS[0]
        self.capped = False
        self.fetching = False

    def compose(self) -> ComposeResult:
        yield TopBar(self.source.environment)
        with Horizontal(id="body"):
            yield Table(id="runs")
            yield RunPreview(id="preview", classes="panel")
        yield from self.footer()

    def on_mount(self) -> None:
        table = self.query_one(Table)
        table.border_title = "Runs"
        for label, key in [("", "status"), ("id", "id"), ("benchmark", "benchmark"), ("agent", "agent")]:
            table.add_column(label, key=key)
        for label, key in [("model", "model"), ("progress", "progress"), ("", "errors"), ("age", "age")]:
            table.add_column(label, key=key)
        self.query_one(KeyBar).show(
            ("enter", "open"),
            ("/", "filter"),
            ("s", "since"),
            ("c", "tracker"),
            ("y", "copy cmd"),
            ("?", "keys"),
            ("q", "quit"),
        )
        self.query_one(CommandLine).show("valk run list")
        self.load_runs()
        self.set_interval(STATUS_POLL_SECONDS, self._poll_statuses_when_idle)
        self.set_interval(LIST_REFRESH_SECONDS, self._load_runs_when_idle)

    def action_switch_environment(self) -> None:
        app = cast("ValkyrieApp", self.app)
        if app.switcher is None:
            self.notify("Sample data has no Trackers to switch to.")
            return
        current = self.source.environment
        choices = app.switcher.choices()
        if current.url not in {choice.url for choice in choices}:
            choices = [current, *choices]
        if len(choices) < 2:
            self.notify("Only this Tracker has a saved login. `VALKYRIE_URL=<url> valk login` adds another.")
            return
        menu = MenuScreen("Tracker", {choice.url: choice.url for choice in choices}, current.url, key="c")
        app.push_screen(menu, self._switch_to)

    def on_top_bar_badge_clicked(self) -> None:
        self.action_switch_environment()

    def _switch_to(self, url: str | None) -> None:
        if url is not None and url != self.source.environment.url:
            cast("ValkyrieApp", self.app).switch_environment(url)

    def _load_runs_when_idle(self) -> None:
        if not self.busy("runs"):
            self.load_runs()

    def _poll_statuses_when_idle(self) -> None:
        if not self.busy("statuses"):
            self.poll_statuses()

    @work(exclusive=True, group="runs")
    async def load_runs(self) -> None:
        if self.rows and not self.is_current:
            return
        since = self._since()
        try:
            runs = await self.source.runs(since, RUN_LIMIT)
        except ValkyrieSDKError as error:
            self.fetching = False
            self.refilter()
            self.report(error)
            return
        self.rows = {str(run.id): run for run in runs}
        self.capped = sum(run.started_at > since for run in runs) >= RUN_LIMIT
        self.fetching = False
        self.refilter()
        self.fresh()

    def refilter(self) -> None:
        table = self.query_one(Table)
        visible = {key: run for key, run in self.rows.items() if self._matches(run)}
        table.show_rows({key: self._cells(run) for key, run in visible.items()})
        count = self.filter_count(len(visible), len(self.rows)) + ("+" if self.capped else "")
        window = f"[@click=screen.pick_window]last {self.window_days} days[/]{' · loading' if self.fetching else ''}"
        table.border_title = f"Runs · {count} · {window}{self.filter_label()}"
        self._show_highlighted()

    def _matches(self, run: BenchmarkTableRow) -> bool:
        fields = (run.name, run.agent_name, run.model, run.label, status_label(run.status), short_id(run.id))
        return matches(self.filter, *fields, run.started_by_email)

    @work(exclusive=True, group="statuses")
    async def poll_statuses(self) -> None:
        live = [run.id for run in self.rows.values() if run.status not in FINAL_STATUSES]
        if not live or not self.is_current:
            return
        try:
            entries = await self.source.statuses(live)
        except ValkyrieSDKError as error:
            self.report(error)
            return
        table = self.query_one(Table)
        changed = False
        for entry in entries:
            key = str(entry.id)
            if key in self.rows:
                changed |= self.rows[key].status != entry.status
                self._apply_status(key, entry)
                if key in table.rows:
                    table.update_row(key, self._cells(self.rows[key]))
        if changed and self.filter:
            self.refilter()
        else:
            self._show_highlighted()
        self.fresh()

    def _apply_status(self, key: str, entry: BenchmarkStatusEntry) -> None:
        run = self.rows[key].model_copy(
            update={
                "status": entry.status,
                "finished_at": entry.finished_at,
                "total_tasks": entry.total_tasks,
                "finished_tasks": entry.finished_tasks,
                "task_state_counts": entry.task_state_counts,
            }
        )
        self.rows[key] = run

    def _cells(self, run: BenchmarkTableRow) -> list[Text]:
        now = cast("ValkyrieApp", self.app).now()
        if run.total_tasks:
            progress = progress_bar(run.task_state_counts, run.total_tasks, 10)
            progress.append(f" {run.finished_tasks}/{run.total_tasks}", style=palette().muted)
        else:
            progress = Text(status_label(run.status), style=run_glyph(run.status)[1])
        errors = run.task_state_counts.get(TaskStatus.ERROR.value, 0)
        return [
            run_status(run.status),
            Text(short_id(run.id), style=palette().muted),
            Text(shown(run.name), style=palette().text),
            Text(shown(run.agent_name), style=palette().muted),
            Text(shown(run.model or ""), style=palette().muted),
            progress,
            Text(f"✗ {errors}" if errors else "", style=palette().error),
            Text(ago(run.started_at, now, compact=True), style=palette().faint, justify="right"),
        ]

    def on_data_table_row_highlighted(self, _event: Table.RowHighlighted) -> None:
        self._show_highlighted()

    def on_data_table_row_selected(self, event: Table.RowSelected) -> None:
        self.app.push_screen(RunScreen(self.source, self.rows[str(event.row_key.value)]))

    def action_reload(self) -> None:
        self.load_runs()

    def action_open_datasets(self) -> None:
        self.app.push_screen(DatasetsScreen(self.source))

    def action_pick_window(self) -> None:
        choices = {str(days): f"last {days} days" for days in WINDOW_DAYS}
        self.app.push_screen(MenuScreen("Runs from", choices, str(self.window_days), key="s"), self._pick_window)

    def _pick_window(self, days: str | None) -> None:
        if days is None or int(days) == self.window_days:
            return
        self.window_days = int(days)
        since = self._since()
        self.rows = {
            key: run for key, run in self.rows.items() if run.started_at > since or run.status not in FINAL_STATUSES
        }
        self.fetching = True
        self.refilter()
        self.load_runs()

    def _since(self) -> datetime:
        return cast("ValkyrieApp", self.app).now() - timedelta(days=self.window_days)

    def _show_highlighted(self) -> None:
        key = self.query_one(Table).highlighted_key()
        run = self.rows.get(key) if key else None
        preview = self.query_one(RunPreview)
        preview.run = run
        preview.border_title = f"{short_id(run.id)} · {escape(shown(run.name))}" if run else ""
        self.query_one(CommandLine).show(f"valk run fetch {run.id} --connect" if run else "valk run list")
