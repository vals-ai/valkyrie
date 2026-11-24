from __future__ import annotations

import os
import re
import shlex
import sys
from functools import cache, wraps
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast

import click
from click.decorators import FC
from pydantic import BaseModel, TypeAdapter

from valkyrie.display import terminal_safe
from valkyrie.palette import DARK, LIGHT, ROLES, STATUSES, Palette, status_label

if TYPE_CHECKING:
    from rich.console import Console
    from rich.text import Text

Page = TypeVar("Page")
_GLYPHS = {glyph for glyph, _ in STATUSES.values()}
_PIPE_WIDTH = 10_000
_LABEL_WIDTH = 17
_LETTER = re.compile("[A-Za-z]")
_NEXT_KEYS = {"l", "\x1b[C", "\xe0M"}
_PREVIOUS_KEYS = {"h", "\x1b[D", "\xe0K"}
_NUMBER = re.compile(r"[-+]?[\d,]*\.?\d+(%|/\d+)?|-|")


def json_option(*, stream: bool = False) -> Callable[[FC], FC]:
    def decorate(function: FC) -> FC:
        @wraps(function)
        def wrapped(*args: Any, json_output: bool = False, jsonl_output: bool = False, **kwargs: Any) -> Any:
            if json_output and jsonl_output:
                raise click.UsageError("--json and --jsonl cannot be combined.")
            return function(
                *args, output_format="jsonl" if jsonl_output else "json" if json_output else "text", **kwargs
            )

        decorated = click.option("--json", "json_output", is_flag=True, help="Print machine-readable JSON.")(wrapped)
        if stream:
            decorated = click.option("--jsonl", "jsonl_output", is_flag=True, help="Print one JSON object per event.")(
                decorated
            )
        return cast(FC, decorated)

    return decorate


def emit(value: Any, output_format: str = "json") -> bool:
    if output_format == "text":
        return False
    data = (
        value.model_dump(mode="json")
        if isinstance(value, BaseModel)
        else TypeAdapter(Any).dump_python(value, mode="json")
    )
    click.echo(TypeAdapter(Any).dump_json(data, ensure_ascii=True).decode())
    return True


def confirm(message: str, *, yes: bool = False) -> bool:
    if yes:
        return True
    ctx = click.get_current_context(silent=True)
    if ctx is not None and (ctx.params.get("json_output") or ctx.params.get("jsonl_output")):
        raise click.UsageError("Use --yes to confirm this action with --json.")
    question = (("? ", "accent"), (message, "bold"))
    if not (sys.stdin.isatty() and sys.stderr.isatty()):
        return click.confirm(styled(*question, err=True), err=True)
    from rich.text import Text

    answer = False
    while True:
        options = [
            Text(f" {label} ", "accent" if picked else "muted")
            for label, picked in [("Yes", answer), ("No", not answer)]
        ]
        options[not answer].stylize("reverse")
        click.echo("\r\x1b[K" + styled(*question, "  ", *options, err=True), nl=False, err=True)
        key = click.getchar()
        if key.lower() in ("y", "n"):
            answer = key.lower() == "y"
            break
        if key in ("\r", "\n"):
            break
        if key in ("\x1b[D", "\x1b[C", "\t", "h", "l"):
            answer = not answer
    click.echo("\r\x1b[K" + styled(*question, " ", ("Yes" if answer else "No", "accent"), err=True), err=True)
    return answer


def console(*, err: bool = False) -> Console:
    from rich.console import Console
    from rich.theme import Theme

    ctx = click.get_current_context(silent=True)
    no_color = "NO_COLOR" in os.environ or (ctx is not None and ctx.color is False)
    stream = sys.stderr if err else sys.stdout
    colors = DARK if no_color or not stream.isatty() else _terminal_palette()
    return Console(
        stderr=err,
        theme=Theme({role: getattr(colors, role) for role in ROLES}),
        no_color=no_color,
        highlight=False,
        soft_wrap=True,
        width=None if stream.isatty() else _PIPE_WIDTH,
    )


def echo(*parts: str | Text | tuple[str, str], err: bool = True) -> None:
    from rich.text import Text

    console(err=err).print(Text.assemble(*parts))


def details(
    title: str,
    rows: Iterable[tuple[str, str | None]],
    steps: Iterable[tuple[str, str | None]] = (),
    *,
    err: bool = False,
) -> None:
    from rich.text import Text

    shown = [(label, value) for label, value in rows if value]
    next_steps = [(label, step) for label, step in steps if step]
    width = max([_LABEL_WIDTH, *(len(label) for label, _ in [*shown, *next_steps])])
    if shown:
        echo((title, "bold"), err=err)
    for label, value in shown:
        indented = value.replace("\n", "\n" + " " * (width + 5))
        echo((f"  {label:<{width}}   ", "muted"), Text.from_ansi(indented), err=err)
    if next_steps:
        if shown:
            echo(err=err)
        echo(("next:", "muted"), err=err)
        for label, step in next_steps:
            echo((f"  {label:<{width}}   ", "muted"), (step, "accent"), err=err)


class Failure(click.ClickException):
    def __init__(self, message: str, *, detail: str | None = None, hint: str | None = None, exit_code: int = 1):
        super().__init__(message)
        self.detail = detail
        self.hint = hint
        self.exit_code = exit_code

    def show(self, file: Any = None) -> None:
        echo((f"error: {self.message}", "error"))
        if self.detail:
            echo(("\n".join(f"  {line}" for line in self.detail.splitlines()), "muted"))
        if self.hint:
            echo(("  try: ", "muted"), (self.hint, "bold"))


def command(*parts: str) -> str:
    ctx = click.get_current_context(silent=True)
    tracker = ctx.meta.get("tracker") if ctx is not None else None
    return " ".join(["valkyrie", *(["--tracker", shlex.quote(tracker)] if tracker else []), *parts])


def subcommand_path(ctx: click.Context | None) -> list[str]:
    return ctx.command_path.split()[1:] if ctx is not None else []


def styled(*parts: str | Text | tuple[str, str], err: bool = False) -> str:
    from rich.text import Text

    output = console(err=err)
    with output.capture() as captured:
        output.print(Text.assemble(*parts), end="")
    return captured.get()


def done(message: str) -> None:
    echo((f"{STATUSES['FINISHED'][0]} ", "finished"), message)


def fail(message: str) -> None:
    echo((f"{STATUSES['ERROR'][0]} ", "error"), message)


def empty(message: str) -> None:
    echo((message, "muted"))


def warn(message: str) -> None:
    echo(("! ", "warning"), message)


def status_text(value: str) -> Text:
    from rich.text import Text

    glyph, role = STATUSES[value]
    return Text(f"{glyph} {status_label(value)}", style=role)


@cache
def _terminal_palette() -> Palette:
    if colorfgbg := os.environ.get("COLORFGBG"):
        return LIGHT if colorfgbg.rpartition(";")[2] in {"7", "15"} else DARK
    background = _terminal_background()
    return LIGHT if background is not None and background > 0.5 else DARK


def _terminal_background() -> float | None:
    try:
        import select
        import termios
        import tty

        fd = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY)
    except (ImportError, OSError):
        return None
    reply = b""
    try:
        if os.tcgetpgrp(fd) != os.getpgrp():
            return None
        saved = termios.tcgetattr(fd)
        tty.setcbreak(fd, termios.TCSANOW)
        try:
            if select.select([fd], [], [], 0)[0]:
                return None
            os.write(fd, b"\x1b]11;?\x07\x1b[c")
            while not re.search(rb"\x1b\[\?[\d;]*c", reply) and select.select([fd], [], [], 0.2)[0]:
                reply += os.read(fd, 256)
        finally:
            termios.tcsetattr(fd, termios.TCSANOW, saved)
    except (termios.error, OSError):
        return None
    finally:
        os.close(fd)
    if match := re.search(rb"rgb:([0-9a-f]+)/([0-9a-f]+)/([0-9a-f]+)", reply, re.IGNORECASE):
        red, green, blue = (int(channel, 16) / (16 ** len(channel) - 1) for channel in match.groups())
        return 0.2126 * red + 0.7152 * green + 0.0722 * blue
    return None


def one_line(value: str) -> str:
    return terminal_safe(value, preserve_newlines=False)


def utc_isoformat(value: datetime | None = None) -> str:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def progress_percent(finished_tasks: int, total_tasks: int) -> float:
    return finished_tasks / total_tasks * 100 if total_tasks > 0 else 0


def _local_datetime(dt: datetime) -> datetime:
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).astimezone()


def local_time(dt: datetime) -> str:
    return _local_datetime(dt).strftime("%Y-%m-%d %H:%M:%S %Z")


def short_local_time(dt: datetime, include_date: bool = True) -> str:
    fmt = "%m/%d %H:%M" if include_date else "%H:%M"
    return _local_datetime(dt).strftime(fmt)


def format_table(
    rows: list[dict[str, str]],
    headers: list[str],
    current_page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
    item_name: str = "item",
) -> None:
    if not rows:
        empty(f"No {item_name}s found.")
        return

    if total_count is None:
        total_count = len(rows)

    from rich import box
    from rich.table import Table
    from rich.text import Text

    table = Table(
        box=box.SIMPLE_HEAD,
        show_edge=False,
        pad_edge=False,
        padding=(0, 1, 0, 0),
        header_style="muted",
        border_style="faint",
    )
    for index, header in enumerate(headers):
        cells = [click.unstyle(str(row.get(header, ""))) for row in rows]
        numeric = all(_NUMBER.fullmatch(cell) for cell in cells)
        table.add_column(
            Text(header, no_wrap=True, overflow="ellipsis"),
            justify="right" if numeric else "left",
            no_wrap=index == 0 or all(cell[:1] in _GLYPHS or not _LETTER.search(cell) for cell in cells),
        )
    for row in rows:
        table.add_row(
            *(Text.from_ansi(str(row.get(header, "")), no_wrap=True, overflow="ellipsis") for header in headers)
        )
    click.echo()
    console().print(table)

    total_text = f"Total: {total_count} {item_name}(s)"
    if total_pages > 1 and not _interactive_terminal():
        click.echo(f"{total_text}, page {current_page}/{total_pages} (paging needs a terminal)")
    elif total_pages > 1:
        echo(total_text, (f"   ← h  {current_page}/{total_pages}  l →   r refresh   q quit", "muted"), err=False)
    else:
        click.echo(total_text)


def _interactive_terminal() -> bool:
    return all(stream is not None and stream.isatty() for stream in (sys.stdin, sys.stdout))


def _clear_pager() -> None:
    click.echo("\033[2J\033[3J\033[1;1H", nl=False, color=True)


def paginate_cli_pages(
    load_page: Callable[[int, int], tuple[int, Page]],
    render_page: Callable[[Page, int, int, int], None],
    *,
    limit: int,
    render_empty: Callable[[], None],
) -> None:
    current_page = 1
    interactive = _interactive_terminal()
    pages: dict[int, tuple[int, Page]] = {}

    while True:
        if current_page not in pages:
            pages[current_page] = load_page((current_page - 1) * limit, limit)
        total_count, page = pages[current_page]
        total_pages = max(1, (total_count + limit - 1) // limit)
        if current_page > total_pages:
            pages.clear()
            current_page = total_pages
            continue

        if interactive:
            _clear_pager()

        if total_count == 0:
            render_empty()
            break

        render_page(page, current_page, total_pages, total_count)

        if total_pages <= 1 or not interactive:
            break

        char = click.getchar()

        if char in _NEXT_KEYS and current_page < total_pages:
            current_page += 1
        elif char in _PREVIOUS_KEYS and current_page > 1:
            current_page -= 1
        elif char == "r":
            pages.clear()
        elif char == "q" or char == "\x03":
            break
