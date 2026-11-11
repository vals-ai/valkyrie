from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import TypeVar

import click
from click.decorators import FC

from valkyrie.display import terminal_safe

Page = TypeVar("Page")


def format_option(help: str = "Output format.", choices: Sequence[str] = ("text", "json")) -> Callable[[FC], FC]:
    return click.option(
        "--format",
        "output_format",
        type=click.Choice(list(choices), case_sensitive=False),
        default="text",
        show_default=True,
        help=help,
    )


def one_line(value: str) -> str:
    return terminal_safe(value, preserve_newlines=False)


def status_label(value: str) -> str:
    return value.replace("_", " ").title()


def utc_isoformat(value: datetime | None = None) -> str:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def progress_percent(finished_tasks: int, total_tasks: int) -> float:
    return finished_tasks / total_tasks * 100 if total_tasks > 0 else 0


def local_time(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def short_local_time(dt: datetime, include_date: bool = True) -> str:
    fmt = "%m/%d %H:%M" if include_date else "%H:%M"
    return dt.astimezone().strftime(fmt)


def format_table(
    rows: list[dict[str, str]],
    headers: list[str],
    current_page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
    item_name: str = "item",
) -> None:
    if not rows:
        click.echo(click.style(f"No {item_name}s found.", fg="yellow"))
        return

    if total_count is None:
        total_count = len(rows)

    column_widths = {}
    for header in headers:
        max_width = len(header)
        for row in rows:
            value = str(row.get(header, ""))
            clean_value = click.unstyle(value)
            max_width = max(max_width, len(clean_value))
        column_widths[header] = max_width

    header_line = "  ".join(f"{header:<{column_widths[header]}}" for header in headers)
    separator = "─" * len(header_line)

    click.echo()
    click.echo(click.style(header_line, bold=True))
    click.echo(separator)

    for row in rows:
        cells: list[str] = []
        for header in headers:
            value = str(row.get(header, ""))
            clean_value = click.unstyle(value)
            padding = column_widths[header] - len(clean_value)
            cells.append(f"{value}{' ' * padding}")
        click.echo("  ".join(cells))

    click.echo(separator)

    total_text = f"Total: {total_count} {item_name}(s)"
    if total_pages > 1 and not _interactive_terminal():
        click.echo(f"{total_text}, page {current_page}/{total_pages} (paging needs a terminal)")
    elif total_pages > 1:
        nav_text_raw = f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit"
        nav_text = click.style(nav_text_raw, fg="bright_black")
        padding = len(separator) - len(total_text) - len(nav_text_raw)
        click.echo(f"{total_text}{' ' * padding}{nav_text}")
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

    while True:
        total_count, page = load_page((current_page - 1) * limit, limit)
        total_pages = max(1, (total_count + limit - 1) // limit)
        if current_page > total_pages:
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

        if char == "l" and current_page < total_pages:
            current_page += 1
        elif char == "h" and current_page > 1:
            current_page -= 1
        elif char == "q" or char == "\x03":
            break
