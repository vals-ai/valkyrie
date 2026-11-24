import io
import os
import select
import sys
import time
import tty
from datetime import datetime, timezone

import click
import pytest

from valkyrie.cli.display import (
    _terminal_background,
    confirm,
    format_table,
    local_time,
    paginate_cli_pages,
    short_local_time,
)


@pytest.mark.description(
    "Tracker timestamps without an offset display the same local time as timestamps with an offset."
)
@pytest.mark.skipif(not hasattr(time, "tzset"), reason="Requires a process timezone override.")
def test_local_time_converts_naive_tracker_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        with monkeypatch.context() as env:
            env.setenv("TZ", "America/Los_Angeles")
            time.tzset()
            timestamp = datetime(2026, 11, 18, 20, 6)
            assert (
                local_time(timestamp) == local_time(timestamp.replace(tzinfo=timezone.utc)) == "2026-11-18 12:06:00 PST"
            )
            assert short_local_time(timestamp) == "11/18 12:06"
    finally:
        time.tzset()


@pytest.mark.description(
    (
        "Shared table pagination should clear each render and recover when totals shrink.\n"
        "\n"
        "Test cases:\n"
        "- Moving forward clears the previous table before rendering the next page.\n"
        "- Pressing `r` drops the cached pages and reloads and redraws the current page.\n"
        "- If a filtered/refetched total invalidates the current page, pagination reloads a valid page."
    )
)
def test_paginate_cli_pages_clears_between_pages_and_handles_filtered_totals(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    load_offsets: list[int] = []
    rendered_pages: list[tuple[list[str], int, int, int]] = []
    page_responses = [
        (8, ["one", "two"]),
        (8, ["three", "four"]),
        (8, ["three", "refreshed"]),
        (8, ["five", "six"]),
        (4, []),
        (1, []),
        (1, ["filtered"]),
    ]
    keys = iter(["l", "r", "l", "l"])

    def load_page(offset: int, _limit: int) -> tuple[int, list[str]]:
        response = page_responses[len(load_offsets)]
        load_offsets.append(offset)

        return response

    def render_page(page: list[str], current_page: int, total_pages: int, total_count: int) -> None:
        rendered_pages.append((page, current_page, total_pages, total_count))
        print(f"page={current_page}/{total_pages}:{','.join(page)}")

    monkeypatch.setattr("valkyrie.cli.display._interactive_terminal", lambda: True)
    monkeypatch.setattr("valkyrie.cli.display.click.getchar", lambda: next(keys))

    paginate_cli_pages(
        load_page,
        render_page,
        limit=2,
        render_empty=lambda: print("empty"),
    )

    output = capsys.readouterr().out
    assert load_offsets == [0, 2, 2, 4, 6, 2, 0]
    assert rendered_pages == [
        (["one", "two"], 1, 4, 8),
        (["three", "four"], 2, 4, 8),
        (["three", "refreshed"], 2, 4, 8),
        (["five", "six"], 3, 4, 8),
        (["filtered"], 1, 1, 1),
    ]
    assert output.count("\033[2J\033[3J\033[1;1H") == 5
    assert "page=2/1" not in output
    assert "empty" not in output


@pytest.mark.description(
    (
        "Without a terminal, paging prints the first page and exits instead of waiting for keys.\n"
        "\n"
        "Test cases:\n"
        "- Only the first page is printed, with its page count, and no key is read.\n"
        "- No screen-clear escapes or key hints are written."
    )
)
def test_paginate_cli_pages_without_a_terminal_prints_the_first_page(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def no_keys() -> str:
        raise AssertionError("paging read a key without a terminal")

    monkeypatch.setattr("valkyrie.cli.display._interactive_terminal", lambda: False)
    monkeypatch.setattr("valkyrie.cli.display.click.getchar", no_keys)
    rows = ["one", "two", "three"]

    paginate_cli_pages(
        lambda offset, limit: (len(rows), rows[offset : offset + limit]),
        lambda page, current_page, total_pages, total_count: format_table(
            [{"Row": row} for row in page], ["Row"], current_page, total_pages, total_count, "row"
        ),
        limit=2,
        render_empty=lambda: None,
    )

    output = capsys.readouterr().out
    assert "one" in output and "two" in output and "three" not in output
    assert "Total: 3 row(s), page 1/2 (paging needs a terminal)" in output
    assert "\033[2J" not in output
    assert "[h]" not in output


@pytest.mark.description("A closed stdin (sys.stdin is None) counts as non-interactive instead of crashing.")
def test_paginate_cli_pages_with_stdin_closed_prints_the_first_page(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("valkyrie.cli.display.sys.stdin", None)
    rows = ["one", "two", "three"]

    paginate_cli_pages(
        lambda offset, limit: (len(rows), rows[offset : offset + limit]),
        lambda page, current_page, total_pages, total_count: format_table(
            [{"Row": row} for row in page], ["Row"], current_page, total_pages, total_count, "row"
        ),
        limit=2,
        render_empty=lambda: None,
    )

    assert "Total: 3 row(s), page 1/2 (paging needs a terminal)" in capsys.readouterr().out


class _Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.mark.description(
    (
        "Tables right-align numbers, and only a color terminal gets ANSI codes and truncated cells.\n"
        "\n"
        "Test cases:\n"
        "- A pipe gets the full table in plain text.\n"
        "- A narrow terminal truncates long text cells and keeps IDs and numbers whole.\n"
        "- `NO_COLOR` keeps a narrow terminal plain."
    )
)
@pytest.mark.parametrize(
    ("stream", "env", "expected", "colored"),
    [
        (
            io.StringIO(),
            {},
            "\nID     Name                                      Tasks  Score\n"
            + "─" * 61
            + "\nrun-1  a-very-long-benchmark-name-for-the-table     12  85.0%\n"
            "run-2  short                                         3      -\nTotal: 2 run(s)\n",
            False,
        ),
        (_Terminal(), {"COLUMNS": "36", "COLORFGBG": "0;0", "TERM": "xterm-256color"}, None, True),
        (
            _Terminal(),
            {"COLUMNS": "36", "NO_COLOR": "1", "TERM": "xterm-256color"},
            "\nID     Name             Tasks  Score\n"
            + "─" * 36
            + "\nrun-1  a-very-long-be…     12  85.0%\nrun-2  short                3      -\nTotal: 2 run(s)\n",
            False,
        ),
    ],
    ids=["pipe", "narrow-terminal", "no-color"],
)
def test_format_table_layout(
    monkeypatch: pytest.MonkeyPatch,
    stream: io.StringIO,
    env: dict[str, str],
    expected: str | None,
    colored: bool,
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, "stdout", stream)

    format_table(
        [
            {"ID": "run-1", "Name": "a-very-long-benchmark-name-for-the-table", "Tasks": "12", "Score": "85.0%"},
            {"ID": "run-2", "Name": "short", "Tasks": "3", "Score": "-"},
        ],
        ["ID", "Name", "Tasks", "Score"],
        item_name="run",
    )

    output = stream.getvalue()
    assert ("\x1b[" in output) is colored
    assert expected is None or output == expected
    assert "run-1  a-very-long-be…" in click.unstyle(output) or expected is not None


@pytest.mark.description(
    (
        "A terminal confirm answers on one key, toggles with the arrows and defaults to no.\n"
        "\n"
        "Test cases:\n"
        "- `y` and `n` answer straight away.\n"
        "- Enter takes the highlighted answer, which starts on no.\n"
        "- An arrow key moves the highlight before Enter."
    )
)
@pytest.mark.parametrize(
    ("keys", "answer"),
    [(["y"], True), (["N"], False), (["\r"], False), (["\x1b[D", "\r"], True), (["\x1b[C", "\x1b[D", "\r"], False)],
    ids=["y", "n", "enter", "arrow", "arrow-back"],
)
def test_terminal_confirm_keys(monkeypatch: pytest.MonkeyPatch, keys: list[str], answer: bool) -> None:
    stderr = _Terminal()
    monkeypatch.setattr(sys, "stdin", _Terminal())
    monkeypatch.setattr(sys, "stderr", stderr)
    pressed = iter(keys)
    monkeypatch.setattr("click.getchar", lambda: next(pressed))

    assert confirm("Stop run?") is answer
    assert click.unstyle(stderr.getvalue()).endswith(f"? Stop run? {'Yes' if answer else 'No'}\n")


@pytest.mark.description("Keys typed before the terminal color query stay unread, and the query is not sent.")
def test_terminal_background_keeps_typed_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    leader, follower = os.openpty()
    tty.setraw(follower)
    os.write(leader, b"y")
    monkeypatch.setattr(os, "open", lambda *_: os.dup(follower))
    monkeypatch.setattr(os, "tcgetpgrp", lambda _: os.getpgrp())

    assert _terminal_background() is None
    assert not select.select([leader], [], [], 0)[0]
    assert select.select([follower], [], [], 0)[0]
    assert os.read(follower, 1) == b"y"
    os.close(leader)
    os.close(follower)
