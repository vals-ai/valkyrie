import pytest

from valkyrie.cli.display import format_table, paginate_cli_pages


@pytest.mark.description(
    (
        "Shared table pagination should clear each render and recover when totals shrink.\n"
        "\n"
        "Test cases:\n"
        "- Moving forward clears the previous table before rendering the next page.\n"
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
        (8, ["five", "six"]),
        (4, []),
        (1, []),
        (1, ["filtered"]),
    ]
    keys = iter(["l", "l", "l"])

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
    assert load_offsets == [0, 2, 4, 6, 2, 0]
    assert rendered_pages == [
        (["one", "two"], 1, 4, 8),
        (["three", "four"], 2, 4, 8),
        (["five", "six"], 3, 4, 8),
        (["filtered"], 1, 1, 1),
    ]
    assert output.count("\033[2J\033[3J\033[1;1H") == 4
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
