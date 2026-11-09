import pytest

import json
from pathlib import Path
from uuid import UUID

import httpx
from click.testing import CliRunner

from valkyrie.cli.run.stop import stop

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


@pytest.mark.description(
    (
        "Resolve valid task selectors and reject ambiguous or empty input.\n"
        "\n"
        "Test cases:\n"
        "- Comma-separated task IDs are trimmed and deduplicated.\n"
        "- A task ID file uses the shared one-per-line parser.\n"
        "- Inline and file selectors cannot be combined.\n"
        "- A delimiter-only selector cannot widen into a full-run stop."
    )
)
def test_stop_task_selection(tracker: MockTracker, tmp_path: Path) -> None:
    tracker.reply("POST", f"/stop-benchmark/{_RUN_ID}", httpx.Response(200, json={"status": "success"}))
    task_ids_file = tmp_path / "task-ids.txt"
    task_ids_file.write_text("task-c\ntask-d\n", encoding="utf-8")
    runner = CliRunner()

    inline_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids", "task-a, task-b,task-a"], input="y\n")
    file_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids-file", str(task_ids_file)], input="y\n")
    conflicting_result = runner.invoke(
        stop, [str(_RUN_ID), "--task-ids", "task-a", "--task-ids-file", str(task_ids_file)]
    )
    empty_result = runner.invoke(stop, [str(_RUN_ID), "--task-ids", ","])

    assert inline_result.exit_code == 0, inline_result.output
    assert file_result.exit_code == 0, file_result.output
    assert [json.loads(request.content)["task_ids"] for request in tracker.requests] == [
        ["task-a", "task-b"],
        ["task-c", "task-d"],
    ]
    assert conflicting_result.exit_code == 2
    assert "mutually exclusive" in conflicting_result.output
    assert empty_result.exit_code == 2
    assert "No task ids provided" in empty_result.output


@pytest.mark.description("`run stop --yes` stops without a prompt, so scripts can run it.")
def test_stop_yes_skips_the_prompt(tracker: MockTracker) -> None:
    tracker.reply("POST", f"/stop-benchmark/{_RUN_ID}", httpx.Response(200, json={"status": "success"}))

    result = CliRunner().invoke(stop, [str(_RUN_ID), "--force", "-y"])

    assert result.exit_code == 0, result.output
    assert "?" not in result.output
    assert len(tracker.requests) == 1
    assert "force stopped" in result.output
