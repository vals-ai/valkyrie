import json
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.update import update

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


@pytest.mark.description(
    "`run update` prints the concurrency the Tracker applied, which can differ from the one asked for."
)
def test_update_prints_the_concurrency_tracker_applied(cli_runner: CliRunner, tracker: MockTracker) -> None:
    effective = {"benchmark_id": str(_RUN_ID), "status": "IN_PROGRESS", "concurrency": 6}
    tracker.reply("PATCH", f"/benchmarks/{_RUN_ID}/concurrency", httpx.Response(200, json=effective))

    result = cli_runner.invoke(update, [str(_RUN_ID), "--concurrency", "9"])

    assert result.exit_code == 0, result.output
    assert [json.loads(request.content) for request in tracker.requests] == [{"concurrency": 9}]
    assert result.output == "✓ Run concurrency updated to 6.\n"


@pytest.mark.description("Invalid concurrency and incomplete shed options fail before contacting the Tracker.")
@pytest.mark.parametrize(
    "options",
    [
        ["--concurrency", "0"],
        ["--concurrency", "-1"],
        ["--concurrency", "1.5"],
        ["--concurrency", "not-an-integer"],
        ["--shed"],
        ["--concurrency", "2", "--dry-run"],
        ["--concurrency", "2", "--yes"],
        ["--concurrency", "2", "--shed", "--dry-run", "--project", "blog"],
    ],
)
def test_update_rejects_non_positive_integer(
    cli_runner: CliRunner,
    options: list[str],
    tracker: MockTracker,
) -> None:
    result = cli_runner.invoke(update, [str(_RUN_ID), *options])

    assert result.exit_code == 2
    assert tracker.requests == []
