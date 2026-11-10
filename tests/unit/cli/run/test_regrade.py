import json
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.regrade import regrade

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_NEW_RUN_ID = UUID("8b1f7b4e-3c2a-4d5e-9f60-1a2b3c4d5e6f")
_REGRADE_PATH = f"/regrade-benchmark/{_RUN_ID}"


@pytest.mark.description("`run regrade` sends the target dataset and prints the new run and the skipped tasks.")
def test_regrade_prints_new_run_and_skipped_tasks(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply(
        "POST",
        _REGRADE_PATH,
        httpx.Response(
            200,
            json={
                "benchmark_id": str(_NEW_RUN_ID),
                "regraded_from_run_id": str(_RUN_ID),
                "task_ids": ["task-pass", "task-fail"],
                "skipped_task_ids": ["task-no-state"],
            },
        ),
    )

    result = cli_runner.invoke(
        regrade, [str(_RUN_ID), "--dataset", "terminal-bench-4.1", "--concurrency", "16", "-H", "x-key", "value"]
    )

    assert result.exit_code == 0, result.output
    (request,) = tracker.requested("POST", _REGRADE_PATH)
    assert json.loads(request.content) == {
        "dataset": "terminal-bench-4.1",
        "dataset_version": None,
        "concurrency": 16,
        "service_headers": {"x-key": "value"},
    }
    assert f"Run {_NEW_RUN_ID} regrades run {_RUN_ID}." in result.output
    assert "Regrading 2 task(s) on terminal-bench-4.1." in result.output
    assert "Skipped 1 task(s) without saved evaluation state: task-no-state" in result.output


@pytest.mark.description("`run regrade` requires a target dataset.")
def test_regrade_requires_dataset(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(regrade, [str(_RUN_ID)])

    assert result.exit_code == 2
    assert "--dataset" in result.output
