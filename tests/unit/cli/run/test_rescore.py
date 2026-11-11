import pytest
from uuid import UUID

import httpx
from click.testing import CliRunner

from valkyrie.cli.run.rescore import rescore

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def _reply(final_score: float | None, **properties: object) -> httpx.Response:
    return httpx.Response(
        200, json={"benchmark_id": str(_RUN_ID), "final_score": final_score, "properties": dict(properties)}
    )


@pytest.mark.description("Rescore prints the new score.")
def test_rescore_prints_the_new_score(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("POST", f"/rescore-benchmark/{_RUN_ID}", _reply(75.0, scorer={"digest": "sha256:x", "cached": False}))

    result = cli_runner.invoke(rescore, [str(_RUN_ID)])

    assert result.exit_code == 0, result.output
    assert f"Run {_RUN_ID} rescored: score 75.00." in result.output


@pytest.mark.description("Rescore says when the script returns no single headline.")
def test_rescore_says_when_the_script_returns_no_single_headline(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("POST", f"/rescore-benchmark/{_RUN_ID}", _reply(None, scorer={"digest": "sha256:x", "cached": False}))

    result = cli_runner.invoke(rescore, [str(_RUN_ID)])

    assert result.exit_code == 0, result.output
    assert "no single headline score" in result.output


@pytest.mark.description("Rescore reports a failing script and exits with an error.")
def test_rescore_reports_a_failing_script_and_exits_with_an_error(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply(
        "POST",
        f"/rescore-benchmark/{_RUN_ID}",
        _reply(None, scorer={"digest": "sha256:x", "error": "The script exited with status 1: boom"}),
    )

    result = cli_runner.invoke(rescore, [str(_RUN_ID)])

    assert result.exit_code == 1
    assert "The metric script failed: The script exited with status 1: boom" in result.output
    assert f"Run {_RUN_ID} has no score" in result.output


@pytest.mark.description("Rescore shows the trackers refusal.")
def test_rescore_shows_the_trackers_refusal(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply(
        "POST",
        f"/rescore-benchmark/{_RUN_ID}",
        httpx.Response(409, json={"detail": "Only a finished run can be scored again"}),
    )

    result = cli_runner.invoke(rescore, [str(_RUN_ID)])

    assert result.exit_code == 1
    assert "Only a finished run can be scored again" in result.output
