import json

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.agent import agent

from tests.unit.cli.conftest import MockTracker


@pytest.mark.description("Agent list prints every agent without paging.")
@pytest.mark.parametrize("count", [0, 12])
@pytest.mark.parametrize("arguments", [["--all"], ["--format", "json"], ["--all", "--format", "JSON"]])
def test_list_without_paging(tracker: MockTracker, cli_runner: CliRunner, count: int, arguments: list[str]) -> None:
    agents = [
        {"name": f"agent-{index}", "last_modified": "2026-07-17T12:00:00+00:00" if index else None}
        for index in range(count)
    ]
    tracker.reply("GET", "/agents", httpx.Response(200, json={"agents": agents}))

    result = cli_runner.invoke(agent, ["list", *arguments])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    if "--format" in arguments:
        assert json.loads(result.output) == {"agents": agents}
    elif count:
        assert all(entry["name"] in result.output for entry in agents)
        assert "[q] quit" not in result.output
    else:
        assert "No agents found" in result.output


@pytest.mark.description(
    "`agent list` exits 1 with the bad timestamp in the message when a timestamp cannot be parsed."
)
def test_list_reports_a_malformed_timestamp_without_a_traceback(tracker: MockTracker, cli_runner: CliRunner) -> None:
    agents = [{"name": "agent", "last_modified": "yesterday"}]
    tracker.reply("GET", "/agents", httpx.Response(200, json={"agents": agents}))

    result = cli_runner.invoke(agent, ["list", "--all"])

    assert result.exit_code == 1
    assert "Invalid isoformat string: 'yesterday'" in result.output


@pytest.mark.description("`agent remove` asks first, and `--yes` removes without a prompt so scripts can run it.")
def test_remove_asks_unless_yes(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("DELETE", "/agents/my-agent", httpx.Response(200, json={"name": "my-agent", "last_modified": None}))

    declined = cli_runner.invoke(agent, ["remove", "my-agent"], input="n\n")
    confirmed = cli_runner.invoke(agent, ["remove", "my-agent", "--yes"])

    assert declined.exit_code == 0, declined.output
    assert "Cancelled." in declined.output
    assert confirmed.exit_code == 0, confirmed.output
    assert "?" not in confirmed.output
    assert "removed successfully" in confirmed.output
    assert len(tracker.requests) == 1
