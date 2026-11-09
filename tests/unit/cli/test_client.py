import click
import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk import ValkyrieClient, ValkyrieSDKError

from valkyrie.cli.client import call
from valkyrie.cli.main import cli
from valkyrie.cli.run.filter_options import filter_options

from tests.unit.cli.conftest import MockTracker


@pytest.mark.description("An SDK error exits 1 with an escaped message and no traceback.")
@pytest.mark.parametrize(
    ("reply", "message"),
    [
        (
            httpx.Response(403, json={"detail": "invalid \x1b[31mkey"}),
            "Valkyrie API request failed (403): invalid \\x1b[31mkey",
        ),
        (httpx.ConnectError("tracker unavailable"), "Valkyrie request failed: tracker unavailable"),
        (httpx.Response(200, json={"benchmark_names": []}), "validation errors for FilterOptionsResponse"),
    ],
    ids=["api", "transport", "malformed"],
)
def test_sdk_failures_exit_1_with_an_escaped_message_and_no_partial_output(
    tracker: MockTracker, cli_runner: CliRunner, reply: httpx.Response | Exception, message: str
) -> None:
    tracker.reply("GET", "/benchmarks/filter-options", reply)

    result = cli_runner.invoke(filter_options, ["--format", "json"])

    assert result.exit_code == 1
    assert result.output.startswith("Error: ")
    assert message in result.output
    assert "\x1b" not in result.output
    assert "Traceback" not in result.output


@pytest.mark.description("A CLI-side bug inside an operation surfaces as its own exception, not a generic error.")
def test_cli_side_value_errors_propagate_with_their_traceback(tracker: MockTracker, cli_runner: CliRunner) -> None:
    async def operation(client: ValkyrieClient) -> tuple[str, str]:
        first, second = "only-one".split(",")
        return first, second

    @click.command()
    def broken() -> None:
        call(operation)

    result = cli_runner.invoke(broken)

    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)
    assert "not enough values to unpack" in str(result.exception)
    assert "Error: " not in result.output


@pytest.mark.description("A value the SDK rejects still exits 1 with its message and no traceback.")
def test_sdk_value_rejections_exit_1_with_their_message(tracker: MockTracker, cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli, ["agent", "install", "https://example.com/not-github"])

    assert result.exit_code == 1
    assert result.output == "Error: Invalid GitHub URL; expected a repository or /tree/ref[/subfolder] URL\n"
    assert isinstance(result.exception, SystemExit)


@pytest.mark.description("An SDK error that is also a ValueError stays a plain CLI error wherever it is raised.")
def test_sdk_errors_that_are_value_errors_exit_1_with_their_message(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    class InputError(ValkyrieSDKError, ValueError):
        pass

    async def operation(client: ValkyrieClient) -> None:
        raise InputError("bad input")

    @click.command()
    def rejected() -> None:
        call(operation)

    result = cli_runner.invoke(rejected)

    assert result.exit_code == 1
    assert result.output == "Error: bad input\n"
