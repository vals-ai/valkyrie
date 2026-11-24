import click
import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieSDKError

from valkyrie.cli.client import call, retry_unavailable
from valkyrie.cli import client as cli_client
from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker


@pytest.mark.description("Unavailable operations retry once with a bounded delay only for a valid Retry-After.")
@pytest.mark.parametrize(
    ("status", "header", "expected_delays"),
    [
        (503, "2", [2]),
        (503, "600", [30]),
        (503, "0", [0]),
        (503, None, []),
        (503, "", []),
        (503, "-1", []),
        (503, "nan", []),
        (503, "bad", []),
        (429, "2", []),
    ],
)
async def test_retry_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    header: str | None,
    expected_delays: list[float],
) -> None:
    delays: list[float] = []
    attempts = 0
    error = ValkyrieAPIError(
        status, "unavailable", httpx.Headers({"Retry-After": header} if header is not None else {})
    )

    async def sleep(delay: float) -> None:
        delays.append(delay)

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise error

    monkeypatch.setattr(cli_client.asyncio, "sleep", sleep)
    with pytest.raises(ValkyrieAPIError) as caught:
        await retry_unavailable(operation)
    assert caught.value is error
    assert delays == expected_delays
    assert attempts == 1 + bool(expected_delays)


@pytest.mark.description("An SDK error exits 1 with an escaped message and no traceback.")
@pytest.mark.parametrize(
    ("reply", "message"),
    [
        (
            httpx.Response(403, json={"detail": "invalid \x1b[31mkey"}),
            "error: invalid \\x1b[31mkey",
        ),
        (
            httpx.ConnectError("tracker unavailable"),
            "error: Could not reach the Tracker.\n  No answer from https://tracker.test: tracker unavailable\n  try: valkyrie whoami",
        ),
        (httpx.Response(200, json={"benchmark_names": []}), "validation errors for FilterOptionsResponse"),
    ],
    ids=["api", "transport", "malformed"],
)
def test_sdk_failures_exit_1_with_an_escaped_message_and_no_partial_output(
    tracker: MockTracker, cli_runner: CliRunner, reply: httpx.Response | Exception, message: str
) -> None:
    tracker.reply("GET", "/benchmarks/filter-options", reply)

    @click.command()
    def filter_options() -> None:
        call(lambda client: client.runs.filter_options())

    result = cli_runner.invoke(filter_options)

    assert result.exit_code == 1
    assert result.output.startswith("error: ")
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
    assert "error: " not in result.output


@pytest.mark.description("A value the SDK rejects still exits 1 with its message and no traceback.")
def test_sdk_value_rejections_exit_1_with_their_message(tracker: MockTracker, cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli, ["agent", "install", "https://example.com/not-github"])

    assert result.exit_code == 1
    assert result.output == "error: Invalid GitHub URL; expected a repository or /tree/ref[/subfolder] URL\n"
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
    assert result.output == "error: bad input\n"


_RUN_ID = "00000000-0000-0000-0000-000000000001"


@pytest.mark.description("Every CLI failure prints a red headline, a muted detail and the exact command to try next.")
@pytest.mark.parametrize(
    ("arguments", "status", "detail", "output", "exit_code"),
    [
        (
            ["run", "show", _RUN_ID],
            401,
            "bad key",
            "error: The Tracker rejected your credentials.\n  try: valkyrie --tracker https://other.test login\n",
            1,
        ),
        (
            ["run", "show", _RUN_ID],
            404,
            "Run not found",
            "error: Run not found\n  try: valkyrie --tracker https://other.test run list\n",
            1,
        ),
        (
            ["run", "show", _RUN_ID],
            422,
            "Sandbox provider configuration is unavailable",
            "error: Sandbox provider configuration is unavailable\n"
            "  try: valkyrie --tracker https://other.test provider list\n",
            1,
        ),
        (
            ["run", "show"],
            None,
            None,
            "error: Missing argument 'RUN_IDS...'.\n  try: valkyrie --tracker https://other.test run show --help\n",
            2,
        ),
    ],
    ids=["auth", "not-found", "provider", "usage"],
)
def test_failures_share_one_shape(
    tracker: MockTracker,
    arguments: list[str],
    status: int | None,
    detail: str | None,
    output: str,
    exit_code: int,
) -> None:
    for method in ("GET", "POST"):
        tracker.reply(method, "/fetch-benchmark", httpx.Response(status or 500, json={"detail": detail}))
        tracker.reply(
            method, f"/fetch-benchmark-metadata/{_RUN_ID}", httpx.Response(status or 500, json={"detail": detail})
        )

    result = CliRunner().invoke(cli, ["--tracker", "https://other.test", *arguments])

    assert result.exit_code == exit_code
    assert result.stderr == output
    assert result.stdout == ""
