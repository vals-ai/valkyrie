import json
from uuid import UUID

import click
import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk.models import BenchmarkStatus

from valkyrie.cli.run.resume import resume, retry_command
from valkyrie.cli import client as cli_client

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_response, ok, run_stream, serve_run

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_RESTART_PATH = f"/retry-or-resume-benchmark/{_RUN_ID}"


_OVERRIDES = ["--benchmark-url", "https://new.example", "--lambda", "vals-format-lambda"]


@pytest.mark.description("Restart JSON and JSONL stdout contain only data, with next steps on stderr.")
@pytest.mark.parametrize(
    ("command", "arguments", "retried"),
    [(resume, ["--json"], False), (retry_command, ["--jsonl", "--follow"], True)],
)
def test_restart_json_keeps_next_steps_off_stdout(
    tracker: MockTracker, cli_runner: CliRunner, command: click.Command, arguments: list[str], retried: bool
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED, finished_tasks=4)
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(finished))
    tracker.reply("POST", _RESTART_PATH, httpx.Response(200, json={"status": "success"}))

    result = cli_runner.invoke(command, [str(_RUN_ID), *arguments])

    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert records[0] == {"run_id": str(_RUN_ID), "retried": retried, "resumed": not retried}
    assert "Get results" in result.stderr


@pytest.mark.description(
    "`-H` headers reach custom benchmark services, and the retry alias always retries errored tasks."
)
@pytest.mark.parametrize(
    ("command", "arguments", "retry"),
    [(resume, [], "false"), (resume, ["--update-agent"], "false"), (retry_command, _OVERRIDES, "true")],
    ids=["resume", "resume-update", "retry"],
)
def test_restart_forwards_headers_overrides_and_mode(
    tracker: MockTracker,
    cli_runner: CliRunner,
    command: click.Command,
    arguments: list[str],
    retry: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(cli_client.asyncio, "sleep", sleep)
    tracker.reply("GET", "/fetch-benchmark", ok(make_fetch_response(_RUN_ID)))
    tracker.reply(
        "POST",
        _RESTART_PATH,
        httpx.Response(503, headers={"Retry-After": "2"}, json={"detail": "warming"}),
        httpx.Response(200, json={"status": "success"}),
    )

    result = cli_runner.invoke(command, [str(_RUN_ID), "-H", "x-descope-api-key", "secret-value", *arguments])

    assert result.exit_code == 0, result.output
    first, request = tracker.requested("POST", _RESTART_PATH)
    assert first.content == request.content
    assert delays == [2]
    assert request.url.params["retry"] == retry
    assert request.url.params["update_agent"] == str("--update-agent" in arguments).lower()

    body = json.loads(request.content)
    assert body["service_headers"] == {"x-descope-api-key": "secret-value"}
    if arguments == _OVERRIDES:
        assert (body["benchmark_url"], body["lambda_function"]) == ("https://new.example", "vals-format-lambda")


@pytest.mark.description("`run resume --follow` streams the run to completion and prints no track progress hint.")
def test_connected_resume_streams_the_run_without_a_track_progress_hint(
    tracker: MockTracker, cli_runner: CliRunner
) -> None:
    finished = make_fetch_response(_RUN_ID, status=BenchmarkStatus.FINISHED, finished_tasks=4)
    serve_run(tracker, make_fetch_response(_RUN_ID), stream=run_stream(finished))
    tracker.reply("POST", _RESTART_PATH, httpx.Response(200, json={"status": "success"}))

    result = cli_runner.invoke(resume, [str(_RUN_ID), "--follow"])

    assert result.exit_code == 0, result.output
    assert "✓ Run completed!" in result.output
    assert "Track progress" not in result.output


@pytest.mark.description("`run retry` retries errored tasks under any invoked name.")
def test_retry_command_always_retries(tracker: MockTracker, cli_runner: CliRunner) -> None:
    tracker.reply("GET", "/fetch-benchmark", ok(make_fetch_response(_RUN_ID)))
    tracker.reply("POST", _RESTART_PATH, httpx.Response(200, json={"status": "success"}))

    result = cli_runner.invoke(retry_command, [str(_RUN_ID)], prog_name="valkyrie-retry")

    assert result.exit_code == 0, result.output
    (request,) = tracker.requested("POST", _RESTART_PATH)
    assert request.url.params["retry"] == "true"
