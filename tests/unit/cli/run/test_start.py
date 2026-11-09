import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, TaskStatus

from valkyrie.cli.run.start import start

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_response, run_stream, serve_run

_FIRST_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
_SECOND_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")
_THIRD_RUN_ID = UUID("00000000-0000-0000-0000-000000000003")
_UNKNOWN_OUTCOME = "outcome may be unknown"
_LOCAL_CONTRACT = """\
name: local-agent
install_cmd: echo install
run_cmd: run {problem_statement_path} --variant {variant}
kwargs:
  variant: {type: str, required: true, choices: [max, min]}
"""

_ROUTER_BENCH = ("--project", "router-bench")
_GOOGLE_REPORT = {
    "id": "11111111-1111-4111-8111-111111111111",
    "handle": "google-report-xyz",
    "name": "Google Report XYZ",
    "spend_type": "customer",
    "subcategory": "report",
    "customer": "google",
    "extra_tags": {"engagement": "q4"},
    "created_at": datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc).isoformat(),
    "created_by_email": "researcher@vals.ai",
}


def _started(run_id: UUID, project: Mapping[str, object] | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "benchmark_name": "swebench",
            "agent_name": "remote-agent",
            "benchmark_id": str(run_id),
            "concurrency": 5,
            "started_at": datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc).isoformat(),
            "task_count": 2,
            "cloudwatch_url": f"https://cloudwatch.example/{run_id}",
            "s3_bucket_url": f"s3://runs/{run_id}",
            "project": project,
        },
    )


def _start_bodies(tracker: MockTracker) -> list[dict[str, object]]:
    return [json.loads(request.content) for request in tracker.requested("POST", "/start-benchmark")]


def _invoke(*arguments: str, agent: str = "remote-agent", project: tuple[str, ...] = _ROUTER_BENCH) -> Result:
    return CliRunner().invoke(start, ["--agent", agent, "--benchmark", "swebench", *project, *arguments])


@pytest.mark.description(
    "Tracker resolves the published contract, so the request carries only name, model, kwargs and secrets."
)
def test_single_start_sends_the_agent_by_name_and_prints_next_steps(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke("--model", "anthropic/claude-opus-5", "-k", "variant", "max", "-s", "API_KEY", "custom-secret")

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    assert body["concurrency"] == 5
    contract = body["contract"]
    assert (contract["name"], contract["model"], contract["run_cmd"]) == ("remote-agent", "anthropic/claude-opus-5", "")
    assert (contract["kwargs"], contract["secrets"]) == ({"variant": "max"}, {"API_KEY": "custom-secret"})
    assert f"valkyrie run fetch {_FIRST_RUN_ID} --connect" in result.output
    assert f"valkyrie run outputs {_FIRST_RUN_ID} --output-dir ." in result.output
    assert "requested runs successfully started" not in result.output


@pytest.mark.description(
    "`run start --connect` keeps streaming while tasks are still being discovered until the run completes."
)
def test_connected_start_stays_connected_while_tasks_are_discovered(tracker: MockTracker) -> None:
    discovered = make_fetch_response(
        _FIRST_RUN_ID, total_tasks=2, finished_tasks=0, task_breakdown={TaskStatus.PENDING: 1, TaskStatus.BUILDING: 1}
    )
    completed = make_fetch_response(_FIRST_RUN_ID, status=BenchmarkStatus.FINISHED, total_tasks=2, finished_tasks=2)
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))
    serve_run(
        tracker,
        make_fetch_response(_FIRST_RUN_ID, total_tasks=0, finished_tasks=0, task_breakdown={}),
        stream=run_stream(discovered, completed),
    )

    result = _invoke("-n", "1", "--connect")

    assert result.exit_code == 0, result.output
    assert "0/0 (0.0%)" in result.output
    assert "Pending: 1" in result.output
    assert "2/2 (100.0%)" in result.output
    assert "✓ Run completed!" in result.output


@pytest.mark.description(
    "`run start --count 2` starts two runs from one contract and prints one track command for both IDs."
)
def test_counted_start_reuses_one_contract_and_reports_every_run(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID), _started(_SECOND_RUN_ID))

    result = _invoke("-k", "temperature", "1", "--label", "stable", "--count", "2")

    assert result.exit_code == 0, result.output
    first, second = _start_bodies(tracker)
    assert first == second
    assert first["label"] == "stable"
    assert first["contract"]["kwargs"] == {"temperature": "1"}

    details, summary = result.output.split("2 / 2 requested runs successfully started.", maxsplit=1)
    assert str(_FIRST_RUN_ID) in details
    assert str(_SECOND_RUN_ID) in details
    assert f"Track progress: valkyrie run status --ids {_FIRST_RUN_ID},{_SECOND_RUN_ID}" in summary


@pytest.mark.description(
    "A counted start of a local agent uploads it once with `overwrite=false` and starts every run with it."
)
def test_counted_local_start_publishes_once_without_overwriting(tracker: MockTracker, tmp_path: Path) -> None:
    (tmp_path / "contract.yaml").write_text(_LOCAL_CONTRACT)
    tracker.reply("PUT", "/agents/local-agent", httpx.Response(200, json={"name": "local-agent"}))
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID), _started(_SECOND_RUN_ID))

    result = _invoke("-k", "variant", "max", "--count", "2", agent=str(tmp_path))

    assert result.exit_code == 0, result.output
    (upload,) = tracker.requested("PUT", "/agents/local-agent")
    assert upload.url.params["overwrite"] == "false"
    assert [body["contract"]["name"] for body in _start_bodies(tracker)] == ["local-agent", "local-agent"]


@pytest.mark.description("A published agent is never replaced, and an invalid contract or kwarg is never uploaded.")
@pytest.mark.parametrize(
    ("contract", "arguments", "upload", "exit_code", "message"),
    [
        (
            _LOCAL_CONTRACT,
            ["-k", "variant", "max"],
            httpx.Response(409, json={"detail": "exists"}),
            2,
            "Use --agent local-agent",
        ),
        (_LOCAL_CONTRACT, ["-k", "variant", "huge"], None, 1, "'variant' has an invalid value"),
        ("name: local-agent\n", [], None, 1, "Invalid contract"),
    ],
    ids=["already-published", "invalid-kwarg", "invalid-contract"],
)
def test_local_start_stops_before_starting_a_run(
    tracker: MockTracker,
    tmp_path: Path,
    contract: str,
    arguments: list[str],
    upload: httpx.Response | None,
    exit_code: int,
    message: str,
) -> None:
    (tmp_path / "contract.yaml").write_text(contract)
    if upload is not None:
        tracker.reply("PUT", "/agents/local-agent", upload)

    result = _invoke(*arguments, agent=str(tmp_path))

    assert result.exit_code == exit_code
    assert message in result.output
    assert len(tracker.requested("PUT", "/agents/local-agent")) == (upload is not None)
    assert _start_bodies(tracker) == []


@pytest.mark.description(
    "A bare `--agent` name runs the published agent and warns that a same-named local directory is not uploaded."
)
def test_bare_agent_name_runs_published_agent_over_matching_directory(
    tracker: MockTracker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "remote-agent").mkdir()
    (tmp_path / "remote-agent" / "contract.yaml").write_text(_LOCAL_CONTRACT)
    monkeypatch.chdir(tmp_path)
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke("-k", "variant", "max", agent="remote-agent")

    assert result.exit_code == 0, result.output
    assert "Use --agent ./remote-agent to run the local directory." in result.output
    assert [request.method for request in tracker.requests] == ["POST"]
    assert [body["contract"]["name"] for body in _start_bodies(tracker)] == ["remote-agent"]


@pytest.mark.description("A path-like `--agent` that is not a directory exits nonzero before any request is sent.")
def test_missing_agent_directory_fails_before_any_request(tracker: MockTracker, tmp_path: Path) -> None:
    result = _invoke(agent=str(tmp_path / "missing"))

    assert result.exit_code == 2
    assert "Agent directory not found" in result.output
    assert tracker.requests == []


@pytest.mark.description("Invalid `run start` options exit nonzero before any request is sent.")
@pytest.mark.parametrize(
    "arguments",
    [
        ["--count", "0"],
        ["--count", "11"],
        ["--concurrency", "0"],
        ["--count", "2", "--connect", "--task-ids-file", "unread.txt"],
    ],
)
def test_invalid_options_fail_before_any_request(tracker: MockTracker, arguments: list[str]) -> None:
    result = _invoke(*arguments)

    assert result.exit_code != 0
    assert tracker.requests == []


@pytest.mark.description("`run start` sends `monitoring` only when network inspection is asked for.")
def test_start_requests_monitoring_only_when_asked(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))
    tracker.reply("POST", "/start-benchmark", _started(_SECOND_RUN_ID))

    assert _invoke().exit_code == 0
    result = _invoke("--monitor")

    assert result.exit_code == 0, result.output
    plain, monitored = _start_bodies(tracker)
    assert "monitoring" not in plain
    assert monitored["monitoring"] == {}


@pytest.mark.description(
    "Without --provider the Tracker uses the user's saved default provider, so the request leaves it out."
)
@pytest.mark.parametrize(
    ("options", "sent"),
    [
        ([], (None, False, None, {})),
        (
            ["--provider", "modal", "--ics", "--priority", "0", "--header", "Authorization", "Bearer cli"],
            ("modal", True, 0, {"Authorization": "Bearer cli"}),
        ),
    ],
    ids=["defaults", "overrides"],
)
def test_start_sends_only_the_provider_and_priority_it_was_given(
    tracker: MockTracker, options: list[str], sent: tuple[object, ...]
) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke(*options)

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    fields = ("sandbox_provider", "ignore_custom_services", "priority", "service_headers")
    assert tuple(body.get(field) for field in fields) == sent
    assert ("Priority override:" in result.output) == ("--priority" in options)


@pytest.mark.description(
    "Stop at the first failed start. Only a failure the server may have acted on is reported as unknown."
)
@pytest.mark.parametrize(
    ("count", "confirmed", "failure", "detail", "unknown_outcome"),
    [
        (3, 1, httpx.Response(403, json={"detail": "invalid token"}), "invalid token", False),
        (3, 1, httpx.Response(502, json={"detail": "benchmark unavailable"}), "benchmark unavailable", True),
        (3, 1, httpx.ConnectError("connection reset"), "connection reset", True),
        (2, 1, httpx.Response(200, json={"benchmark_id": str(_SECOND_RUN_ID)}), "StartBenchmarkResponse", True),
        (2, 0, httpx.Response(400, text="plain rejection"), "plain rejection", False),
        (1, 0, httpx.Response(503, text="tracker unavailable"), "tracker unavailable", True),
    ],
    ids=["authentication", "benchmark-service", "transport", "malformed-success", "first", "single"],
)
def test_failure_stops_and_reports_confirmed_progress(
    tracker: MockTracker,
    count: int,
    confirmed: int,
    failure: httpx.Response | Exception,
    detail: str,
    unknown_outcome: bool,
) -> None:
    successes = [_started(run_id) for run_id in (_FIRST_RUN_ID,)[:confirmed]]
    tracker.reply("POST", "/start-benchmark", *successes, failure, _started(_THIRD_RUN_ID))

    result = _invoke(*([] if count == 1 else ["--count", str(count)]))

    assert result.exit_code == 1
    assert len(_start_bodies(tracker)) == confirmed + 1
    assert detail in result.output
    assert str(_THIRD_RUN_ID) not in result.output
    assert (_UNKNOWN_OUTCOME in result.output) is unknown_outcome
    if count > 1:
        assert f"{confirmed} / {count} requested runs successfully started." in result.output
    assert (f"valkyrie run status --ids {_FIRST_RUN_ID}" in result.output) is bool(confirmed)


@pytest.mark.description("`run start --project` sends the project and prints its spend type, subcategory, and tags.")
def test_start_sends_the_project_and_shows_its_spend_fields(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID, _GOOGLE_REPORT))

    result = _invoke(project=("--project", " google-report-xyz "))

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    assert body["project"] == "google-report-xyz"
    assert "│ Spend:            customer · google-report-xyz · report · google · engagement=q4" in result.output


@pytest.mark.description("When the Tracker saved no project, `run start` shows the spend as `untagged`.")
def test_start_shows_untagged_when_the_tracker_saved_no_project(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke()

    assert result.exit_code == 0, result.output
    assert "│ Spend:            untagged" in result.output


@pytest.mark.description("`run start` shows escape characters in project fields as visible `\\x1b` text.")
def test_start_escapes_terminal_control_characters_in_the_project(tracker: MockTracker) -> None:
    hostile = {**_GOOGLE_REPORT, "handle": "Blog\x1b[2K", "extra_tags": {"note": "a\x1b[1Ab"}}
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID, hostile))

    result = _invoke()

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert "Blog\\x1b[2K" in result.output
    assert "note=a\\x1b[1Ab" in result.output


@pytest.mark.description(
    "Starting in an unknown project lists the existing handles with escapes shown as text, plus the flags to create one."
)
def test_start_in_an_unknown_project_shows_the_existing_ids_escaped(tracker: MockTracker) -> None:
    detail = (
        "Project 'router-bnech' does not exist. Existing projects: router-bench, evil\x1b[2K. "
        "Create it with: valkyrie project create router-bnech --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY --budget AMOUNT --budget-reason REASON"
    )
    tracker.reply("POST", "/start-benchmark", httpx.Response(422, json={"detail": detail}))

    result = _invoke(project=("--project", "router-bnech"))

    assert result.exit_code == 1
    assert "\x1b" not in result.output
    assert "Existing projects: router-bench, evil\\x1b[2K." in result.output
    assert (
        "valkyrie project create router-bnech --name NAME --spend-type development|internal|customer "
        "--subcategory SUBCATEGORY"
    ) in result.output


@pytest.mark.description("`run start` without `--project` sends a start request with no project and starts the run.")
def test_start_without_a_project_sends_no_project(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke(project=())

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    assert "project" not in body
    assert "│ Spend:            untagged" in result.output


@pytest.mark.description("When the organization requires a project, `run start` prints the Tracker's message in full.")
def test_start_without_a_project_shows_the_organizations_requirement(tracker: MockTracker) -> None:
    detail = (
        "This organization requires a project for every run. Pass --project HANDLE (or project= in the SDK); "
        "create one with: valkyrie project create HANDLE --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY"
    )
    tracker.reply("POST", "/start-benchmark", httpx.Response(422, json={"detail": detail}))

    result = _invoke(project=())

    assert result.exit_code == 1
    assert "Run failed to start!" in result.output
    assert f"Error: {detail}" in result.output


@pytest.mark.description("`run start` with a blank `--project` names the problem and sends nothing.")
def test_start_with_a_blank_project_names_it_before_any_request(tracker: MockTracker) -> None:
    result = _invoke(project=("--project", "  "))

    assert result.exit_code == 2
    assert "must name the project this run is for" in result.output
    assert tracker.requests == []
