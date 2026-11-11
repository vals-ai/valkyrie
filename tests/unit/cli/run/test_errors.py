import json
from pathlib import Path
from uuid import uuid4

import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, FailureCategory, FinalViewResponse

from valkyrie.cli.run.errors import errors

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_final_view, ok


def invoke_with_tracker(tracker: MockTracker, response: FinalViewResponse, *args: str) -> Result:
    tracker.reply("GET", "/retrieve-results", ok(response))
    return CliRunner().invoke(errors, [str(response.benchmark_id), *args])


@pytest.mark.description("Text errors group identical messages without writing files.")
def test_errors_text_groups_identical_messages_without_writing_files(tracker: MockTracker) -> None:
    run_id = uuid4()
    shared_message = "Required output artifact was not produced."
    task_errors = {f"task-{index:03}": shared_message for index in range(100)}
    task_errors["other-task"] = "Evaluator returned an invalid response."
    tracker.reply("GET", "/retrieve-results", ok(make_final_view(run_id, task_errors=task_errors)))
    runner = CliRunner()

    with runner.isolated_filesystem():
        result = runner.invoke(errors, [str(run_id)])
        assert list(Path.cwd().iterdir()) == []

    assert result.exit_code == 0, result.output
    assert "Run Errors" in result.stdout
    assert "Stored run error" in result.stdout
    assert "Task errors (101 tasks, 2 distinct messages)" in result.stdout
    assert "[100 tasks]" in result.stdout
    assert result.stdout.count(shared_message) == 1
    assert "task-000" in result.stdout
    assert "task-004" in result.stdout
    assert "task-005" not in result.stdout
    assert "(+95 more)" in result.stdout
    (request,) = tracker.requests
    assert dict(request.url.params) == {"benchmark_id": str(run_id), "s3": "false"}


@pytest.mark.description("Text errors handle run, task, and empty states.")
@pytest.mark.parametrize(
    ("error_message", "task_errors", "status", "expected", "unexpected"),
    [
        ("Run failed before task execution.", None, BenchmarkStatus.ERROR, "Stored run error", "Task errors ("),
        ("Previous attempt failed.", None, BenchmarkStatus.IN_PROGRESS, "Stored run error", "Task errors ("),
        (None, {"task-a": "Task failed."}, BenchmarkStatus.FINISHED, "Task errors (1 task", "Stored run error"),
        (None, None, BenchmarkStatus.ERROR, "No current error messages recorded.", "Stored run error"),
    ],
)
def test_errors_text_handles_run_task_and_empty_states(
    tracker: MockTracker,
    error_message: str | None,
    task_errors: dict[str, str] | None,
    status: BenchmarkStatus,
    expected: str,
    unexpected: str,
) -> None:
    run_id = uuid4()
    result = invoke_with_tracker(
        tracker, make_final_view(run_id, status=status, error_message=error_message, task_errors=task_errors)
    )

    assert result.exit_code == 0, result.output
    assert expected in result.stdout
    assert unexpected not in result.stdout
    assert status.value.replace("_", " ").title() in result.stdout


@pytest.mark.description("Text errors keep empty messages.")
def test_errors_text_preserves_empty_messages(tracker: MockTracker) -> None:
    result = invoke_with_tracker(tracker, make_final_view(uuid4(), error_message="", task_errors={"task-a": ""}))

    assert result.exit_code == 0, result.output
    assert result.stdout.count("(empty error message)") == 2


@pytest.mark.description("Text errors strip terminal control characters.")
def test_errors_text_sanitizes_terminal_controls(tracker: MockTracker) -> None:
    unsafe_message = "\x1b]8;;https://example.invalid\x07click\x1b]8;;\x07\rrewritten\nnext\tline\u202e"
    response = make_final_view(uuid4(), error_message=unsafe_message, task_errors={"task\x1b[31m": "boom\b"})

    result = invoke_with_tracker(tracker, response)

    assert result.exit_code == 0, result.output
    for control in ("\x1b", "\x07", "\r", "\b", "\t", "\u202e"):
        assert control not in result.stdout
    assert "\\x1b" in result.stdout
    assert "\\x07" in result.stdout
    assert "\\r" in result.stdout
    assert "\\u202e" in result.stdout
    assert "next    line" in result.stdout


@pytest.mark.description("JSON errors print the SDK `FinalViewResponse` with every task's error and category.")
@pytest.mark.parametrize("task_errors", [{"task-b": "task failed\n\x1b[31mred", "task-a": "another failure"}, None])
def test_errors_json_prints_the_sdk_final_view(tracker: MockTracker, task_errors: dict[str, str] | None) -> None:
    response = make_final_view(
        uuid4(),
        error_message=None,
        task_errors=task_errors,
        task_failure_categories={"task-b": FailureCategory.AGENT} if task_errors else None,
    )

    result = invoke_with_tracker(tracker, response, "--format", "json")

    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 1
    assert "\x1b" not in result.stdout
    assert json.loads(result.stdout) == response.model_dump(mode="json")


@pytest.mark.description("Text errors label each group with its category.")
def test_errors_text_labels_groups_with_category(tracker: MockTracker) -> None:
    response = make_final_view(
        uuid4(),
        error_message=None,
        task_errors={"task-a": "sandbox setup failed", "task-b": "legacy failure"},
        task_failure_categories={"task-a": FailureCategory.INFRASTRUCTURE},
    )

    result = invoke_with_tracker(tracker, response)

    assert result.exit_code == 0, result.output
    assert "[1 task, infrastructure] task-a" in result.stdout
    assert "[1 task] task-b" in result.stdout
