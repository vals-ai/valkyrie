import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, BenchmarkStatusEntry, BenchmarkStatusResponse

from valkyrie.cli.run.status import status_runs

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import ok


def make_entry(index: int, *, status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS) -> BenchmarkStatusEntry:
    return BenchmarkStatusEntry(
        id=UUID(int=index + 1),
        status=status,
        finished_at=datetime(2026, 7, 9, 13, 0, tzinfo=timezone.utc) if status == BenchmarkStatus.FINISHED else None,
        total_tasks=4,
        finished_tasks=1,
        task_state_counts={"FINISHED": 1, "IN_PROGRESS": 3},
    )


def invoke_with_entries(tracker: MockTracker, entries: list[BenchmarkStatusEntry], args: list[str]) -> Result:

    def statuses(request: httpx.Request) -> httpx.Response:
        requested = set(request.url.params["ids"].split(","))
        return ok(BenchmarkStatusResponse(entries=[entry for entry in entries if str(entry.id) in requested]))

    tracker.reply("GET", "/benchmarks/status", statuses)
    return CliRunner().invoke(status_runs, args)


def _requested_ids(tracker: MockTracker) -> list[list[str]]:
    return [request.url.params["ids"].split(",") for request in tracker.requests]


@pytest.mark.description("JSON status removes duplicate IDs and keeps the requested order.")
def test_status_json_deduplicates_and_restores_requested_order(tracker: MockTracker) -> None:
    first = make_entry(0)
    second = make_entry(1, status=BenchmarkStatus.FINISHED)

    result = invoke_with_entries(
        tracker, [second, first], ["--ids", f" {first.id}, {second.id}, {first.id} ", "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 1
    assert "\x1b" not in result.stdout
    assert json.loads(result.stdout) == BenchmarkStatusResponse(entries=[first, second]).model_dump(mode="json")
    assert _requested_ids(tracker) == [[str(first.id), str(second.id)]]


@pytest.mark.description(
    "`run status --json` prints the runs it found, warns on stderr about the rest, and exits nonzero."
)
def test_status_json_reports_missing_ids_and_exits_nonzero(tracker: MockTracker) -> None:
    found = make_entry(0)
    missing = UUID(int=2)

    result = invoke_with_entries(tracker, [found], ["--ids", f"{found.id},{missing}", "--format", "json"])

    assert result.exit_code != 0
    assert [entry["id"] for entry in json.loads(result.stdout)["entries"]] == [str(found.id)]
    assert "not found or are not accessible" in result.stderr


@pytest.mark.description("JSON status splits large ID sets into chunks.")
def test_status_json_chunks_large_id_sets(tracker: MockTracker) -> None:
    entries = [make_entry(index) for index in range(51)]

    result = invoke_with_entries(
        tracker, entries, ["--ids", ",".join(str(entry.id) for entry in entries), "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    assert [len(ids) for ids in _requested_ids(tracker)] == [50, 1]
    assert len(json.loads(result.stdout)["entries"]) == 51


@pytest.mark.description("A malformed `--ids` value fails with an error naming the flag before any request.")
@pytest.mark.parametrize("ids_value", ["", " , ", "not-a-uuid"])
def test_status_rejects_invalid_ids_before_any_request(tracker: MockTracker, ids_value: str) -> None:
    result = CliRunner().invoke(status_runs, ["--ids", ids_value, "--format", "json"])

    assert result.exit_code == 2
    assert "--ids" in result.output
    assert tracker.requests == []
