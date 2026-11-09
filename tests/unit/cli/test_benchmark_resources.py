import json
from dataclasses import dataclass

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker

_TIMES = {"created_at": "2026-10-24T04:20:24.675682+00:00", "updated_at": "2026-10-24T04:25:00+00:00"}
_SWE_BENCH = {"name": "swe-bench", "description": "SWE-bench tasks", "created_by_email": "ada@example.com", **_TIMES}
_AIDER = {"name": "aider", "description": "", "created_by_email": None, **_TIMES}
_VERIFIED = {**_SWE_BENCH, "benchmark": "swe-bench", "name": "verified"}
_LITE = {**_AIDER, "benchmark": "swe-bench", "name": "lite"}
_ARCHIVED_AT = "2026-10-25T01:00:00+00:00"


@dataclass(frozen=True)
class Kind:
    group: str
    parent: tuple[str, ...]
    name: str
    collection: str
    path: str
    shown_at: str
    shown_body: dict[str, object]
    key: str
    label: str
    shown: str
    record: dict[str, object]
    first: dict[str, object]


_BENCHMARKS = Kind(
    group="benchmark",
    parent=(),
    name="swe-bench",
    collection="/v1/benchmarks",
    path="/v1/benchmarks/swe-bench",
    shown_at="/v1/benchmarks/swe-bench",
    shown_body=_SWE_BENCH,
    key="benchmarks",
    label="Benchmark",
    shown="swe-bench",
    record=_SWE_BENCH,
    first=_AIDER,
)
_DATASETS = Kind(
    group="dataset",
    parent=("swe-bench",),
    name="verified",
    collection="/v1/benchmarks/swe-bench/datasets",
    path="/v1/benchmarks/swe-bench/datasets/verified",
    shown_at="/v1/benchmarks/swe-bench/datasets/verified",
    shown_body=_VERIFIED,
    key="datasets",
    label="Dataset",
    shown="swe-bench/verified",
    record=_VERIFIED,
    first=_LITE,
)


@pytest.fixture(params=[_BENCHMARKS, _DATASETS], ids=["benchmark", "dataset"])
def kind(request: pytest.FixtureRequest) -> Kind:
    return request.param


def _invoke(kind: Kind, *arguments: str, input: str | None = None) -> Result:
    return CliRunner().invoke(cli, [kind.group, *arguments], input=input)


@pytest.mark.description("The benchmark resource subcommands work but are not listed in `--help`.")
def test_the_benchmark_commands_run_but_stay_out_of_help() -> None:
    help_text = CliRunner().invoke(cli, ["benchmark", "--help"]).output

    assert "tasks" in help_text
    for name in ("create", "list", "show", "update", "remove", "restore"):
        assert f"  {name} " not in help_text


@pytest.mark.description("`create` sends the name and `--description` and prints the created resource.")
def test_create_sends_the_name_and_description(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("POST", kind.collection, httpx.Response(200, json=kind.record))

    result = _invoke(kind, "create", *kind.parent, kind.name, "--description", "SWE-bench tasks")

    assert result.exit_code == 0, result.output
    (request,) = tracker.requests
    assert json.loads(request.content) == {"name": kind.name, "description": "SWE-bench tasks"}
    assert f"{kind.label} created: {kind.shown}" in result.output


@pytest.mark.description("A name that is not lowercase letters, digits, and hyphens fails before any request.")
@pytest.mark.parametrize("bad", ["Upper", "a b", "../agents", "x" * 65])
def test_commands_reject_invalid_names_before_any_request(tracker: MockTracker, kind: Kind, bad: str) -> None:
    target = [*kind.parent, kind.name]
    verbs = (("create",), ("show",), ("update", "--description", "x"), ("remove", "-y"), ("restore",))

    for position in range(len(target)):
        names = [bad if index == position else name for index, name in enumerate(target)]
        for verb, *options in verbs:
            result = _invoke(kind, verb, *names, *options)

            assert result.exit_code == 2, result.output
            assert "must be lowercase letters" in result.output

    assert tracker.requests == []


@pytest.mark.description("Creating a resource with a used name exits 1 with the Tracker's detail.")
def test_a_taken_name_is_reported_as_an_error(tracker: MockTracker, kind: Kind) -> None:
    detail = f"{kind.label} '{kind.name}' already exists"
    tracker.reply("POST", kind.collection, httpx.Response(409, json={"detail": detail}))

    result = _invoke(kind, "create", *kind.parent, kind.name)

    assert result.exit_code == 1
    assert detail in result.output


@pytest.mark.description("List shows the resources or says there are none.")
def test_list_reports_the_resources_or_that_there_are_none(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("GET", kind.collection, httpx.Response(200, json={kind.key: []}))

    assert f"No {kind.key} found." in _invoke(kind, "list", *kind.parent).output

    tracker.reply("GET", kind.collection, httpx.Response(200, json={kind.key: [kind.first, kind.record]}))
    text = _invoke(kind, "list", *kind.parent)
    as_json = _invoke(kind, "list", *kind.parent, "--format", "json")

    assert text.exit_code == 0, text.output
    assert "SWE-bench tasks" in text.output
    assert f"Total: 2 {kind.label.lower()}(s)" in text.output
    assert [row["name"] for row in json.loads(as_json.output)[kind.key]] == [kind.first["name"], kind.name]


@pytest.mark.description("`show` prints the resource with its creator, or its JSON with `--json`.")
def test_show_prints_the_resource_or_its_json(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("GET", kind.shown_at, httpx.Response(200, json=kind.shown_body))

    text = _invoke(kind, "show", *kind.parent, kind.name)
    as_json = _invoke(kind, "show", *kind.parent, kind.name, "--format", "json")

    assert text.exit_code == 0, text.output
    assert f"Name:        {kind.name}" in text.output
    assert "Description: SWE-bench tasks" in text.output
    assert "by ada@example.com" in text.output
    assert json.loads(as_json.output)["name"] == kind.name


@pytest.mark.description("`show` for an unknown resource exits 1 with the Tracker's detail.")
def test_showing_a_missing_resource_is_an_error(tracker: MockTracker, kind: Kind) -> None:
    detail = f"{kind.label} '{kind.name}' not found"
    tracker.reply("GET", kind.shown_at, httpx.Response(404, json={"detail": detail}))

    result = _invoke(kind, "show", *kind.parent, kind.name)

    assert result.exit_code == 1
    assert detail in result.output


@pytest.mark.description("`list` hides archived resources unless `--all` asks for them with `include_archived`.")
def test_list_leaves_out_archived_resources_unless_asked(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("GET", kind.collection, httpx.Response(200, json={kind.key: [kind.first]}))

    default = _invoke(kind, "list", *kind.parent)

    assert "Archived" not in default.output
    assert "include_archived" not in tracker.requests[0].url.params

    archived = {**kind.record, "archived_at": _ARCHIVED_AT}
    tracker.reply("GET", kind.collection, httpx.Response(200, json={kind.key: [kind.first, archived]}))

    everything = _invoke(kind, "list", *kind.parent, "--include-archived")

    assert everything.exit_code == 0, everything.output
    assert "Archived" in everything.output
    assert tracker.requests[1].url.params["include_archived"] == "true"


@pytest.mark.description("`show` prints an `Archived:` line for an archived resource.")
def test_show_says_when_a_resource_was_archived(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("GET", kind.shown_at, httpx.Response(200, json={**kind.shown_body, "archived_at": _ARCHIVED_AT}))

    result = _invoke(kind, "show", *kind.parent, kind.name)

    assert "Archived:" in result.output


@pytest.mark.description("`dataset show` prints the parent benchmark's name.")
def test_a_dataset_shows_its_benchmark(tracker: MockTracker) -> None:
    tracker.reply("GET", _DATASETS.shown_at, httpx.Response(200, json=_VERIFIED))

    result = _invoke(_DATASETS, "show", "swe-bench", "verified")

    assert "Benchmark:   swe-bench" in result.output


@pytest.mark.description("`update` sends only the new description.")
def test_update_sends_only_the_description(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("PATCH", kind.path, httpx.Response(200, json=kind.record))

    missing_option = _invoke(kind, "update", *kind.parent, kind.name)
    result = _invoke(kind, "update", *kind.parent, kind.name, "--description", "Changed")

    assert missing_option.exit_code == 2
    assert result.exit_code == 0, result.output
    (request,) = tracker.requests
    assert json.loads(request.content) == {"description": "Changed"}
    assert f"{kind.label} updated: {kind.shown}" in result.output


@pytest.mark.description("`remove` asks for confirmation and archives the resource only after a yes.")
def test_remove_asks_first_and_archives_on_yes(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("DELETE", kind.path, httpx.Response(200, json=kind.record))

    declined = _invoke(kind, "remove", *kind.parent, kind.name, input="n\n")

    assert "Cancelled." in declined.output
    assert tracker.requests == []

    confirmed = _invoke(kind, "remove", *kind.parent, kind.name, input="y\n")
    skipped_prompt = _invoke(kind, "remove", *kind.parent, kind.name, "--yes")

    assert confirmed.exit_code == skipped_prompt.exit_code == 0
    assert f"{kind.label} '{kind.shown}' archived." in confirmed.output
    assert [request.method for request in tracker.requests] == ["DELETE", "DELETE"]


@pytest.mark.description("`restore` sends one POST and prints the restored resource.")
def test_restore_brings_an_archived_resource_back(tracker: MockTracker, kind: Kind) -> None:
    tracker.reply("POST", f"{kind.path}/restore", httpx.Response(200, json=kind.record))

    result = _invoke(kind, "restore", *kind.parent, kind.name)

    assert result.exit_code == 0, result.output
    assert f"{kind.label} restored: {kind.shown}" in result.output
    assert [request.method for request in tracker.requests] == ["POST"]
