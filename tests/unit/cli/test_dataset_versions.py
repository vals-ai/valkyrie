import json

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker

_URL = "/v1/benchmarks/swe-bench/datasets/verified"
_VERSION = {
    "benchmark": "swe-bench",
    "dataset": "verified",
    "revision": 2,
    "digest": "sha256:" + "ef" * 32,
    "task_count": 3,
    "tags": ["latest", "stable"],
    "created_by_email": "ada@example.com",
    "created_at": "2026-10-24T04:20:24+00:00",
}
_FIRST = {**_VERSION, "revision": 1, "digest": "sha256:" + "ab" * 32, "task_count": 2, "tags": []}


def _invoke(*arguments: str) -> Result:
    return CliRunner().invoke(cli, ["dataset", *arguments])


@pytest.mark.description("Versions lists each revision with its tags or says there are none.")
def test_versions_lists_each_revision_with_its_tags_or_says_there_are_none(tracker: MockTracker) -> None:
    tracker.reply("GET", f"{_URL}/versions", httpx.Response(200, json={"versions": []}))

    assert "No versions found." in _invoke("versions", "swe-bench/verified").output

    tracker.reply("GET", f"{_URL}/versions", httpx.Response(200, json={"versions": [_VERSION, _FIRST]}))
    text = _invoke("versions", "swe-bench/verified")
    as_json = _invoke("versions", "swe-bench/verified", "--format", "json")

    assert text.exit_code == 0, text.output
    assert "efefefefefef" in text.output and "latest, stable" in text.output
    assert "Total: 2 version(s)" in text.output
    assert [version["revision"] for version in json.loads(as_json.output)["versions"]] == [2, 1]


@pytest.mark.description("Tag points a tag at the version after the at sign or the latest.")
def test_tag_points_a_tag_at_the_version_after_the_at_sign_or_the_latest(tracker: MockTracker) -> None:
    tracker.reply("PUT", f"{_URL}/tags/stable", httpx.Response(200, json=_VERSION))

    named = _invoke("tag", "swe-bench/verified@2", "stable")
    latest = _invoke("tag", "swe-bench/verified", "stable")

    assert named.exit_code == latest.exit_code == 0, named.output + latest.output
    assert "Tagged swe-bench/verified@2 as stable" in named.output
    assert [json.loads(request.content) for request in tracker.requested("PUT", f"{_URL}/tags/stable")] == [
        {"ref": "2", "force": False},
        {"ref": "latest", "force": False},
    ]


@pytest.mark.description("Tag sends `force` so the tracker moves a tag that points at another version.")
def test_tag_with_force_moves_an_existing_tag(tracker: MockTracker) -> None:
    tracker.reply("PUT", f"{_URL}/tags/stable", httpx.Response(200, json=_VERSION))

    result = _invoke("tag", "swe-bench/verified@2", "stable", "--force")

    assert result.exit_code == 0, result.output
    assert [json.loads(request.content) for request in tracker.requested("PUT", f"{_URL}/tags/stable")] == [
        {"ref": "2", "force": True}
    ]


@pytest.mark.description("Tag reports a tag the tracker refuses.")
def test_tag_reports_a_tag_the_tracker_refuses(tracker: MockTracker) -> None:
    tracker.reply("PUT", f"{_URL}/tags/7", httpx.Response(400, json={"detail": "Invalid tag '7'"}))

    result = _invoke("tag", "swe-bench/verified@1", "7")

    assert result.exit_code == 1
    assert "Invalid tag '7'" in result.output


@pytest.mark.description("Untag removes a tag and names the version it pointed at.")
def test_untag_removes_a_tag_and_names_the_version_it_pointed_at(tracker: MockTracker) -> None:
    tracker.reply("DELETE", f"{_URL}/tags/stable", httpx.Response(200, json=_VERSION))

    result = _invoke("untag", "swe-bench/verified", "stable")

    assert result.exit_code == 0, result.output
    assert "Removed tag stable from swe-bench/verified@2" in result.output


@pytest.mark.description("Commands that take no version refuse one.")
@pytest.mark.parametrize("command", [["versions"], ["untag"]])
def test_commands_that_take_no_version_refuse_one(tracker: MockTracker, command: list[str]) -> None:
    extra = ["stable"] if command == ["untag"] else []

    result = _invoke(*command, "swe-bench/verified@2", *extra)

    assert result.exit_code == 2, result.output
    assert tracker.requests == []


_DATASET = {
    "name": "verified",
    "benchmark": "swe-bench",
    "description": "Checked issues",
    "created_at": "2026-10-24T04:20:24+00:00",
    "updated_at": "2026-10-24T04:25:00+00:00",
}
_MANIFEST = {
    "name": "acme/verified",
    "version": "1.0",
    "description": "Checked issues",
    "authors": [{"name": "Ada", "email": "ada@example.com"}, {"name": "Grace"}],
    "keywords": ["code", "swe"],
    "metadata": {"source": "https://example.test", "difficulty": "hard"},
}


@pytest.mark.description("Show prints the latest version and the dataset toml it was published with.")
def test_show_prints_the_latest_version_and_the_dataset_toml_it_was_published_with(tracker: MockTracker) -> None:
    tracker.reply("GET", _URL, httpx.Response(200, json=_DATASET))
    tracker.reply("GET", f"{_URL}/versions/latest", httpx.Response(200, json={**_VERSION, "manifest": _MANIFEST}))

    result = _invoke("show", "swe-bench", "verified")

    assert result.exit_code == 0, result.output
    assert "Version:     @2 (efefefefefef), 3 task(s), tags latest, stable" in result.output
    assert "Source:      acme/verified 1.0" in result.output
    assert "Authors:     Ada <ada@example.com>, Grace" in result.output
    assert "Keywords:    code, swe" in result.output
    assert 'Metadata:    {"difficulty": "hard", "source": "https://example.test"}' in result.output


@pytest.mark.description("Show says when nothing is published and when there is no dataset toml.")
def test_show_says_when_nothing_is_published_and_when_there_is_no_dataset_toml(tracker: MockTracker) -> None:
    tracker.reply("GET", _URL, httpx.Response(200, json=_DATASET))
    tracker.reply("GET", f"{_URL}/versions/latest", httpx.Response(404, json={"detail": "not found"}))

    nothing = _invoke("show", "swe-bench", "verified")

    assert "Version:     none published" in nothing.output

    tracker.reply("GET", f"{_URL}/versions/latest", httpx.Response(200, json=_VERSION))
    plain = _invoke("show", "swe-bench", "verified")

    assert "Version:     @2" in plain.output and "Source:" not in plain.output


@pytest.mark.description("Show as json stays the dataset.")
def test_show_as_json_stays_the_dataset(tracker: MockTracker) -> None:
    tracker.reply("GET", _URL, httpx.Response(200, json=_DATASET))

    result = _invoke("show", "swe-bench", "verified", "--format", "json")

    assert json.loads(result.output)["name"] == "verified"
    assert tracker.requested("GET", f"{_URL}/versions/latest") == []
