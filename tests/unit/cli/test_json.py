import json
from collections.abc import Iterator
from uuid import UUID

import click
import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.main import cli
from valkyrie.cli.display import emit, json_option
from tests.unit.cli.conftest import MockTracker


def _leaves(group: click.Group, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], click.Command]]:
    context = click.Context(group)
    for name in group.list_commands(context):
        command = group.get_command(context, name)
        assert command is not None
        if isinstance(command, click.Group):
            yield from _leaves(command, (*path, name))
        else:
            yield (*path, name), command


@pytest.mark.description("Every data command exposes JSON, and none exposes the removed format option.")
@pytest.mark.parametrize(
    ("path", "command"), list(_leaves(cli)), ids=lambda value: str(value) if isinstance(value, tuple) else None
)
def test_leaf_json_options(path: tuple[str, ...], command: click.Command) -> None:
    options = {option for param in command.params if isinstance(param, click.Option) for option in param.opts}
    assert "--format" not in options
    if path not in {("completion",), ("tracker", "start"), ("tui",)}:
        assert "--json" in options


_RUN = str(UUID(int=1))


@pytest.mark.description("JSON action output is parseable, and confirmation never reads input without --yes.")
@pytest.mark.parametrize(
    ("args", "method", "path", "expected"),
    [
        (
            ["run", "stop", _RUN],
            "POST",
            f"/stop-benchmark/{_RUN}",
            {"run_id": _RUN, "stopped": True, "force": False, "task_ids": None},
        ),
        (["secret", "remove", "key"], "DELETE", "/secrets/key", {"removed": "key"}),
        (["agent", "remove", "agent"], "DELETE", "/agents/agent", {"removed": "agent"}),
    ],
)
def test_json_action_confirmation(
    tracker: MockTracker, args: list[str], method: str, path: str, expected: dict[str, object]
) -> None:
    tracker.reply(method, path, httpx.Response(200, json={"name": "entry", "reference": "ref", "status": "success"}))
    blocked = CliRunner().invoke(cli, [*args, "--json"], input="y\n")
    assert blocked.exit_code == 2, blocked.output
    assert blocked.stdout == ""
    assert "--yes" in blocked.stderr
    assert tracker.requests == []

    completed = CliRunner().invoke(cli, [*args, "--json", "--yes"])
    assert completed.exit_code == 0, completed.output
    assert json.loads(completed.stdout) == expected


@pytest.mark.description("Shared JSON output handles empty data and rejects conflicting stream flags.")
def test_json_output_empty_and_conflicting_flags() -> None:
    @click.command()
    @json_option(stream=True)
    def command(output_format: str) -> None:
        emit([], output_format)

    result = CliRunner().invoke(command, ["--json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == []
    assert result.stderr == ""
    conflict = CliRunner().invoke(command, ["--json", "--jsonl"])
    assert conflict.exit_code == 2
    assert conflict.stdout == ""
