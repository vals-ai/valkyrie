import json
import os
import subprocess
import sys
from pathlib import Path

import click
import pytest
from click.shell_completion import CompletionItem
from click.testing import CliRunner

from valkyrie.cli import entry
from valkyrie.cli import main as cli_main
from valkyrie.cli.agent import agent
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.completion import NuComplete
from valkyrie.cli.lazy import LazyGroup
from valkyrie.cli.legacy import upgrade_args
from valkyrie.cli.logging import CLI_LOGS_ENV_VAR
from valkyrie.cli.run import run

_SERVER_MODULES = ("tracker", "daytona", "modal", "fastapi", "textual")


RUN = "123e4567-e89b-12d3-a456-426614174000"


@pytest.mark.description("Old command and flag names run their replacement and name it in a warning.")
@pytest.mark.parametrize(
    ("old", "new", "notes"),
    [
        (f"run fetch {RUN} --connect", f"run show {RUN} --follow", 2),
        (f"--tracker run run status --ids {RUN},x --format=json", f"--tracker run run show --json {RUN} x", 3),
        (f"run outputs {RUN} --output-dir out", f"run download {RUN} --output out", 2),
        (f"run output {RUN} -o out", f"run download {RUN} -o out ", 1),
        (f"run output {RUN} a/b -o out", f"run download {RUN} a/b -o out", 1),
        (f"run output {RUN} -- -a", f"run download {RUN} -- -a", 1),
        (f"run artifacts {RUN} --prefix a/b", f"run files {RUN} a/b", 2),
        (f"run task-artifacts {RUN} t", f"run task {RUN} t --links", 1),
        (f"run retag {RUN} --project p", f"run update {RUN} --project p", 1),
        (f"run shed {RUN} --concurrency 2", f"run update {RUN} --concurrency 2 --shed", 1),
        ("run start -n 2 --task-ids-file ids.txt", "run start --count 2 --task-ids @ids.txt", 2),
        ("run start -n2", "run start --count 2", 1),
        (f"run resume {RUN} --retry", f"run retry {RUN}", 1),
        (f"run results {RUN} --path r.json --format text", f"run results {RUN} --output r.json", 2),
        ("run list --agent-name a --benchmark-name b --order-by c", "run list --agent a --benchmark b --order c", 3),
        ("agent list --format JSON", "agent list --json", 1),
        ("agent install url -n name", "agent install url -n name", 0),
        (f"run show {RUN} -- --connect", f"run show {RUN} -- --connect", 0),
    ],
)
def test_legacy_names(old: str, new: str, notes: int) -> None:
    upgraded, warnings = upgrade_args(old.split())

    assert upgraded == new.split(" ")
    assert len(warnings) == notes


@pytest.mark.description("A removed run command exits 2 and stays out of help.")
def test_removed_run_command() -> None:
    result = CliRunner().invoke(cli_main.cli, ["run", "filter-options"])

    assert result.exit_code == 2
    assert "error: run filter-options has been removed\n  try: valkyrie run --help\n" in result.output
    assert "filter-options" not in run.list_commands(click.Context(run))


_COMMANDS_PROBE = f"""
import sys
import click
from valkyrie.cli.main import cli

def load(group):
    ctx = click.Context(group)
    for name in [*group.list_commands(ctx), *getattr(group, "hidden_commands", ())]:
        if isinstance(command := group.get_command(ctx, name), click.Group):
            load(command)

load(cli)
print(" ".join(name for name in {_SERVER_MODULES!r} if name in sys.modules))
"""


@pytest.mark.description(
    (
        "Interrupted commands must return the standard shell cancellation status.\n"
        "\n"
        "Test cases:\n"
        "- KeyboardInterrupt raised by Click exits with status 130 instead of a traceback."
    )
)
def test_entrypoint_maps_keyboard_interrupt_to_shell_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:

    def interrupt(*args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_main, "cli", interrupt)

    monkeypatch.setenv("PYDANTIC_DISABLE_PLUGINS", "__all__")

    with pytest.raises(SystemExit) as error:
        entry.main()

    assert error.value.code == 130


@pytest.mark.description(
    "`secret --help` defines pydantic models, so Logfire's plugin would import Logfire if plugins were left on.\n"
    "\n"
    "Test cases:\n"
    "- The console script entry point.\n"
    "- `python -m valkyrie.cli.main`."
)
@pytest.mark.parametrize(
    "invoke",
    ["from valkyrie.cli import entry; entry.main()", "runpy.run_module('valkyrie.cli.main', run_name='__main__')"],
    ids=["console-script", "python-m"],
)
def test_entrypoint_disables_pydantic_plugins_before_commands_load(tmp_path: Path, invoke: str) -> None:
    probe = f"""
import io, runpy, sys
from contextlib import redirect_stdout

with redirect_stdout(io.StringIO()):
    try:
        {invoke}
    except SystemExit:
        pass
print("logfire" in sys.modules)
"""
    environment = {name: value for name, value in os.environ.items() if name != "PYDANTIC_DISABLE_PLUGINS"}
    result = subprocess.run(
        [sys.executable, "-c", probe, "secret", "--help"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "False"


@pytest.mark.description("The registry's help listing must match what Click prints after importing every subcommand.")
@pytest.mark.parametrize("group", [run, agent, benchmark], ids=lambda group: group.name)
def test_lazy_help_matches_imported_commands(group: LazyGroup) -> None:
    ctx = click.Context(group)
    lazy_help = click.HelpFormatter()
    imported_help = click.HelpFormatter()

    group.format_commands(ctx, lazy_help)
    click.Group.format_commands(group, ctx, imported_help)

    assert lazy_help.getvalue() == imported_help.getvalue()


@pytest.mark.description("Top-level help groups commands without truncation or importing the SDK.")
def test_top_level_help_stays_lazy() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from click.testing import CliRunner
from valkyrie.cli.main import cli
result = CliRunner().invoke(cli, ["--help"])
print(result.output)
assert result.exit_code == 0
assert "valkyrie.sdk" not in sys.modules
""",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    help = result.stdout
    assert help.index("Runs:") < help.index("Organization:") < help.index("Account:") < help.index("Browse:")
    assert "..." not in help.split("Runs:", 1)[1]
    for name in cli_main.cli.lazy_commands:
        assert f"  {name} " in help


@pytest.mark.description("Version output includes both installed package versions without loading the SDK.")
def test_version_stays_lazy() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            r"""
import sys
from importlib.metadata import version
from click.testing import CliRunner
from valkyrie.cli.main import cli
result = CliRunner().invoke(cli, ["--version"])
assert result.exit_code == 0
assert result.output == f"valkyrie CLI {version('valkyrie-cli')}\nvalkyrie SDK {version('valkyrie-sdk')}\n"
assert "valkyrie.sdk" not in sys.modules
""",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.description("Completion prints Click's script for each supported shell and executable name.")
@pytest.mark.parametrize("shell", ["bash", "zsh", "fish", "nu"])
@pytest.mark.parametrize("program", ["valkyrie", "valk"])
def test_completion(shell: str, program: str) -> None:
    result = CliRunner().invoke(cli_main.cli, ["completion", shell], prog_name=program)
    assert result.exit_code == 0, result.output
    assert f"_{program.upper()}_COMPLETE" in result.output
    assert f"{shell}_complete" in result.output
    assert "# Install:" in result.output
    assert "valkyrie" in result.output and "valk" in result.output


@pytest.mark.description("Nushell delegates path arguments and preserves descriptions for plain suggestions.")
@pytest.mark.parametrize("kind", ["plain", "file", "dir"])
def test_nushell_path_fallback(kind: str) -> None:
    completer = NuComplete(cli_main.cli, {}, "valk", "_VALK_COMPLETE")
    result = json.loads(completer.format_completion(CompletionItem("./ag", type=kind, help="Agent directory.")))
    if kind == "plain":
        assert result == {"value": "./ag", "description": "Agent directory."}
    else:
        assert result is None


@pytest.mark.description("All completion protocols return command and flag help without importing the SDK.")
@pytest.mark.parametrize("shell", ["bash", "zsh", "fish", "nu"])
@pytest.mark.parametrize("program", ["valkyrie", "valk"])
@pytest.mark.parametrize(
    ("words", "word", "expected", "description"),
    [
        ("run sh", "sh", "show", "Show one run"),
        ("run update --", "--", "--shed", "Force stop"),
        ("run zzz", "zzz", "", ""),
    ],
)
def test_completion_protocol_stays_lazy(
    shell: str, program: str, words: str, word: str, expected: str, description: str
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from valkyrie.cli.entry import main
sys.argv = [sys.argv[1]]
try:
    main()
finally:
    assert not any(name.startswith("valkyrie.sdk") for name in sys.modules)
""",
            program,
        ],
        env={
            **os.environ,
            f"_{program.upper()}_COMPLETE": f"{shell}_complete",
            "COMP_WORDS": f"{program} {words}",
            "COMP_CWORD": word if shell == "fish" else str(len(words.split())),
            "_VALKYRIE_COMPLETE_ARGS": json.dumps(words.split()),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    if not expected:
        assert not result.stdout.strip()
        return
    assert expected in result.stdout
    if shell != "bash":
        assert description in result.stdout


@pytest.mark.description("Global flags apply only to this call and disable colors even on a color terminal.")
@pytest.mark.parametrize("no_color", [["--no-color"], []])
def test_global_options(monkeypatch: pytest.MonkeyPatch, no_color: list[str]) -> None:
    @click.command()
    @click.pass_context
    def probe(ctx: click.Context) -> None:
        assert os.environ["VALKYRIE_URL"] == "https://override.test"
        assert os.environ[CLI_LOGS_ENV_VAR] == "true"
        click.echo(click.style("colored", fg="green"))

    monkeypatch.setattr(cli_main.cli, "get_command", lambda ctx, name: probe)
    monkeypatch.setenv("VALKYRIE_URL", "https://original.test")
    monkeypatch.setenv(CLI_LOGS_ENV_VAR, "false")
    if not no_color:
        monkeypatch.setenv("NO_COLOR", "")
    result = CliRunner().invoke(
        cli_main.cli,
        ["--tracker", "https://override.test", "-v", *no_color, "probe"],
        color=True,
    )
    assert result.exit_code == 0, result.output
    assert result.output == "colored\n"
    assert os.environ["VALKYRIE_URL"] == "https://original.test"
    assert os.environ[CLI_LOGS_ENV_VAR] == "false"


@pytest.mark.description("Tracker overrides reject unknown names and URLs with credentials or extra components.")
@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("dev", "unknown Tracker dev"),
        ("", "unknown Tracker ."),
        ("ftp://host", "plain http(s) URL"),
        ("https://u:p@host", "plain http(s) URL"),
        ("https://host?x=y", "plain http(s) URL"),
        ("https://host/#x", "plain http(s) URL"),
    ],
)
def test_invalid_tracker(value: str, message: str) -> None:
    result = CliRunner().invoke(cli_main.cli, ["--tracker", value, "whoami"])
    assert result.exit_code == 2
    assert message in result.output


@pytest.mark.description("Verbose errors include the full exception chain while ordinary errors stay concise.")
@pytest.mark.parametrize("verbose", [False, True])
def test_verbose_tracebacks(monkeypatch: pytest.MonkeyPatch, verbose: bool) -> None:
    @click.command()
    def probe() -> None:
        try:
            raise ValueError("original failure")
        except ValueError as error:
            raise click.ClickException("request\nfailed") from error

    monkeypatch.setattr(cli_main.cli, "get_command", lambda ctx, name: probe)
    result = CliRunner().invoke(cli_main.cli, [*(["-v"] if verbose else []), "probe"])
    assert result.exit_code == 1
    assert result.stderr.endswith("error: request failed\n")
    assert ("Traceback" in result.output) is verbose
    assert ("original failure" in result.output) is verbose


@pytest.mark.description(
    "Hidden commands such as `benchmark create` can be called but are not listed, and unknown names resolve to nothing."
)
def test_hidden_commands_resolve_but_are_not_listed() -> None:
    ctx = click.Context(benchmark)

    assert benchmark.get_command(ctx, "create") is not None
    assert "create" not in benchmark.list_commands(ctx)
    assert benchmark.get_command(ctx, "no-such-command") is None


@pytest.mark.description("Imports every command, in a subprocess because this test process has already imported them.")
def test_command_modules_do_not_import_server_dependencies(tmp_path: Path) -> None:
    probe = subprocess.run(
        [sys.executable, "-c", _COMMANDS_PROBE], cwd=tmp_path, capture_output=True, text=True, check=True
    )

    assert probe.stdout.strip() == ""


@pytest.mark.description("Help for SDK-backed commands loads neither `httpx` nor the SDK client.")
@pytest.mark.parametrize("arguments", [["whoami", "--help"], ["run", "list", "--help"], ["org", "use", "--help"]])
def test_sdk_command_help_skips_http_client(arguments: list[str]) -> None:
    script = (
        "import sys\n"
        "from valkyrie.cli.main import cli\n"
        f"cli.main({arguments!r}, standalone_mode=False)\n"
        "print(sorted(name for name in ('httpx', 'valkyrie.sdk.client') if name in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True)

    assert result.stdout.splitlines()[-1] == "[]"
