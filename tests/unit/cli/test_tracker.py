import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import FrameType

import click
import pytest
from click.testing import CliRunner

import valkyrie.cli.main as cli_main
import valkyrie.cli.tracker_profiles as tracker_profiles
from valkyrie.sdk import LoginSession

HOSTED = "https://benchmark-tracker-prod.vals.ai"
SAVED = {"default": "team", "trackers": {"team": "https://team.test", "local": "http://127.0.0.1:9000"}}


def _invoke(*args: str) -> click.testing.Result:
    return CliRunner().invoke(cli_main.cli, list(args))


@pytest.mark.description(
    "The Tracker comes from `--tracker`, then `VALKYRIE_URL`, then `.env`, then the saved default, then `vals`."
)
@pytest.mark.parametrize(
    ("args", "environ", "dotenv", "saved", "expected"),
    [
        ([], None, None, None, HOSTED),
        (["--tracker", "local"], None, None, None, "http://127.0.0.1:8000"),
        (["--tracker", "https://direct.test/"], None, None, "not json", "https://direct.test"),
        ([], None, None, SAVED, "https://team.test"),
        (["--tracker", "local"], None, None, SAVED, "http://127.0.0.1:9000"),
        ([], None, "https://dotenv.test", SAVED, "https://dotenv.test"),
        ([], "https://env.test", "https://dotenv.test", SAVED, "https://env.test"),
        (["--tracker", "team"], "https://env.test", None, SAVED, "https://team.test"),
    ],
    ids=["hosted", "built-in", "url-ignores-broken-file", "saved", "saved-overrides-built-in", "dotenv", "env", "flag"],
)
def test_tracker_resolution(
    monkeypatch: pytest.MonkeyPatch,
    args: list[str],
    environ: str | None,
    dotenv: str | None,
    saved: dict[str, object] | str | None,
    expected: str,
) -> None:
    @click.command()
    def probe() -> None:
        click.echo(os.environ["VALKYRIE_URL"])

    monkeypatch.setattr(cli_main.cli, "get_command", lambda ctx, name: probe)
    if environ:
        monkeypatch.setenv("VALKYRIE_URL", environ)
    if dotenv:
        monkeypatch.setattr(cli_main, "load_cli_environment", lambda: os.environ.setdefault("VALKYRIE_URL", dotenv))
    if saved is not None:
        tracker_profiles.USER_FILE.write_text(saved if isinstance(saved, str) else json.dumps(saved))

    assert _invoke(*args, "probe").output == f"{expected}\n"


@pytest.mark.description("Saved Trackers can be added, chosen, listed as JSON, and removed along with their login.")
def test_tracker_profiles() -> None:
    assert _invoke("tracker", "add", "team", "https://team.test/").exit_code == 0
    assert _invoke("tracker", "add", "local", "https://x.test").exit_code == 1
    assert _invoke("tracker", "add", "Bad_Name", "https://x.test").exit_code == 2
    assert _invoke("tracker", "use", "missing").exit_code == 1
    assert _invoke("tracker", "use", "team").exit_code == 0
    assert json.loads(tracker_profiles.USER_FILE.read_text()) == {
        "default": "team",
        "trackers": {"team": "https://team.test"},
    }

    LoginSession(
        tracker_url="https://team.test",
        project_id="P1",
        refresh_url="https://login.test",
        access_token="a",
        expires_at=0,
    ).save()
    rows = {row["name"]: row for row in json.loads(_invoke("tracker", "list", "--json").stdout)}
    assert list(rows) == ["vals", "local", "team"]
    assert rows["team"]["default"] and rows["team"]["logged_in"]
    assert not rows["vals"]["default"]

    assert _invoke("tracker", "remove", "team").exit_code == 1
    assert _invoke("tracker", "remove", "local").exit_code == 1
    assert _invoke("tracker", "use", "local").exit_code == 0
    assert _invoke("tracker", "remove", "team").exit_code == 0
    assert LoginSession.load("https://team.test") is None
    assert "team" not in tracker_profiles.TrackerProfiles.load().trackers


@pytest.mark.description(
    "With `XDG_CONFIG_HOME` set, saved Trackers and their logins both live in its `valkyrie` folder."
)
def test_xdg_config_home(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from click.testing import CliRunner
from valkyrie.cli.main import cli
from valkyrie.sdk import LoginSession
CliRunner().invoke(cli, ["tracker", "add", "team", "https://team.test"])
LoginSession(
    tracker_url="https://team.test", project_id="P1", refresh_url="https://login.test", access_token="a", expires_at=0
).save()
print(CliRunner().invoke(cli, ["tracker", "list", "--json"]).stdout)
""",
        ],
        env={**os.environ, "HOME": str(tmp_path / "home"), "XDG_CONFIG_HOME": str(tmp_path / "xdg")},
        capture_output=True,
        text=True,
        check=True,
    )

    rows = {row["name"]: row for row in json.loads(result.stdout)}
    assert rows["team"]["logged_in"]
    assert (tmp_path / "xdg" / "valkyrie" / "trackers.json").exists()
    assert len(list((tmp_path / "xdg" / "valkyrie" / "sessions").glob("*.json"))) == 1
    assert not (tmp_path / "home").exists()


@pytest.fixture
def checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local-server.yaml").touch()
    monkeypatch.delenv("EXECUTOR_BOOTSTRAP_KEY", raising=False)
    monkeypatch.delenv("EXECUTOR_PAYLOAD_LOCAL_KEY", raising=False)
    return tmp_path


@pytest.mark.description("Starting Tracker twice reuses its private keys without requiring a dotenv file.")
def test_start_reuses_local_keys(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launches: list[tuple[str, str]] = []

    class Tracker:
        def __init__(self, command: list[str], *, env: dict[str, str], start_new_session: bool) -> None:
            assert command == [
                "make",
                "--no-print-directory",
                "-C",
                str(checkout / "services" / "tracker"),
                "local-tracker",
            ]
            launches.append((env["EXECUTOR_BOOTSTRAP_KEY"], env["EXECUTOR_PAYLOAD_LOCAL_KEY"]))
            self.pid = 0

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(subprocess, "Popen", Tracker)
    ignored: dict[int, object] = {}
    monkeypatch.setattr(signal, "signal", ignored.__setitem__)
    for _ in range(2):
        result = CliRunner().invoke(cli_main.cli, ["tracker", "start"])
        assert result.exit_code == 0, result.output
        assert "valkyrie tracker use local" in result.output

    key_file = checkout / ".local" / "executor.keys"
    assert key_file.stat().st_mode & 0o777 == 0o600
    assert launches[0] == launches[1] == tuple(key_file.read_text().splitlines())


@pytest.mark.parametrize("stop_signal", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
@pytest.mark.description("Each stop signal interrupts the whole Tracker process group, so it shuts down as on Ctrl+C.")
def test_stop_signal_interrupts_tracker_group(
    checkout: Path, monkeypatch: pytest.MonkeyPatch, stop_signal: signal.Signals
) -> None:
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}
    interrupted: list[tuple[int, int]] = []

    class Tracker:
        pid = 4242

        def __init__(self, command: list[str], *, env: dict[str, str], start_new_session: bool) -> None:
            assert start_new_session

        def wait(self) -> int:
            handlers[stop_signal](stop_signal, None)
            return 2

    monkeypatch.setattr(subprocess, "Popen", Tracker)
    monkeypatch.setattr(signal, "signal", handlers.__setitem__)

    def killpg(pid: int, sent: int) -> None:
        interrupted.append((pid, sent))

    monkeypatch.setattr(os, "killpg", killpg)
    result = CliRunner().invoke(cli_main.cli, ["tracker", "start"])

    assert interrupted == [(4242, signal.SIGINT)]
    assert result.exit_code == 2


@pytest.mark.description(
    "A stop signal that arrives while Tracker is launching interrupts its process group once it starts."
)
def test_stop_signal_during_launch_interrupts_tracker(checkout: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}
    interrupted: list[tuple[int, int]] = []

    class Tracker:
        pid = 4242

        def __init__(self, command: list[str], *, env: dict[str, str], start_new_session: bool) -> None:
            handlers[signal.SIGTERM](signal.SIGTERM, None)

        def wait(self) -> int:
            return 2

    monkeypatch.setattr(subprocess, "Popen", Tracker)
    monkeypatch.setattr(signal, "signal", handlers.__setitem__)

    def killpg(pid: int, sent: int) -> None:
        interrupted.append((pid, sent))

    monkeypatch.setattr(os, "killpg", killpg)
    result = CliRunner().invoke(cli_main.cli, ["tracker", "start"])

    assert interrupted == [(4242, signal.SIGINT)]
    assert result.exit_code == 2
