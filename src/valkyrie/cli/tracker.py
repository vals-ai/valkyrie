import base64
import os
import re
import secrets
import signal
import subprocess
from pathlib import Path
from types import FrameType

import click

from valkyrie.cli.display import command, details, done, emit, format_table, json_option
from valkyrie.cli.tracker_profiles import TrackerProfiles, checked_url

_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _name(ctx: click.Context, param: click.Parameter, value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", value):
        raise click.BadParameter("use lowercase letters, digits, and hyphens, starting with a letter or digit.")
    return value


def _url(ctx: click.Context, param: click.Parameter, value: str) -> str:
    return checked_url(value)


@click.group(help="Save Trackers by name and choose the one commands use by default.")
def tracker() -> None:
    pass


def _local_keys(path: Path) -> tuple[str, str]:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        with open(path, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as file:
            file.write(secrets.token_hex(32) + "\n" + base64.b64encode(secrets.token_bytes(32)).decode() + "\n")
    except FileExistsError:
        pass
    bootstrap, payload = path.read_text().splitlines()
    return bootstrap, payload


def _run_until_stopped(command: list[str], env: dict[str, str]) -> int:
    stop_requested = False
    process: subprocess.Popen[bytes] | None = None

    def interrupt(_signal: int, _frame: FrameType | None) -> None:
        nonlocal stop_requested
        stop_requested = True
        if process is not None:
            os.killpg(process.pid, signal.SIGINT)

    for stop_signal in _STOP_SIGNALS:
        signal.signal(stop_signal, interrupt)
    process = subprocess.Popen(command, env=env, start_new_session=True)
    if stop_requested:
        os.killpg(process.pid, signal.SIGINT)
    return process.wait()


@tracker.command("start", help="Run a local Tracker from a Valkyrie checkout until Ctrl+C.")
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=Path),
    help="Local Tracker configuration file. Defaults to `local-server.yaml` with `.env` as the secrets file.",
)
def tracker_start(config: Path | None) -> None:
    cwd = Path.cwd()
    checkout = next((path for path in (cwd, *cwd.parents) if (path / "local-server.yaml").is_file()), None)
    if checkout is None:
        raise click.ClickException("Run this command from a Valkyrie checkout.")

    profiles = TrackerProfiles.load()
    try:
        bootstrap, payload = _local_keys(checkout / ".local" / "executor.keys")
        env = os.environ.copy()
        env.setdefault("EXECUTOR_BOOTSTRAP_KEY", bootstrap)
        env.setdefault("EXECUTOR_PAYLOAD_LOCAL_KEY", payload)
        command_line = ["make", "--no-print-directory", "-C", str(checkout / "services" / "tracker"), "local-tracker"]
        if config is not None:
            command_line.append(f"CONFIG={config}")
        dotenv = checkout / ".env"
        details(
            "Local Tracker",
            [
                ("URL", profiles.built_in["local"]),
                ("Secrets", str(dotenv) if config is None and dotenv.is_file() else None),
            ],
            [("Use it by default", command("tracker", "use", "local") if profiles.default != "local" else None)],
            err=True,
        )
        code = _run_until_stopped(command_line, env)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if code:
        raise click.exceptions.Exit(code)


@tracker.command("list", help="List saved Trackers, marking the default and those you are signed in to.")
@json_option()
def tracker_list(output_format: str) -> None:
    from valkyrie.sdk import LoginSession

    profiles = TrackerProfiles.load()
    rows = [
        {"name": name, "url": url, "default": name == profiles.default, "logged_in": LoginSession.load(url) is not None}
        for name, url in profiles.trackers.items()
    ]
    if emit(rows, output_format):
        return
    format_table(
        [
            {
                "Name": str(row["name"]),
                "URL": str(row["url"]),
                "Default": "yes" if row["default"] else "",
                "Logged in": "yes" if row["logged_in"] else "",
            }
            for row in rows
        ],
        ["Name", "URL", "Default", "Logged in"],
        item_name="Tracker",
    )


@tracker.command("add", help="Save a Tracker URL under a name.")
@click.argument("name", callback=_name)
@click.argument("url", callback=_url)
@json_option()
def tracker_add(name: str, url: str, output_format: str) -> None:
    TrackerProfiles.load().add(name, url)
    if emit({"name": name, "url": url}, output_format):
        return
    done(f"Tracker {name} saved at {url}.")
    details("", [], [("Make it the default", command("tracker", "use", name))], err=True)


@tracker.command("use", help="Make a saved Tracker the default for later commands.")
@click.argument("name")
@json_option()
def tracker_use(name: str, output_format: str) -> None:
    url = TrackerProfiles.load().use(name)
    if emit({"name": name, "url": url}, output_format):
        return
    done(f"Commands now use {name} at {url} by default.")


@tracker.command("remove", help="Remove a saved Tracker and forget its login.")
@click.argument("name")
@json_option()
def tracker_remove(name: str, output_format: str) -> None:
    from valkyrie.sdk import LoginSession

    url = TrackerProfiles.load().remove(name)
    LoginSession.forget(url)
    if emit({"name": name, "url": url}, output_format):
        return
    done(f"Tracker {name} at {url} removed.")
