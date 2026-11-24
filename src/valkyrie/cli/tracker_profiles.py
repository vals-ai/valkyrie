import json
import os
import tempfile
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Self
from urllib.parse import urlsplit

import click

BUILT_IN_FILE = files("valkyrie.cli") / "trackers.json"
USER_FILE = Path(os.environ.get("XDG_CONFIG_HOME") or "~/.config") / "valkyrie" / "trackers.json"


def checked_url(value: str) -> str:
    try:
        url = urlsplit(value)
        valid = (
            url.scheme in {"http", "https"}
            and url.hostname
            and url.port != 0
            and not (url.username or url.password or url.query or url.fragment)
            and not any(character.isspace() for character in value)
        )
    except ValueError:
        valid = False
    if not valid:
        raise click.BadParameter("provide a plain http(s) URL without credentials, query, or fragment.")
    return value.rstrip("/")


def _read(path: Traversable) -> tuple[str | None, dict[str, str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, {}
    except (OSError, ValueError) as error:
        raise click.ClickException(f"Could not read {path}: {error}") from error
    trackers = data.get("trackers", {}) if isinstance(data, dict) else None
    if not (
        isinstance(trackers, dict)
        and isinstance(data.get("default", ""), str)
        and all(isinstance(url, str) for url in trackers.values())
    ):
        raise click.ClickException(f"{path} must hold a `default` name and a `trackers` object of names and URLs.")
    return data.get("default"), trackers


@dataclass
class TrackerProfiles:
    built_in: dict[str, str]
    saved: dict[str, str]
    default: str

    @classmethod
    def load(cls) -> Self:
        built_in_default, built_in = _read(BUILT_IN_FILE)
        default, saved = _read(USER_FILE.expanduser())
        return cls(built_in, saved, default or built_in_default or "")

    @property
    def trackers(self) -> dict[str, str]:
        return {**self.built_in, **self.saved}

    def resolve(self, value: str | None = None) -> str:
        name = self.default if value is None else value
        if url := self.trackers.get(name):
            return url
        known = ", ".join(self.trackers)
        if value is None:
            from valkyrie.cli.display import Failure, command

            raise Failure(f"The default Tracker {name} is not saved.", hint=command("tracker", "use", "NAME"))
        raise click.BadParameter(f"unknown Tracker {name}. Use a URL or one of: {known}.")

    def describe(self, url: str) -> str:
        names = [name for name, saved in self.trackers.items() if saved == url]
        name = self.default if self.default in names else next(iter(names), None)
        return f"{name} at {url}" if name else url

    def add(self, name: str, url: str) -> None:
        if name in self.trackers:
            from valkyrie.cli.display import Failure, command

            raise Failure(f"Tracker {name} already exists.", hint=command("tracker", "remove", name))
        self.saved[name] = url
        self._save()

    def use(self, name: str) -> str:
        if name not in self.trackers:
            from valkyrie.cli.display import Failure, command

            raise Failure(f"Unknown Tracker {name}.", hint=command("tracker", "add", name, "URL"))
        self.default = name
        self._save()
        return self.trackers[name]

    def remove(self, name: str) -> str:
        from valkyrie.cli.display import Failure, command

        if name in self.built_in:
            raise Failure(f"{name} is built in and can't be removed.")
        if name not in self.saved:
            raise Failure(f"Unknown Tracker {name}.", hint=command("tracker", "list"))
        if name == self.default:
            raise Failure(f"{name} is the default Tracker.", hint=command("tracker", "use", "NAME"))
        url = self.saved.pop(name)
        self._save()
        return url

    def _save(self) -> None:
        path = USER_FILE.expanduser()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete_on_close=False) as file:
            json.dump({"default": self.default, "trackers": self.saved}, file, indent=2)
            file.write("\n")
            file.close()
            os.replace(file.name, path)
