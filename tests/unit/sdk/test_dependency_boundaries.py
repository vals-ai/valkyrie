from __future__ import annotations

import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CLIENT_PACKAGES = ("valkyrie-cli", "valkyrie-sdk")
SERVER_PACKAGES = frozenset({"tracker"})


def runtime_closure(lock: Mapping[str, Any], root: str) -> frozenset[str]:
    packages = {package["name"]: package for package in lock["package"]}
    seen: set[str] = set()
    pending: list[Mapping[str, Any]] = [{"name": root}]
    while pending:
        dependency = pending.pop()
        package = packages[dependency["name"]]
        optional = package.get("optional-dependencies", {})
        pending.extend(entry for extra in dependency.get("extra", ()) for entry in optional.get(extra, ()))
        if dependency["name"] not in seen:
            seen.add(dependency["name"])
            pending.extend(package.get("dependencies", ()))
    return frozenset(seen)


@pytest.mark.description("Installing a client package from `uv.lock` never pulls in the Tracker server.")
@pytest.mark.parametrize("client", CLIENT_PACKAGES)
def test_client_runtime_dependencies_exclude_tracker(client: str) -> None:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))

    assert runtime_closure(lock, client) & SERVER_PACKAGES == set()


@pytest.mark.description(
    "`runtime_closure` follows direct, transitive, and extra dependencies but skips dependency groups.\n"
    "\n"
    "Test cases:\n"
    "- A dependency reached through another package is included.\n"
    "- A dependency reached only through a requested extra is included.\n"
    "- A dev group dependency is left out."
)
def test_runtime_closure_follows_runtime_edges_only() -> None:
    lock = {
        "package": [
            {
                "name": "valkyrie",
                "dependencies": [{"name": "client-lib"}, {"name": "with-extras", "extra": ["server"]}],
                "dev-dependencies": {"dev": [{"name": "dev-only"}]},
            },
            {"name": "client-lib", "dependencies": [{"name": "transitive"}]},
            {"name": "transitive"},
            {"name": "with-extras", "optional-dependencies": {"server": [{"name": "tracker"}]}},
            {"name": "tracker"},
            {"name": "dev-only"},
        ]
    }

    assert runtime_closure(lock, "valkyrie") == {"valkyrie", "client-lib", "transitive", "with-extras", "tracker"}
