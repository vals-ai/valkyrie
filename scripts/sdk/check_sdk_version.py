from __future__ import annotations

import argparse
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path

from packaging.version import Version

PACKAGE_PYPROJECT = Path("packages/valkyrie-sdk/pyproject.toml")
PACKAGE_ROOT = PACKAGE_PYPROJECT.parent


class VersionError(RuntimeError):
    pass


def read_version(contents: bytes) -> Version:
    return Version(tomllib.loads(contents.decode())["project"]["version"])


def current_version() -> Version:
    return read_version(PACKAGE_PYPROJECT.read_bytes())


def base_version(base_sha: str) -> Version | None:
    result = subprocess.run(
        ["git", "show", f"{base_sha}:{PACKAGE_PYPROJECT.as_posix()}"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    return read_version(result.stdout)


def package_changed(base_sha: str) -> bool:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}..HEAD", "--", PACKAGE_ROOT.as_posix()],
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def validate_version_change(*, base: Version | None, current: Version, distribution_changed: bool) -> None:
    if not distribution_changed:
        return
    if base is None:
        if current != Version("0.1.0"):
            raise VersionError(f"initial SDK version must be 0.1.0, found {current}")
        return
    if current <= base:
        raise VersionError(f"SDK version {current} must be greater than base version {base}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Require an SDK version bump when publishable package files change.")
    parser.add_argument("base_sha")
    args = parser.parse_args(argv)

    base = base_version(args.base_sha)
    current = current_version()
    changed = package_changed(args.base_sha)
    validate_version_change(base=base, current=current, distribution_changed=changed)
    print(f"SDK version check passed: base={base}, current={current}, package_changed={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
