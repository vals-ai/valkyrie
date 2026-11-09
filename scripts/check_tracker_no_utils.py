import subprocess
import sys
from pathlib import PurePosixPath

ROOT = "services/tracker/"
MESSAGE = (
    "Tracker bans utils/ folders. A utils folder has no owner and grows into a catch-all, which is why tracker.utils "
    "was replaced by tracker.services. Put the code in the module that owns its domain noun: "
    "services/<noun> for logic, repositories/ for database queries, clients/ for external systems. "
    "See 'Tracker services' in AGENTS.md."
)


def utils_folders(paths: list[str]) -> list[str]:
    found = set()
    for path in paths:
        parts = PurePosixPath(path).parts
        if "utils" in parts[:-1]:
            found.add("/".join(parts[: parts.index("utils") + 1]) + "/")
    return sorted(found)


def main() -> int:
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", ROOT], text=True
    ).split()
    found = utils_folders(paths)
    for folder in found:
        print(f"{folder}: {MESSAGE}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
