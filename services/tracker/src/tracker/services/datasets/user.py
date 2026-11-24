import shlex
import shutil
from pathlib import Path

from benchmark_service import DockerfileSource, ImageSource

from tracker.services.datasets.config import check_user
from tracker.services.datasets.folder import DOCKERFILE

ROOT_USERS = frozenset({"", "root", "0"})
IMAGE_ENVIRON = "/proc/1/environ"


def non_root_user(user: str | None) -> str | None:
    account, _, group = (user or "").partition(":")
    if account in ROOT_USERS:
        return None
    return ":".join(check_user(part) for part in (account, group) if part)


def run_as(user: str, command: str) -> str:
    account, _, group = user.partition(":")
    quoted = shlex.quote(account)
    groups = f"--regid={shlex.quote(group)} --clear-groups" if group else '--regid="$g" --init-groups'
    no_setpriv = f'echo "The sandbox cannot switch to {user} without setpriv" >&2; exit 1; '
    script = (
        f"c={shlex.quote(command)}; "
        f'h="$(tr \\\\0 \\\\n <{IMAGE_ENVIRON} 2>/dev/null | sed -n s/^HOME=//p | head -n 1)"; '
        '[ "$h" = /root ] && h=; '
        "unset USER LOGNAME; "
        "setpriv --help 2>&1 | grep -q -- --reuid && p=1; "
        f'e="$(getent passwd {quoted})"; '
        'if [ -n "$e" ]; then '
        'l="$(printf %s "$e" | cut -d: -f1)"; '
        'g="$(printf %s "$e" | cut -d: -f4)"; '
        'export HOME="${h:-$(printf %s "$e" | cut -d: -f6)}"; '
        f'[ -n "$p" ] && exec setpriv --reuid="$l" {groups} /bin/sh -c "$c"; '
        + (no_setpriv if group else 'exec su -m -s /bin/sh -c "$c" "$l"; ')
        + "fi; "
        f'case {quoted} in *[!0-9]*) echo "The sandbox has no user {account}" >&2; exit 1;; esac; '
        f'[ -n "$p" ] || {{ {no_setpriv}}}; '
        'export HOME="${h:-/}"; '
        f'exec setpriv --reuid={quoted} --regid={shlex.quote(group or "0")} --clear-groups /bin/sh -c "$c"'
    )
    return f"sh -c {shlex.quote(script)}"


def root_source(
    source: ImageSource | DockerfileSource, directory: Path, build_timeout: float | None
) -> DockerfileSource:
    if isinstance(source, ImageSource):
        base = f"FROM {source.image}"
    else:
        shutil.copytree(Path(source.dockerfile).parent, directory, dirs_exist_ok=True)
        base = Path(source.dockerfile).read_text()
    dockerfile = directory / DOCKERFILE
    dockerfile.write_text(f"{base}\nUSER root\n")
    return DockerfileSource(dockerfile=str(dockerfile), build_timeout=build_timeout)
