import shlex


def run_as(user: str, command: str) -> str:
    account = shlex.quote(user)
    script = (
        f'entry="$(getent passwd {account})"; '
        f'[ -n "$entry" ] || {{ echo {shlex.quote(f"The sandbox has no user {user}")} >&2; exit 1; }}; '
        'login="$(printf %s "$entry" | cut -d: -f1)"; '
        'export HOME="$(printf %s "$entry" | cut -d: -f6)" USER="$login" LOGNAME="$login"; '
        "if su --help 2>&1 | grep -q session-command; then flag=--session-command; else flag=-c; fi; "
        f'exec su -m -s /bin/sh "$login" $flag {shlex.quote(command)}'
    )
    return f"sh -c {shlex.quote(script)}"
