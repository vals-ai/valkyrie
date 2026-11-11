import subprocess
from pathlib import Path

import pytest

from tracker.services.datasets.user import run_as

UTIL_LINUX_HELP = "      --session-command <command>"
BUSYBOX_HELP = "Usage: su [-lmp] [-s SH] [-c CMD] [USER]"


@pytest.fixture
def stand_ins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "getent").write_text(
        '#!/bin/sh\ncase "$2" in agent|1000) echo "agent:x:1000:1000::/home/agent:/bin/bash" ;; esac\n'
    )
    (tmp_path / "getent").chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    return tmp_path


def install_su(directory: Path, help_text: str) -> None:
    su = directory / "su"
    su.write_text(
        f'#!/bin/sh\nif [ "$1" = "--help" ]; then echo "{help_text}"; exit 1; fi\n'
        'echo "switched to $4 with $5"\nexec "$3" -c "$6"\n'
    )
    su.chmod(0o755)


def run(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False)


@pytest.mark.description("The user is looked up first, then the command runs under it with the environment kept.")
class TestRunAs:
    @pytest.mark.description("A name or uid switches to the account keeping the session.")
    @pytest.mark.parametrize(
        ("help_text", "flag"),
        [(UTIL_LINUX_HELP, "--session-command"), (BUSYBOX_HELP, "-c")],
        ids=["util-linux", "busybox"],
    )
    @pytest.mark.parametrize("user", ["agent", "1000"])
    def test_a_name_or_uid_switches_to_the_account_keeping_the_session(
        self, stand_ins: Path, user: str, help_text: str, flag: str
    ) -> None:
        install_su(stand_ins, help_text)

        result = run(run_as(user, "echo $HOME $USER $LOGNAME"))

        assert result.returncode == 0
        assert result.stdout == f"switched to agent with {flag}\n/home/agent agent agent\n"

    @pytest.mark.description("The result is one simple command so a provider can put a timeout in front of it.")
    def test_the_result_is_one_simple_command_so_a_provider_can_put_a_timeout_in_front_of_it(
        self, stand_ins: Path
    ) -> None:
        install_su(stand_ins, UTIL_LINUX_HELP)
        (stand_ins / "timeout").write_text('#!/bin/sh\nshift\nexec "$@"\n')
        (stand_ins / "timeout").chmod(0o755)

        result = run(f"timeout 30 {run_as('agent', 'echo $USER')}")

        assert result.returncode == 0
        assert result.stdout.endswith("\nagent\n")

    @pytest.mark.description("An unknown user fails without running the command.")
    def test_an_unknown_user_fails_without_running_the_command(self, stand_ins: Path) -> None:
        install_su(stand_ins, UTIL_LINUX_HELP)

        result = run(run_as("nobody-here", "echo hi"))

        assert result.returncode == 1
        assert "The sandbox has no user nobody-here" in result.stderr
        assert result.stdout == ""
