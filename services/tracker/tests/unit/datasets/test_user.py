import subprocess
from pathlib import Path

import pytest

from tracker.services.datasets import user as user_module
from tracker.services.datasets.user import non_root_user, run_as

PROBE = "echo $HOME ${USER-unset} ${LOGNAME-unset}"


@pytest.fixture
def stand_ins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "getent").write_text(
        '#!/bin/sh\ncase "$2" in agent|1000) echo "agent:x:1000:1000::/home/agent:/bin/bash" ;; esac\n'
    )
    (tmp_path / "getent").chmod(0o755)
    (tmp_path / "su").write_text('#!/bin/sh\necho "switched to $6"\nexec "$3" -c "$5"\n')
    (tmp_path / "su").chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    monkeypatch.setenv("USER", "root")
    monkeypatch.setenv("LOGNAME", "root")
    monkeypatch.setattr(user_module, "IMAGE_ENVIRON", str(tmp_path / "environ"))
    return tmp_path


def install_setpriv(directory: Path, *, util_linux: bool = True) -> None:
    setpriv = directory / "setpriv"
    help_text = "--reuid <user>" if util_linux else "Usage: setpriv [OPTIONS] PROG ARGS"
    setpriv.write_text(
        f'#!/bin/sh\nif [ "$1" = "--help" ]; then echo "{help_text}"; exit 0; fi\n'
        'echo "dropped with $1 $2 $3"\nshift 3\nexec "$@"\n'
    )
    setpriv.chmod(0o755)


def image_environment(directory: Path, **variables: str) -> None:
    (directory / "environ").write_bytes(b"".join(f"{name}={value}\0".encode() for name, value in variables.items()))


def run(command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False)


@pytest.mark.description("A command switches users the way `docker exec --user` does.")
class TestRunAs:
    @pytest.mark.description(
        "A name or UID with an account runs with its own groups and home, and no `USER` or `LOGNAME`."
    )
    @pytest.mark.parametrize("user", ["agent", "1000"])
    def test_an_account_runs_with_its_own_groups_and_home(self, stand_ins: Path, user: str) -> None:
        install_setpriv(stand_ins)

        result = run(run_as(user, PROBE))

        assert result.returncode == 0
        assert result.stdout == "dropped with --reuid=agent --regid=1000 --init-groups\n/home/agent unset unset\n"

    @pytest.mark.description("Without util-linux `setpriv`, a name switches with `su` and keeps the environment.")
    @pytest.mark.parametrize("busybox_setpriv", [True, False], ids=["busybox setpriv", "no setpriv"])
    def test_without_setpriv_a_name_switches_with_su(self, stand_ins: Path, busybox_setpriv: bool) -> None:
        if busybox_setpriv:
            install_setpriv(stand_ins, util_linux=False)
        else:
            (stand_ins / "setpriv").write_text("#!/bin/sh\nexit 127\n")
            (stand_ins / "setpriv").chmod(0o755)

        result = run(run_as("agent", PROBE))

        assert result.returncode == 0
        assert result.stdout == "switched to agent\n/home/agent unset unset\n"

    @pytest.mark.description("An image's own `HOME` wins over the account's, and root's default `HOME` does not.")
    @pytest.mark.parametrize(("image_home", "home"), [("/app", "/app"), ("/root", "/home/agent")])
    def test_an_images_own_home_wins(self, stand_ins: Path, image_home: str, home: str) -> None:
        install_setpriv(stand_ins)
        image_environment(stand_ins, PATH="/opt/venv/bin", HOME=image_home)

        result = run(run_as("agent", "echo $HOME"))

        assert result.stdout.splitlines()[-1] == home

    @pytest.mark.description("The result is one simple command so a provider can put a timeout in front of it.")
    def test_the_result_is_one_simple_command_so_a_provider_can_put_a_timeout_in_front_of_it(
        self, stand_ins: Path
    ) -> None:
        install_setpriv(stand_ins)
        (stand_ins / "timeout").write_text('#!/bin/sh\nshift\nexec "$@"\n')
        (stand_ins / "timeout").chmod(0o755)

        result = run(f"timeout 30 {run_as('agent', 'echo $HOME')}")

        assert result.returncode == 0
        assert result.stdout.endswith("\n/home/agent\n")

    @pytest.mark.description("An unknown user fails without running the command.")
    def test_an_unknown_user_fails_without_running_the_command(self, stand_ins: Path) -> None:
        install_setpriv(stand_ins)

        result = run(run_as("nobody-here", "echo hi"))

        assert result.returncode == 1
        assert "The sandbox has no user nobody-here" in result.stderr
        assert result.stdout == ""

    @pytest.mark.description("A name with a group runs as that account with only that group.")
    def test_a_name_with_a_group_runs_with_only_that_group(self, stand_ins: Path) -> None:
        install_setpriv(stand_ins)

        result = run(run_as("agent:shared", PROBE))

        assert result.returncode == 0
        assert result.stdout == "dropped with --reuid=agent --regid=shared --clear-groups\n/home/agent unset unset\n"

    @pytest.mark.description(
        "A numeric ID without an account runs as that ID and group, else group 0, with `HOME` at `/`."
    )
    @pytest.mark.parametrize(("user", "group"), [("2000", "0"), ("2000:3000", "3000")])
    def test_a_numeric_id_without_an_account_drops_to_that_id(self, stand_ins: Path, user: str, group: str) -> None:
        install_setpriv(stand_ins)

        result = run(run_as(user, "echo $HOME"))

        assert result.returncode == 0
        assert result.stdout == f"dropped with --reuid=2000 --regid={group} --clear-groups\n/\n"

    @pytest.mark.description("A group or an ID without an account fails without util-linux `setpriv` and runs nothing.")
    @pytest.mark.parametrize("user", ["agent:shared", "2000"])
    def test_a_switch_that_needs_setpriv_fails_without_it(self, stand_ins: Path, user: str) -> None:
        install_setpriv(stand_ins, util_linux=False)

        result = run(run_as(user, "echo hi"))

        assert result.returncode == 1
        assert f"The sandbox cannot switch to {user} without setpriv" in result.stderr
        assert result.stdout == ""


@pytest.mark.description("An image's `USER` becomes a checked sandbox user, or none when it is root.")
class TestNonRootUser:
    @pytest.mark.description("Root by name or ID, with or without a group, is no user to switch to.")
    @pytest.mark.parametrize("user", [None, "", "root", "0", "0:0", "root:root"])
    def test_root_is_no_user_to_switch_to(self, user: str | None) -> None:
        assert non_root_user(user) is None

    @pytest.mark.description("A non-root user keeps its group.")
    @pytest.mark.parametrize("user", ["agent", "1000", "1000:1000", "agent:staff"])
    def test_a_non_root_user_keeps_its_group(self, user: str) -> None:
        assert non_root_user(user) == user

    @pytest.mark.description("A user that is not a plain name or ID is refused.")
    @pytest.mark.parametrize("user", ["a b", "$(id)", "agent:-x;rm"])
    def test_a_user_that_is_not_a_plain_name_or_id_is_refused(self, user: str) -> None:
        with pytest.raises(ValueError, match="a user is a name or UID"):
            non_root_user(user)
