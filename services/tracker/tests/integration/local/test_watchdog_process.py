from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

import pytest


_CHILD = """
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
child = subprocess.Popen([
    sys.executable, '-c',
    'import signal,time; from pathlib import Path; '
    'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
    'path = Path(' + repr(str(root / 'descendant')) + '); '
    'temporary = path.with_name(path.name + ".tmp"); '
    'temporary.write_text(str(__import__("os").getpid())); '
    'temporary.replace(path); time.sleep(30)',
])
signal.signal(signal.SIGTERM, lambda *_: (root / "terminated").touch())
(root / 'executor').write_text(str(os.getpid()))
channel = socket.socket(fileno=int(os.environ['TRACKER_WATCHDOG_FD']))
channel.sendall(b'IDENTITY 00000000-0000-0000-0000-000000000002 process-invocation\\n')
assert channel.recv(64) == b'IDENTIFIED\\n'
(root / 'identified').touch()
while True:
    time.sleep(.05)
"""

_DRIVER = """
import ctypes
import subprocess
import sys
from pathlib import Path
from uuid import UUID
from tracker.executor import watchdog

original = subprocess.Popen
root, script = sys.argv[1:]
assert ctypes.CDLL(None).prctl(36, 1, 0, 0, 0) == 0
def launch(command, **kwargs):
    return original([sys.executable, script, root], **kwargs)
watchdog.subprocess.Popen = launch
watchdog._GRACE_SECONDS = .3
def report_exit(dispatch_id: UUID, benchmark_id: UUID, invocation_key: str) -> None:
    assert not watchdog._group_alive(int((Path(root) / "executor").read_text()))
    (Path(root) / "reported").write_text(f"{dispatch_id} {benchmark_id} {invocation_key}")
watchdog._report_exit = report_exit
raise SystemExit(watchdog.supervise(UUID(int=1)))
"""

_REPORT_CHILD = """
import os
import socket
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
channel = socket.socket(fileno=int(os.environ['TRACKER_WATCHDOG_FD']))
channel.sendall(b'IDENTITY 00000000-0000-0000-0000-000000000002 process-invocation\\n')
assert channel.recv(64) == b'IDENTIFIED\\n'
(root / 'executor').write_text(str(os.getpid()))
(root / 'ready').touch()
while not (root / 'exit').exists():
    time.sleep(.005)
"""

_REPORT_DRIVER = """
import subprocess
import sys
from uuid import UUID
from tracker.executor import watchdog

original = subprocess.Popen
root, script = sys.argv[1:]
def launch(command, **kwargs):
    return original([sys.executable, script, root], **kwargs)
watchdog.subprocess.Popen = launch
raise SystemExit(watchdog.supervise(UUID(int=1)))
"""


def _wait_for(path: Path, *, seconds: float = 5) -> None:
    deadline = time.monotonic() + seconds
    while not path.exists():
        assert time.monotonic() < deadline, f"Missing process marker: {path.name}"
        time.sleep(0.005)


def _alive(pid: int) -> bool:
    status = Path(f"/proc/{pid}/status")
    return status.exists() and "State:\tZ" not in status.read_text()


@pytest.mark.description(
    "A terminated watchdog kills the executor process group after the grace period and reports the exit only once the group is gone."
)
def test_watchdog_terminates_group_and_reports_exit(tmp_path: Path) -> None:
    script = tmp_path / "child.py"
    script.write_text(_CHILD)
    driver = subprocess.Popen(
        [sys.executable, "-c", _DRIVER, str(tmp_path), str(script)],
        start_new_session=True,
    )
    try:
        _wait_for(tmp_path / "identified")
        _wait_for(tmp_path / "descendant")
        assert not (tmp_path / "reported").exists()
        executor_pid = int((tmp_path / "executor").read_text())
        descendant_pid = int((tmp_path / "descendant").read_text())
        assert os.getpgid(executor_pid) == os.getpgid(descendant_pid)
        assert os.getpgid(driver.pid) != os.getpgid(executor_pid)
        driver.send_signal(signal.SIGTERM)
        assert driver.wait(timeout=5) == 143
        assert (tmp_path / "terminated").exists()
        assert not _alive(executor_pid)
        assert not _alive(descendant_pid)
        assert (tmp_path / "reported").read_text() == (
            "00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002 process-invocation"
        )
    finally:
        if driver.poll() is None:
            driver.terminate()
            driver.wait(timeout=5)
        if (tmp_path / "executor").exists():
            try:
                os.killpg(int((tmp_path / "executor").read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("status, retries", [(408, True), (409, True), (503, True), (401, False), (422, False)])
@pytest.mark.description(
    "A childless watchdog retries temporary exit-report failures and exits on permanent rejection.\n\n"
    "Test cases:\n"
    "- Temporary rejections preserve the original request until accepted.\n"
    "- Permanent rejections end reporting without another request."
)
def test_watchdog_exit_report_rejection(tmp_path: Path, status: int, retries: bool) -> None:
    requests: list[tuple[str, str | None, str | None, bytes]] = []
    second_request = Event()
    accept_second = Event()

    class ControlHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(
                (
                    self.path,
                    self.headers.get("Authorization"),
                    self.headers.get("X-Executor-Dispatch-ID"),
                    self.rfile.read(int(self.headers["Content-Length"])),
                )
            )
            if len(requests) == 2:
                second_request.set()
                accept_second.wait(5)
            self.send_response(status if len(requests) == 1 else 200)
            self.end_headers()
            self.wfile.write(b"{}")

    server = HTTPServer(("127.0.0.1", 0), ControlHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    script = tmp_path / "child.py"
    script.write_text(_REPORT_CHILD)
    driver = subprocess.Popen(
        [sys.executable, "-c", _REPORT_DRIVER, str(tmp_path), str(script)],
        env={**os.environ, "EXECUTOR_CONTROL_URL": f"http://127.0.0.1:{server.server_port}"},
    )
    try:
        _wait_for(tmp_path / "ready")
        (tmp_path / "exit").touch()
        if retries:
            assert second_request.wait(5)
            assert driver.poll() is None
        else:
            assert driver.wait(timeout=5) != 0
        assert not _alive(int((tmp_path / "executor").read_text()))
        accept_second.set()
        if retries:
            assert driver.wait(timeout=5) == 0
        assert len(requests) == (2 if retries else 1)
        for path, authorization, dispatch_id, body in requests:
            assert path == "/executor-control/v1/queue-exit"
            assert authorization == "Bearer process-invocation"
            assert dispatch_id == str(UUID(int=1))
            assert json.loads(body) == {"benchmark_id": str(UUID(int=2)), "dispatch_id": str(UUID(int=1))}
    finally:
        accept_second.set()
        if driver.poll() is None:
            driver.kill()
            driver.wait(timeout=5)
        if (tmp_path / "executor").exists():
            try:
                os.killpg(int((tmp_path / "executor").read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
