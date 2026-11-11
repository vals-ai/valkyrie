from __future__ import annotations

import argparse
import json
import os
import selectors
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

_GRACE_SECONDS = 30


class DeadlineChannel:
    def __init__(self, fd: int) -> None:
        self.socket = socket.socket(fileno=fd)
        self.stream = self.socket.makefile("rwb", buffering=0)

    def identify(self, benchmark_id: UUID, invocation_key: str) -> None:
        self.stream.write(f"IDENTITY {benchmark_id} {invocation_key}\n".encode())
        if self.stream.readline() != b"IDENTIFIED\n":
            raise RuntimeError("Watchdog did not record the executor invocation")

    def close(self) -> None:
        self.stream.close()
        self.socket.close()


def _signal_group(pid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _group_alive(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _report_exit(dispatch_id: UUID, benchmark_id: UUID, invocation_key: str) -> None:
    request = Request(
        f"{os.environ['EXECUTOR_CONTROL_URL'].rstrip('/')}/executor-control/v1/queue-exit",
        data=json.dumps({"benchmark_id": str(benchmark_id), "dispatch_id": str(dispatch_id)}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {invocation_key}",
            "X-Executor-Dispatch-ID": str(dispatch_id),
        },
        method="POST",
    )
    delay = 0.1
    while True:
        try:
            with urlopen(request, timeout=8) as response:
                response.read()
            return
        except HTTPError as error:
            if error.code not in (408, 409) and not 500 <= error.code < 600:
                raise
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(delay)
        delay = min(delay * 2, 5.0)


def supervise(dispatch_id: UUID) -> int:
    parent, child = socket.socketpair()
    environment = os.environ.copy()
    environment["TRACKER_WATCHDOG_FD"] = str(child.fileno())
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "tracker.executor.entrypoint", "--dispatch-id", str(dispatch_id)],
            env=environment,
            pass_fds=(child.fileno(),),
            start_new_session=True,
        )
    finally:
        child.close()
    parent.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(parent, selectors.EVENT_READ)
    termination_at: float | None = None
    buffer = bytearray()
    identity: tuple[UUID, str] | None = None
    forced = False

    def forward(sig: int, _frame: object) -> None:
        nonlocal termination_at
        _signal_group(process.pid, signal.Signals(sig))
        if termination_at is None:
            termination_at = time.monotonic()

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    try:
        while True:
            now = time.monotonic()
            if termination_at is not None and now >= termination_at + _GRACE_SECONDS:
                _signal_group(process.pid, signal.SIGKILL)
                forced = True
                process.wait()
            if process.poll() is not None:
                while True:
                    try:
                        if os.waitpid(-1, os.WNOHANG)[0] == 0:
                            break
                    except ChildProcessError:
                        break
                if not _group_alive(process.pid):
                    if identity is not None:
                        _report_exit(dispatch_id, *identity)
                    return 143 if forced else process.returncode
            timeout = 0.1
            if termination_at is not None and now < termination_at + _GRACE_SECONDS:
                timeout = min(timeout, termination_at + _GRACE_SECONDS - now)
            for _key, _events in selector.select(timeout):
                chunk = parent.recv(4096)
                if not chunk:
                    selector.unregister(parent)
                    continue
                buffer.extend(chunk)
                while b"\n" in buffer:
                    line, _, remainder = buffer.partition(b"\n")
                    buffer = bytearray(remainder)
                    if not line.startswith(b"IDENTITY "):
                        raise RuntimeError("Unexpected watchdog command")
                    _, benchmark_id, invocation_key = line.split()
                    identity = UUID(benchmark_id.decode()), invocation_key.decode()
                    parent.sendall(b"IDENTIFIED\n")
    finally:
        selector.close()
        parent.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dispatch-id", required=True, type=UUID)
    raise SystemExit(supervise(parser.parse_args().dispatch_id))


if __name__ == "__main__":
    main()
