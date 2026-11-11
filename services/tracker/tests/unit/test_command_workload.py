import asyncio
import os
import subprocess
import sys
import uuid
from pathlib import Path
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from benchmark_service.sandbox import SandboxCommandError as ProviderSandboxCommandError
from benchmark_service.sandbox import SandboxError as ProviderSandboxError

from benchmark_service import ExecResult

import tracker.sandbox as sandbox_module
from tracker.sandbox import _CommandWorkload, _in_own_process_group, _kill_agent_group


class _Sandbox:
    def __init__(self, *chunks: str, error: BaseException | None = None, hang: bool = False) -> None:
        self.chunks = chunks
        self.error = error
        self.hang = hang
        self.calls: list[tuple[str, str | None, dict[str, str] | None]] = []
        self.cancelled = asyncio.Event()

    async def command(
        self, command: str, *, cwd: str | None = None, env_vars: dict[str, str] | None = None
    ) -> AsyncIterator[str]:
        self.calls.append((command, cwd, env_vars))
        try:
            for chunk in self.chunks:
                yield chunk
            if self.hang:
                await asyncio.Event().wait()
            if self.error is not None:
                raise self.error
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


async def _drain(workload: _CommandWorkload) -> list[str]:
    return [chunk async for chunk in workload.output()]


@pytest.mark.asyncio
class TestCommandWorkload:
    @pytest.mark.description("A finished command streams its output and reports exit code 0.")
    async def test_streams_output_and_reports_success(self) -> None:
        sandbox = _Sandbox("a", "b")
        workload = _CommandWorkload(cast(Any, sandbox), "run", cwd="/w", env_vars={"K": "V"})

        assert await _drain(workload) == ["a", "b"]
        completed = await workload.wait()

        assert completed.result.exit_code == 0
        assert workload.agent_absence_confirmed_at == completed.absence_confirmed_at
        assert sandbox.calls == [("run", "/w", {"K": "V"})]

    @pytest.mark.description("A failed command reports its exit code instead of raising.")
    async def test_reports_command_exit_code(self) -> None:
        workload = _CommandWorkload(
            cast(Any, _Sandbox("x", error=ProviderSandboxCommandError(23))), "run", cwd="/w", env_vars=None
        )

        assert await _drain(workload) == ["x"]
        assert (await workload.wait()).result.exit_code == 23

    @pytest.mark.description("A provider error other than a command exit is raised from wait.")
    async def test_raises_provider_error(self) -> None:
        workload = _CommandWorkload(
            cast(Any, _Sandbox(error=ProviderSandboxError("transport"))), "run", cwd="/w", env_vars=None
        )

        assert await _drain(workload) == []
        with pytest.raises(ProviderSandboxError, match="transport"):
            await workload.wait()

    @pytest.mark.description("Kill cancels the running command, ends output and reports SIGKILL.")
    async def test_kill_cancels_running_command(self) -> None:
        sandbox = _Sandbox("started", hang=True)
        workload = _CommandWorkload(cast(Any, sandbox), "run", cwd="/w", env_vars=None)
        output = workload.output()
        assert await anext(output) == "started"
        assert workload.agent_absence_confirmed_at is None

        await workload.kill()

        assert sandbox.cancelled.is_set()
        assert [chunk async for chunk in output] == []
        assert (await workload.wait()).result.exit_code == 137
        assert workload.agent_absence_confirmed_at is not None

    @pytest.mark.description("Kill after the command finished keeps its real exit code.")
    async def test_kill_after_exit_keeps_result(self) -> None:
        workload = _CommandWorkload(cast(Any, _Sandbox("done")), "run", cwd="/w", env_vars=None)
        await _drain(workload)

        await workload.kill()

        assert (await workload.wait()).result.exit_code == 0


class _LocalSandbox:
    id = name = "local"
    state = "started"

    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.commands: list[str] = []

    async def exec(self, command: str) -> ExecResult:
        self.commands.append(command)
        if self.error is not None:
            raise self.error
        result = await asyncio.to_thread(subprocess.run, ["/bin/sh", "-c", command], capture_output=True, text=True)
        return ExecResult(exit_code=result.returncode, output=result.stdout + result.stderr)


def _alive(process: subprocess.Popen[bytes]) -> bool:
    return process.poll() is None


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")
@pytest.mark.asyncio
class TestAgentGroupKill:
    @pytest.mark.description(
        "The timeout kill reaches the agent's own process group and spares a service started before the agent."
    )
    async def test_kill_spares_setup_service(self) -> None:
        token = uuid.uuid4().hex
        service = subprocess.Popen(["/bin/sh", "-c", "exec sleep 300"], start_new_session=True)
        agent = subprocess.Popen(["/bin/sh", "-c", _in_own_process_group("sleep 300 & sleep 300", token)])
        try:
            await asyncio.sleep(0.5)
            agent_group = next(group for pid in os.listdir("/proc") if (group := _token_group(pid, token)))
            assert agent_group not in (os.getpgid(0), os.getpgid(service.pid))
            await _kill_agent_group(cast(Any, _LocalSandbox()), token)
            await asyncio.to_thread(agent.wait, 10)
            await asyncio.sleep(0.2)
            assert _alive(service)
            with pytest.raises(ProcessLookupError):
                os.killpg(agent_group, 0)
        finally:
            for process in (service, agent):
                process.kill()
                process.wait()

    @pytest.mark.description(
        "Without `setsid --wait`, as on BusyBox, the kill still reaches the agent when its shell leads its own group."
    )
    async def test_kill_without_setsid_wait(self, tmp_path: Path) -> None:
        fake_setsid = tmp_path / "setsid"
        fake_setsid.write_text("#!/bin/sh\necho 'Usage: setsid [-c] PROG ARGS'\n")
        fake_setsid.chmod(0o755)
        token = uuid.uuid4().hex
        agent = subprocess.Popen(
            ["/bin/sh", "-c", _in_own_process_group("sleep 300 & sleep 300", token)],
            env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
            start_new_session=True,
        )
        try:
            await asyncio.sleep(0.5)
            groups = {group for pid in os.listdir("/proc") if (group := _token_group(pid, token))}
            assert groups == {agent.pid}
            await _kill_agent_group(cast(Any, _LocalSandbox()), token)
            await asyncio.to_thread(agent.wait, 10)
            await asyncio.sleep(0.2)
            with pytest.raises(ProcessLookupError):
                os.killpg(agent.pid, 0)
        finally:
            agent.kill()
            agent.wait()

    @pytest.mark.description("The timeout kill reaches an agent that runs only shell built-ins.")
    async def test_kill_shell_only_agent(self) -> None:
        token = uuid.uuid4().hex
        agent = subprocess.Popen(["/bin/sh", "-c", _in_own_process_group("while :; do :; done", token)])
        try:
            await asyncio.sleep(0.5)
            await _kill_agent_group(cast(Any, _LocalSandbox()), token)
            await asyncio.to_thread(agent.wait, 10)
            assert agent.returncode is not None
        finally:
            agent.kill()
            agent.wait()

    @pytest.mark.description("A timeout kill that finds no agent group is a logged no-op.")
    async def test_no_matching_group_is_noop(self, caplog: pytest.LogCaptureFixture) -> None:
        sandbox = _LocalSandbox()
        sandbox_module.logger.addHandler(caplog.handler)
        try:
            await _kill_agent_group(cast(Any, sandbox), uuid.uuid4().hex)
        finally:
            sandbox_module.logger.removeHandler(caplog.handler)
        assert len(sandbox.commands) == 1
        assert "No agent process group found" in caplog.text

    @pytest.mark.description("A failed timeout kill is logged and never raises.")
    async def test_failed_kill_does_not_raise(self) -> None:
        sandbox = _LocalSandbox(error=ProviderSandboxError("sandbox gone"))
        await _kill_agent_group(cast(Any, sandbox), uuid.uuid4().hex)
        assert len(sandbox.commands) == 1


def _token_group(pid: str, token: str) -> int | None:
    if not pid.isdigit():
        return None
    try:
        with open(f"/proc/{pid}/environ", "rb") as environ:
            if f"VALKYRIE_AGENT_GROUP={token}".encode() not in environ.read():
                return None
        return os.getpgid(int(pid))
    except OSError:
        return None
