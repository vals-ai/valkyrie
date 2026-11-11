import asyncio
from typing import cast

import pytest
from benchmark_service.sandbox.types import ExecResult, Sandbox

from tracker.services.datasets.config import HealthcheckConfig
from tracker.services.datasets.healthcheck import HealthcheckError, wait_until_healthy


class ScriptedSandbox:
    def __init__(self, exit_codes: list[int]) -> None:
        self.exit_codes = exit_codes
        self.runs: list[tuple[str, float | None]] = []

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.runs.append((command, timeout))
        return ExecResult(exit_code=self.exit_codes.pop(0), output="not ready")


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    waited: list[float] = []

    async def sleep(seconds: float) -> None:
        waited.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    return waited


async def wait(sandbox: ScriptedSandbox, **settings: float) -> None:
    await wait_until_healthy(cast(Sandbox, sandbox), HealthcheckConfig(command="ready?", **settings))


@pytest.mark.description(
    ("Docker HEALTHCHECK semantics: one pass is enough, and only failures after the start period count.")
)
class TestWaitUntilHealthy:
    @pytest.mark.description("The first pass returns at once under the check timeout.")
    async def test_the_first_pass_returns_at_once_under_the_check_timeout(self, sleeps: list[float]) -> None:
        sandbox = ScriptedSandbox([0])

        await wait(sandbox, timeout_sec=7)

        assert sandbox.runs == [("ready?", 7)]
        assert sleeps == []

    @pytest.mark.description("Failures below the retry limit are waited out.")
    async def test_failures_below_the_retry_limit_are_waited_out(self, sleeps: list[float]) -> None:
        sandbox = ScriptedSandbox([1, 1, 0])

        await wait(sandbox, interval_sec=2, retries=3)

        assert sleeps == [2, 2]

    @pytest.mark.description("Consecutive failures past the limit fail with the last output.")
    async def test_consecutive_failures_past_the_limit_fail_with_the_last_output(self, sleeps: list[float]) -> None:
        sandbox = ScriptedSandbox([1, 1, 1])

        with pytest.raises(HealthcheckError, match="failed 3 times in a row: ready\\?\nnot ready"):
            await wait(sandbox, retries=3)

    @pytest.mark.description("Failures in the start period do not count.")
    async def test_failures_in_the_start_period_do_not_count(self, sleeps: list[float]) -> None:
        sandbox = ScriptedSandbox([1, 1, 1, 1, 0])

        await wait(sandbox, start_period_sec=3600, start_interval_sec=1, retries=2)

        assert sleeps == [1, 1, 1, 1]
