import asyncio
import time

from benchmark_service.sandbox.types import Sandbox

from tracker.services.datasets.config import HealthcheckConfig


class HealthcheckError(RuntimeError):
    pass


async def wait_until_healthy(sandbox: Sandbox, healthcheck: HealthcheckConfig) -> None:
    start_period_ends = time.monotonic() + healthcheck.start_period_sec
    failures = 0
    while True:
        in_start_period = time.monotonic() < start_period_ends
        result = await sandbox.exec(healthcheck.command, timeout=healthcheck.timeout_sec)
        if result.exit_code == 0:
            return
        if in_start_period:
            await asyncio.sleep(healthcheck.start_interval_sec)
            continue
        failures += 1
        if failures >= healthcheck.retries:
            raise HealthcheckError(
                f"The healthcheck failed {failures} times in a row: {healthcheck.command}\n{result.output}"
            )
        await asyncio.sleep(healthcheck.interval_sec)
