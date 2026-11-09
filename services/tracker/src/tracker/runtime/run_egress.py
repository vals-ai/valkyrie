from collections.abc import AsyncGenerator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

from benchmark_service import Sandbox

from tracker.egress import EgressPolicy
from tracker.sandbox import apply_egress_policy


@dataclass(frozen=True)
class RunAttempt:
    benchmark_id: UUID
    task_row_id: UUID
    attempt_id: str
    expires_at: datetime
    gateway_url: str | None = None


@dataclass(frozen=True)
class SandboxEnvironment:
    env_vars: dict[str, str]
    secrets: dict[str, str] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class ScopedGateway:
    url: str
    token: str = field(repr=False)


@dataclass(frozen=True)
class RunStageAccess:
    env_vars: dict[str, str] = field(default_factory=dict, repr=False)
    secrets: tuple[str, ...] = field(default=(), repr=False)


class RunStageEgress(Protocol):
    def sandbox_environment(
        self, environment: SandboxEnvironment, gateway: ScopedGateway | None
    ) -> SandboxEnvironment: ...

    def run_stage(
        self,
        sandbox: Sandbox,
        policy: EgressPolicy,
        attempt: RunAttempt,
        *,
        local_hosts: Sequence[str] = (),
        inspect: Sequence[str] = (),
    ) -> AbstractAsyncContextManager[RunStageAccess]: ...


class SandboxAllowlist:
    def sandbox_environment(self, environment: SandboxEnvironment, gateway: ScopedGateway | None) -> SandboxEnvironment:
        return environment

    @asynccontextmanager
    async def run_stage(
        self,
        sandbox: Sandbox,
        policy: EgressPolicy,
        attempt: RunAttempt,
        *,
        local_hosts: Sequence[str] = (),
        inspect: Sequence[str] = (),
    ) -> AsyncGenerator[RunStageAccess]:
        await apply_egress_policy(sandbox, policy)
        yield RunStageAccess()
