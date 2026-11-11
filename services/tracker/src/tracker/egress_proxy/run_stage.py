from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager

from benchmark_service import ChildEgress, ChildPolicy, Sandbox

from tracker.egress import EgressPolicy, child_key_id
from tracker.egress_proxy.client import (
    install_proxy_ca,
    proxy_environment,
    route_providers_through_gateway,
    start_proxy_tunnel,
)
from tracker.egress_proxy.resolve import resolve_child_egress_proxy, resolve_run_egress_proxy
from tracker.executor.control_tasks import TaskControl
from tracker.egress_proxy.settings import EgressProxyEndpoint
from tracker.logging import get_logger
from tracker.runtime.run_egress import RunAttempt, RunStageAccess, SandboxEnvironment, ScopedGateway
from tracker.sandbox import apply_egress_policy


logger = get_logger(__name__)


class EgressProxy:
    def __init__(self, endpoint: EgressProxyEndpoint, control: TaskControl) -> None:
        self._endpoint = endpoint
        self._control = control

    def sandbox_environment(self, environment: SandboxEnvironment, gateway: ScopedGateway | None) -> SandboxEnvironment:
        if gateway is None or not gateway.url.startswith("https://"):
            return environment
        return route_providers_through_gateway(environment, gateway.url, gateway.token)

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
        proxy = resolve_run_egress_proxy(policy, attempt.attempt_id, attempt.gateway_url, inspect)
        if proxy is None:
            await apply_egress_policy(sandbox, policy)
            yield RunStageAccess()
            return

        await self._control.grant_egress_policy(proxy, attempt)
        try:
            await apply_egress_policy(sandbox, list(self._endpoint.cidrs))
            await install_proxy_ca(sandbox, self._endpoint.ca_certificate)
            await start_proxy_tunnel(sandbox, self._endpoint)
            yield RunStageAccess(
                env_vars=proxy_environment(proxy.key_id, proxy.token, local_hosts=local_hosts),
                secrets=(proxy.token,),
            )
        finally:
            try:
                await self._control.revoke_egress_policy(proxy.key_id, attempt)
            except Exception:
                logger.exception(
                    "Failed to revoke egress proxy policy",
                    extra={"benchmark_id": str(attempt.benchmark_id), "attempt_id": attempt.attempt_id},
                )

    @asynccontextmanager
    async def child_egress(
        self, name: str, profile: ChildPolicy, attempt: RunAttempt, gateway: ScopedGateway | None
    ) -> AsyncGenerator[ChildEgress]:
        proxy = resolve_child_egress_proxy(profile, child_key_id(attempt.attempt_id, name), attempt.gateway_url)
        await self._control.grant_egress_policy(proxy, attempt)
        try:
            yield ChildEgress(
                proxy_url=f"https://{proxy.key_id}:{proxy.token}@{self._endpoint.hosts[0]}:{self._endpoint.port}",
                proxy_cidrs=list(self._endpoint.cidrs),
                ca_certificate=self._endpoint.ca_certificate,
                gateway_url=gateway.url if gateway else None,
                gateway_token=gateway.token if gateway else None,
            )
        finally:
            try:
                await self._control.revoke_egress_policy(proxy.key_id, attempt)
            except Exception:
                logger.exception(
                    "Failed to revoke child egress proxy policy",
                    extra={"benchmark_id": str(attempt.benchmark_id), "attempt_id": attempt.attempt_id},
                )
