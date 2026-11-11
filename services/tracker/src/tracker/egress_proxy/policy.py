from datetime import datetime
from uuid import UUID

from sqlalchemy.engine import Engine
from sqlmodel import Session

from tracker.database.models import EgressProxyPolicy
from tracker.egress_proxy.resolve import (
    AttemptStoppedError,
    RunEgressProxy,
    resolve_child_egress_proxy as resolve_child_egress_proxy,
    resolve_run_egress_proxy as resolve_run_egress_proxy,
)
from tracker.repositories.egress_policy import EgressPolicyRepository


def policy_row(proxy: RunEgressProxy, benchmark_id: UUID, expires_at: datetime) -> EgressProxyPolicy:
    return EgressProxyPolicy(
        key_id=proxy.key_id,
        benchmark_id=benchmark_id,
        token_sha256=proxy.token_sha256,
        methods=sorted(proxy.methods),
        domains=sorted(proxy.domains) if proxy.domains is not None else None,
        expires_at=expires_at,
        inspect_domains=sorted(proxy.inspect_domains),
        gateway_url=proxy.gateway_url,
    )


class PolicyStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def grant(self, proxy: RunEgressProxy, benchmark_id: UUID, task_row_id: UUID, expires_at: datetime) -> None:
        with Session(self._engine) as session:
            if not EgressPolicyRepository(session).grant(
                benchmark_id, task_row_id, policy_row(proxy, benchmark_id, expires_at)
            ):
                raise AttemptStoppedError(proxy.key_id)
            session.commit()

    def revoke(self, key_id: str) -> None:
        with Session(self._engine) as session:
            EgressPolicyRepository(session).revoke(key_id)
            session.commit()

    def revoke_finished(self) -> int:
        with Session(self._engine) as session:
            deleted = EgressPolicyRepository(session).revoke_finished()
            session.commit()
            return deleted
