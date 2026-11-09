import hashlib
import ipaddress
import secrets
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy.engine import Engine
from benchmark_service import ChildPolicy
from sqlmodel import Session

from tracker.database.models import EgressProxyPolicy
from tracker.egress import EgressPolicy
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.repositories.egress_policy import EgressPolicyRepository


_OPEN_METHODS = frozenset({"GET", "HEAD", "POST"})
_LISTED_METHODS = frozenset({"GET", "HEAD"})


def _policy_domain(entry: str) -> str:
    entry = entry.strip()
    try:
        ipaddress.ip_network(entry, strict=False)
    except ValueError:
        parsed_entry = entry if "://" in entry else f"//{entry}"
        host = urlsplit(parsed_entry).hostname or entry
        if "*" in host or host.startswith("."):
            raise InvalidSandboxConfigurationError(
                f"Egress entry {entry!r} is not supported: wildcard and leading-dot entries never match; "
                "list the domain itself, which also allows its subdomains"
            )
        return host
    return entry


def validate_proxy_entries(policy: EgressPolicy, inspect: Sequence[str]) -> None:
    for entry in [*([] if policy == "*" else policy), *inspect]:
        _policy_domain(entry)


@dataclass(frozen=True)
class RunEgressProxy:
    key_id: str
    token: str = field(repr=False)
    domains: frozenset[str] | None
    methods: frozenset[str]
    gateway_url: str | None = None
    inspect_domains: frozenset[str] = frozenset()

    @property
    def token_sha256(self) -> str:
        return hashlib.sha256(self.token.encode()).hexdigest()


def resolve_run_egress_proxy(
    policy: EgressPolicy, key_id: str, gateway_url: str | None = None, inspect: Sequence[str] = ()
) -> RunEgressProxy | None:
    proxy = _egress_proxy(policy, key_id, gateway_url, inspect)
    if policy == [] and proxy.gateway_url is None:
        return None
    return proxy


def resolve_child_egress_proxy(profile: ChildPolicy, key_id: str, gateway_url: str | None) -> RunEgressProxy:
    return _egress_proxy(profile.egress, key_id, gateway_url, profile.inspect)


def _egress_proxy(policy: EgressPolicy, key_id: str, gateway_url: str | None, inspect: Sequence[str]) -> RunEgressProxy:
    return RunEgressProxy(
        key_id=key_id,
        token=secrets.token_urlsafe(32),
        domains=None if policy == "*" else frozenset(_policy_domain(entry) for entry in policy),
        methods=_OPEN_METHODS if policy == "*" else _LISTED_METHODS,
        gateway_url=gateway_url if gateway_url and gateway_url.startswith("https://") else None,
        inspect_domains=frozenset(_policy_domain(entry) for entry in inspect),
    )


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


class AttemptStoppedError(Exception):
    pass


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
