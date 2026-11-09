import os
from dataclasses import dataclass


@dataclass(frozen=True)
class EgressProxyEndpoint:
    hosts: tuple[str, ...]
    port: int
    cidrs: tuple[str, ...]
    ca_certificate: str


def _list(name: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in os.environ.get(name, "").split(",") if item.strip())


def load_egress_proxy_endpoint() -> EgressProxyEndpoint | None:
    hosts = _list("EGRESS_PROXY_HOSTS")
    cidrs = _list("EGRESS_PROXY_CIDRS")
    if not hosts or not cidrs:
        return None
    ca_certificate = os.environ.get("EGRESS_PROXY_CA_CERT")
    if not ca_certificate:
        raise ValueError("EGRESS_PROXY_CA_CERT is required with EGRESS_PROXY_HOSTS and EGRESS_PROXY_CIDRS")
    port = int(os.environ.get("EGRESS_PROXY_PORT", "3128"))
    if port <= 0:
        raise ValueError("EGRESS_PROXY_PORT must be a positive integer")
    return EgressProxyEndpoint(hosts, port, cidrs, ca_certificate)


EGRESS_PROXY = load_egress_proxy_endpoint()
