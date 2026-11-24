from collections.abc import Callable, Mapping
from urllib.parse import urlsplit

from tracker.clients.model_gateway import GENERATION_MODEL_GATEWAY_URL, MODEL_GATEWAY_URL
from tracker.egress import EgressPolicy
from tracker.egress_proxy.client import PROVIDERS

NO_INFERENCE_HOST_WARNING = (
    "[WARNING]: No model endpoint was found in the agent's environment, so none was added to the task's "
    f"restricted network policy. Model requests may be blocked. Set {MODEL_GATEWAY_URL} or a provider base URL, "
    "or list the model host in the task's allowed_hosts."
)


def _endpoint_host(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
        _ = parts.port
    except ValueError:
        return None
    if parts.scheme not in {"http", "https"} or not parts.hostname or "*" in parts.hostname:
        return None
    return parts.hostname


def _inference_hosts(env: Mapping[str, str]) -> list[str]:
    urls = [
        env.get(MODEL_GATEWAY_URL),
        env.get(GENERATION_MODEL_GATEWAY_URL),
        *(
            env.get(provider.base_url) or (provider.default_url if any(env.get(key) for key in provider.keys) else None)
            for provider in PROVIDERS
        ),
    ]
    return list(dict.fromkeys(host for url in urls if (host := _endpoint_host(url))))


def allow_inference_hosts(
    policy: EgressPolicy, env: Mapping[str, str], log_line: Callable[[str], None]
) -> EgressPolicy:
    if policy == "*":
        return policy
    hosts = _inference_hosts(env)
    if not hosts:
        log_line(NO_INFERENCE_HOST_WARNING)
        return policy
    return list(dict.fromkeys([*policy, *hosts]))
