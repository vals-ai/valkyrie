import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files

from benchmark_service import Sandbox
from benchmark_service.sandbox import SandboxError as ProviderSandboxError

from tracker.egress_proxy.settings import EgressProxyEndpoint
from tracker.exceptions import InvalidSandboxConfigurationError, SandboxSetupError
from tracker.runtime.run_egress import SandboxEnvironment
from tracker.sandbox import run_setup_command


_NO_PROXY = "localhost,127.0.0.1,::1"
_CA_FILE = "/tmp/egress-proxy-ca.crt"
_CA_BUNDLE = "/tmp/egress-proxy-ca-bundle.pem"
_INSTALL_SCRIPT = "/tmp/egress-proxy-install-ca.sh"
_TUNNEL_SCRIPT = "/tmp/egress-proxy-tunnel.py"
_TUNNEL_PID_FILE = "/tmp/egress-proxy-tunnel.pid"
_TUNNEL_PORT = 3128
_NO_PYTHON_EXIT = 3
_PYTHON_SEARCH_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def proxy_environment(key_id: str, token: str, *, local_hosts: Sequence[str] = ()) -> dict[str, str]:
    no_proxy = ",".join((_NO_PROXY, *local_hosts))
    proxy_url = f"http://{key_id}:{token}@127.0.0.1:{_TUNNEL_PORT}"
    return {
        "HTTP_PROXY": proxy_url,
        "HTTPS_PROXY": proxy_url,
        "http_proxy": proxy_url,
        "https_proxy": proxy_url,
        "NO_PROXY": no_proxy,
        "no_proxy": no_proxy,
        "NODE_USE_ENV_PROXY": "1",
        "SSL_CERT_FILE": _CA_BUNDLE,
        "REQUESTS_CA_BUNDLE": _CA_BUNDLE,
        "CURL_CA_BUNDLE": _CA_BUNDLE,
        "NODE_EXTRA_CA_CERTS": _CA_FILE,
        "PIP_CERT": _CA_BUNDLE,
    }


@dataclass(frozen=True)
class _Provider:
    route: str
    base_url: str
    keys: tuple[str, ...]


PROVIDERS = (
    _Provider("/passthrough/openai/v1", "OPENAI_BASE_URL", ("OPENAI_API_KEY",)),
    _Provider("/passthrough/anthropic", "ANTHROPIC_BASE_URL", ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")),
    _Provider("/passthrough/google", "GOOGLE_GEMINI_BASE_URL", ("GEMINI_API_KEY", "GOOGLE_API_KEY")),
    _Provider("/passthrough/openrouter/api/v1", "OPENROUTER_BASE_URL", ("OPENROUTER_API_KEY",)),
)


def route_providers_through_gateway(
    environment: SandboxEnvironment, gateway_url: str, token: str
) -> SandboxEnvironment:
    routed = dict(environment.env_vars)
    secrets = dict(environment.secrets)
    for provider in PROVIDERS:
        if routed.get(provider.base_url) or provider.base_url in secrets:
            continue
        for key in provider.keys:
            routed.pop(key, None)
            secrets.pop(key, None)
        routed[provider.base_url] = gateway_url.rstrip("/") + provider.route
        routed[provider.keys[0]] = token
    return SandboxEnvironment(routed, secrets)


async def install_proxy_ca(sandbox: Sandbox, certificate: str) -> None:
    try:
        await sandbox.upload_file(_CA_FILE, certificate.encode())
        await sandbox.upload_file(_INSTALL_SCRIPT, files("tracker.egress_proxy").joinpath("install_ca.sh").read_bytes())
    except ProviderSandboxError as error:
        raise SandboxSetupError("Failed to upload the egress proxy CA to the sandbox.") from error
    await run_setup_command(
        sandbox,
        f"sh {shlex.quote(_INSTALL_SCRIPT)} {shlex.quote(_CA_FILE)} {shlex.quote(_CA_BUNDLE)}",
        "Failed to install egress proxy CA trust; root or non-interactive sudo is required.",
    )


async def start_proxy_tunnel(sandbox: Sandbox, endpoint: EgressProxyEndpoint) -> None:
    arguments = shlex.join((endpoint.hosts[0], str(endpoint.port), _CA_FILE, str(_TUNNEL_PORT), _TUNNEL_PID_FILE))
    command = (
        f'PATH="$PATH:{_PYTHON_SEARCH_PATH}"; '
        f"command -v python3 >/dev/null || exit {_NO_PYTHON_EXIT}; python3 {_TUNNEL_SCRIPT} {arguments}"
    )
    failure = "Failed to start the egress proxy tunnel."
    try:
        await sandbox.upload_file(_TUNNEL_SCRIPT, files("benchmark_service").joinpath("egress_tunnel.py").read_bytes())
        result = await sandbox.exec(command, timeout=60)
    except ProviderSandboxError as error:
        raise SandboxSetupError(failure) from error
    if result.exit_code == _NO_PYTHON_EXIT:
        raise InvalidSandboxConfigurationError(
            "The egress proxy tunnel needs python3 in the sandbox image, and none was found."
        )
    if result.exit_code != 0:
        raise SandboxSetupError(failure)
