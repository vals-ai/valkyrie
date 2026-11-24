import json
import logging
import re
from typing import Any, NamedTuple

import httpx

from tracker.outbound_security import is_public_host

logger = logging.getLogger(__name__)

DOCKER_HUB = "registry-1.docker.io"
DOCKER_HUB_NAMES = {"docker.io", "index.docker.io"}
MANIFEST_TYPES = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
PLATFORM = {"os": "linux", "architecture": "amd64"}
TIMEOUT_SEC = 15
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
CHALLENGE_FIELD = re.compile(r'(\w+)="([^"]*)"')


class ImageConfig(NamedTuple):
    user: str | None
    workdir: str | None


def image_reference(image: str) -> tuple[str, str, str]:
    name, _, digest = image.partition("@")
    first, _, rest = name.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        registry, path = first, rest
    else:
        registry, path = DOCKER_HUB, name
    if registry in DOCKER_HUB_NAMES:
        registry = DOCKER_HUB
    if registry == DOCKER_HUB and "/" not in path:
        path = f"library/{path}"
    repository, _, tag = path.rpartition(":") if ":" in path.rsplit("/", 1)[-1] else (path, "", "latest")
    return registry, repository, digest or tag


async def _public_https_only(request: httpx.Request) -> None:
    if request.url.scheme != "https" or not is_public_host(request.url.host):
        raise httpx.RequestError(f"Refusing to contact {request.url.host} for image config", request=request)


async def image_config(image: str) -> ImageConfig | None:
    registry, repository, reference = image_reference(image)
    base = f"https://{registry}/v2/{repository}"
    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT_SEC, follow_redirects=True, event_hooks={"request": [_public_https_only]}
        ) as client:
            headers = {"Accept": MANIFEST_TYPES}
            try:
                manifest = await _get_json(client, f"{base}/manifests/{reference}", headers=headers)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 401:
                    raise
                headers["Authorization"] = f"Bearer {await _anonymous_token(client, error.response)}"
                manifest = await _get_json(client, f"{base}/manifests/{reference}", headers=headers)
            if "manifests" in manifest:
                chosen = next(
                    entry
                    for entry in manifest["manifests"]
                    if all(entry.get("platform", {}).get(key) == value for key, value in PLATFORM.items())
                )
                manifest = await _get_json(client, f"{base}/manifests/{chosen['digest']}", headers=headers)
            settings: dict[str, Any] = (
                await _get_json(client, f"{base}/blobs/{manifest['config']['digest']}", headers=headers)
            ).get("config") or {}
            return ImageConfig(str(settings.get("User") or "") or None, str(settings.get("WorkingDir") or "") or None)
    except (httpx.HTTPError, httpx.InvalidURL, ValueError, KeyError, TypeError, AttributeError, StopIteration) as error:
        logger.warning(
            "Could not read the config of image %s, so its user and working directory are unknown: %r", image, error
        )
        return None


async def _get_json(client: httpx.AsyncClient, url: str, **options: Any) -> Any:
    async with client.stream("GET", url, **options) as response:
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError(f"{url} returned more than {MAX_RESPONSE_BYTES} bytes")
    return json.loads(body)


async def _anonymous_token(client: httpx.AsyncClient, response: httpx.Response) -> str:
    challenge = dict(CHALLENGE_FIELD.findall(response.headers.get("www-authenticate", "")))
    realm = challenge.pop("realm")
    token = await _get_json(client, realm, params=challenge)
    return token.get("token") or token["access_token"]
