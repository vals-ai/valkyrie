import re
from pathlib import Path
from typing import Any

import httpx

from tracker.outbound_security import is_public_host

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
CHALLENGE_FIELD = re.compile(r'(\w+)="([^"]*)"')


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


def dockerfile_base_image(dockerfile: Path) -> str | None:
    stages: set[str] = set()
    base: str | None = None
    for line in dockerfile.read_text().splitlines():
        words = [word for word in line.split() if not word.startswith("--")]
        if len(words) < 2 or words[0].upper() != "FROM":
            continue
        base = None if words[1].lower() in stages or "$" in words[1] else words[1]
        if len(words) >= 4 and words[2].upper() == "AS":
            stages.add(words[3].lower())
    return base


async def _public_https_only(request: httpx.Request) -> None:
    if request.url.scheme != "https" or not is_public_host(request.url.host):
        raise httpx.RequestError(f"Refusing to contact {request.url.host} for image config", request=request)


async def image_workdir(image: str) -> str | None:
    registry, repository, reference = image_reference(image)
    base = f"https://{registry}/v2/{repository}"
    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT_SEC, follow_redirects=True, event_hooks={"request": [_public_https_only]}
        ) as client:
            headers = {"Accept": MANIFEST_TYPES}
            response = await client.get(f"{base}/manifests/{reference}", headers=headers)
            if response.status_code == 401:
                headers["Authorization"] = f"Bearer {await _anonymous_token(client, response)}"
                response = await client.get(f"{base}/manifests/{reference}", headers=headers)
            manifest: dict[str, Any] = response.raise_for_status().json()
            if "manifests" in manifest:
                chosen = next(
                    entry
                    for entry in manifest["manifests"]
                    if all(entry.get("platform", {}).get(key) == value for key, value in PLATFORM.items())
                )
                manifest = (
                    (await client.get(f"{base}/manifests/{chosen['digest']}", headers=headers))
                    .raise_for_status()
                    .json()
                )
            config = (
                (await client.get(f"{base}/blobs/{manifest['config']['digest']}", headers=headers))
                .raise_for_status()
                .json()
            )
    except (httpx.HTTPError, ValueError, KeyError, StopIteration):
        return None
    return config.get("config", {}).get("WorkingDir") or None


async def _anonymous_token(client: httpx.AsyncClient, response: httpx.Response) -> str:
    challenge = dict(CHALLENGE_FIELD.findall(response.headers.get("www-authenticate", "")))
    realm = challenge.pop("realm")
    token = (await client.get(realm, params=challenge)).raise_for_status().json()
    return token.get("token") or token["access_token"]
