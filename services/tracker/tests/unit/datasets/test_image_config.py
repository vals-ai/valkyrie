import httpx
import pytest

from tracker.services.datasets import image_config as image_config_module
from tracker.services.datasets.image_config import (
    DOCKER_HUB,
    _get_json,
    _public_https_only,
    image_config,
    image_reference,
)


@pytest.mark.description("An image name splits into registry repository and reference.")
@pytest.mark.parametrize(
    ("image", "reference"),
    [
        ("golang:1.23", (DOCKER_HUB, "library/golang", "1.23")),
        ("node", (DOCKER_HUB, "library/node", "latest")),
        ("docker.io/library/golang:1.23", (DOCKER_HUB, "library/golang", "1.23")),
        ("org/app:2", (DOCKER_HUB, "org/app", "2")),
        ("ghcr.io/org/app@sha256:abc", ("ghcr.io", "org/app", "sha256:abc")),
        ("localhost:5000/team/app:dev", ("localhost:5000", "team/app", "dev")),
    ],
)
def test_an_image_name_splits_into_registry_repository_and_reference(
    image: str, reference: tuple[str, str, str]
) -> None:
    assert image_reference(image) == reference


@pytest.mark.description("Image config lookups contact only public hosts over https.")
@pytest.mark.parametrize(
    ("url", "allowed"),
    [
        ("https://auth.docker.io/token", True),
        ("http://auth.docker.io/token", False),
        ("https://169.254.169.254/latest/meta-data", False),
        ("https://localhost:5000/v2/app/manifests/dev", False),
        ("https://tracker.internal/token", False),
        ("https://10.0.0.5/token", False),
    ],
)
async def test_image_config_lookups_contact_only_public_hosts_over_https(url: str, allowed: bool) -> None:
    request = httpx.Request("GET", url)
    if allowed:
        await _public_https_only(request)
    else:
        with pytest.raises(httpx.RequestError):
            await _public_https_only(request)


@pytest.mark.description("A registry response over the size cap is refused while it is read.")
async def test_a_registry_response_over_the_size_cap_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(image_config_module, "MAX_RESPONSE_BYTES", 8)
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, content=b'{"config": "too long"}'))

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ValueError, match="more than 8 bytes"):
            await _get_json(client, "https://registry.example/v2/app/manifests/1")


@pytest.mark.description("An image whose registry address is not a valid URL has an unknown config.")
async def test_an_image_with_an_invalid_registry_address_has_an_unknown_config() -> None:
    assert await image_config("registry:99999/app") is None
