from pathlib import Path

import httpx
import pytest

from tracker.services.datasets.image_config import (
    DOCKER_HUB,
    _public_https_only,
    dockerfile_base_image,
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


@pytest.mark.description("A dockerfile starts from the image of its last stage.")
@pytest.mark.parametrize(
    ("dockerfile", "base"),
    [
        ("FROM golang:1.23\nRUN go version\n", "golang:1.23"),
        ("FROM --platform=linux/amd64 python:3.12 AS build\nFROM golang:1.23\n", "golang:1.23"),
        ("FROM golang:1.23 AS build\nRUN true\nFROM build\n", None),
        ("ARG BASE=golang\nFROM ${BASE}:1.23\n", None),
    ],
    ids=["one stage", "last stage", "earlier stage", "build argument"],
)
def test_a_dockerfile_starts_from_the_image_of_its_last_stage(
    tmp_path: Path, dockerfile: str, base: str | None
) -> None:
    path = tmp_path / "Dockerfile"
    path.write_text(dockerfile)

    assert dockerfile_base_image(path) == base


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
