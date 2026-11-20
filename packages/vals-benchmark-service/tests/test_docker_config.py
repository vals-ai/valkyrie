"""Docker configuration and command cleanup checks; run pytest tests/test_docker_config.py."""

import pytest

from benchmark_service import DockerProviderConfig

from vals_benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


def test_docker_grading_selects_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve Docker when selected for sandbox grading."""
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "docker")
    assert isinstance(_grading_provider_config(), DockerProviderConfig)
