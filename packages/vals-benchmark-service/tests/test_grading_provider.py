import pytest

from benchmark_service.sandbox import DaytonaProviderConfig, DaytonaV2ProviderConfig, MissingSandboxConfigError
from vals_benchmark_service.app import _grading_provider_config  # pyright: ignore[reportPrivateUsage]


def test_grading_provider_selects_v2_only_when_opted_in(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("DAYTONA_API_KEY", "DAYTONA_TARGET"):
        monkeypatch.setenv(name, "v0")
    monkeypatch.setenv("DAYTONA_API_URL", "https://app.daytona.io/api")
    monkeypatch.delenv("GRADING_SANDBOX_PROVIDER", raising=False)
    assert isinstance(_grading_provider_config(), DaytonaProviderConfig)

    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "daytona_v2")
    monkeypatch.setenv("DAYTONA_V2_API_KEY", "v2-key")
    monkeypatch.setenv("DAYTONA_V2_REGION", "us")
    monkeypatch.setenv("DAYTONA_V2_SPOT", "true")
    assert _grading_provider_config() == DaytonaV2ProviderConfig(
        DAYTONA_V2_API_KEY="v2-key", DAYTONA_V2_REGION="us", DAYTONA_V2_SPOT=True
    )

    monkeypatch.delenv("DAYTONA_V2_API_KEY")
    with pytest.raises(MissingSandboxConfigError, match="DAYTONA_V2_API_KEY"):
        _grading_provider_config()
