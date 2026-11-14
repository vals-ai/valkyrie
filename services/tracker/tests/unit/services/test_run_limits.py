import json
from pathlib import Path
from threading import Event

import pytest
import yaml
from botocore.exceptions import EndpointConnectionError

import tracker.services.runs.limits as run_limits_module
from tracker.services.runs.limits import RUN_LIMITS_REFRESH_SECONDS, OrgRunLimits, OrgRunLimitsCache


@pytest.mark.description(
    "An exempt org has no limit, an override replaces the default, and other orgs get the default."
)
def test_org_run_limits_resolve_by_org_name() -> None:
    limits = OrgRunLimits(default=10, exempt=frozenset({"vals.ai"}), overrides={"big-co": 25, "paused-co": 0})

    assert limits.for_org("vals.ai") is None
    assert limits.for_org("big-co") == 25
    assert limits.for_org("paused-co") == 0
    assert limits.for_org("other-co") == 10
    assert OrgRunLimits().for_org("other-co") is None


@pytest.mark.description("The limits file that deploys write to SSM is valid and caps orgs at 10 by default.")
def test_deployed_org_run_limits_file_is_valid() -> None:
    limits_file = Path(__file__).resolve().parents[5] / "infra" / "org_run_limits.yaml"
    limits = OrgRunLimits.model_validate(yaml.safe_load(limits_file.read_text(encoding="utf-8")))

    assert limits.for_org("acme") == 10
    assert limits.for_org("vals.ai") is None
    assert limits.for_org("vals-smith-research") is None
    assert limits.for_org("smith-research") is None


@pytest.mark.description("An org whose whole name matches an exempt pattern has no limit.")
def test_org_run_limits_exempt_patterns_match_whole_name() -> None:
    limits = OrgRunLimits.model_validate_json(json.dumps({"default": 10, "exempt_patterns": ["vals-smith-.*"]}))

    assert limits.for_org("vals-smith-research") is None
    assert limits.for_org("vals-smith-prod-gh-52346350") is None
    assert limits.for_org("acme-vals-smith-dev") == 10


class _Parameter:
    def __init__(self, value: str | None) -> None:
        self.value = value
        self.error: Exception | None = None
        self.reads = 0

    def read(self) -> str | None:
        self.reads += 1
        if self.error is not None:
            raise self.error
        return self.value


@pytest.mark.description("A missing parameter means no limit, and a new value applies only after the next refresh.")
def test_run_limits_cache_applies_value_on_refresh() -> None:
    parameter = _Parameter(None)
    cache = OrgRunLimitsCache(read=parameter.read)

    cache.refresh()
    assert cache.current().for_org("other-co") is None
    parameter.value = json.dumps({"default": 10})
    assert cache.current().for_org("other-co") is None
    cache.refresh()
    assert cache.current().for_org("other-co") == 10
    assert parameter.reads == 2


@pytest.mark.description("A read failure or invalid parameter keeps the last good limits.")
@pytest.mark.parametrize("failure", ["read", "invalid", "bad pattern"])
def test_run_limits_cache_keeps_last_good_limits(failure: str) -> None:
    parameter = _Parameter(json.dumps({"default": 10}))
    cache = OrgRunLimitsCache(read=parameter.read)
    cache.refresh()
    assert cache.current().for_org("other-co") == 10

    if failure == "read":
        parameter.error = EndpointConnectionError(endpoint_url="https://ssm.us-east-1.amazonaws.com")
    elif failure == "invalid":
        parameter.value = json.dumps({"default": -1})
    else:
        parameter.value = json.dumps({"default": 1, "exempt_patterns": ["vals-smith-("]})
    cache.refresh()

    assert cache.current().for_org("other-co") == 10


@pytest.mark.description("The refresh loop rereads the parameter after each interval until stopped.")
def test_run_limits_refresh_loop_rereads_until_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    parameter = _Parameter(json.dumps({"default": 3}))
    cache = OrgRunLimitsCache(read=parameter.read)
    monkeypatch.setattr(run_limits_module, "run_limits", cache)
    stop_event = Event()
    waits: list[float] = []

    def wait(seconds: float) -> bool:
        waits.append(seconds)
        if len(waits) == 2:
            stop_event.set()
        return stop_event.is_set()

    monkeypatch.setattr(stop_event, "wait", wait)
    run_limits_module.run_limits_refresh_loop(stop_event, interval_seconds=RUN_LIMITS_REFRESH_SECONDS)

    assert (parameter.reads, waits) == (1, [RUN_LIMITS_REFRESH_SECONDS] * 2)
    assert cache.current().for_org("other-co") == 3


@pytest.mark.description("Starting the refresh loads the limits before the Tracker serves its first request.")
def test_automatic_refresh_start_loads_limits_first(monkeypatch: pytest.MonkeyPatch) -> None:
    parameter = _Parameter(json.dumps({"default": 3}))
    cache = OrgRunLimitsCache(read=parameter.read)
    monkeypatch.setattr(run_limits_module, "run_limits", cache)
    refresh = run_limits_module.AutomaticRunLimitsRefresh(interval_seconds=3600)

    refresh.start()
    try:
        assert cache.current().for_org("other-co") == 3
    finally:
        refresh.stop()
