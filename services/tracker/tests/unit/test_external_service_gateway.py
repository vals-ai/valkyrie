from collections.abc import Callable

import httpx
import pytest

from tracker import config as tracker_config
from tracker.external_service_gateway import (
    AccountingSnapshot,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
    GenerationAccountingSummary,
    combine_accounting,
)


@pytest.mark.description("A zero, negative, or infinite credit cap raises `ValueError`.")
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1"])
def test_credit_cap_must_be_positive_and_finite(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("TEST_CREDIT_CAP", value)
    with pytest.raises(ValueError, match="positive finite"):
        tracker_config._positive_float_setting("TEST_CREDIT_CAP")  # pyright: ignore[reportPrivateUsage]


def _snapshot(overhead_ms: int = 0, revision: int = 0, accounting_id: str = "digest") -> AccountingSnapshot:
    return AccountingSnapshot(
        accounting_id=accounting_id, cumulative_neutral_overhead_ms=overhead_ms, revision=revision
    )


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[ExternalServiceGatewayClient, httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ExternalServiceGatewayClient("http://gateway.test", "tracker-control", client=http_client), http_client


@pytest.mark.description(
    "An active generation interval reports the same deadline at every observation time, so cutoffs derived from it match exactly."
)
def test_active_deadline_does_not_depend_on_observation_time() -> None:
    controller = ExternalServiceDeadlineController(base_allowance_seconds=1.0)
    controller.begin_generation(4095.1949631)
    assert controller.deadline(4095.1949631) == controller.deadline(4095.3280151000004) == 4095.1949631 + 1.0


@pytest.mark.description("A successful counter digest read extends the Tracker's deadline.")
async def test_authenticated_counter_read_extends_tracker_deadline() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["X-SSP-Control-Token"] == "tracker-control"
        return httpx.Response(200, json=_snapshot(1200, 7).model_dump())

    client, http_client = _client(handler)
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=0.0)
        assert controller.deadline(0.0) == 10.0
        await controller.refresh()
        assert controller.deadline(0.0) == 11.2
        assert controller.summary().external_service_credit_applied_seconds == 1.2
    finally:
        await http_client.aclose()
    assert [(request.method, request.url.path) for request in requests] == [("GET", "/counters/digest")]


@pytest.mark.description("A counter read raises storage errors instead of reporting zero credit.")
async def test_counter_read_propagates_storage_errors_instead_of_zero_credit() -> None:
    client, http_client = _client(lambda _request: httpx.Response(503))
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        with pytest.raises(httpx.HTTPStatusError):
            await controller.refresh()
        assert controller.effective_allowance_seconds() == 10.0
    finally:
        await http_client.aclose()


@pytest.mark.description("Active intervals leave out idle gaps and keep credit across rounds.")
async def test_active_intervals_exclude_idle_gaps_and_preserve_credit_across_rounds() -> None:
    controller = ExternalServiceDeadlineController(
        base_allowance_seconds=30.0, credit_cap_seconds=10.0, snapshot=_snapshot(12_500, 4)
    )
    controller.begin_generation(now=105.0)
    assert controller.deadline(115.0) == 145.0
    controller.end_generation(now=117.0)
    assert controller.elapsed_seconds(500.0) == 12.0
    controller.begin_generation(now=300.0)
    assert controller.deadline(305.0) == 328.0
    controller.end_generation(now=310.0)
    assert controller.effective_allowance_seconds() == 40.0
    assert controller.summary() == GenerationAccountingSummary(
        accounting_id="digest",
        base_generation_allowance_seconds=30.0,
        cumulative_time_credit_cap_seconds=10.0,
        external_service_overhead_seconds=12.5,
        external_service_credit_applied_seconds=10.0,
        external_service_credit_revision=4,
        generation_elapsed_seconds=22.0,
    )


@pytest.mark.description("A counter read after the cutoff cannot change saved credit or observations.")
async def test_late_counter_after_cutoff_cannot_change_persisted_credit_or_observation() -> None:
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json=_snapshot(4000 if requests == 1 else 8000, requests).model_dump())

    client, http_client = _client(handler)
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=10.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=0.0)
        await controller.refresh()
        assert controller.deadline(12.0) == 14.0
        controller.freeze_credit()
        await controller.finalize_after_confirmed_stop(15.0)
        assert requests == 1
        assert controller.summary().external_service_credit_applied_seconds == 4.0
        assert controller.summary().external_service_overhead_seconds == 4.0
        assert controller.summary().external_service_credit_revision == 1
        assert controller.summary().generation_elapsed_seconds == 15.0
    finally:
        await http_client.aclose()


@pytest.mark.description("A final read applies credit, and an unlimited clock stays unbounded.")
async def test_natural_terminal_read_applies_credit_and_unlimited_clock_remains_unbounded() -> None:
    client, http_client = _client(lambda _request: httpx.Response(200, json=_snapshot(10_000, 3).model_dump()))
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=None, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=5.0)
        assert controller.deadline(10.0) is None
        await controller.finalize_after_confirmed_stop(17.0)
        summary = controller.summary()
        assert summary.external_service_credit_applied_seconds == 5.0
        assert controller.effective_allowance_seconds() is None
        assert summary.generation_elapsed_seconds == 12.0
    finally:
        await http_client.aclose()


@pytest.mark.description("A counter digest for a different token is ignored.")
async def test_counter_for_another_token_is_rejected() -> None:
    client, http_client = _client(
        lambda _request: httpx.Response(200, json=_snapshot(accounting_id="other").model_dump())
    )
    try:
        controller = ExternalServiceDeadlineController(base_allowance_seconds=10.0, client=client, snapshot=_snapshot())
        with pytest.raises(ValueError, match="different accounting ID"):
            await controller.refresh()
    finally:
        await http_client.aclose()


@pytest.mark.description("Credit counts only the overhead after the baseline.")
def test_credit_counts_only_the_overhead_after_the_baseline() -> None:
    controller = ExternalServiceDeadlineController(
        base_allowance_seconds=10.0, credit_cap_seconds=5.0, snapshot=_snapshot(4000), baseline_overhead_ms=3000
    )

    assert controller.applied_credit_seconds() == 1.0
    assert controller.summary().external_service_overhead_seconds == 1.0
    assert controller.effective_allowance_seconds() == 11.0
    behind = ExternalServiceDeadlineController(
        base_allowance_seconds=10.0, snapshot=_snapshot(2000), baseline_overhead_ms=3000
    )
    assert behind.applied_credit_seconds() == 0.0


@pytest.mark.description("Combined summaries add up and keep the latest counter.")
def test_combined_summaries_add_up_and_keep_the_latest_counter() -> None:
    first = GenerationAccountingSummary("digest", 10.0, 60.0, 2.0, 2.0, 1, 4.0)
    second = GenerationAccountingSummary("digest", 20.0, 60.0, 3.0, 3.0, 2, 6.0)
    ungated = GenerationAccountingSummary(None, 5.0, None, None, None, None, 1.0)
    unlimited = GenerationAccountingSummary(None, None, None, None, None, None, 1.0)

    assert combine_accounting([first, second]) == GenerationAccountingSummary("digest", 30.0, 120.0, 5.0, 5.0, 2, 10.0)
    assert combine_accounting([ungated, ungated]) == GenerationAccountingSummary(
        None, 10.0, None, None, None, None, 2.0
    )
    assert combine_accounting([unlimited, first]).base_generation_allowance_seconds is None
    assert combine_accounting([first, unlimited]).base_generation_allowance_seconds is None
