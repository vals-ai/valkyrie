from decimal import Decimal

import pytest

from tracker.search_cost import SearchCharge, attempt_totals, parse_search_costs


@pytest.mark.description("Each valid report line becomes a charge, and each invalid line is counted instead of priced.")
def test_parse_keeps_valid_lines_and_counts_invalid_ones() -> None:
    content = "\n".join(
        [
            '{"provider": "tavily", "amount_usd": "0.016", "source_id": "req-1"}',
            '{"provider": "exa", "amount_usd": 0.007}',
            "",
            '{"provider": "exa", "amount_usd": "-1"}',
            '{"provider": "exa", "amount_usd": "NaN"}',
            '{"provider": "exa", "amount_usd": "1e400"}',
            '{"provider": "exa", "amount_usd": "100.01"}',
            '{"provider": "exa"}',
            '{"provider": "Tavily", "amount_usd": "1"}',
            '{"provider": "exa", "amount_usd": "1", "model": "x"}',
            "not json",
            "[" * 100_000 + "]" * 100_000,
        ]
    )

    charges, invalid = parse_search_costs(content)

    assert charges == [
        SearchCharge("tavily", Decimal("0.016"), "req-1"),
        SearchCharge("exa", Decimal("0.007")),
    ]
    assert invalid == 9


@pytest.mark.description(
    "Charges without a request id are summed per provider under the attempt id, and a repeated id counts once."
)
def test_attempt_totals_groups_unidentified_charges_by_attempt() -> None:
    charges = [
        SearchCharge("exa", Decimal("0.007")),
        SearchCharge("tavily", Decimal("0.016"), "req-1"),
        SearchCharge("exa", Decimal("0.005")),
        SearchCharge("tavily", Decimal("0.016"), "req-1"),
        SearchCharge("tavily", Decimal("0.008")),
    ]

    assert attempt_totals(charges, "task:attempt") == [
        SearchCharge("exa", Decimal("0.012"), "task:attempt"),
        SearchCharge("tavily", Decimal("0.016"), "req-1"),
        SearchCharge("tavily", Decimal("0.008"), "task:attempt"),
    ]
