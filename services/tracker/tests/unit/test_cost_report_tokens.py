from uuid import uuid4

import pytest
from pydantic import ValidationError

from tracker.services.cost_reports import (
    COST_REPORT_TOKEN_HEADER,
    ReportedCost,
    cost_report_token_hash,
    issued_cost_report_tokens,
    with_cost_report_token,
)


@pytest.mark.description(
    "Each run start sends the service a fresh token in place of any the caller supplied, and stores only its hash."
)
def test_run_start_sends_a_fresh_token_and_stores_its_hash() -> None:
    supplied = {"x-valkyrie-cost-report-token": "forged", "X-Customer": "kept"}

    headers = with_cost_report_token(supplied)
    [stored] = issued_cost_report_tokens(run_id := uuid4(), headers)

    token = headers.pop(COST_REPORT_TOKEN_HEADER)
    assert headers == {"X-Customer": "kept"}
    assert token != with_cost_report_token(supplied)[COST_REPORT_TOKEN_HEADER]
    assert (stored.benchmark_id, stored.token_hash) == (run_id, cost_report_token_hash(token))


@pytest.mark.description("A reported cost whose JSON number overflows to infinity fails validation.")
@pytest.mark.parametrize("field", ["quantity", "amount_usd"])
def test_reported_cost_refuses_infinite_numbers(field: str) -> None:
    numbers = {"quantity": 1, "amount_usd": 1, field: "1e309"}
    body = (
        '{"cost_type": "cluster_gpu", "provider": "coreweave", "source_id": "nodes", "attempt_id": "attempt-1", '
        f'"unit": "node_hour", "quantity": {numbers["quantity"]}, "amount_usd": {numbers["amount_usd"]}}}'
    )

    with pytest.raises(ValidationError, match="finite number"):
        ReportedCost.model_validate_json(body)
