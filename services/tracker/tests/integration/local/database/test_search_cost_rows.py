from decimal import Decimal
from uuid import uuid4

import pytest
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tracker.database.models import BenchmarkStatus, Org, RunCost
from tracker.repositories.cost import CostRepository
from tracker.search_cost import SearchCharge, search_cost_source_id


@pytest.mark.description(
    "Unidentified search charges from two steps of one attempt are saved as two rows, and a repeated report adds none."
)
def test_each_step_of_an_attempt_keeps_its_own_search_charge(postgres_session: Session) -> None:
    org = Org(id=uuid4(), name="search-cost-steps")
    postgres_session.add(org)
    postgres_session.commit()
    run = make_benchmark(status=BenchmarkStatus.FINISHED, org_id=org.id, session=postgres_session)
    repository = CostRepository(postgres_session)
    charges = [SearchCharge("exa", Decimal("0.007"))]

    saved = [
        repository.add_search_costs(
            benchmark_id=run.id, attempt_id=search_cost_source_id("task:attempt", step), charges=charges
        )
        for step in ("one", "two")
    ]
    postgres_session.commit()
    repeated = repository.add_search_costs(
        benchmark_id=run.id, attempt_id=search_cost_source_id("task:attempt", "one"), charges=charges
    )

    rows = postgres_session.exec(select(RunCost.source_id).where(RunCost.benchmark_id == run.id)).all()
    assert saved == [1, 1]
    assert repeated == 0
    assert sorted(rows) == ["task:attempt:one", "task:attempt:two"]
