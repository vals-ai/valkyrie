from uuid import uuid4

import pytest
from fastapi import HTTPException

from tests.factories import make_benchmark
from tracker.database.models import Org
from tracker.database.scoping import assert_org


def _make_org(name: str = "test-org") -> Org:
    return Org(id=uuid4(), name=name)


@pytest.mark.description("Organization ownership checks for database rows.")
class TestAssertOrg:
    @pytest.mark.description("`assert_org` returns the row when its org matches.")
    def test_assert_org_returns_row_for_matching_org(self) -> None:
        org = _make_org()
        bench = make_benchmark(org_id=org.id)
        result = assert_org(bench, org)
        assert result is bench

    @pytest.mark.description("`assert_org` raises 404 for a missing row.")
    def test_assert_org_raises_404_for_none(self) -> None:
        org = _make_org()
        with pytest.raises(HTTPException) as exc_info:
            assert_org(None, org)
        assert exc_info.value.status_code == 404

    @pytest.mark.description("`assert_org` raises 404 for a row in another org.")
    def test_assert_org_raises_404_for_wrong_org(self) -> None:
        org_a = _make_org("org-a")
        org_b = _make_org("org-b")
        bench = make_benchmark(org_id=org_a.id)
        with pytest.raises(HTTPException) as exc_info:
            assert_org(bench, org_b)
        assert exc_info.value.status_code == 404
