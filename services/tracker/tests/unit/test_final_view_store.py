from typing import cast
from uuid import uuid4

import pytest
from tracker.exceptions import S3Error
from tracker.runtime.storage import ObjectStore
from tracker.services.final_views import stored_final_view_key, upload_final_view
from tracker.types import FinalEvaluation, FinalViewResponse


class _MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put_bytes(self, key: str, content: bytes) -> None:
        self.objects[key] = content

    async def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise S3Error(f"missing {key}")
        return self.objects[key]


def _final_view(name: str) -> FinalViewResponse:
    return FinalViewResponse.model_construct(benchmark_id=uuid4(), benchmark_name=name)


@pytest.mark.description("A stored final view matches a recomputed one that differs only in its final evaluation ID.")
async def test_stored_final_view_key_matches_identical_bytes() -> None:
    store = _MemoryStore()
    object_store = cast(ObjectStore, store)
    final_view = _final_view("swebench")

    assert await stored_final_view_key(final_view, object_store) is None

    key = await upload_final_view(final_view, object_store)

    assert await stored_final_view_key(final_view, object_store) == key

    evaluation = FinalEvaluation(org_id=uuid4(), benchmark=final_view.benchmark_id, final_score=0.5)
    scored_view = final_view.model_copy(update={"final_evaluation": evaluation})
    await upload_final_view(scored_view, object_store)
    rescored_view = scored_view.model_copy(update={"final_evaluation": evaluation.model_copy(update={"id": uuid4()})})

    assert await stored_final_view_key(rescored_view, object_store) == key

    flagged_view = scored_view.model_copy(
        update={"final_evaluation": evaluation.model_copy(update={"properties": {"flag": True}})}
    )
    await upload_final_view(flagged_view, object_store)
    counted_view = scored_view.model_copy(
        update={"final_evaluation": evaluation.model_copy(update={"properties": {"flag": 1}})}
    )

    assert await stored_final_view_key(counted_view, object_store) is None

    store.objects[key] = b"{}"

    assert await stored_final_view_key(final_view, object_store) is None
