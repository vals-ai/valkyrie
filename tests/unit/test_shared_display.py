import pytest
from valkyrie.sdk.models import FailureCategory

from valkyrie.display import group_task_errors


@pytest.mark.description("Task errors are grouped by message.")
@pytest.mark.parametrize(
    ("task_errors", "categories", "groups"),
    [
        (
            {"task-c": "same", "task-b": "different\x1b", "task-a": "same", "task-d": "different\\x1b"},
            None,
            [
                (None, "same", ("task-a", "task-c")),
                (None, "different\x1b", ("task-b",)),
                (None, "different\\x1b", ("task-d",)),
            ],
        ),
        (
            {"task-a": "same", "task-b": "same", "task-c": "same"},
            {"task-a": FailureCategory.INFRASTRUCTURE, "task-b": FailureCategory.INFRASTRUCTURE},
            [(FailureCategory.INFRASTRUCTURE, "same", ("task-a", "task-b")), (None, "same", ("task-c",))],
        ),
    ],
    ids=["raw-messages-largest-first", "split-by-category"],
)
def test_group_task_errors(
    task_errors: dict[str, str],
    categories: dict[str, FailureCategory] | None,
    groups: list[tuple[FailureCategory | None, str, tuple[str, ...]]],
) -> None:
    assert group_task_errors(task_errors, categories) == groups
