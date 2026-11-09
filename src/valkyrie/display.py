from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from valkyrie.sdk.models import FailureCategory


def terminal_safe(value: str, *, preserve_newlines: bool) -> str:
    escaped: list[str] = []
    for character in value:
        if character == "\n" and preserve_newlines:
            escaped.append(character)
        elif character == "\t":
            escaped.append("    ")
        elif character.isprintable():
            escaped.append(character)
        else:
            escaped.append(character.encode("unicode_escape").decode("ascii"))

    return "".join(escaped)


def group_task_errors(
    task_errors: Mapping[str, str],
    task_failure_categories: Mapping[str, FailureCategory] | None = None,
) -> list[tuple[FailureCategory | None, str, tuple[str, ...]]]:
    categories = task_failure_categories or {}
    grouped: defaultdict[tuple[FailureCategory | None, str], list[str]] = defaultdict(list)
    for task_id, message in task_errors.items():
        grouped[(categories.get(task_id), message)].append(task_id)

    groups = [(category, message, tuple(sorted(task_ids))) for (category, message), task_ids in grouped.items()]
    return sorted(groups, key=lambda group: (-len(group[2]), group[2][0]))
