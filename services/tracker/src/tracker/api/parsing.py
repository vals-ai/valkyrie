from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def parse_csv(raw: str, convert: Callable[[str], T]) -> list[T]:
    parsed: list[T] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        parsed.append(convert(token))
    return parsed
