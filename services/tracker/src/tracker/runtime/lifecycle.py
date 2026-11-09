from asyncio import CancelledError, Task, shield
from typing import TypeVar

T = TypeVar("T")


async def finish_cleanup(task: Task[T]) -> T:
    cancelled = False
    while not task.done():
        try:
            await shield(task)
        except CancelledError:
            cancelled = True

    result = task.result()
    if cancelled:
        raise CancelledError
    return result
