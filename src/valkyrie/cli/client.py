import asyncio
import traceback
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import click
from valkyrie.sdk import SessionAuth, ValkyrieAPIError, ValkyrieClient, ValkyrieConfig, ValkyrieSDKError

from valkyrie.display import terminal_safe

Result = TypeVar("Result")


def call(operation: Callable[[ValkyrieClient], Awaitable[Result]], auth: SessionAuth | None = None) -> Result:

    async def run() -> Result:
        async with ValkyrieClient(auth=auth) as client:
            return await operation(client)

    try:
        return asyncio.run(run())
    except ValkyrieAPIError as error:
        if error.status_code == 401:
            raise click.ClickException(
                _unauthorized(auth is not None or ValkyrieConfig().http_auth() is not None)
            ) from error
        message = f"{_describe(error.detail)} ({error.status_code})" if isinstance(error.detail, list) else str(error)
        raise click.ClickException(terminal_safe(message, preserve_newlines=True)) from error
    except (ValkyrieSDKError, ValueError, OSError) as error:
        if isinstance(error, ValueError) and not _rejected_by_sdk(error):
            raise
        raise click.ClickException(terminal_safe(str(error), preserve_newlines=True)) from error


def _rejected_by_sdk(error: ValueError) -> bool:
    return isinstance(error, ValkyrieSDKError) or any(
        frame.f_globals.get("__name__", "").startswith("valkyrie.sdk")
        for frame, _ in traceback.walk_tb(error.__traceback__)
    )


def _unauthorized(has_credentials: bool) -> str:
    problem = "The Tracker rejected your credentials." if has_credentials else "You are not logged in."
    return f"{problem} Run `valkyrie login`."


def _describe(detail: Any) -> Any:
    if isinstance(detail, list):
        return "; ".join(f"{error['loc'][-1]}: {error['msg']}" for error in detail)
    return detail
