from __future__ import annotations

import asyncio
import math
import traceback
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar

import click

from valkyrie.display import terminal_safe

if TYPE_CHECKING:
    from valkyrie.sdk import SessionAuth, ValkyrieClient

Result = TypeVar("Result")


async def retry_unavailable(operation: Callable[[], Awaitable[Result]]) -> Result:
    from valkyrie.sdk import ValkyrieAPIError

    try:
        return await operation()
    except ValkyrieAPIError as error:
        if error.status_code != 503:
            raise
        try:
            delay = float(error.headers["Retry-After"])
        except (KeyError, ValueError):
            raise error
        if not math.isfinite(delay) or delay < 0:
            raise
        await asyncio.sleep(min(delay, 30))
    return await operation()


def call(operation: Callable[[ValkyrieClient], Awaitable[Result]], auth: SessionAuth | None = None) -> Result:
    from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieConfig, ValkyrieSDKError

    async def run() -> Result:
        async with ValkyrieClient(auth=auth) as client:
            return await operation(client)

    from valkyrie.cli.display import Failure, command
    from valkyrie.sdk import ValkyrieTransportError

    try:
        return asyncio.run(run())
    except ValkyrieAPIError as error:
        if error.status_code == 401:
            raise Failure(
                _unauthorized(auth is not None or ValkyrieConfig().http_auth() is not None), hint=command("login")
            ) from error
        message = terminal_safe(str(_describe(error.detail)), preserve_newlines=True)
        ctx = click.get_current_context(silent=True)
        verbose = ctx is not None and ctx.find_root().params.get("verbose")
        raise Failure(
            message,
            detail=f"The Tracker answered {error.status_code}." if verbose else None,
            hint=_hint(error.status_code, message),
        ) from error
    except ValkyrieTransportError as error:
        raise Failure(
            "Could not reach the Tracker.",
            detail=terminal_safe(
                f"No answer from {ValkyrieConfig().url}: {error.__cause__ or error}", preserve_newlines=True
            ),
            hint=command("whoami"),
        ) from error
    except (ValkyrieSDKError, ValueError, OSError) as error:
        if isinstance(error, ValueError) and not _rejected_by_sdk(error):
            raise
        raise Failure(terminal_safe(str(error), preserve_newlines=True)) from error


def _hint(status_code: int, message: str) -> str | None:
    from valkyrie.cli.display import command, subcommand_path

    ctx = click.get_current_context(silent=True)
    if (
        status_code == 404
        and ctx is not None
        and isinstance(group := ctx.parent and ctx.parent.command, click.Group)
        and ctx.command.name != "list"
        and (listing := group.get_command(ctx, "list")) is not None
        and not any(param.required for param in listing.params)
    ):
        return command(*subcommand_path(ctx.parent), "list")
    if status_code in {400, 422}:
        if "provider" in message.lower():
            return command("provider", "list")
        return command(*subcommand_path(ctx), "--help")
    return None


def _rejected_by_sdk(error: ValueError) -> bool:
    from valkyrie.sdk import ValkyrieSDKError

    return isinstance(error, ValkyrieSDKError) or any(
        frame.f_globals.get("__name__", "").startswith("valkyrie.sdk")
        for frame, _ in traceback.walk_tb(error.__traceback__)
    )


def _unauthorized(has_credentials: bool) -> str:
    return "The Tracker rejected your credentials." if has_credentials else "You are not logged in."


def _describe(detail: Any) -> Any:
    if isinstance(detail, list):
        return "; ".join(f"{error['loc'][-1]}: {error['msg']}" for error in detail)
    return detail
