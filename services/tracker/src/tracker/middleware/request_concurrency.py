import asyncio
from typing import TYPE_CHECKING

from tracker.database.session import SERVER_THREADS

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

UNLIMITED_PATHS = frozenset({"/health"})


class RequestConcurrencyLimitMiddleware:
    def __init__(self, app: "ASGIApp", limit: int = SERVER_THREADS):
        self.app = app
        self.slots = asyncio.Semaphore(limit)

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http" or scope["path"] in UNLIMITED_PATHS:
            await self.app(scope, receive, send)
            return
        await self.slots.acquire()
        held = True

        async def send_releasing_at_response_start(message: "Message") -> None:
            nonlocal held
            if held and message["type"] == "http.response.start":
                held = False
                self.slots.release()
            await send(message)

        try:
            await self.app(scope, receive, send_releasing_at_response_start)
        finally:
            if held:
                self.slots.release()
