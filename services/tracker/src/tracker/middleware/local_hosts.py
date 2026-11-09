from typing import TYPE_CHECKING

from starlette.middleware.trustedhost import TrustedHostMiddleware

from tracker.local import config as local_config

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send

LOCAL_ALLOWED_HOSTS = ["127.0.0.1", "localhost"]


class LocalTrustedHostMiddleware:
    def __init__(self, app: "ASGIApp"):
        self.app = app
        self.trusted = TrustedHostMiddleware(app, allowed_hosts=LOCAL_ALLOWED_HOSTS)

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if local_config.resources is None:
            await self.app(scope, receive, send)
            return
        await self.trusted(scope, receive, send)
