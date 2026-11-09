"""Async client for the Valkyrie API."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, BinaryIO, TypeVar

import httpx
from pydantic import BaseModel

from valkyrie.sdk._transfer import download
from valkyrie.sdk.config import ValkyrieConfig
from valkyrie.sdk.errors import ValkyrieAPIError, ValkyrieTransportError
from valkyrie.sdk.resources.account import AccountResource
from valkyrie.sdk.resources.agents import AgentsResource
from valkyrie.sdk.resources.artifacts import ArtifactsResource
from valkyrie.sdk.resources.benchmarks import BenchmarksResource
from valkyrie.sdk.resources.datasets import BenchmarkDefinitionsResource, DatasetsResource
from valkyrie.sdk.resources.logs import LogsResource
from valkyrie.sdk.resources.projects import ProjectsResource
from valkyrie.sdk.resources.runs import RunsResource
from valkyrie.sdk.resources.scheduler import SchedulerResource
from valkyrie.sdk.resources.secrets import SecretsResource
from valkyrie.sdk.resources.settings import ProvidersResource, SettingsResource
from valkyrie.sdk.resources.services import BenchmarkServicesResource

ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class ValkyrieClient:
    """Async client for Valkyrie API resources; without a config it uses `ValkyrieConfig()`.

    `auth` replaces the credential the config picks, for example `SessionAuth` to act as a saved login while
    `VALKYRIE_API_KEY` is set.
    """

    def __init__(
        self,
        config: ValkyrieConfig | None = None,
        *,
        timeout: float | httpx.Timeout = 120,
        transport: httpx.AsyncBaseTransport | None = None,
        auth: httpx.Auth | None = None,
    ) -> None:
        self.config = config or ValkyrieConfig()
        self._timeout = timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout)
        self._client = httpx.AsyncClient(
            base_url=self.config.url,
            timeout=self._timeout,
            auth=auth or self.config.http_auth(),
            transport=transport,
        )
        self.runs = RunsResource(self)
        self.logs = LogsResource(self)
        self.benchmarks = BenchmarksResource(self)
        self.agents = AgentsResource(self)
        self.artifacts = ArtifactsResource(self)
        self.services = BenchmarkServicesResource(self)
        self.scheduler = SchedulerResource(self)
        self.secrets = SecretsResource(self)
        self.providers = ProvidersResource(self)
        self.settings = SettingsResource(self)
        self.projects = ProjectsResource(self)
        self.benchmark_definitions = BenchmarkDefinitionsResource(self)
        self.datasets = DatasetsResource(self)
        self.account = AccountResource(self)

    async def __aenter__(self) -> ValkyrieClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    async def request_model(
        self,
        method: str,
        path: str,
        model: type[ResponseModel],
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content: AsyncIterable[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ResponseModel:
        """Send a request and validate its response as a Pydantic model."""
        response = await self._send(method, path, params=params, json=json, content=content, headers=headers)
        return model.model_validate(response.json())

    async def request_no_content(self, method: str, path: str, *, json: Any = None) -> None:
        """Send a request whose success response has no body."""
        await self._send(method, path, json=json)

    async def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content: AsyncIterable[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            response = await self._client.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise ValkyrieTransportError(f"Valkyrie request failed: {exc}") from exc

        self.raise_for_status(response)
        return response

    @staticmethod
    def raise_for_status(response: httpx.Response) -> None:
        """Convert a non-success response into an SDK API error."""
        if not response.is_success:
            raise ValkyrieAPIError.from_response(response)

    def download_auth(self, url: str) -> httpx.Auth | None:
        """Authenticate downloads only on this client's Tracker origin."""
        target = httpx.URL(url)
        tracker = self._client.base_url
        if target.scheme in {"http", "https"} and (target.scheme, target.host, target.port) == (
            tracker.scheme,
            tracker.host,
            tracker.port,
        ):
            return self._client.auth
        return None

    async def download(
        self, url: str, output: BinaryIO, *, max_bytes: float = float("inf"), too_large: str = ""
    ) -> int:
        """Stream a download URL into `output` through this client's transport and timeouts.

        Tracker credentials are sent only to the Tracker origin. Returns the number of bytes written and raises
        ValueError with `too_large` once more than `max_bytes` arrive.
        """
        return await download(
            self._client, url, output, auth=self.download_auth(url), max_bytes=max_bytes, too_large=too_large
        )

    @asynccontextmanager
    async def stream_response(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> AsyncGenerator[httpx.Response]:
        """Open a streaming response without a read timeout.

        Raises ValkyrieAPIError for an error status, and ValkyrieTransportError when the connection
        fails while the response is open, including mid-body.
        """
        timeout = httpx.Timeout(
            connect=self._timeout.connect,
            read=None,
            write=self._timeout.write,
            pool=self._timeout.pool,
        )
        try:
            async with self._client.stream(method, path, params=params, json=json, timeout=timeout) as response:
                if not response.is_success:
                    await response.aread()
                    self.raise_for_status(response)
                yield response
        except httpx.HTTPError as exc:
            raise ValkyrieTransportError(f"Valkyrie stream failed: {exc}") from exc
