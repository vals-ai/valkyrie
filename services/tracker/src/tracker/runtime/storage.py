from collections.abc import AsyncIterable, AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class StoredObject:
    key: str
    last_modified: datetime | None = None
    size: int = field(kw_only=True)


@dataclass(frozen=True)
class StoredObjectCopy:
    deletion_token: str | None


class ObjectStore(Protocol):
    async def put_bytes(self, key: str, content: bytes) -> None:
        raise NotImplementedError  # pragma: no cover

    async def put_stream(
        self,
        key: str,
        chunks: AsyncIterable[bytes],
        *,
        should_continue: Callable[[], bool] | None = None,
        overwrite: bool = True,
    ) -> int:
        raise NotImplementedError  # pragma: no cover

    async def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError  # pragma: no cover

    def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        raise NotImplementedError  # pragma: no cover

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        raise NotImplementedError  # pragma: no cover

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        raise NotImplementedError  # pragma: no cover

    async def exists(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        raise NotImplementedError  # pragma: no cover

    async def stat(self, key: str) -> StoredObject:
        raise NotImplementedError  # pragma: no cover

    async def list_objects_page(
        self, prefix: str, *, cursor: str | None, limit: int
    ) -> tuple[list[StoredObject], str | None]:
        raise NotImplementedError  # pragma: no cover

    async def temporary_download_url(self, key: str, *, expires_in: int) -> str | None:
        raise NotImplementedError  # pragma: no cover


class ArtifactLocations(Protocol):
    def object_location(self, key: str) -> str:
        raise NotImplementedError  # pragma: no cover

    def prefix_location(self, prefix: str) -> str:
        raise NotImplementedError  # pragma: no cover
