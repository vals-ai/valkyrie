"""Tracker-backed benchmarks an organization owns and the datasets that belong to them."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from valkyrie.sdk._transfer import file_chunks
from valkyrie.sdk.errors import ValkyrieInputError, ValkyrieStreamError, ValkyrieTransportError
from valkyrie.sdk.models.datasets import (
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    DatasetResponse,
    DatasetsResponse,
    DatasetTaskDownloadResponse,
    DatasetTaskResponse,
    DatasetTasksResponse,
    validate_name,
    validate_task_id,
)

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


def _valid(name: str, kind: str) -> str:
    try:
        return validate_name(name)
    except ValueError as error:
        raise ValkyrieInputError(f"Invalid {kind} name: {error}") from error


def _valid_task_id(task_id: str) -> str:
    try:
        return validate_task_id(task_id)
    except ValueError as error:
        raise ValkyrieInputError(f"Invalid task ID: {error}") from error


def _archived_params(include_archived: bool) -> dict[str, bool] | None:
    return {"include_archived": True} if include_archived else None


def _benchmark_path(benchmark: str) -> str:
    return f"/v1/benchmarks/{_valid(benchmark, 'benchmark')}"


def _dataset_path(benchmark: str, name: str) -> str:
    return f"{_benchmark_path(benchmark)}/datasets/{_valid(name, 'dataset')}"


class BenchmarkDefinitionsResource:
    """Create and manage the benchmarks your organization owns in Valkyrie."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def create(self, name: str, *, description: str = "") -> BenchmarkDefinitionResponse:
        """Create a benchmark named with up to 64 lowercase letters and digits joined by single `.`, `_`, or `-`.

        An archived benchmark keeps its name, so restore it instead of creating it again.
        """
        return await self._sdk.request_model(
            "POST",
            "/v1/benchmarks",
            BenchmarkDefinitionResponse,
            json={"name": _valid(name, "benchmark"), "description": description},
        )

    async def list(self, *, include_archived: bool = False) -> BenchmarkDefinitionsResponse:
        """List your organization's benchmarks, adding archived ones when include_archived is true."""
        return await self._sdk.request_model(
            "GET", "/v1/benchmarks", BenchmarkDefinitionsResponse, params=_archived_params(include_archived)
        )

    async def get(self, name: str) -> BenchmarkDefinitionResponse:
        """Fetch one benchmark by name."""
        return await self._sdk.request_model("GET", _benchmark_path(name), BenchmarkDefinitionResponse)

    async def update(self, name: str, *, description: str) -> BenchmarkDefinitionResponse:
        """Replace a benchmark's description."""
        return await self._sdk.request_model(
            "PATCH", _benchmark_path(name), BenchmarkDefinitionResponse, json={"description": description}
        )

    async def remove(self, name: str) -> BenchmarkDefinitionResponse:
        """Archive a benchmark once all its datasets are archived; restore brings it back."""
        return await self._sdk.request_model("DELETE", _benchmark_path(name), BenchmarkDefinitionResponse)

    async def restore(self, name: str) -> BenchmarkDefinitionResponse:
        """Restore an archived benchmark."""
        return await self._sdk.request_model("POST", f"{_benchmark_path(name)}/restore", BenchmarkDefinitionResponse)


def _task_path(benchmark: str, dataset: str, task_id: str) -> str:
    return f"{_dataset_path(benchmark, dataset)}/tasks/{_valid_task_id(task_id)}"


def _publish_new(partial: Path, target: Path) -> None:
    try:
        os.link(partial, target)
    except FileExistsError:
        raise
    except OSError:
        with target.open("xb"):
            pass
        try:
            os.replace(partial, target)
        except OSError:
            target.unlink()
            raise


class DatasetTasksResource:
    """Upload, list, download, and remove the task archives in a dataset."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def put(self, benchmark: str, dataset: str, task_id: str, archive: str | Path) -> DatasetTaskResponse:
        """Upload a zip archive as a task, replacing any task with the same ID."""
        path = _task_path(benchmark, dataset, task_id)
        source = Path(archive)
        size = (await asyncio.to_thread(source.stat)).st_size
        stream = await asyncio.to_thread(source.open, "rb")
        try:
            return await self._sdk.request_model(
                "PUT",
                path,
                DatasetTaskResponse,
                content=file_chunks(stream),
                headers={"Content-Type": "application/zip", "Content-Length": str(size)},
            )
        finally:
            await asyncio.to_thread(stream.close)

    async def list(
        self, benchmark: str, dataset: str, *, cursor: str | None = None, limit: int = 100
    ) -> DatasetTasksResponse:
        """Fetch one page of tasks; pass next_cursor back as cursor until it is null."""
        params: dict[str, str | int] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        return await self._sdk.request_model(
            "GET", f"{_dataset_path(benchmark, dataset)}/tasks", DatasetTasksResponse, params=params
        )

    async def list_all(self, benchmark: str, dataset: str) -> DatasetTasksResponse:
        """Fetch every task in a dataset by following next_cursor."""
        response = await self.list(benchmark, dataset, limit=1000)
        tasks = list(response.tasks)
        cursors: set[str] = set()
        while response.next_cursor is not None:
            if response.next_cursor in cursors:
                raise ValkyrieStreamError("Tracker returned a repeated dataset task cursor")
            cursors.add(response.next_cursor)
            response = await self.list(benchmark, dataset, cursor=response.next_cursor, limit=1000)
            tasks.extend(response.tasks)
        return DatasetTasksResponse(tasks=tasks)

    async def get(self, benchmark: str, dataset: str, task_id: str) -> DatasetTaskDownloadResponse:
        """Fetch a task's metadata and a short-lived download URL for its archive."""
        return await self._sdk.request_model(
            "GET", _task_path(benchmark, dataset, task_id), DatasetTaskDownloadResponse
        )

    async def download(
        self, benchmark: str, dataset: str, task_id: str, destination: str | Path, *, overwrite: bool = False
    ) -> Path:
        """Download a task archive to destination, refusing to replace an existing file unless overwrite is true."""
        target = Path(destination)
        exists = FileExistsError(f"Target already exists: {target}; use overwrite to replace it")
        if target.exists() and not overwrite:
            raise exists
        task = await self.get(benchmark, dataset, task_id)
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(dir=target.parent) as staging:
                partial = Path(staging) / "download"
                with partial.open("xb") as stream:
                    await self._sdk.download(task.download_url, stream)
                if overwrite:
                    await asyncio.to_thread(os.replace, partial, target)
                else:
                    try:
                        await asyncio.to_thread(_publish_new, partial, target)
                    except FileExistsError as error:
                        raise exists from error
        except httpx.HTTPError as error:
            raise ValkyrieTransportError("Task archive download failed") from error
        return target

    async def remove(self, benchmark: str, dataset: str, task_id: str) -> DatasetTaskResponse:
        """Delete a task from a dataset."""
        return await self._sdk.request_model("DELETE", _task_path(benchmark, dataset, task_id), DatasetTaskResponse)


class DatasetsResource:
    """Create and manage the datasets that belong to a benchmark; tasks manages their contents."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client
        self.tasks = DatasetTasksResource(client)

    async def create(self, benchmark: str, name: str, *, description: str = "") -> DatasetResponse:
        """Create a dataset in a benchmark, named like a benchmark.

        An archived dataset keeps its name, so restore it instead of creating it again.
        """
        return await self._sdk.request_model(
            "POST",
            f"{_benchmark_path(benchmark)}/datasets",
            DatasetResponse,
            json={"name": _valid(name, "dataset"), "description": description},
        )

    async def list(self, benchmark: str, *, include_archived: bool = False) -> DatasetsResponse:
        """List a benchmark's datasets, adding archived ones when include_archived is true."""
        return await self._sdk.request_model(
            "GET",
            f"{_benchmark_path(benchmark)}/datasets",
            DatasetsResponse,
            params=_archived_params(include_archived),
        )

    async def get(self, benchmark: str, name: str) -> DatasetResponse:
        """Fetch one dataset by name."""
        return await self._sdk.request_model("GET", _dataset_path(benchmark, name), DatasetResponse)

    async def update(self, benchmark: str, name: str, *, description: str) -> DatasetResponse:
        """Replace a dataset's description."""
        return await self._sdk.request_model(
            "PATCH", _dataset_path(benchmark, name), DatasetResponse, json={"description": description}
        )

    async def remove(self, benchmark: str, name: str) -> DatasetResponse:
        """Archive a dataset; restore brings it back."""
        return await self._sdk.request_model("DELETE", _dataset_path(benchmark, name), DatasetResponse)

    async def restore(self, benchmark: str, name: str) -> DatasetResponse:
        """Restore an archived dataset."""
        return await self._sdk.request_model("POST", f"{_dataset_path(benchmark, name)}/restore", DatasetResponse)
