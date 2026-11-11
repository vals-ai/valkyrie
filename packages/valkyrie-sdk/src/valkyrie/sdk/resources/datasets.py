"""Tracker-backed benchmarks an organization owns and the datasets that belong to them."""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

from valkyrie.sdk._transfer import file_chunks
from valkyrie.sdk.errors import (
    ValkyrieAPIError,
    ValkyrieDatasetPushError,
    ValkyrieInputError,
    ValkyrieRunError,
    ValkyrieSDKError,
    ValkyrieTransportError,
)
from valkyrie.sdk.models import AgentContractRequest, StartBenchmarkRequest, StartBenchmarkResponse
from valkyrie.sdk.models.datasets import (
    DIGEST_PATTERN,
    folder_run_name,
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    BlobResponse,
    DatasetLocation,
    DatasetRef,
    DatasetResponse,
    DatasetsResponse,
    DatasetTaskDownloadResponse,
    DatasetVersionDetailResponse,
    DatasetTasksPage,
    DatasetTaskResponse,
    DatasetVersionResponse,
    DatasetVersionsResponse,
    HostedDataset,
    MissingBlobsResponse,
    VersionManifest,
    validate_name,
    validate_tag,
    validate_task_id,
)
from valkyrie.sdk.resources.runs import check_start_options, normalize_contract, post_start
from valkyrie.sdk.dataset_install import download_dataset
from valkyrie.sdk.dataset_manifest import check_digests, check_script, lock_folders, read_manifest, wire_manifest
from valkyrie.sdk.metric_script import metric_digest, read_metric_script
from valkyrie.sdk.task_archive import TaskFolderError, find_task_folders, pack_task, task_problems
from valkyrie.sdk.task_content import left_out, task_digest

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


async def _bytes_chunks(content: bytes) -> AsyncIterator[bytes]:
    yield content


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


def _valid_digest(digest: str) -> str:
    if not DIGEST_PATTERN.fullmatch(digest):
        raise ValueError(f"Invalid digest {digest!r}: write sha256: and 64 lowercase hex characters")
    return digest


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


def _version_path(benchmark: str, dataset: str, ref: str) -> str:
    return f"{_dataset_path(benchmark, dataset)}/versions/{quote(ref, safe=':')}"


class DatasetVersionsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def list(self, benchmark: str, dataset: str) -> DatasetVersionsResponse:
        return await self._sdk.request_model(
            "GET", f"{_dataset_path(benchmark, dataset)}/versions", DatasetVersionsResponse
        )

    async def get(self, benchmark: str, dataset: str, ref: str = "latest") -> DatasetVersionDetailResponse:
        return await self._sdk.request_model(
            "GET", _version_path(benchmark, dataset, ref), DatasetVersionDetailResponse
        )

    async def tasks(self, benchmark: str, dataset: str, ref: str = "latest") -> list[DatasetTaskResponse]:
        """List every task of a dataset version in task ID order, reading one page at a time."""
        tasks: list[DatasetTaskResponse] = []
        params: dict[str, Any] = {}
        while True:
            page = await self._sdk.request_model(
                "GET", f"{_version_path(benchmark, dataset, ref)}/tasks", DatasetTasksPage, params=params
            )
            tasks.extend(page.tasks)
            if page.next_cursor is None:
                return tasks
            params = {"cursor": page.next_cursor}

    async def get_task(
        self, benchmark: str, dataset: str, task_id: str, ref: str = "latest"
    ) -> DatasetTaskDownloadResponse:
        return await self._sdk.request_model(
            "GET",
            f"{_version_path(benchmark, dataset, ref)}/tasks/{_valid_task_id(task_id)}",
            DatasetTaskDownloadResponse,
        )

    async def tag(
        self, benchmark: str, dataset: str, tag: str, ref: str, force: bool = False
    ) -> DatasetVersionResponse:
        return await self._sdk.request_model(
            "PUT",
            f"{_dataset_path(benchmark, dataset)}/tags/{quote(tag, safe='')}",
            DatasetVersionResponse,
            json={"ref": ref, "force": force},
        )

    async def untag(self, benchmark: str, dataset: str, tag: str) -> DatasetVersionResponse:
        return await self._sdk.request_model(
            "DELETE", f"{_dataset_path(benchmark, dataset)}/tags/{quote(tag, safe='')}", DatasetVersionResponse
        )

    async def download(
        self,
        benchmark: str,
        dataset: str,
        task_id: str,
        destination: str | Path,
        *,
        ref: str = "latest",
        overwrite: bool = False,
    ) -> Path:
        """Download a task archive to destination, refusing to replace an existing file unless overwrite is true."""
        target = Path(destination)
        exists = FileExistsError(f"Target already exists: {target}; use overwrite to replace it")
        if target.exists() and not overwrite:
            raise exists
        task = await self.get_task(benchmark, dataset, task_id, ref)
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


class DatasetsResource:
    """Create and manage the datasets that belong to a benchmark; tasks manages their contents."""

    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client
        self.versions = DatasetVersionsResource(client)

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

    async def _missing(self, benchmark: str, dataset: str, digests: Sequence[str]) -> MissingBlobsResponse:
        return await self._sdk.request_model(
            "POST",
            f"{_dataset_path(benchmark, dataset)}/blobs/missing",
            MissingBlobsResponse,
            json={"digests": [_valid_digest(digest) for digest in digests]},
        )

    async def _put_blob(self, benchmark: str, dataset: str, digest: str, archive: str | Path) -> BlobResponse:
        path = f"{_dataset_path(benchmark, dataset)}/blobs/{_valid_digest(digest)}"
        source = Path(archive)
        size = (await asyncio.to_thread(source.stat)).st_size
        stream = await asyncio.to_thread(source.open, "rb")
        try:
            return await self._sdk.request_model(
                "PUT",
                path,
                BlobResponse,
                content=file_chunks(stream),
                headers={"Content-Type": "application/zip", "Content-Length": str(size)},
            )
        finally:
            await asyncio.to_thread(stream.close)

    async def _put_file(self, benchmark: str, dataset: str, digest: str, content: bytes) -> BlobResponse:
        return await self._sdk.request_model(
            "PUT",
            f"{_dataset_path(benchmark, dataset)}/files/{_valid_digest(digest)}",
            BlobResponse,
            content=_bytes_chunks(content),
            headers={"Content-Type": "text/x-python", "Content-Length": str(len(content))},
        )

    async def _publish(
        self,
        benchmark: str,
        dataset: str,
        tasks: Mapping[str, str],
        *,
        tags: Sequence[str] = (),
        manifest: VersionManifest | None = None,
        metric_digest: str | None = None,
    ) -> DatasetVersionDetailResponse:
        body: dict[str, Any] = {
            "tasks": {_valid_task_id(task_id): _valid_digest(digest) for task_id, digest in tasks.items()},
            "tags": list(tags),
        }
        if manifest is not None:
            body["manifest"] = manifest.model_dump(mode="json")
        if metric_digest is not None:
            body["metric_digest"] = _valid_digest(metric_digest)
        return await self._sdk.request_model(
            "POST", f"{_dataset_path(benchmark, dataset)}/versions", DatasetVersionDetailResponse, json=body
        )

    async def push(
        self,
        benchmark: str,
        dataset: str,
        folder: str | Path,
        *,
        tags: Sequence[str] = (),
        concurrency: int = 4,
        on_stored: Callable[[str, BlobResponse], None] | None = None,
        on_created: Callable[[str], None] | None = None,
        on_left_out: Callable[[str, list[str]], None] | None = None,
        on_warning: Callable[[str], None] | None = None,
    ) -> DatasetVersionDetailResponse:
        """Publish the task folders under ``folder`` as a version of the dataset, creating it and its benchmark if
        they are missing.

        Only the tasks whose files the dataset does not store yet are uploaded, so pushing an unchanged folder
        uploads nothing and returns the version it already made. ``on_stored`` hears each task that was uploaded,
        ``on_created`` the name of each resource this call created, ``on_left_out`` each task and the entries in
        its folder that are not part of a task and so are not stored, and ``on_warning`` what a dataset.toml in
        ``folder`` leaves out or names differently.

        A dataset.toml in ``folder`` is a lock: the version holds exactly the tasks it lists, each task's files must
        still hash to the digest it lists, and its version label becomes a tag.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be greater than 0")
        root = Path(folder).resolve()
        folders = await asyncio.to_thread(find_task_folders, root)
        manifest = await asyncio.to_thread(read_manifest, root)
        script = await asyncio.to_thread(read_metric_script, root)
        check_script(manifest, script)
        locked = None if manifest is None else await asyncio.to_thread(lock_folders, manifest, folders, root)
        if locked is not None:
            folders = locked.folders
        problems = {task_id: await asyncio.to_thread(task_problems, path) for task_id, path in folders.items()}
        if invalid := {task_id: found for task_id, found in problems.items() if found}:
            raise TaskFolderError("\n".join(f"{task_id}: {'; '.join(found)}" for task_id, found in invalid.items()))

        gate = asyncio.Semaphore(concurrency)

        async def digest_one(path: Path) -> str:
            async with gate:
                return await asyncio.to_thread(task_digest, path)

        digests = dict(
            zip(folders, await asyncio.gather(*(digest_one(path) for path in folders.values())), strict=True)
        )
        if locked is not None:
            check_digests(locked, digests)

        target = await self._target(benchmark, dataset, on_created)
        if target.archived_at is not None:
            raise ValueError(f"Dataset '{benchmark}/{dataset}' is archived; restore it first")
        if locked is not None:
            info = locked.manifest.dataset
            if on_warning is not None:
                if locked.unlisted:
                    on_warning(
                        f"dataset.toml does not list these task folders, so they are left out: {', '.join(locked.unlisted)}"
                    )
                if info.name != f"{benchmark}/{dataset}":
                    on_warning(f"dataset.toml names the dataset {info.name}; pushing to {benchmark}/{dataset} as asked")
            if info.description and not target.description:
                await self.update(benchmark, dataset, description=info.description)
        script_digest = None
        if script is not None:
            script_digest = metric_digest(script)
            await self._put_file(benchmark, dataset, script_digest, script)
        if on_left_out is not None:
            for task_id, path in folders.items():
                if left := await asyncio.to_thread(left_out, path):
                    on_left_out(task_id, left)

        missing = set((await self._missing(benchmark, dataset, sorted(set(digests.values())))).missing)
        owners = {digest: task_id for task_id, digest in reversed(digests.items()) if digest in missing}
        stored: list[str] = []
        failed: dict[str, str] = {}

        async def upload_one(digest: str, task_id: str) -> None:
            async with gate:
                try:
                    with tempfile.TemporaryDirectory() as staging:
                        archive = Path(staging) / f"{task_id}.zip"
                        await asyncio.to_thread(pack_task, folders[task_id], archive)
                        blob = await self._put_blob(benchmark, dataset, digest, archive)
                except (ValkyrieSDKError, ValueError, OSError) as error:
                    failed[task_id] = str(error)
                    return
            stored.append(task_id)
            if on_stored is not None:
                on_stored(task_id, blob)

        await asyncio.gather(*(upload_one(digest, task_id) for digest, task_id in owners.items()))
        if failed:
            raise ValkyrieDatasetPushError(stored, failed)
        if locked is None:
            return await self._publish(benchmark, dataset, digests, tags=tags, metric_digest=script_digest)
        label = [] if locked.tag is None else [locked.tag]
        return await self._publish(
            benchmark,
            dataset,
            digests,
            tags=list(dict.fromkeys([*tags, *label])),
            manifest=wire_manifest(locked, digests),
            metric_digest=script_digest,
        )

    async def install(
        self,
        benchmark: str,
        dataset: str,
        source: str,
        *,
        location: DatasetLocation,
        tags: Sequence[str] = (),
        concurrency: int = 4,
        on_stored: Callable[[str, BlobResponse], None] | None = None,
        on_created: Callable[[str], None] | None = None,
        on_left_out: Callable[[str, list[str]], None] | None = None,
        on_warning: Callable[[str], None] | None = None,
    ) -> DatasetVersionDetailResponse:
        """Download a dataset from Harbor, GitHub, or Hugging Face into a temporary folder and push it.

        ``source`` is a Harbor registry name such as ``terminal-bench/terminal-bench`` for ``harbor``, and a
        repository as ``owner/name`` or its URL, optionally ending in ``/tree/<ref>/<subfolder>``, for ``github`` and
        ``hugging_face``. The push works as in `push`, and the temporary folder is removed afterwards.
        """
        _valid(benchmark, "benchmark")
        _valid(dataset, "dataset")
        for tag in tags:
            try:
                validate_tag(tag)
            except ValueError as error:
                raise ValkyrieInputError(str(error)) from error
        if concurrency < 1:
            raise ValueError("concurrency must be greater than 0")
        async with download_dataset(location, source) as folder:
            return await self.push(
                benchmark,
                dataset,
                folder,
                tags=tags,
                concurrency=concurrency,
                on_stored=on_stored,
                on_created=on_created,
                on_left_out=on_left_out,
                on_warning=on_warning,
            )

    async def start_run(
        self,
        agent: str | AgentContractRequest,
        dataset: DatasetRef,
        *,
        benchmark: str | None = None,
        model: str | None = None,
        concurrency: int = 5,
        priority: int | None = None,
        task_ids: Sequence[str] | None = None,
        slice_str: str | None = None,
        label: str | None = None,
        lambda_function: str | None = None,
        provider: str | None = None,
        agent_kwargs: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        project: str | None = None,
    ) -> StartBenchmarkResponse:
        """Start a run on the tasks of a hosted dataset or of a folder on a local Tracker."""
        check_start_options(concurrency, priority, task_ids, slice_str)
        if isinstance(dataset, HostedDataset):
            if benchmark not in (None, dataset.benchmark):
                raise ValkyrieRunError(f"benchmark must be {dataset.benchmark!r} for the dataset it names")
            name = dataset.benchmark
        else:
            name = benchmark or folder_run_name(dataset.path)
        payload = StartBenchmarkRequest(
            contract=normalize_contract(agent, model=model, agent_kwargs=agent_kwargs, secrets=secrets),
            benchmark_name=name,
            concurrency=concurrency,
            priority=priority,
            task_ids=list(task_ids) if task_ids else None,
            slice_str=slice_str,
            label=label,
            lambda_function=lambda_function,
            sandbox_provider=provider,
            project=project,
            dataset_ref=dataset,
        )
        return await post_start(self._sdk, payload)

    async def _target(self, benchmark: str, dataset: str, on_created: Callable[[str], None] | None) -> DatasetResponse:
        try:
            return await self.get(benchmark, dataset)
        except ValkyrieAPIError as error:
            if error.status_code != 404:
                raise
        try:
            await self._sdk.benchmark_definitions.get(benchmark)
        except ValkyrieAPIError as error:
            if error.status_code != 404:
                raise
            await _create_unless_taken(
                self._sdk.benchmark_definitions.create(benchmark), f"benchmark {benchmark}", on_created
            )
        created = await _create_unless_taken(
            self.create(benchmark, dataset), f"dataset {benchmark}/{dataset}", on_created
        )
        return created if created is not None else await self.get(benchmark, dataset)


async def _create_unless_taken[Created](
    create: Awaitable[Created], name: str, on_created: Callable[[str], None] | None
) -> Created | None:
    try:
        created = await create
    except ValkyrieAPIError as error:
        if error.status_code != 409:
            raise
        return None
    if on_created is not None:
        on_created(name)
    return created
