"""Download the task folders of a dataset from Harbor, GitHub, or Hugging Face so they can be pushed."""

import asyncio
import os
import re
import shutil
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import get_args
from urllib.parse import quote, unquote

import httpx

from valkyrie.sdk.errors import ValkyrieInputError, ValkyrieTransportError
from valkyrie.sdk.github_download import download_github
from valkyrie.sdk.models.datasets import DatasetLocation
from valkyrie.sdk.task_archive import CONFIG_FILE

_HARBOR_NAME = re.compile(
    r"(?:[A-Za-z0-9][A-Za-z0-9._-]*/)?[A-Za-z0-9][A-Za-z0-9._-]*(?:@[A-Za-z0-9][A-Za-z0-9._:-]*)?"
)
_HUGGING_FACE_SOURCE = re.compile(
    r"(?:https://huggingface\.co/datasets/)?([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(?:/tree/([^/]+)(?:/(.+?))?)?/?"
)
_HUGGING_FACE_URL = "https://huggingface.co"


async def _download_harbor(name: str, destination: Path) -> None:
    if _HARBOR_NAME.fullmatch(name) is None:
        raise ValkyrieInputError("Invalid Harbor dataset; expected org/name or org/name@ref")
    harbor = ["uvx", "--from", "harbor>=0.25", "harbor"] if shutil.which("uvx") else ["harbor"]
    try:
        process = await asyncio.create_subprocess_exec(
            *harbor,
            "download",
            name,
            "--export",
            "--output-dir",
            str(destination),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except FileNotFoundError as error:
        raise ValkyrieInputError("Installing from Harbor needs the harbor CLI or uv on PATH") from error
    try:
        output, _ = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise ValkyrieInputError(f"Harbor could not download {name}: {output.decode(errors='replace').strip()}")


def _hugging_face_parts(source: str) -> tuple[str, str, str]:
    match = _HUGGING_FACE_SOURCE.fullmatch(source)
    if match is None:
        raise ValkyrieInputError(
            "Invalid Hugging Face dataset; expected owner/name or a huggingface.co/datasets URL, "
            "optionally with /tree/revision[/subfolder]"
        )
    owner, name, quoted_revision, subfolder = match.groups()
    revision = unquote(quoted_revision) if quoted_revision else None
    parts = [owner, name, *(revision or "").split("/"), *(subfolder or "").split("/")]
    if any(part in {".", ".."} for part in parts) or (subfolder and "" in subfolder.split("/")):
        raise ValkyrieInputError("Invalid Hugging Face dataset, revision, or subfolder")
    return f"{owner}/{name}", revision or "main", subfolder or ""


async def _download_hugging_face(source: str, destination: Path) -> None:
    repository, revision, subfolder = _hugging_face_parts(source)
    token = os.environ.get("HF_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    tree = f"/api/datasets/{repository}/tree/{quote(revision, safe='')}"
    page: str | None = f"{tree}/{quote(subfolder)}" if subfolder else tree
    prefix = f"{subfolder}/" if subfolder else ""
    try:
        async with httpx.AsyncClient(
            base_url=_HUGGING_FACE_URL, headers=headers, follow_redirects=True, timeout=300
        ) as client:
            files: list[str] = []
            params: dict[str, str] | None = {"recursive": "true"}
            while page is not None:
                response = await client.get(page, params=params)
                if response.status_code in {401, 404}:
                    raise ValkyrieInputError(f"Hugging Face dataset not found or not readable: {source}")
                response.raise_for_status()
                files.extend(entry["path"] for entry in response.json() if entry["type"] == "file")
                page, params = response.links.get("next", {}).get("url"), None
            if not files:
                raise ValkyrieInputError(f"Hugging Face dataset has no files: {source}")

            gate = asyncio.Semaphore(8)
            root = destination.resolve()

            async def download(path: str) -> None:
                target = (destination / path.removeprefix(prefix)).resolve()
                if not target.is_relative_to(root):
                    raise ValueError(f"Hugging Face file escapes the dataset: {path}")
                await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
                async with (
                    gate,
                    client.stream(
                        "GET", f"/datasets/{repository}/resolve/{quote(revision, safe='')}/{quote(path)}"
                    ) as stream,
                ):
                    stream.raise_for_status()
                    with target.open("wb") as handle:
                        async for chunk in stream.aiter_bytes():
                            handle.write(chunk)
                with target.open("rb") as handle:
                    if handle.read(2) == b"#!":
                        target.chmod(0o755)

            downloads = [asyncio.create_task(download(path)) for path in files]
            try:
                await asyncio.gather(*downloads)
            except BaseException:
                for task in downloads:
                    task.cancel()
                await asyncio.gather(*downloads, return_exceptions=True)
                raise
    except httpx.HTTPError as error:
        raise ValkyrieTransportError(f"Hugging Face download failed: {error}") from error


@asynccontextmanager
async def download_dataset(location: DatasetLocation, source: str) -> AsyncGenerator[Path, None]:
    """Download a dataset's task folders into a temporary folder that is removed on exit.

    ``source`` is a Harbor registry name such as ``terminal-bench/terminal-bench-2`` for ``harbor``, a GitHub
    repository as ``owner/repo`` or its URL for ``github``, and a Hugging Face dataset as ``owner/name`` or its URL for
    ``hugging_face``. GitHub and Hugging Face URLs may end in ``/tree/<ref>/<subfolder>``, with a slash in a Hugging Face
    ref written as ``%2F`` the way its URLs show it. Hugging Face reads
    ``HF_TOKEN`` for private datasets, and Harbor runs ``harbor>=0.25`` through ``uvx``, or the ``harbor`` CLI on
    PATH without uv.
    """
    if location not in get_args(DatasetLocation):
        raise ValkyrieInputError(f"Invalid dataset location {location!r}; expected harbor, github or hugging_face")
    if location == "github":
        async with download_github(source if source.startswith("https://") else f"https://github.com/{source}") as path:
            yield path
        return
    with TemporaryDirectory() as temporary:
        destination = Path(temporary)
        if location == "hugging_face":
            await _download_hugging_face(source, destination)
            yield destination
            return
        await _download_harbor(source, destination)
        downloaded = list(destination.iterdir())
        if len(downloaded) == 1 and downloaded[0].is_dir() and not (downloaded[0] / CONFIG_FILE).is_file():
            yield downloaded[0]
        else:
            yield destination
