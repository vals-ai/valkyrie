"""GitHub downloads that agents and datasets are installed from."""

import asyncio
import logging
import re
import shutil
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin

from valkyrie.sdk.errors import ValkyrieInputError

logger = logging.getLogger(__name__)


async def _run_git_command(repo_path: Path | None, *args: str) -> str:
    command = [
        "git",
        "-c",
        "protocol.allow=never",
        "-c",
        "protocol.https.allow=always",
        "-c",
        "http.followRedirects=false",
        "-c",
        "submodule.recurse=false",
    ]
    if repo_path is not None:
        command.extend(["-C", str(repo_path)])
    process = await asyncio.create_subprocess_exec(
        *command,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(300):
            stdout, stderr = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise ChildProcessError(f"Git command failed: {stderr.decode(errors='replace')}")

    return stdout.decode()


def _overlaps(path: str, within: str | None) -> bool:
    return within is None or path == within or path.startswith(f"{within}/") or within.startswith(f"{path}/")


def _discard_partial_submodule(repository_path: Path, gitdir: Path, name: str, path: str) -> None:
    for container, relative in (((gitdir / "modules").resolve(), name), (repository_path.resolve(), path)):
        target = container / relative
        if target.name in {"", ".", ".."}:
            continue
        parent = target.parent.resolve()
        if parent.is_relative_to(container):
            shutil.rmtree(parent / target.name, ignore_errors=True)


async def _fetch_submodule(repository_path: Path, name: str, path: str) -> None:
    command = ("submodule", "update", "--init", "--checkout")
    try:
        await _run_git_command(repository_path, *command, "--depth", "1", "--", path)
    except ChildProcessError:
        logger.warning("Shallow fetch of submodule %s failed; fetching full history", path)
        gitdir = Path((await _run_git_command(repository_path, "rev-parse", "--absolute-git-dir")).rstrip("\n"))
        await asyncio.to_thread(_discard_partial_submodule, repository_path, gitdir, name, path)
        await _run_git_command(repository_path, *command, "--no-recommend-shallow", "--", path)


async def _checkout_submodules(repository_path: Path, repository_url: str, within: str | None = None) -> None:
    if not await asyncio.to_thread((repository_path / ".gitmodules").is_file):
        return
    config = await _run_git_command(repository_path, "config", "--file", ".gitmodules", "--null", "--list")
    submodules: dict[str, dict[str, str]] = {}
    for entry in config.split("\0"):
        key, _, value = entry.partition("\n")
        match = re.fullmatch(r"submodule\.(.+)\.(path|url)", key)
        if match:
            name, field = match.groups()
            submodules.setdefault(name, {})[field] = value
    for name, fields in submodules.items():
        path, url = fields.get("path", ""), fields.get("url", "")
        if not _overlaps(path, within):
            continue
        if url.startswith(("./", "../")):
            url = urljoin(repository_url.rstrip("/") + "/", url)
        match = re.fullmatch(r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/?", url)
        if match is None or any(part in {".", ".."} for part in match.groups()):
            raise ValueError("Submodules must use HTTPS GitHub repository URLs")
        if "\\" in path or any(part in {"", ".", ".."} for part in path.split("/")):
            raise ValueError("Invalid submodule path")
        submodule_path = repository_path / path
        resolved = await asyncio.to_thread(submodule_path.resolve)
        if not resolved.is_relative_to(repository_path.resolve()):
            raise ValueError("Submodule path escapes repository")
        await _run_git_command(repository_path, "config", "--local", f"submodule.{name}.url", url)
        await _fetch_submodule(repository_path, name, path)
        remaining = within.removeprefix(f"{path}/") if within and within.startswith(f"{path}/") else None
        await _checkout_submodules(submodule_path, url, within=remaining)


async def _split_tree_path(repository_path: Path, parts: list[str]) -> tuple[str, str]:
    output = await _run_git_command(
        repository_path, "for-each-ref", "--format=%(refname)", "refs/remotes/origin", "refs/tags"
    )
    references = {
        reference.removeprefix("refs/remotes/origin/").removeprefix("refs/tags/") for reference in output.splitlines()
    }
    end = next((end for end in range(len(parts), 0, -1) if "/".join(parts[:end]) in references), 1)
    return "/".join(parts[:end]), "/".join(parts[end:])


@asynccontextmanager
async def download_github(github_url: str) -> AsyncGenerator[Path, None]:
    """Download a GitHub repository or /tree/ref[/subfolder] URL into a temporary folder, including submodules."""
    match = re.fullmatch(
        r"https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)(?:/tree/(.+?))?/?",
        github_url,
    )
    if match is None:
        raise ValkyrieInputError("Invalid GitHub URL; expected a repository or /tree/ref[/subfolder] URL")
    owner, repository, tree_path = match.groups()
    if owner in {".", ".."} or repository in {".", ".."}:
        raise ValkyrieInputError("Invalid GitHub repository")
    tree_parts = tree_path.split("/") if tree_path else []
    if tree_path and (
        "\\" in tree_path or tree_path.startswith("-") or any(part in {"", ".", ".."} for part in tree_parts)
    ):
        raise ValkyrieInputError("Invalid GitHub branch or subfolder")

    with TemporaryDirectory() as temporary:
        repository_url = f"https://github.com/{owner}/{repository}"
        repository_path = Path(temporary) / "repository"
        clone_args = ["clone", "--no-recurse-submodules"]
        if tree_parts:
            clone_args.extend(["--no-checkout", "--filter=blob:none"])
        clone_args.extend([repository_url, str(repository_path)])
        await _run_git_command(None, *clone_args)
        reference, subfolder = await _split_tree_path(repository_path, tree_parts) if tree_parts else ("HEAD", "")
        if subfolder:
            await _run_git_command(repository_path, "sparse-checkout", "init", "--cone")
            await _run_git_command(repository_path, "sparse-checkout", "set", "--", subfolder)
        await _run_git_command(repository_path, "checkout", reference, "--")
        await _checkout_submodules(repository_path, repository_url, within=subfolder or None)
        checkout_path = repository_path / subfolder if subfolder else repository_path
        if not checkout_path.resolve().is_relative_to(repository_path.resolve()):
            raise ValueError("GitHub subfolder escapes repository")
        if not checkout_path.is_dir():
            raise FileNotFoundError(f"Subfolder not found: {subfolder}")

        yield checkout_path
