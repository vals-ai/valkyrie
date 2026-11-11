import logging
import posixpath
import shlex
import uuid
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass

from benchmark_service.sandbox.types import Sandbox

from tracker.services.datasets.config import MAIN_SERVICE, ArtifactConfig, CollectConfig
from tracker.services.datasets.grading import ARTIFACTS_DIR
from tracker.services.datasets.user import run_as

logger = logging.getLogger(__name__)

STAGING_ROOT = "/var/tmp"
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_ARTIFACT_FILES = 20_000
ARCHIVE_TIMEOUT_SEC = 600
ULIMIT_BLOCK_BYTES = 512

CONVENTION_ARTIFACT = ArtifactConfig(source=ARTIFACTS_DIR)
ArtifactSink = Callable[[str, str, AsyncIterable[bytes]], Awaitable[None]]
Say = Callable[[str], None]


@dataclass(frozen=True)
class StagedArtifact:
    name: str
    source: str
    archive_path: str
    service: str


@dataclass(frozen=True)
class SkippedArtifact:
    source: str
    reason: str
    absent: bool


def _last_number(output: str) -> int | None:
    words = output.split()
    return int(words[-1]) if words and words[-1].isdigit() else None


async def run_collect_hooks(
    services: Mapping[str, Sandbox], hooks: Sequence[CollectConfig], say: Say, cwd: str
) -> None:
    for hook in hooks:
        command = hook.command if hook.user is None else run_as(hook.user, hook.command)
        try:
            result = await services[hook.service].exec(
                command, cwd=cwd if hook.service == MAIN_SERVICE else None, timeout=hook.timeout_sec
            )
        except Exception as error:
            say(f"The collect hook {hook.command!r} failed: {error}")
            continue
        if result.exit_code != 0:
            say(f"The collect hook {hook.command!r} exited with code {result.exit_code}")


async def stage_artifacts(
    services: Mapping[str, Sandbox], artifacts: Sequence[ArtifactConfig], cwd: str
) -> tuple[list[StagedArtifact], list[SkippedArtifact]]:
    directories: dict[str, str] = {}
    staged: list[StagedArtifact] = []
    skipped: list[SkippedArtifact] = []
    for index, artifact in enumerate(artifacts):
        service = artifact.service or MAIN_SERVICE
        sandbox = services[service]
        if service not in directories:
            directory = f"{STAGING_ROOT}/valkyrie-artifacts-{uuid.uuid4().hex}"
            if (await sandbox.exec(f"mkdir -m 700 {shlex.quote(directory)}")).exit_code != 0:
                skipped.append(SkippedArtifact(artifact.source, "the staging folder couldn't be created", False))
                continue
            directories[service] = directory
        archive_path = f"{directories[service]}/{index}.tar.gz"
        source = posixpath.normpath(
            artifact.source if artifact.source.startswith("/") else posixpath.join(cwd, artifact.source)
        )
        try:
            skip = await _archive(sandbox, artifact, source, archive_path)
        except Exception as error:
            skip = SkippedArtifact(artifact.source, f"{type(error).__name__}: {error}", False)
        if skip is None:
            staged.append(StagedArtifact(f"{artifact.name}.tar.gz", source, archive_path, service))
        else:
            skipped.append(skip)
    for service, directory in directories.items():
        if not any(artifact.service == service for artifact in staged):
            await services[service].exec(f"rm -rf {shlex.quote(directory)}")
    return staged, skipped


async def upload_artifacts(
    task_id: str,
    services: Mapping[str, Sandbox],
    staged: Sequence[StagedArtifact],
    sink: ArtifactSink,
    say: Say,
    folder: str = "artifacts",
) -> None:
    for artifact in staged:
        try:
            await sink(
                task_id, f"{folder}/{artifact.name}", services[artifact.service].stream_download(artifact.archive_path)
            )
        except Exception as error:
            say(f"Artifact {artifact.source} could not be saved: {error}")


async def discard_staged(services: Mapping[str, Sandbox], staged: Sequence[StagedArtifact]) -> None:
    for service, directory in {(artifact.service, posixpath.dirname(artifact.archive_path)) for artifact in staged}:
        try:
            await services[service].exec(f"rm -rf {shlex.quote(directory)}")
        except Exception:
            logger.warning("The staged artifacts in %s were not removed", directory, exc_info=True)


async def _archive(
    sandbox: Sandbox, artifact: ArtifactConfig, source: str, archive_path: str
) -> SkippedArtifact | None:
    def skip(reason: str, *, absent: bool = False) -> SkippedArtifact:
        return SkippedArtifact(artifact.source, reason, absent)

    quoted = shlex.quote(source)
    if (await sandbox.exec(f"test -e {quoted} || test -L {quoted}")).exit_code != 0:
        return skip("nothing was written there", absent=True)
    if (await sandbox.exec(f"test -L {quoted}")).exit_code == 0:
        return skip("it is a symbolic link, which isn't followed")
    archive = shlex.quote(archive_path)
    excluded = "".join(f" --exclude={shlex.quote(pattern)}" for pattern in artifact.exclude)
    parent, name = posixpath.split(source)
    blocks = MAX_ARTIFACT_BYTES // ULIMIT_BLOCK_BYTES
    script = (
        f"ulimit -f {blocks}; exec tar -czf {archive}{excluded} -C {shlex.quote(parent or '/')} -- {shlex.quote(name)}"
    )
    packed = await sandbox.exec(f"sh -c {shlex.quote(script)}", timeout=ARCHIVE_TIMEOUT_SEC)
    if packed.exit_code > 1:
        await sandbox.exec(f"rm -f {archive}")
        return skip(f"tar exited with code {packed.exit_code}: {packed.output.strip()[-200:]}")
    if (await sandbox.exec(f"tar -tzf {archive} > /dev/null")).exit_code != 0:
        await sandbox.exec(f"rm -f {archive}")
        return skip("the archive is incomplete, because it grew past its size limit or the disk filled")
    entries = _last_number((await sandbox.exec(f"tar -tzf {archive} | wc -l")).output)
    if entries is None or entries > MAX_ARTIFACT_FILES:
        await sandbox.exec(f"rm -f {archive}")
        return skip(
            "its entries couldn't be counted" if entries is None else f"it holds more than {MAX_ARTIFACT_FILES} files"
        )
    return None
