import logging
import posixpath
import shlex
import uuid
from asyncio import CancelledError
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from contextlib import asynccontextmanager

from benchmark_service.sandbox.types import Sandbox

from tracker.runtime.artifacts import EPOCH, task_artifact_key
from tracker.runtime.storage import ObjectStore
from tracker.services.datasets.config import MAIN_SERVICE, ArtifactConfig, CollectConfig
from tracker.services.datasets.grading import ARTIFACTS_DIR, VERIFIER_DIR
from tracker.services.datasets.user import run_as

logger = logging.getLogger(__name__)

STAGING_ROOT = "/var/tmp"
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
MAX_ARTIFACT_FILES = 20_000
ARCHIVE_TIMEOUT_SEC = 600
ULIMIT_BLOCK_BYTES = 512

CONVENTION_ARTIFACT = ArtifactConfig(source=ARTIFACTS_DIR)
SAVED_ARTIFACTS_RUN = "saved_artifacts_run_id"
ARCHIVE_SUFFIX = ".tar.gz"
ABSENT_SUFFIX = ".absent"
ArtifactSink = Callable[[str, str, AsyncIterable[bytes]], Awaitable[None]]
Say = Callable[[str], None]


@dataclass(frozen=True)
class StagedArtifact:
    name: str
    source: str
    archive_path: str
    service: str
    read: Callable[[], AsyncIterable[bytes]]
    archive_root: str | None = None


@dataclass(frozen=True)
class SkippedArtifact:
    source: str
    reason: str
    absent: bool
    name: str


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
                skipped.append(
                    SkippedArtifact(artifact.source, "the staging folder couldn't be created", False, artifact.name)
                )
                continue
            directories[service] = directory
        archive_path = f"{directories[service]}/{index}{ARCHIVE_SUFFIX}"
        source = posixpath.normpath(posixpath.join(cwd, artifact.source))
        try:
            skip = await _archive(sandbox, artifact, source, archive_path)
        except Exception as error:
            skip = SkippedArtifact(artifact.source, f"{type(error).__name__}: {error}", False, artifact.name)
        if skip is None:
            staged.append(
                StagedArtifact(
                    f"{artifact.name}{ARCHIVE_SUFFIX}",
                    source,
                    archive_path,
                    service,
                    partial(sandbox.stream_download, archive_path),
                )
            )
        else:
            skipped.append(skip)
    for service, directory in directories.items():
        if not any(artifact.service == service for artifact in staged):
            await services[service].exec(f"rm -rf {shlex.quote(directory)}")
    return staged, skipped


class MissingSavedArtifactError(RuntimeError):
    pass


async def _nothing() -> AsyncIterator[bytes]:
    return
    yield b""


async def upload_artifacts(
    task_id: str,
    staged: Sequence[StagedArtifact],
    skipped: Sequence[SkippedArtifact],
    sink: ArtifactSink,
    say: Say,
    folder: str = "artifacts",
    *,
    required: bool = False,
) -> None:
    for source, name, chunks in [
        *((artifact.source, artifact.name, artifact.read()) for artifact in staged),
        *((artifact.source, f"{artifact.name}{ABSENT_SUFFIX}", _nothing()) for artifact in skipped if artifact.absent),
    ]:
        try:
            await sink(task_id, posixpath.join(folder, name), chunks)
        except Exception as error:
            if required:
                raise RuntimeError(f"Required artifact {source} could not be saved: {error}") from error
            say(f"Artifact {source} could not be saved: {error}")


@asynccontextmanager
async def collect_verifier_output(
    task_id: str, sandbox: Sandbox, sink: ArtifactSink | None, folder: str = "artifacts"
) -> AsyncIterator[None]:
    grading_failed = False
    cancelled = False
    try:
        yield
    except CancelledError:
        cancelled = True
        raise
    except BaseException:
        grading_failed = True
        raise
    finally:
        try:
            if sink is not None and not cancelled:
                services = {MAIN_SERVICE: sandbox}
                staged, skipped = await stage_artifacts(
                    services, [ArtifactConfig(source=VERIFIER_DIR, destination="verifier_output")], "/"
                )
                try:
                    if skipped:
                        raise RuntimeError(f"Verifier output could not be saved: {skipped[0].reason}")
                    await upload_artifacts(
                        task_id, staged, [], sink, lambda _message: None, posixpath.dirname(folder), required=True
                    )
                finally:
                    await discard_staged(services, staged)
        except Exception:
            if not grading_failed:
                raise
            logger.exception("Verifier output for task %s could not be saved after grading failed", task_id)


async def saved_artifacts(
    objects: ObjectStore, run_id: str, task_id: str, artifacts: Sequence[ArtifactConfig], cwd: str
) -> tuple[list[StagedArtifact], list[SkippedArtifact]]:
    staged: list[StagedArtifact] = []
    skipped: list[SkippedArtifact] = []
    for artifact in {
        CONVENTION_ARTIFACT.name: CONVENTION_ARTIFACT,
        **{artifact.name: artifact for artifact in artifacts},
    }.values():
        key = task_artifact_key(run_id, task_id, f"artifacts/{artifact.name}")
        saved = [
            stored
            async for stored in objects.list_objects(key)
            if stored.key in (f"{key}{ARCHIVE_SUFFIX}", f"{key}{ABSENT_SUFFIX}")
        ]
        if not saved:
            if artifact is CONVENTION_ARTIFACT:
                continue
            legacy = task_artifact_key(run_id, task_id, "agent_output.tar.gz")
            if (
                not artifact.source.startswith("/")
                or artifact.exclude
                or artifact.service not in (None, MAIN_SERVICE)
                or not await objects.exists(legacy)
            ):
                raise MissingSavedArtifactError(f"Run {run_id} saved no artifact {artifact.source} for task {task_id}")
            staged.append(
                StagedArtifact(
                    f"{artifact.name}{ARCHIVE_SUFFIX}",
                    posixpath.normpath(artifact.source),
                    legacy,
                    artifact.service or MAIN_SERVICE,
                    partial(objects.get_stream, legacy),
                    posixpath.normpath(artifact.source).lstrip("/"),
                )
            )
            continue
        newest = max(saved, key=lambda stored: (stored.last_modified or EPOCH, stored.key)).key
        if newest.endswith(ABSENT_SUFFIX):
            skipped.append(SkippedArtifact(artifact.source, "the agent never wrote it", True, artifact.name))
            continue
        staged.append(
            StagedArtifact(
                f"{artifact.name}{ARCHIVE_SUFFIX}",
                posixpath.normpath(posixpath.join(cwd, artifact.source)),
                newest,
                artifact.service or MAIN_SERVICE,
                partial(objects.get_stream, newest),
            )
        )
    return staged, skipped


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
        return SkippedArtifact(artifact.source, reason, absent, artifact.name)

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
