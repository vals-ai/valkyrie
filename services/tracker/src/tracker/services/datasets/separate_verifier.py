import asyncio
import gzip
import io
import logging
import posixpath
import re
import shlex
import tarfile
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Any, NamedTuple
from uuid import uuid4

from benchmark_service import (
    ComposeSource,
    DockerfileSource,
    ImageSource,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxProvider,
)
from benchmark_service.sandbox.created import AnnouncedSandboxProvider, sandbox_created_scope
from benchmark_service.sandbox.types import SandboxSource
from benchmark_service.schemas import SandboxCreatedData

from tracker.services.datasets.artifacts import MAX_ARTIFACT_BYTES, MAX_ARTIFACT_FILES, SkippedArtifact, StagedArtifact
from tracker.services.datasets.compose import ComposeRuntime, compose_runtime, compose_source, start_compose_runtime
from tracker.services.datasets.config import MAIN_SERVICE, EnvironmentConfig, TaskConfig
from tracker.services.datasets.folder import COMPOSE_FILE, DOCKERFILE
from tracker.services.datasets.grading import ARTIFACTS_DIR, OPEN_LOG_DIRS, VERIFIER_DIR, grade_task, keep
from tracker.services.datasets.retrieve_task import sandbox_resources, stage_egress
from tracker.services.datasets.steps import step_layers, step_verifier
from tracker.sandbox import apply_egress_policy, runtime_sandbox

logger = logging.getLogger(__name__)

TRANSFER_DIR = "/var/tmp/valkyrie-transfer"
CREATE_TIMEOUT_SEC = 900
CREATE_ATTEMPTS = 2
AUTO_STOP_MARGIN_MINUTES = 30
NAME_PREFIX_LENGTH = 40
RETRYABLE_CREATE_MESSAGES = ("already exists", "entered error state")
MAX_UNPACKED_BYTES = MAX_ARTIFACT_BYTES + MAX_ARTIFACT_FILES * 1024
NAME_CHARS_IN_MESSAGES = 80


class SeparateVerifierError(RuntimeError):
    pass


class _BoundedReader(io.RawIOBase):
    def __init__(self, source: io.BufferedIOBase, limit: int) -> None:
        self._source = source
        self._left = limit

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        data = self._source.read(len(buffer))
        self._left -= len(data)
        if self._left < 0:
            raise SeparateVerifierError("An artifact unpacks to more than this run will carry")
        buffer[: len(data)] = data
        return len(data)


class VerifierEnvironment(NamedTuple):
    source: SandboxSource
    upload_tests: bool
    compose: ComposeRuntime | None = None


class VerifierKind(StrEnum):
    IMAGE = "image"
    COMPOSE = "compose"
    DOCKERFILE = "dockerfile"
    AGENT = "agent"


Layers = Sequence[tuple[EnvironmentConfig | None, Path]]


def _layer_kind(environment: EnvironmentConfig | None, tests_dir: Path) -> VerifierKind | None:
    if (tests_dir / COMPOSE_FILE).is_file():
        return VerifierKind.COMPOSE
    if environment is not None and environment.docker_image:
        return VerifierKind.IMAGE
    if (tests_dir / DOCKERFILE).is_file():
        return VerifierKind.DOCKERFILE
    return None


def verifier_kind(layers: Layers) -> VerifierKind:
    return next((kind for layer in layers if (kind := _layer_kind(*layer)) is not None), VerifierKind.AGENT)


def verifier_environment(
    task_id: str,
    tests_dir: Path,
    config: TaskConfig,
    agent_source: SandboxSource,
    agent_compose: ComposeRuntime | None,
    layers: Layers | None = None,
) -> VerifierEnvironment:
    environment = config.verifier.environment
    for layer_environment, layer_tests in layers or [(environment, tests_dir)]:
        match _layer_kind(layer_environment, layer_tests):
            case VerifierKind.IMAGE if layer_environment is not None and layer_environment.docker_image:
                return VerifierEnvironment(ImageSource(image=layer_environment.docker_image), False)
            case VerifierKind.COMPOSE:
                own = config.model_copy(update={"environment": environment or EnvironmentConfig()})
                return VerifierEnvironment(compose_source(task_id, own), False, compose_runtime(tests_dir, own))
            case VerifierKind.DOCKERFILE:
                build_timeout = (environment or config.environment).build_timeout_sec
                return VerifierEnvironment(
                    DockerfileSource(dockerfile=str((tests_dir / DOCKERFILE).absolute()), build_timeout=build_timeout),
                    False,
                )
            case _:
                continue
    return VerifierEnvironment(agent_source, True, agent_compose)


def verifier_runs_compose(config: TaskConfig, task_dir: Path) -> bool:
    if not config.steps:
        layers = [(config.verifier.environment, task_dir / "tests")]
        return config.verifier.is_separate and verifier_kind(layers) == VerifierKind.COMPOSE
    return any(
        step_verifier(config, step).is_separate
        and verifier_kind(step_layers(config, step, task_dir)) == VerifierKind.COMPOSE
        for step in config.steps
    )


def _sandbox_name(task_id: str) -> str:
    prefix = re.sub(r"[^a-z0-9-]", "-", task_id.lower())[:NAME_PREFIX_LENGTH].strip("-") or "task"
    return f"{prefix}-verifier-{uuid4().hex[:12]}"


@contextmanager
def _collect_announcements(enabled: bool) -> Iterator[list[SandboxCreatedData]]:
    announced: list[SandboxCreatedData] = []
    if not enabled:
        yield announced
        return

    async def announce(provider: AnnouncedSandboxProvider, sandbox_id: str) -> None:
        announced.append(SandboxCreatedData(provider=provider, sandbox_id=sandbox_id))

    with sandbox_created_scope(announce):
        yield announced


async def _create_sandbox(
    provider: SandboxProvider,
    task_id: str,
    config: TaskConfig,
    source: SandboxSource,
    env_vars: dict[str, str],
    labels: dict[str, str],
) -> Sandbox:
    environment = config.verifier.environment or config.environment
    policy = stage_egress(config.verifier, environment)
    block_network = policy == [] and not isinstance(source, ComposeSource)
    for attempt in range(1, CREATE_ATTEMPTS + 1):
        try:
            return await provider.create_sandbox(
                SandboxCreateRequest(
                    source=source.outer if isinstance(source, ComposeSource) else source,
                    resources=sandbox_resources(environment, config.environment),
                    name=_sandbox_name(task_id),
                    labels=labels,
                    env_vars=env_vars,
                    auto_stop_interval=int(config.verifier.timeout_sec // 60) + AUTO_STOP_MARGIN_MINUTES,
                    create_timeout=CREATE_TIMEOUT_SEC,
                    network_block_all=block_network,
                )
            )
        except SandboxError as error:
            if attempt == CREATE_ATTEMPTS or not any(
                message in str(error).lower() for message in RETRYABLE_CREATE_MESSAGES
            ):
                raise
            logger.warning("The verifier sandbox for %s failed on attempt %d, retrying: %s", task_id, attempt, error)
    raise SeparateVerifierError(f"The verifier sandbox for {task_id} could not be created")


def _inside(path: str, name: str) -> bool:
    return path == name or path.startswith(f"{name}/")


def _plain(member: tarfile.TarInfo) -> tarfile.TarInfo:
    member.uid = member.gid = 0
    member.uname = member.gname = ""
    member.mode &= 0o777
    return member


def _clean_archive(archive_path: Path, name: str, source: str, scratch: Path) -> Path:
    unpacked = scratch / "unpacked"
    unpacked.mkdir()
    files = 0

    def keep_inside(member: tarfile.TarInfo, destination: str) -> tarfile.TarInfo:
        nonlocal files
        member = tarfile.data_filter(member, destination)
        path = posixpath.normpath(member.name)
        if not _inside(path, name):
            raise SeparateVerifierError(
                f"Artifact {source} holds {member.name[:NAME_CHARS_IN_MESSAGES]!r}, outside itself"
            )
        if member.issym() or member.islnk():
            base = posixpath.dirname(path) if member.issym() else ""
            if not _inside(posixpath.normpath(posixpath.join(base, member.linkname)), name):
                raise SeparateVerifierError(f"Artifact {source} holds a link that leaves it")
        files += 1
        if files > MAX_ARTIFACT_FILES:
            raise SeparateVerifierError(f"Artifact {source} holds more than {MAX_ARTIFACT_FILES} entries")
        return member

    try:
        with gzip.open(archive_path, "rb") as compressed:
            reader = io.BufferedReader(_BoundedReader(compressed, MAX_UNPACKED_BYTES))
            with tarfile.open(fileobj=reader, mode="r|") as archive:
                archive.extractall(unpacked, filter=keep_inside)
        clean = scratch / "clean.tar.gz"
        with tarfile.open(clean, "w:gz") as repacked:
            repacked.add(unpacked / name, arcname=name, filter=_plain)
    except (tarfile.TarError, OSError, EOFError, KeyError) as error:
        raise SeparateVerifierError(f"Artifact {source} can't be carried: {str(error)[:200]}") from error
    return clean


async def _download(source_sandbox: Sandbox, remote_path: str, destination: Path, source: str) -> None:
    total = 0
    sink = await asyncio.to_thread(destination.open, "wb")
    try:
        async for chunk in source_sandbox.stream_download(remote_path):
            total += len(chunk)
            if total > MAX_ARTIFACT_BYTES:
                raise SeparateVerifierError(f"Artifact {source} is over {MAX_ARTIFACT_BYTES} bytes")
            await asyncio.to_thread(sink.write, chunk)
    finally:
        await asyncio.to_thread(sink.close)


def ignore(_message: str) -> None:
    return None


async def _carry(source_sandbox: Sandbox, verifier: Sandbox, artifact: StagedArtifact) -> None:
    name = posixpath.basename(artifact.source)
    parent = posixpath.dirname(artifact.source) or "/"
    with tempfile.TemporaryDirectory(prefix="artifact-") as scratch_name:
        scratch = Path(scratch_name)
        archive_path = scratch / "artifact.tar.gz"
        await _download(source_sandbox, artifact.archive_path, archive_path, artifact.source)
        clean = await asyncio.to_thread(_clean_archive, archive_path, name, artifact.source, scratch)
        remote = f"{TRANSFER_DIR}/{uuid4().hex}.tar.gz"
        await verifier.exec(f"mkdir -p {shlex.quote(TRANSFER_DIR)} {shlex.quote(parent)}")
        await verifier.upload_file(remote, await asyncio.to_thread(clean.read_bytes))
    unpacked = await verifier.exec(
        f"tar -xzf {shlex.quote(remote)} -C {shlex.quote(parent)} && rm -f {shlex.quote(remote)}"
    )
    if unpacked.exit_code != 0:
        raise SeparateVerifierError(f"Could not unpack artifact {artifact.source}: {unpacked.output.strip()[-300:]}")


async def _carry_artifacts(
    services: Mapping[str, Sandbox],
    verifier: Sandbox,
    staged: Sequence[StagedArtifact],
    skipped: Sequence[SkippedArtifact],
    say: Callable[[str], None] = ignore,
) -> None:
    for artifact in skipped:
        if artifact.absent:
            continue
        if artifact.source != ARTIFACTS_DIR:
            raise SeparateVerifierError(f"Artifact {artifact.source} can't be carried: {artifact.reason}")
        say(f"The folder {ARTIFACTS_DIR} could not be carried to the verifier: {artifact.reason}")
    for artifact in staged:
        try:
            await _carry(services[artifact.service], verifier, artifact)
        except SeparateVerifierError as error:
            if artifact.source != ARTIFACTS_DIR:
                raise
            say(f"The folder {ARTIFACTS_DIR} could not be carried to the verifier: {error}")
    await verifier.exec(f"rm -rf {shlex.quote(VERIFIER_DIR)}")
    await verifier.exec(OPEN_LOG_DIRS)


async def grade_separately(
    task_id: str,
    config: TaskConfig,
    env: dict[str, str],
    sandbox_env: dict[str, str],
    tests_dir: Path,
    services: Mapping[str, Sandbox],
    agent_source: SandboxSource,
    agent_compose: ComposeRuntime | None,
    provider: SandboxProvider,
    staged: Sequence[StagedArtifact],
    skipped: Sequence[SkippedArtifact],
    on_message: Callable[[str], None] | None = None,
    on_created: Callable[[SandboxCreatedData], None] | None = None,
    redact: Callable[[str], str] = keep,
    layers: Layers | None = None,
) -> dict[str, Any]:
    environment = verifier_environment(task_id, tests_dir, config, agent_source, agent_compose, layers)
    source = environment.source
    if on_message is not None:
        on_message(f"Starting a separate verifier for {task_id}")
    labels = {**(services[MAIN_SERVICE].labels or {}), "valkyrie-role": "verifier"}
    with _collect_announcements(on_created is not None) as announced:
        verifier = await _create_sandbox(provider, task_id, config, source, sandbox_env, labels)
    try:
        if on_created is not None:
            for created in announced:
                on_created(created)
        if isinstance(source, ComposeSource) and environment.compose is not None:
            await start_compose_runtime(verifier, source, environment.compose, sandbox_env)
        policy = stage_egress(config.verifier, config.verifier.environment or config.environment)
        if policy != "*" and (policy or isinstance(source, ComposeSource)):
            await apply_egress_policy(verifier, policy)
        grader = runtime_sandbox(verifier, source)
        await _carry_artifacts(services, grader, staged, skipped, on_message or ignore)
        result = await grade_task(
            task_id, config, env, tests_dir, grader, on_message, redact=redact, upload_tests=environment.upload_tests
        )
        result["metadata"]["verifier_environment_mode"] = "separate"
        return result
    finally:
        try:
            await provider.delete_sandbox(verifier.id)
        except Exception:
            logger.warning("The verifier sandbox %s for %s was not deleted", verifier.id, task_id, exc_info=True)
