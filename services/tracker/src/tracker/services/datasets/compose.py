import asyncio
import json
import re
import shlex
from collections.abc import Mapping
from pathlib import Path

from benchmark_service import ComposeSource, ImageSource, Sandbox
from benchmark_service.sandbox import SandboxProviderConfig
from pydantic import BaseModel

from tracker.services.datasets.config import MAIN_SERVICE, NetworkMode, TaskConfig
from tracker.services.datasets.grading import make_directories, upload_dir
from tracker.exceptions import SandboxError
from tracker.sandbox import BUILD_LOG_CHARS, bundle_path

DIND_IMAGE = "docker:28.3.3-dind"
COMPOSE_DIR = "/valkyrie/compose"
ENVIRONMENT_DIR_REMOTE = "/valkyrie/environment"
TASK_COMPOSE_FILE = f"{ENVIRONMENT_DIR_REMOTE}/docker-compose.yaml"
RESOURCES_FILE = "resources.json"
BUNDLE_FILE = "bundle.json"
BUNDLE_MOUNT = {"type": "bind", "source": str(bundle_path), "target": str(bundle_path)}
LOGS_DIR_REMOTE = "/valkyrie/logs"
LOG_FOLDERS = {"AGENT_LOGS": "agent", "VERIFIER_LOGS": "verifier", "ARTIFACTS": "artifacts"}
LOG_MOUNTS = [
    {"type": "bind", "source": f"{LOGS_DIR_REMOTE}/{folder}", "target": f"/logs/{folder}"}
    for folder in LOG_FOLDERS.values()
]
TRANSIENT_MARKERS = (
    "toomanyrequests",
    "tls handshake timeout",
    "i/o timeout",
    "connection reset",
    "temporary failure in name resolution",
)
SERVICE_ENV_FILE = "main-env.json"
TASK_ENV_FILE = "task.env"
DOCKERD_RUNNING = "pidof dockerd > /dev/null || pgrep -f '[d]ockerd-entrypoint' > /dev/null"
DOCKERD_LOG = "/var/log/dockerd.log"
START_DOCKERD = f"dockerd-entrypoint.sh dockerd > {DOCKERD_LOG} 2>&1 < /dev/null &"
DOCKER_READY_ATTEMPTS = 30
DOCKER_READY_INTERVAL_SEC = 1.0
READY_TIMEOUT_SEC = 60.0
BUILD_OVERLAY = 'services:\n  main:\n    build:\n      context: ${CONTEXT_DIR}\n    pull_policy: build\n    command: [ "sh", "-c", "sleep infinity" ]\n'
PREBUILT_OVERLAY = (
    'services:\n  main:\n    image: ${PREBUILT_IMAGE_NAME}\n    command: [ "sh", "-c", "sleep infinity" ]\n'
)
NO_NETWORK_OVERLAY = "services:\n  main:\n    network_mode: none\n"


def _dotenv(env: Mapping[str, str]) -> str:
    def quoted(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("\n", "\\n")
        return f'"{escaped}"'

    return "".join(f"{name}={quoted(value)}\n" for name, value in env.items())


def runs_compose(provider: SandboxProviderConfig | None) -> bool:
    if provider is None:
        return False
    return provider.type == "daytona" or (provider.type == "modal" and provider.runtime == "vm")


class ComposeRuntime(BaseModel):
    environment_dir: str
    overlays: dict[str, str]
    build_timeout_sec: float


class ComposeRuntimeError(SandboxError):
    pass


def _project(task_id: str) -> str:
    project = re.sub(r"[^a-z0-9_-]", "-", task_id.lower())
    return project if project and project[0].isalnum() else f"0{project}"


def _overlays(config: TaskConfig) -> dict[str, str]:
    environment = config.environment
    limits: dict[str, str] = {}
    if environment.cpus is not None:
        limits["cpus"] = f"{environment.cpus:g}"
    if environment.memory_mb is not None:
        limits["memory"] = f"{environment.memory_mb}M"
    main: dict[str, object] = {"deploy": {"resources": {"limits": limits, "reservations": limits}}} if limits else {}
    overlays = {
        RESOURCES_FILE: json.dumps({"services": {MAIN_SERVICE: main}}),
        "main.yaml": PREBUILT_OVERLAY if environment.docker_image else BUILD_OVERLAY,
        BUNDLE_FILE: json.dumps({"services": {MAIN_SERVICE: {"volumes": [BUNDLE_MOUNT, *LOG_MOUNTS]}}}),
    }
    if environment.network_mode == NetworkMode.NO_NETWORK:
        overlays["no-network.yaml"] = NO_NETWORK_OVERLAY
    return overlays


def compose_runtime(task_dir: Path, config: TaskConfig) -> ComposeRuntime:
    return ComposeRuntime(
        environment_dir=str(task_dir.absolute()),
        overlays=_overlays(config),
        build_timeout_sec=config.environment.build_timeout_sec,
    )


def compose_source(task_id: str, config: TaskConfig) -> ComposeSource:
    project = _project(task_id)
    environment = config.environment
    settings = {"MAIN_IMAGE_NAME": f"valkyrie__{project}", "CONTEXT_DIR": ENVIRONMENT_DIR_REMOTE}
    if environment.docker_image:
        settings["PREBUILT_IMAGE_NAME"] = environment.docker_image
    if environment.cpus is not None:
        settings["CPUS"] = f"{environment.cpus:g}"
    if environment.memory_mb is not None:
        settings["MEMORY"] = f"{environment.memory_mb}M"
    for name, folder in LOG_FOLDERS.items():
        settings[f"HOST_{name}_PATH"] = f"{LOGS_DIR_REMOTE}/{folder}"
        settings[f"ENV_{name}_PATH"] = f"/logs/{folder}"
    files = [
        f"{COMPOSE_DIR}/{RESOURCES_FILE}",
        f"{COMPOSE_DIR}/main.yaml",
        f"{COMPOSE_DIR}/{BUNDLE_FILE}",
        f"{COMPOSE_DIR}/{SERVICE_ENV_FILE}",
        TASK_COMPOSE_FILE,
        *(f"{COMPOSE_DIR}/{name}" for name in _overlays(config) if name == "no-network.yaml"),
    ]
    command = [
        "docker",
        "--log-level",
        "error",
        "compose",
        "-p",
        project,
        "--project-directory",
        ENVIRONMENT_DIR_REMOTE,
        "--env-file",
        f"{COMPOSE_DIR}/{TASK_ENV_FILE}",
    ]
    for path in files:
        command.extend(["-f", path])
    prefix = [f"{name}={value}" for name, value in settings.items()]
    return ComposeSource(outer=ImageSource(image=DIND_IMAGE), compose_command=shlex.join([*prefix, *command]))


async def _run(sandbox: Sandbox, command: str, *, timeout: float) -> None:
    result = await sandbox.exec(command, timeout=timeout)
    if result.exit_code != 0:
        transient = any(marker in result.output.lower() for marker in TRANSIENT_MARKERS)
        raise ComposeRuntimeError(f"Command failed: {command}\n{result.output[-BUILD_LOG_CHARS:]}", transient=transient)


async def _wait_for_docker(sandbox: Sandbox) -> None:
    for attempt in range(DOCKER_READY_ATTEMPTS):
        if (await sandbox.exec("docker info", timeout=10)).exit_code == 0:
            return
        if attempt < DOCKER_READY_ATTEMPTS - 1:
            await asyncio.sleep(DOCKER_READY_INTERVAL_SEC)
    log = await sandbox.exec(f"tail -c {BUILD_LOG_CHARS} {DOCKERD_LOG}", timeout=10)
    raise ComposeRuntimeError(
        f"Docker daemon did not become ready inside the compose sandbox\n{log.output}", transient=True
    )


async def start_compose_runtime(
    outer: Sandbox, source: ComposeSource, runtime: ComposeRuntime, env: Mapping[str, str]
) -> None:
    if (await outer.exec(DOCKERD_RUNNING, timeout=10)).exit_code != 0:
        await _run(outer, START_DOCKERD, timeout=10)
    await _wait_for_docker(outer)
    log_dirs = [mount["source"] for mount in LOG_MOUNTS]
    await make_directories(outer, [COMPOSE_DIR, str(bundle_path), *log_dirs])
    await outer.exec(f"chmod 777 {' '.join(log_dirs)}")
    escaped = {name: value.replace("$", "$$") for name, value in env.items()}
    overlays = {
        **runtime.overlays,
        SERVICE_ENV_FILE: json.dumps({"services": {MAIN_SERVICE: {"environment": escaped}}}),
    }
    for name, content in overlays.items():
        await outer.upload_file(f"{COMPOSE_DIR}/{name}", content.encode())
    await outer.upload_file(f"{COMPOSE_DIR}/{TASK_ENV_FILE}", _dotenv(env).encode())
    await upload_dir(outer, Path(runtime.environment_dir), ENVIRONMENT_DIR_REMOTE)

    compose = source.compose_command
    await _run(outer, f"{compose} build", timeout=round(runtime.build_timeout_sec))
    await _run(outer, f"{compose} up --detach --wait", timeout=round(runtime.build_timeout_sec))
    await _run(outer, f"{compose} exec -T {MAIN_SERVICE} true", timeout=READY_TIMEOUT_SEC)
