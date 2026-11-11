import math
from pathlib import Path, PurePosixPath
from typing import Any

from benchmark_service import ComposeSource, DockerfileSource, ImageSource, Resources
from benchmark_service.schemas import BenchmarkEgressPlan, EgressPolicy, RetrieveTaskResponse
from pydantic import BaseModel, Field

from tracker.services.datasets.compose import ComposeRuntime, compose_runtime, compose_source
from tracker.services.datasets.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    MCPServerConfig,
    MultiStepRewardStrategy,
    NetworkMode,
    NetworkSettings,
    TaskConfig,
)
from tracker.services.datasets.folder import (
    COMPOSE_FILE,
    DOCKERFILE,
    ENVIRONMENT_DIR,
    INSTRUCTION_FILE,
    SandboxKind,
    sandbox_kind,
)
from tracker.services.datasets.steps import step_agent, step_verifier

DEFAULT_CWD = "/workspace"
DEFAULT_MEMORY_GB = 1
DEFAULT_DISK_GB = 15


class UnsupportedTaskError(ValueError):
    pass


class StepPlan(BaseModel):
    name: str
    agent_timeout: float | None = None
    agent_user: str | None = None
    egress_run: EgressPolicy
    egress_evaluation: EgressPolicy


class TaskFolderResponse(RetrieveTaskResponse):
    env: dict[str, str] = Field(default_factory=dict)
    secret_env: list[str] = Field(default_factory=list)
    agent_user: str | None = None
    healthcheck: HealthcheckConfig | None = None
    compose: ComposeRuntime | None = None
    steps: list[StepPlan] = Field(default_factory=list)
    reward_strategy: MultiStepRewardStrategy = MultiStepRewardStrategy.MEAN
    cwd_from_image: bool = False
    mcp_servers: dict[str, dict[str, Any]] = Field(default_factory=dict)
    skills_dir: str | None = None


def dockerfile_workdir(dockerfile: Path) -> str | None:
    if not dockerfile.is_file():
        return None
    workdir: str | None = None
    for line in dockerfile.read_text().splitlines():
        instruction, _, value = line.strip().partition(" ")
        value = value.strip()
        keyword = instruction.upper()
        if keyword == "FROM":
            workdir = None
        elif keyword == "WORKDIR" and value:
            workdir = value if value.startswith("/") else str(PurePosixPath(workdir or "/") / value)
    return workdir


def stage_egress(stage: NetworkSettings, environment: EnvironmentConfig) -> EgressPolicy:
    settings = stage if stage.network_mode is not None else environment
    match settings.network_mode:
        case NetworkMode.NO_NETWORK:
            return []
        case NetworkMode.ALLOWLIST:
            return list(settings.allowed_hosts or [])
        case _:
            return "*"


def sandbox_source(task_dir: Path, config: TaskConfig) -> ImageSource | DockerfileSource | ComposeSource:
    match sandbox_kind(task_dir, config):
        case SandboxKind.COMPOSE:
            return compose_source(task_dir.name, config)
        case SandboxKind.IMAGE if image := config.environment.docker_image:
            return ImageSource(image=image)
        case SandboxKind.DOCKERFILE:
            return DockerfileSource(
                dockerfile=str((task_dir / ENVIRONMENT_DIR / DOCKERFILE).absolute()),
                build_timeout=config.environment.build_timeout_sec,
            )
        case _:
            raise UnsupportedTaskError(
                f"Task {task_dir.name!r} has no docker_image, {ENVIRONMENT_DIR}/{DOCKERFILE} "
                f"or {ENVIRONMENT_DIR}/{COMPOSE_FILE} to run"
            )


def mcp_server_entries(servers: list[MCPServerConfig]) -> dict[str, dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    for server in servers:
        if server.transport == "stdio":
            entries[server.name] = {"type": "stdio", "command": server.command, "args": server.args}
            continue
        entry: dict[str, Any] = {"type": "http" if server.transport == "streamable-http" else "sse", "url": server.url}
        if server.headers:
            entry["headers"] = server.headers
        entries[server.name] = entry
    return entries


def sandbox_resources(environment: EnvironmentConfig, fallback: EnvironmentConfig | None = None) -> Resources:
    gpus = environment.gpus or 0
    cpus = environment.cpus or (fallback.cpus if fallback else None)
    memory_mb = environment.memory_mb or (fallback.memory_mb if fallback else None)
    storage_mb = environment.storage_mb or (fallback.storage_mb if fallback else None)
    return Resources(
        vcpu=cpus or 1,
        memory=math.ceil(memory_mb / 1024) if memory_mb else DEFAULT_MEMORY_GB,
        disk=math.ceil(storage_mb / 1024) if storage_mb else DEFAULT_DISK_GB,
        gpu=gpus,
        gpu_type=environment.gpu_types if gpus and environment.gpu_types else None,
    )


def retrieve_task_response(task_dir: Path, config: TaskConfig) -> TaskFolderResponse:
    environment = config.environment
    source = sandbox_source(task_dir, config)
    compose = isinstance(source, ComposeSource)
    workdir = environment.workdir or dockerfile_workdir(task_dir / ENVIRONMENT_DIR / DOCKERFILE)
    cwd = workdir or DEFAULT_CWD
    return TaskFolderResponse(
        source=source,
        problem_path=f"{cwd}/{INSTRUCTION_FILE}",
        cwd=cwd,
        agent_timeout=config.agent.timeout_sec,
        resources=sandbox_resources(environment),
        egress=BenchmarkEgressPlan(
            setup_task="*" if compose else stage_egress(environment, environment),
            run=stage_egress(config.agent, environment),
            evaluation=stage_egress(config.verifier, environment),
        ),
        env=dict(environment.env),
        agent_user=config.agent.user,
        healthcheck=environment.healthcheck,
        compose=compose_runtime(task_dir / ENVIRONMENT_DIR, config) if compose else None,
        steps=[
            StepPlan(
                name=step.name,
                agent_timeout=step_agent(config, step).timeout_sec,
                agent_user=step_agent(config, step).user,
                egress_run=stage_egress(step_agent(config, step), environment),
                egress_evaluation=stage_egress(step_verifier(config, step), environment),
            )
            for step in config.steps
        ],
        reward_strategy=config.reward_strategy,
        cwd_from_image=workdir is None and isinstance(source, (ImageSource, DockerfileSource)),
        mcp_servers=mcp_server_entries(environment.mcp_servers),
        skills_dir=environment.skills_dir,
    )
