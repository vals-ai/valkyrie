import re
import tempfile
import tomllib
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from tracker.services.datasets.config import (
    HEADER_TEMPLATE,
    AgentConfig,
    EnvironmentConfig,
    HealthcheckConfig,
    MCPServerConfig,
    SolutionConfig,
    StepConfig,
    TaskConfig,
    TaskOS,
    MAIN_SERVICE,
    VerifierConfig,
    is_table,
)
from tracker.services.datasets.env import TEMPLATE
from tracker.services.datasets.extract import extract
from tracker.services.datasets.folder import (
    CONFIG_FILE,
    DOCKERFILE,
    INSTRUCTION_FILE,
    TEST_SCRIPT,
    SandboxKind,
    sandbox_kind,
)
from tracker.services.datasets.separate_verifier import VerifierKind, verifier_kind
from tracker.services.datasets.source import TASK_FILES
from tracker.services.datasets.steps import STEPS_DIR, step_layers, step_verifier
from tracker.runtime.artifacts import TASK_ID_PATTERN

PLATFORM_ENV_NAMES = {
    "RUN_ID",
    "TASK_ID",
    "ATTEMPT_ID",
    "IDENTITY",
    "MODEL_GATEWAY_URL",
    "MODEL_GATEWAY_API_KEY",
    "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS",
}
OLDER_ENVIRONMENT_KEYS = {"memory", "storage", "allow_internet"}
UNSUPPORTED_ENVIRONMENT_KEYS = {"tpu"}
ENVIRONMENT_TABLES = (("[environment]", ("environment",)), ("[verifier.environment]", ("verifier", "environment")))
NEWEST_MAJOR_VERSIONS = {"0", "1"}
ARCHIVE_TASK_ID = "task"


@dataclass(frozen=True)
class TaskReport:
    task_id: str
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    config: TaskConfig | None = None

    @property
    def ok(self) -> bool:
        return not self.errors


def _table(raw: dict[str, Any], *path: str) -> dict[str, Any]:
    node: Any = raw
    for key in path:
        node = node.get(key) if is_table(node) else None
    return node if is_table(node) else {}


def _location(location: Sequence[int | str]) -> str:
    parts = [str(part) for part in location]
    if not parts:
        return CONFIG_FILE
    if len(parts) == 1:
        return parts[0]
    return f"[{parts[0]}]" + "".join(f".{part}" for part in parts[1:])


def _invalid_fields(error: ValidationError) -> list[str]:
    return [f"{_location(item['loc'])}: {item['msg'].removeprefix('Value error, ')}" for item in error.errors()]


def _verifier_environments(config: TaskConfig) -> dict[str, EnvironmentConfig]:
    environments: dict[str, EnvironmentConfig] = {}
    if config.verifier.environment is not None:
        environments["[verifier.environment]"] = config.verifier.environment
    for step in config.steps:
        if step.verifier.environment is not None:
            environments[f"[steps.{step.name}.verifier.environment]"] = step.verifier.environment
    return environments


def _environments(config: TaskConfig) -> dict[str, EnvironmentConfig]:
    return {"[environment]": config.environment, **_verifier_environments(config)}


def _unsupported(config: TaskConfig, raw: dict[str, Any], kind: SandboxKind | None) -> list[str]:
    found: list[str] = []
    for label, path in ENVIRONMENT_TABLES:
        if "tpu" in _table(raw, *path):
            found.append(f"{label}.tpu is not supported yet")
    for step in raw.get("steps", []):
        if "tpu" in _table(step, "verifier", "environment"):
            found.append(f"[steps.{step.get('name')}.verifier.environment].tpu is not supported yet")
    for label, environment in _environments(config).items():
        if environment.os != TaskOS.LINUX:
            found.append(f"{label}.os {environment.os.value!r} is not supported yet; only linux runs")

    if kind == SandboxKind.COMPOSE and config.environment.gpus:
        found.append("a compose task can't request GPUs")
    if kind != SandboxKind.COMPOSE:
        services = {hook.service for hook in config.verifier.collect} | {
            artifact.service for artifact in config.artifacts
        }
        for step in config.steps:
            services |= {hook.service for hook in step.verifier.collect}
            services |= {artifact.service for artifact in step.artifacts}
        for service in sorted(name for name in services if name not in (None, MAIN_SERVICE)):
            found.append(f"the service {service!r} is named, but only a compose task has services")
    return found


def _compose_verifier_problems(config: TaskConfig, task_dir: Path, kind: SandboxKind | None) -> list[str]:
    graders = (
        [(None, config.verifier, [(config.verifier.environment, task_dir / "tests")])]
        if not config.steps
        else [(step.name, step_verifier(config, step), step_layers(config, step, task_dir)) for step in config.steps]
    )
    problems: list[str] = []
    for step_name, verifier, layers in graders:
        layer = verifier_kind(layers)
        runs_compose = layer == VerifierKind.COMPOSE or (layer == VerifierKind.AGENT and kind == SandboxKind.COMPOSE)
        if verifier.is_separate and runs_compose and (verifier.environment or config.environment).gpus:
            subject = "a compose verifier" if step_name is None else f"the compose verifier of step {step_name!r}"
            problems.append(f"{subject} can't request GPUs")
    return problems


def _hidden_verifier_dockerfile(config: TaskConfig, tests_dir: Path) -> list[str]:
    environment = config.verifier.environment
    if environment is None or not environment.docker_image or not (tests_dir / DOCKERFILE).is_file():
        return []
    return [f"tests/{DOCKERFILE} is ignored because [verifier.environment].docker_image takes precedence"]


def _ignored_verifier_settings(config: TaskConfig) -> list[str]:
    warnings: list[str] = []
    for label, environment in _verifier_environments(config).items():
        ignored = {
            "healthcheck": environment.healthcheck,
            "mcp_servers": environment.mcp_servers,
            "skills_dir": environment.skills_dir,
        }
        warnings.extend(
            f"{label}.{name} only applies to the agent's environment, so it is ignored"
            for name, value in ignored.items()
            if value
        )
    return warnings


def _unset_header_references(config: TaskConfig) -> list[str]:
    notes: list[str] = []
    for server in config.environment.mcp_servers:
        for header, value in server.headers.items():
            required = {match.group(1) for match in HEADER_TEMPLATE.finditer(value) if match.group(2) is None}
            unset = sorted(required - config.environment.env.keys() - PLATFORM_ENV_NAMES)
            if unset:
                names = ", ".join(f"${{{name}}}" for name in unset)
                notes.append(
                    f"The MCP server {server.name!r} header {header!r} uses {names}, which "
                    "[environment].env doesn't set; the agent resolves it from its own environment"
                )
    return notes


def _env_problems(config: TaskConfig) -> list[str]:
    problems: list[str] = []
    envs = {
        "[environment].env": config.environment.env,
        "[verifier].env": config.verifier.env,
        "[solution].env": config.solution.env,
        **{f"[steps.{step.name}.verifier].env": step.verifier.env for step in config.steps},
    }
    for label, env in envs.items():
        for key, value in env.items():
            if "${" in value and TEMPLATE.fullmatch(value) is None:
                problems.append(
                    f"{label} {key} has a partial ${{...}}; a value is either a whole ${{NAME}} or plain text"
                )
    problems.extend(
        f"[environment].env can't set {key}, which the platform owns"
        for key in config.environment.env
        if key in PLATFORM_ENV_NAMES or key.startswith("VALKYRIE_")
    )
    return problems


def _unknown_keys(raw: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    tables: tuple[tuple[str, tuple[str, ...], type[BaseModel], set[str]], ...] = (
        ("[environment]", ("environment",), EnvironmentConfig, OLDER_ENVIRONMENT_KEYS | UNSUPPORTED_ENVIRONMENT_KEYS),
        ("[agent]", ("agent",), AgentConfig, set()),
        ("[verifier]", ("verifier",), VerifierConfig, set()),
        ("[solution]", ("solution",), SolutionConfig, set()),
        (
            "[verifier.environment]",
            ("verifier", "environment"),
            EnvironmentConfig,
            OLDER_ENVIRONMENT_KEYS | UNSUPPORTED_ENVIRONMENT_KEYS,
        ),
    )
    for label, path, model, tolerated in tables:
        for key in sorted(set(_table(raw, *path)) - set(model.model_fields) - tolerated):
            warnings.append(f"{label}.{key} is not a field this version knows, so it is ignored")
    for label, path in ENVIRONMENT_TABLES:
        environment = _table(raw, *path)
        for key in sorted(set(_table(environment, "healthcheck")) - set(HealthcheckConfig.model_fields)):
            warnings.append(f"{label}.healthcheck.{key} is not a field this version knows, so it is ignored")
        servers = environment.get("mcp_servers", [])
        for index, server in enumerate(servers):
            for key in sorted(set(server) - set(MCPServerConfig.model_fields)):
                warnings.append(
                    f"{label}.mcp_servers[{index}].{key} is not a field this version knows, so it is ignored"
                )
        names = [server["name"] for server in servers]
        warnings.extend(
            f"{label}.mcp_servers declares {name!r} more than once, so the last one is used"
            for name in dict.fromkeys(name for name in names if names.count(name) > 1)
        )
    for index, step in enumerate(raw.get("steps", [])):
        label = f"steps[{index}]"
        for key in sorted(set(step) - set(StepConfig.model_fields)):
            warnings.append(f"{label}.{key} is not a field this version knows, so it is ignored")
        for table, model in (("agent", AgentConfig), ("verifier", VerifierConfig)):
            for key in sorted(set(_table(step, table)) - set(model.model_fields)):
                warnings.append(f"{label}.{table}.{key} is not a field this version knows, so it is ignored")
    return warnings


def _step_problems(task_dir: Path, config: TaskConfig | None) -> list[str]:
    if config is None or not config.steps:
        return [f"{name} is missing" for name in (INSTRUCTION_FILE, TEST_SCRIPT) if not (task_dir / name).is_file()]
    problems: list[str] = []
    for step in config.steps:
        folder = task_dir / STEPS_DIR / step.name
        if not (folder / INSTRUCTION_FILE).is_file():
            problems.append(f"{STEPS_DIR}/{step.name}/{INSTRUCTION_FILE} is missing")
        if not (folder / TEST_SCRIPT).is_file() and not (task_dir / TEST_SCRIPT).is_file():
            problems.append(f"step {step.name!r} has no {TEST_SCRIPT} of its own or in the task's tests")
    return problems


def validate_task(task_dir: Path) -> TaskReport:
    errors: list[str] = []
    warnings: list[str] = []
    if not re.fullmatch(TASK_ID_PATTERN, task_dir.name):
        errors.append(f"The folder name {task_dir.name!r} is not a valid task ID")

    config: TaskConfig | None = None
    config_path = task_dir / CONFIG_FILE
    if not config_path.is_file():
        errors.append(f"{CONFIG_FILE} is missing")
        errors.extend(_step_problems(task_dir, None))
    else:
        try:
            raw = tomllib.loads(config_path.read_text())
            config = TaskConfig.model_validate(raw)
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as error:
            errors.append(f"{CONFIG_FILE} is not valid TOML: {error}")
            errors.extend(_step_problems(task_dir, None))
        except ValidationError as error:
            errors.extend(_invalid_fields(error))
            errors.extend(_step_problems(task_dir, None))
        else:
            errors.extend(_step_problems(task_dir, config))
            if config.multi_step_reward_strategy is not None and not config.steps:
                warnings.append("multi_step_reward_strategy only applies to a task with steps, so it is ignored")
            kind = sandbox_kind(task_dir, config)
            if kind is None:
                errors.append(
                    "A task needs a sandbox source: [environment].docker_image, environment/Dockerfile "
                    "or environment/docker-compose.yaml"
                )
            errors.extend(_unsupported(config, raw, kind))
            errors.extend(_compose_verifier_problems(config, task_dir, kind))
            errors.extend(_env_problems(config))
            warnings.extend(_unknown_keys(raw))
            warnings.extend(_unset_header_references(config))
            warnings.extend(_ignored_verifier_settings(config))
            warnings.extend(_hidden_verifier_dockerfile(config, task_dir / "tests"))
            if config.schema_version.split(".")[0] not in NEWEST_MAJOR_VERSIONS:
                warnings.append(
                    f"schema_version {config.schema_version!r} is newer than this version reads, so settings may be ignored"
                )
    return TaskReport(task_dir.name, tuple(errors), tuple(warnings), config)


def validate_archive(archive: zipfile.ZipFile) -> TaskReport:
    with tempfile.TemporaryDirectory(prefix="task-") as scratch:
        task_dir = Path(scratch) / ARCHIVE_TASK_ID
        task_dir.mkdir()
        extract(archive, TASK_FILES, task_dir)
        return validate_task(task_dir)
