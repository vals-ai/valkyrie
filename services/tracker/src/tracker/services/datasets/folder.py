import re
from enum import StrEnum
from pathlib import Path

from tracker.services.datasets.config import TaskConfig

CONFIG_FILE = "task.toml"
INSTRUCTION_FILE = "instruction.md"
ENVIRONMENT_DIR = "environment"
TEST_SCRIPT = "tests/test.sh"
REQUIRED_FILES = (CONFIG_FILE, INSTRUCTION_FILE, TEST_SCRIPT)
DOCKERFILE = "Dockerfile"
COMPOSE_FILE = "docker-compose.yaml"
CANARY_LINE = re.compile(r"^(<!--.*canary.*-->|#.*canary.*)$", re.IGNORECASE)


def read_instruction(path: Path) -> str:
    lines = path.read_text().split("\n")
    start = 0
    while start < len(lines) and CANARY_LINE.match(lines[start].strip()):
        start += 1
    while start < len(lines) and not lines[start].strip():
        start += 1
    return "\n".join(lines[start:])


class SandboxKind(StrEnum):
    IMAGE = "image"
    DOCKERFILE = "dockerfile"
    COMPOSE = "compose"


def sandbox_kind(task_dir: Path, config: TaskConfig) -> SandboxKind | None:
    environment = task_dir / ENVIRONMENT_DIR
    if (environment / COMPOSE_FILE).is_file():
        return SandboxKind.COMPOSE
    if config.environment.docker_image:
        return SandboxKind.IMAGE
    if (environment / DOCKERFILE).is_file():
        return SandboxKind.DOCKERFILE
    return None
