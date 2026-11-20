"""Write and check Harbor datasets: task folders that grade themselves, for Valkyrie and plain Harbor.

An exporter maps each native task record to a `HarborTask` and passes them to `write_dataset`. Each
task's grader writes one `TaskEvaluation` to `/logs/verifier/details.json`, whose fields match the
Vals format task evaluation. `dataset_problems` reports every way a folder departs from this layout.
"""

import json
import os
import posixpath
import re
import tomllib
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any, Literal, TypeGuard, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from valkyrie.sdk.metric_script import METRIC_FILE, MetricScriptError, read_metric_script
from valkyrie.sdk.task_archive import CONFIG_FILE, INSTRUCTION_FILE, TaskFolderError, find_task_folders, task_problems

type TomlScalar = str | int | float | bool
type TomlValue = TomlScalar | list[TomlScalar] | dict[str, TomlValue] | list[dict[str, TomlValue]]

TASK_SCHEMA_VERSION = "1.3"
TASK_FILE = CONFIG_FILE
SCORE_TYPES_FILE = "score_types.json"
HARBOR_JOB_FILE = "harbor.yaml"
VERIFIER_RESULT_FILE = "details.json"
TESTS_MOUNT = "/tests"

_TASK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_TASK_NAME = re.compile(r"[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*")
_PINNED_IMAGE = re.compile(r"\S+@sha256:[0-9a-f]{64}")
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_HEADER_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:(?!\$\{)[^}])*))?\}")
_HEADER_CREDENTIAL = re.compile(
    rf"\s*(?:[A-Za-z]+\s+)?{_HEADER_REFERENCE.pattern}(?:[\s:-]*{_HEADER_REFERENCE.pattern})*\s*"
)
_SENSITIVE_NAME = re.compile(r"KEY|SECRET|TOKEN(?!S)|PASSWORD|CREDENTIAL|AUTH", re.IGNORECASE)
_AGENT_ONLY_KEYS = ("mcp_servers", "skills_dir")
_RESERVED_METADATA = ("valkyrie", "category", "tags")
_DATASET_FILES = (METRIC_FILE, SCORE_TYPES_FILE, HARBOR_JOB_FILE)


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ScoreType(_Model):
    """One score a dataset reports, as in the Vals format `score_types` table."""

    unit: str
    description: str | None = None


class Score(_Model):
    value: float | None
    stderr: float | None = None


class TaskEvaluation(_Model):
    """What a grader writes to `details.json`: the Vals format evaluation of one task.

    `status` is `success` or `error`. Every key of `scores` must be declared in the dataset's
    `score_types`. `metadata` holds the judge evidence: verdicts, rubric hits, and judge cost.
    """

    status: str = Field(pattern="^(success|error)$")
    scores: dict[str, Score] = Field(default_factory=dict[str, Score])
    metadata: dict[str, Any] = Field(default_factory=dict[str, Any])


def _valid_name(name: object) -> TypeGuard[str]:
    return isinstance(name, str) and bool(_TASK_NAME.fullmatch(name)) and ".." not in name


def _require_pinned(image: str) -> str:
    if not _PINNED_IMAGE.fullmatch(image):
        raise ValueError(f"Pin the image by digest, as name@sha256:<64 hex>: {image!r}")
    return image


def _check_header(name: str, value: str) -> None:
    if not _HEADER_NAME.fullmatch(name) or "\r" in value or "\n" in value:
        raise ValueError(f"{name!r} is not a valid one-line HTTP header")
    if "${" in _HEADER_REFERENCE.sub("", value):
        raise ValueError(f"The header {name!r} has a malformed ${{NAME}} reference")
    sensitive = bool(_SENSITIVE_NAME.search(name)) or name.lower() in ("cookie", "set-cookie")
    for variable, default in _HEADER_REFERENCE.findall(value):
        if default and (sensitive or _SENSITIVE_NAME.search(variable)):
            raise ValueError(f"The header {name!r} must not give the credential ${{{variable}}} a default")
    if sensitive and value.strip() and not _HEADER_CREDENTIAL.fullmatch(value):
        raise ValueError(f"The header {name!r} must reference its credential as ${{NAME}}, such as 'Bearer ${{TOKEN}}'")


def _is_sandbox_folder(path: object) -> TypeGuard[str]:
    return isinstance(path, str) and path.startswith("/") and path != "/" and posixpath.normpath(path) == path


class MCPServer(_Model):
    """A tool server the task gives to the agent, as one `[[environment.mcp_servers]]` entry.

    Valkyrie and Harbor hand the servers to whichever agent runs. A `streamable-http` or `sse`
    server needs a `url` and may send `headers`, which reference credentials as `${NAME}`. A
    `stdio` server needs a `command`, with `args`, that runs in the agent sandbox.
    """

    name: str = Field(min_length=1)
    transport: Literal["streamable-http", "sse", "stdio"] = "sse"
    url: str | None = None
    command: str | None = None
    args: list[str] = Field(default_factory=list[str])
    headers: dict[str, str] = Field(default_factory=dict[str, str])

    @field_validator("transport", mode="before")
    @classmethod
    def _http(cls, value: object) -> object:
        return "streamable-http" if value == "http" else value

    @model_validator(mode="after")
    def _check(self) -> "MCPServer":
        if self.transport == "stdio" and (not self.command or self.url is not None or self.headers):
            raise ValueError(f"MCP server {self.name!r}: stdio needs a command, and takes no url or headers")
        if self.transport != "stdio" and (not self.url or self.command is not None or self.args):
            raise ValueError(f"MCP server {self.name!r}: {self.transport} needs a url, and takes no command or args")
        for header, value in self.headers.items():
            _check_header(header, value)
        return self


class Verifier(_Model):
    """How a task grades itself, in its own sandbox built from a pinned grader image.

    `tests/Dockerfile` is `FROM image` with `files` copied to `/tests`, so the agent never sees
    them. `tests/test.sh` execs `command`, which writes `/logs/verifier/reward.json` and a
    `TaskEvaluation` to `/logs/verifier/details.json`. `env` values are `task.toml` references such
    as `"${MODEL_GATEWAY_URL:-}"`, which Valkyrie and Harbor resolve at verification time.
    """

    image: str
    command: str
    files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])
    env: dict[str, str] = Field(default_factory=dict[str, str])
    timeout_sec: float = Field(default=1800.0, gt=0)
    cpus: float = Field(default=1.0, gt=0)
    memory_mb: int = Field(default=1024, gt=0)
    storage_mb: int = Field(default=1024, gt=0)

    @model_validator(mode="after")
    def _check(self) -> "Verifier":
        _require_pinned(self.image)
        if not self.command.strip() or "\n" in self.command:
            raise ValueError("The verifier command must be one non-empty line")
        return self


class HarborTask(_Model):
    """One task folder of a Harbor dataset.

    `name` is Harbor's registry name, `<org>/<task>` in lowercase, and must stay the same across
    exports. `category` and `tags` become the task's Vals format category and tags. The agent
    sandbox comes from exactly one of `environment_image` (pinned by digest) or
    `environment_dockerfile`, whose build context is `environment/` with `environment_files`.
    `solution`, when set, becomes `solution/solve.sh`. `files` holds any other file of the folder
    by relative path, outside `environment/`, `solution/`, and `tests/`.

    `mcp_servers` are the tools the benchmark gives the agent. An HTTP server beside the agent runs as a
    service of `environment/docker-compose.yaml`, passed in `environment_files`. A stdio server runs
    in the agent sandbox. `skills_dir` is the absolute path of the task's skills folder in the agent
    sandbox. On Tracker, the agent command uses `{mcp_config_path}` to load the server configuration
    and `{skills_dir}` to locate the skills.
    """

    task_id: str
    name: str
    instruction: str
    verifier: Verifier
    category: str | None = None
    tags: list[str] = Field(default_factory=list[str])
    environment_image: str | None = None
    environment_dockerfile: str | None = None
    environment_files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])
    metadata: dict[str, TomlValue] = Field(default_factory=dict[str, TomlValue])
    agent_timeout_sec: float | None = Field(default=None, gt=0)
    artifacts: list[str] = Field(default_factory=list[str])
    solution: str | None = None
    files: dict[str, bytes | str] = Field(default_factory=dict[str, bytes | str])
    mcp_servers: list[MCPServer] = Field(default_factory=list[MCPServer])
    skills_dir: str | None = None

    @model_validator(mode="after")
    def _check(self) -> "HarborTask":
        if not _TASK_ID.fullmatch(self.task_id):
            raise ValueError(f"Not a valid task ID: {self.task_id!r}")
        if self.task_id.casefold() in _DATASET_FILES:
            raise ValueError(f"Task ID {self.task_id!r} is reserved for a dataset file")
        if not _valid_name(self.name):
            raise ValueError(f"The task name must be lowercase <org>/<task>: {self.name!r}")
        if not self.instruction.strip():
            raise ValueError("The instruction must not be empty")
        if (self.environment_image is None) == (self.environment_dockerfile is None):
            raise ValueError("Set exactly one of environment_image and environment_dockerfile")
        if self.environment_image is not None:
            _require_pinned(self.environment_image)
            if self.environment_files:
                raise ValueError("environment_files need environment_dockerfile")
        servers = [server.name for server in self.mcp_servers]
        if len(set(servers)) != len(servers):
            raise ValueError(f"Duplicate MCP server names: {sorted({n for n in servers if servers.count(n) > 1})}")
        if self.skills_dir is not None and not _is_sandbox_folder(self.skills_dir):
            raise ValueError(f"skills_dir must be a normalized absolute path in the sandbox: {self.skills_dir!r}")
        reserved = sorted(set(_RESERVED_METADATA) & set(self.metadata))
        if reserved:
            raise ValueError(f"metadata keys {reserved} are written by HarborTask")
        for path in self.files:
            if path.split("/")[0] in ("environment", "solution", "tests"):
                raise ValueError(f"Put {path!r} in environment_files, solution, or verifier.files instead")
        self.files_by_path()
        return self

    def task_toml(self) -> str:
        """The `task.toml` text. It reads back as exactly the data it was written from."""
        verifier = self.verifier
        metadata: dict[str, TomlValue] = {**self.metadata}
        if self.category is not None:
            metadata["category"] = self.category
        if self.tags:
            metadata["tags"] = list(self.tags)
        metadata["valkyrie"] = {"verifier_result_file": VERIFIER_RESULT_FILE}
        environment: dict[str, TomlValue] = {
            **({"docker_image": self.environment_image} if self.environment_image else {}),
            **({"skills_dir": self.skills_dir} if self.skills_dir is not None else {}),
        }
        if self.mcp_servers:
            environment["mcp_servers"] = [
                {"name": server.name, "transport": server.transport}
                | server.model_dump(exclude={"name", "transport"}, exclude_defaults=True)
                for server in self.mcp_servers
            ]
        data: dict[str, TomlValue] = {
            "schema_version": TASK_SCHEMA_VERSION,
            "task": {"name": self.name},
            **({"artifacts": list(self.artifacts)} if self.artifacts else {}),
            "metadata": metadata,
            **({"agent": {"timeout_sec": self.agent_timeout_sec}} if self.agent_timeout_sec is not None else {}),
            **({"environment": environment} if environment else {}),
            "verifier": {
                "environment_mode": "separate",
                "timeout_sec": verifier.timeout_sec,
                "environment": {
                    "cpus": verifier.cpus,
                    "memory_mb": verifier.memory_mb,
                    "storage_mb": verifier.storage_mb,
                },
                **({"env": dict(verifier.env)} if verifier.env else {}),
            },
        }
        text = _toml_document(data)
        try:
            parsed = tomllib.loads(text)
        except tomllib.TOMLDecodeError as error:
            raise ValueError(f"task.toml for {self.task_id!r} is not valid TOML: {error}") from error
        if parsed != data:
            raise ValueError(f"task.toml for {self.task_id!r} does not round-trip")
        return text

    def files_by_path(self) -> dict[str, bytes]:
        """Every file of the folder, keyed by path relative to the folder."""
        generated: dict[str, bytes | str] = {
            "tests/Dockerfile": f"FROM {self.verifier.image}\nCOPY . {TESTS_MOUNT}/\n",
            "tests/test.sh": f"#!/bin/sh\nset -eu\nexec {self.verifier.command}\n",
        }
        if self.environment_dockerfile is not None:
            generated["environment/Dockerfile"] = self.environment_dockerfile
        if self.solution is not None:
            generated["solution/solve.sh"] = self.solution
        parts = (
            generated,
            {f"tests/{path}": content for path, content in self.verifier.files.items()},
            {f"environment/{path}": content for path, content in self.environment_files.items()},
            self.files,
        )
        paths = [path for part in parts for path in part]
        duplicates = sorted({path for path in paths if paths.count(path) > 1})
        if duplicates:
            raise ValueError(f"Task {self.task_id!r} writes these paths twice: {duplicates}")
        _check_file_paths(paths)
        files = {INSTRUCTION_FILE: self.instruction.encode(), TASK_FILE: self.task_toml().encode()}
        for part in parts:
            files.update({path: c.encode() if isinstance(c, str) else c for path, c in part.items()})
        return files


def write_dataset(
    root: Path, tasks: Iterable[HarborTask], *, metric: str, score_types: Mapping[str, ScoreType]
) -> list[str]:
    """Write each task folder under `root`, plus `metric.py`, `score_types.json`, and `harbor.yaml`.

    `root` must not exist yet. Returns the task IDs in the order written.
    """
    tasks = list(tasks)
    if not tasks:
        raise ValueError("A dataset needs at least one task")
    if not score_types:
        raise ValueError("Declare at least one score type")
    task_ids = [task.task_id for task in tasks]
    folded = [task_id.casefold() for task_id in task_ids]
    if len(set(folded)) != len(folded):
        raise ValueError(f"Duplicate task IDs: {sorted({t for t in task_ids if folded.count(t.casefold()) > 1})}")
    names = [task.name for task in tasks]
    if len(set(names)) != len(names):
        raise ValueError(f"Duplicate task names: {sorted({n for n in names if names.count(n) > 1})}")
    root.mkdir(parents=True, exist_ok=False)
    for task in tasks:
        for path, content in task.files_by_path().items():
            target = root / task.task_id / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            if path in ("tests/test.sh", "solution/solve.sh"):
                target.chmod(0o755)
    (root / METRIC_FILE).write_text(metric)
    (root / SCORE_TYPES_FILE).write_text(
        json.dumps({key: value.model_dump(exclude_none=True) for key, value in score_types.items()}, indent=2) + "\n"
    )
    (root / HARBOR_JOB_FILE).write_text(
        "datasets:\n  - path: .\nmetrics:\n  - type: uv-script\n    kwargs:\n      script_path: metric.py\n"
    )
    return task_ids


def dataset_problems(root: Path, details: Iterable[Path] = ()) -> list[str]:
    """Every problem with the Harbor dataset at `root`, or an empty list.

    `details` are `details.json` files a grader wrote on sample tasks. Each must be a valid
    `TaskEvaluation` that uses only declared score types.
    """
    problems: list[str] = []
    score_types = _read_score_types(root, problems)
    try:
        if read_metric_script(root) is None:
            problems.append(f"{METRIC_FILE}: missing. Write it with write_dataset.")
    except MetricScriptError as error:
        problems.append(str(error))
    try:
        task_dirs = find_task_folders(root)
    except (TaskFolderError, OSError) as error:
        return [*problems, str(error)]
    names: dict[str, str] = {}
    for task_id, task_dir in task_dirs.items():
        problems.extend(f"{task_id}: {problem}" for problem in task_problems(task_dir))
        problems.extend(_check_task(task_id, task_dir, names))
    for path in details:
        problems.extend(_check_details(path, score_types))
    return problems


def _read_score_types(root: Path, problems: list[str]) -> dict[str, ScoreType] | None:
    try:
        raw = json.loads((root / SCORE_TYPES_FILE).read_text())
        return {key: ScoreType.model_validate(value) for key, value in raw.items()}
    except (OSError, ValueError, AttributeError) as error:
        problems.append(f"{SCORE_TYPES_FILE}: {error}")
        return None


def _check_task(task_id: str, task_dir: Path, names: dict[str, str]) -> list[str]:
    try:
        config = tomllib.loads((task_dir / TASK_FILE).read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        return [f"{task_id}: {TASK_FILE}: {error}"]
    problems: list[str] = []
    try:
        instruction = (task_dir / INSTRUCTION_FILE).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        instruction = ""
    if not instruction.strip():
        problems.append(f"{task_id}: {INSTRUCTION_FILE} must be readable UTF-8 and not empty")
    name = _table(config, "task").get("name")
    if not _valid_name(name):
        problems.append(f"{task_id}: [task].name must be lowercase <org>/<task>, got {name!r}")
    elif name in names:
        problems.append(f"{task_id}: [task].name {name!r} is also used by {names[name]}")
    else:
        names[name] = task_id
    verifier = _table(config, "verifier")
    if verifier.get("environment_mode") != "separate":
        problems.append(f"{task_id}: [verifier].environment_mode must be 'separate'")
    result_file = _table(_table(config, "metadata"), "valkyrie").get("verifier_result_file")
    if result_file != VERIFIER_RESULT_FILE:
        problems.append(f"{task_id}: metadata.valkyrie.verifier_result_file must be {VERIFIER_RESULT_FILE!r}")
    image = _table(config, "environment").get("docker_image")
    if image is not None and not _PINNED_IMAGE.fullmatch(str(image)):
        problems.append(f"{task_id}: [environment].docker_image must be pinned by digest")
    verifier_image = _table(verifier, "environment").get("docker_image")
    if verifier_image is not None and not _PINNED_IMAGE.fullmatch(str(verifier_image)):
        problems.append(f"{task_id}: [verifier.environment].docker_image must be pinned by digest")
    problems.extend(_agent_input_problems(task_id, config))
    test_script = task_dir / "tests/test.sh"
    if not test_script.is_file() or not os.access(test_script, os.X_OK):
        problems.append(f"{task_id}: tests/test.sh must exist and be executable")
    dockerfile = task_dir / "tests/Dockerfile"
    lines = dockerfile.read_text(errors="replace").splitlines() if dockerfile.is_file() else []
    first = lines[0].split() if lines else []
    if len(first) < 2 or first[0] != "FROM" or not _PINNED_IMAGE.fullmatch(first[1]):
        problems.append(f"{task_id}: tests/Dockerfile must start with FROM a grader image pinned by digest")
    return problems


def _agent_input_problems(task_id: str, config: Mapping[str, Any]) -> list[str]:
    environment = _table(config, "environment")
    verifier_environment = _table(_table(config, "verifier"), "environment")
    problems = [
        f"{task_id}: [verifier.environment].{key} is ignored. Declare it under [environment] for the agent."
        for key in _AGENT_ONLY_KEYS
        if key in verifier_environment
    ]
    declared: object = environment.get("mcp_servers", [])
    if not isinstance(declared, list):
        problems.append(f"{task_id}: [environment].mcp_servers must be an array of tables, [[environment.mcp_servers]]")
    servers = cast(list[object], declared) if isinstance(declared, list) else []
    names: list[str] = []
    for index, server in enumerate(servers):
        try:
            names.append(MCPServer.model_validate(server).name)
        except ValidationError as error:
            reasons = "; ".join(_field_reason(item["loc"], item["msg"]) for item in error.errors())
            problems.append(f"{task_id}: [environment].mcp_servers[{index}]: {reasons}")
    problems.extend(
        f"{task_id}: [environment].mcp_servers declares {name!r} more than once"
        for name in sorted({name for name in names if names.count(name) > 1})
    )
    skills_dir = environment.get("skills_dir")
    if skills_dir is not None and not _is_sandbox_folder(skills_dir):
        problems.append(f"{task_id}: [environment].skills_dir must be a normalized absolute path, got {skills_dir!r}")
    return problems


def _field_reason(loc: tuple[int | str, ...], message: str) -> str:
    reason = message.removeprefix("Value error, ")
    return f"{'.'.join(map(str, loc))}: {reason}" if loc else reason


def _table(config: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = config.get(key)
    return value if isinstance(value, dict) else {}


def _check_details(path: Path, score_types: Mapping[str, ScoreType] | None) -> list[str]:
    try:
        evaluation = TaskEvaluation.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as error:
        return [f"{path}: not a TaskEvaluation: {error}"]
    undeclared = sorted(set(evaluation.scores) - set(score_types or {}))
    return [f"{path}: scores {undeclared} are not declared in {SCORE_TYPES_FILE}"] if undeclared else []


def _check_file_paths(paths: Iterable[str]) -> None:
    files = {INSTRUCTION_FILE, TASK_FILE}
    for path in paths:
        pure = PurePosixPath(path)
        if not path or pure.is_absolute() or ".." in pure.parts or pure.as_posix() != path or "\\" in path:
            raise ValueError(f"File path must be a plain relative POSIX path such as 'a/b.json': {path!r}")
        if path in (INSTRUCTION_FILE, TASK_FILE):
            raise ValueError(f"File path is reserved by the task folder: {path!r}")
        files.add(path)
    for path in sorted(files):
        for parent in PurePosixPath(path).parents:
            if str(parent) in files:
                raise ValueError(f"File path {path!r} is below {str(parent)!r}, which is a file")


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def _toml_key(key: str) -> str:
    return key if _BARE_KEY.fullmatch(key) else _toml_string(key)


def _is_inline(value: TomlValue) -> TypeGuard[TomlScalar | list[TomlScalar]]:
    return not isinstance(value, dict) and not (isinstance(value, list) and any(isinstance(i, dict) for i in value))


def _toml_value(value: TomlScalar | list[TomlScalar]) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return _toml_string(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    return json.dumps(value) if isinstance(value, int) else repr(float(value))


def _toml_document(data: Mapping[str, TomlValue]) -> str:
    lines: list[str] = []

    def table(name: str, values: Mapping[str, TomlValue], *, array: bool = False) -> None:
        scalars = {key: value for key, value in values.items() if _is_inline(value)}
        if array:
            lines.append(f"\n[[{name}]]")
        elif name and (scalars or not values):
            lines.append(f"\n[{name}]")
        lines.extend(f"{_toml_key(key)} = {_toml_value(value)}" for key, value in scalars.items())
        for key, value in values.items():
            child = f"{name}.{_toml_key(key)}" if name else _toml_key(key)
            if isinstance(value, dict):
                table(child, value)
            elif not _is_inline(value):
                for item in cast(list[dict[str, TomlValue]], value):
                    table(child, item, array=True)

    table("", data)
    return "\n".join(lines).lstrip("\n") + "\n"
