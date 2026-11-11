import ipaddress
import posixpath
import re
import tomllib
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Self, TypeGuard

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    FiniteFloat,
    field_validator,
    model_validator,
)

USER_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")
HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
IPV4_RANGE = re.compile(r"[0-9]{1,3}(?:\.[0-9]{1,3}){3}/[0-9]{1,2}")
SERVICE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*")
MAIN_SERVICE = "main"
MAX_ARTIFACTS = 20
MAX_STEPS = 100
MAX_NAME_PART_BYTES = 200
MAX_NAME_BYTES = 700
HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
HEADER_TEMPLATE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:(?!\$\{)[^}])*))?\}")
HEADER_CREDENTIAL = re.compile(
    rf"\s*(?:[A-Za-z]+\s+)?{HEADER_TEMPLATE.pattern}(?:[\s:-]*{HEADER_TEMPLATE.pattern})*\s*"
)
SENSITIVE_NAME = re.compile(r"KEY|SECRET|TOKEN(?!S)|PASSWORD|CREDENTIAL|AUTH", re.IGNORECASE)
COOKIE_HEADERS = {"cookie", "set-cookie"}
STEP_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
PACKAGE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]*/[a-zA-Z0-9][a-zA-Z0-9._-]*")
SIZE_UNITS = {"G": 1024.0, "M": 1.0, "K": 1 / 1024}


class NetworkMode(StrEnum):
    NO_NETWORK = "no-network"
    PUBLIC = "public"
    ALLOWLIST = "allowlist"


class TaskOS(StrEnum):
    LINUX = "linux"
    WINDOWS = "windows"


class VerifierEnvironmentMode(StrEnum):
    SHARED = "shared"
    SEPARATE = "separate"


class MultiStepRewardStrategy(StrEnum):
    MEAN = "mean"
    FINAL = "final"


def is_table(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def _is_ipv4_range(value: str) -> bool:
    if not IPV4_RANGE.fullmatch(value):
        return False
    try:
        ipaddress.IPv4Network(value)
    except ValueError:
        return False
    return True


def _normalize_hosts(hosts: list[str] | None) -> list[str] | None:
    if hosts is None:
        return None
    normalized: list[str] = []
    for entry in hosts:
        host = entry.strip().lower()
        if _is_ipv4_range(host):
            normalized.append(host)
            continue
        host = host.rstrip(".")
        if not host:
            raise ValueError("allowed_hosts entries must be non-empty hostnames or IPv4 ranges.")
        if "://" in host or "/" in host or ":" in host:
            raise ValueError(
                "allowed_hosts entries must be hostnames or IPv4 ranges such as 10.0.0.0/8, not URLs, ports or paths."
            )
        suffix = host.removeprefix("*.")
        if not suffix or "*" in suffix:
            raise ValueError("allowed_hosts wildcards must be a leading '*.' followed by a hostname.")
        if not all(HOST_LABEL.fullmatch(label) for label in suffix.split(".")):
            raise ValueError("allowed_hosts entries must be hostnames made of letters, digits, hyphens and dots.")
        normalized.append(host)
    return normalized


def _size_in_mb(size: str) -> int:
    text = size.strip().upper()
    unit = text[-1:]
    if unit not in SIZE_UNITS:
        raise ValueError(f"Invalid size: {size!r}. Use a number with a unit, such as '1G' or '512M'.")
    return int(float(text[:-1]) * SIZE_UNITS[unit])


class NetworkSettings(BaseModel):
    network_mode: NetworkMode | None = None
    allowed_hosts: list[str] | None = None

    @field_validator("allowed_hosts")
    @classmethod
    def check_hosts(cls, hosts: list[str] | None) -> list[str] | None:
        return _normalize_hosts(hosts)

    @model_validator(mode="after")
    def hosts_need_an_allowlist(self) -> Self:
        if self.allowed_hosts and self.network_mode != NetworkMode.ALLOWLIST:
            raise ValueError("allowed_hosts is only valid when network_mode='allowlist'.")
        return self


class Author(BaseModel):
    name: str
    email: str | None = None


class PackageInfo(BaseModel):
    name: str
    version: str | None = None
    description: str = ""
    authors: list[Author] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def check_name(cls, name: str) -> str:
        if not PACKAGE_NAME.fullmatch(name) or ".." in name:
            raise ValueError(
                f"The task name must look like 'org/name' with letters, digits, '.', '_' and '-'. Got: {name}"
            )
        return name


class HealthcheckConfig(BaseModel):
    command: str
    interval_sec: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    timeout_sec: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    start_period_sec: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    start_interval_sec: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    retries: int = Field(default=3, ge=1)


def _check_header(name: str, value: str) -> None:
    if not HEADER_NAME.fullmatch(name):
        raise ValueError(f"{name!r} is not a valid HTTP header name.")
    if "\r" in value or "\n" in value:
        raise ValueError(f"the header {name!r} must not contain newlines.")
    if "${" in HEADER_TEMPLATE.sub("", value):
        raise ValueError(f"the header {name!r} has a malformed ${{NAME}} reference.")
    sensitive_header = bool(SENSITIVE_NAME.search(name)) or name.lower() in COOKIE_HEADERS
    for variable, default in HEADER_TEMPLATE.findall(value):
        if default and (sensitive_header or SENSITIVE_NAME.search(variable)):
            raise ValueError(f"the header {name!r} must not carry a default for the credential ${{{variable}}}.")
    if sensitive_header and value.strip() and HEADER_CREDENTIAL.fullmatch(value) is None:
        raise ValueError(
            f"the header {name!r} must reference its credential as ${{NAME}} without a default, "
            "such as 'Bearer ${TOKEN}'."
        )


class MCPServerConfig(BaseModel):
    name: str
    transport: Literal["sse", "streamable-http", "stdio"] = "sse"
    url: str | None = None
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    headers: dict[str, str] = Field(default_factory=dict, exclude_if=lambda headers: not headers)

    @field_validator("transport", mode="before")
    @classmethod
    def accept_http(cls, value: Any) -> Any:
        return "streamable-http" if value == "http" else value

    @model_validator(mode="after")
    def transport_has_its_target(self) -> Self:
        if self.transport == "stdio" and not self.command:
            raise ValueError("an MCP server with transport 'stdio' needs a command.")
        if self.transport != "stdio" and not self.url:
            raise ValueError(f"an MCP server with transport {self.transport!r} needs a url.")
        if self.transport == "stdio" and self.headers:
            raise ValueError("headers are only for an MCP server over http.")
        for name, value in self.headers.items():
            _check_header(name, value)
        return self


def _check_user(value: Any) -> Any:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("a user is a name or a numeric UID.")
    user = str(value)
    if not USER_NAME.fullmatch(user):
        raise ValueError(f"a user is a name or UID made of letters, digits, '_', '.' and '-'. Got: {user!r}")
    return user


SandboxUser = Annotated[str, BeforeValidator(_check_user)]


def _check_service(name: str | None) -> str | None:
    if name is not None and not SERVICE_NAME.fullmatch(name):
        raise ValueError(f"service names use letters, digits, '_', '.' and '-'. Got: {name!r}")
    return name


class ArtifactConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    destination: str | None = None
    exclude: list[str] = Field(default_factory=list)
    service: str | None = None

    @model_validator(mode="before")
    @classmethod
    def from_path(cls, data: Any) -> Any:
        return {"source": data} if isinstance(data, str) else data

    @field_validator("service")
    @classmethod
    def check_service(cls, service: str | None) -> str | None:
        return _check_service(service)

    @model_validator(mode="after")
    def check_paths(self) -> Self:
        if not self.destination and ".." in PurePosixPath(self.source).parts:
            raise ValueError(f"an artifact source must not contain '..'. Got: {self.source!r}")
        if self.service not in (None, MAIN_SERVICE) and not self.source.startswith("/"):
            raise ValueError(
                f"an artifact from service {self.service!r} needs an absolute source. Got: {self.source!r}"
            )
        parts = self.name.split("/")
        if (
            not all(part not in ("", ".", "..") and len(part.encode()) <= MAX_NAME_PART_BYTES for part in parts)
            or "\\" in self.name
            or "\x00" in self.name
            or len(self.name.encode()) > MAX_NAME_BYTES
        ):
            raise ValueError(f"an artifact's name must be a relative path of ordinary parts. Got: {self.name!r}")
        return self

    @property
    def name(self) -> str:
        name = self.destination or posixpath.normpath(self.source).strip("/")
        return name if self.service in (None, MAIN_SERVICE) else f"{self.service}/{name}"


class CollectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str
    service: str = MAIN_SERVICE
    timeout_sec: float = 60.0
    user: SandboxUser | None = None

    @field_validator("service")
    @classmethod
    def check_service(cls, service: str) -> str:
        return _check_service(service) or MAIN_SERVICE


class AgentConfig(NetworkSettings):
    timeout_sec: float | None = None
    user: SandboxUser | None = None


class EnvironmentConfig(NetworkSettings):
    network_mode: NetworkMode | None = NetworkMode.PUBLIC
    build_timeout_sec: float = 600.0
    docker_image: str | None = None
    os: TaskOS = TaskOS.LINUX
    cpus: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    memory_mb: int | None = Field(default=None, gt=0)
    storage_mb: int | None = Field(default=None, gt=0)
    gpus: int | None = None
    gpu_types: list[str] | None = None
    env: dict[str, str] = Field(default_factory=dict)
    skills_dir: str | None = None
    healthcheck: HealthcheckConfig | None = None
    mcp_servers: list[MCPServerConfig] = Field(default_factory=list)
    workdir: str | None = None

    @field_validator("mcp_servers")
    @classmethod
    def a_later_server_replaces_an_earlier_one_of_the_same_name(
        cls, servers: list[MCPServerConfig]
    ) -> list[MCPServerConfig]:
        return list({server.name: server for server in servers}.values())

    @field_validator("os", mode="before")
    @classmethod
    def lowercase_os(cls, value: Any) -> Any:
        return value.lower() if isinstance(value, str) else value

    @model_validator(mode="before")
    @classmethod
    def accept_older_names(cls, data: Any) -> Any:
        if not is_table(data):
            return data
        data = dict(data)
        allow_internet = data.pop("allow_internet", None)
        if allow_internet is not None and "network_mode" not in data and "allowed_hosts" not in data:
            data["network_mode"] = NetworkMode.PUBLIC if allow_internet else NetworkMode.NO_NETWORK
        for old, new in (("memory", "memory_mb"), ("storage", "storage_mb")):
            if old not in data:
                continue
            value = data.pop(old)
            if isinstance(value, str):
                megabytes = _size_in_mb(value)
                if new in data and data[new] != megabytes:
                    raise ValueError(f"Conflicting '{old}' and '{new}' values: {value!r} is not {data[new]!r} MB.")
                data.setdefault(new, megabytes)
        return data


class VerifierConfig(NetworkSettings):
    timeout_sec: float = 600.0
    env: dict[str, str] = Field(default_factory=dict)
    user: SandboxUser | None = None
    environment_mode: VerifierEnvironmentMode | None = None
    environment: EnvironmentConfig | None = None
    collect: list[CollectConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def mode_agrees_with_environment(self) -> Self:
        if self.environment_mode == VerifierEnvironmentMode.SHARED and self.environment is not None:
            raise ValueError(
                "[verifier].environment_mode='shared' is incompatible with [verifier.environment]; omit the "
                "environment or set environment_mode='separate'."
            )
        return self

    @property
    def is_separate(self) -> bool:
        return self.environment_mode == VerifierEnvironmentMode.SEPARATE or (
            self.environment_mode is None and self.environment is not None
        )


class SolutionConfig(BaseModel):
    env: dict[str, str] = Field(default_factory=dict)


class StepConfig(BaseModel):
    name: str
    agent: AgentConfig = Field(default_factory=AgentConfig)
    verifier: VerifierConfig = Field(default_factory=VerifierConfig)
    min_reward: FiniteFloat | dict[str, FiniteFloat] | None = None
    healthcheck: HealthcheckConfig | None = None
    artifacts: list[ArtifactConfig] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def check_name(cls, name: str) -> str:
        if not STEP_NAME.fullmatch(name) or len(name.encode()) > MAX_NAME_PART_BYTES:
            raise ValueError(f"a step's name is a folder name of letters, digits, '_', '.' and '-'. Got: {name!r}")
        return name


class TaskConfig(BaseModel):
    schema_version: str = "1.3"
    task: PackageInfo | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    verifier: VerifierConfig = Field(default_factory=VerifierConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)
    environment: EnvironmentConfig = Field(default_factory=EnvironmentConfig)
    solution: SolutionConfig = Field(default_factory=SolutionConfig)
    artifacts: list[ArtifactConfig] = Field(default_factory=list)
    steps: list[StepConfig] = Field(default_factory=list, max_length=MAX_STEPS)
    multi_step_reward_strategy: MultiStepRewardStrategy | None = None

    @model_validator(mode="before")
    @classmethod
    def accept_version_key(cls, data: Any) -> Any:
        if is_table(data) and "version" in data:
            data = dict(data)
            data.setdefault("schema_version", data.pop("version"))
        return data

    @model_validator(mode="after")
    def artifacts_are_distinct(self) -> Self:
        task_names = [artifact.name for artifact in self.artifacts]
        for artifacts in (self.artifacts, *(step.artifacts for step in self.steps)):
            names = [artifact.name for artifact in artifacts]
            if len(names) > MAX_ARTIFACTS:
                raise ValueError(f"a task or a step may name at most {MAX_ARTIFACTS} artifacts.")
            if len(set(names)) != len(names):
                raise ValueError("two artifacts would be stored under the same name.")
        for step in self.steps:
            if len({*task_names, *(artifact.name for artifact in step.artifacts)}) > MAX_ARTIFACTS:
                raise ValueError(
                    f"step {step.name!r} would collect more than {MAX_ARTIFACTS} artifacts with the task's."
                )
        return self

    @model_validator(mode="after")
    def step_names_are_distinct(self) -> Self:
        names = [step.name.lower() for step in self.steps]
        if len(set(names)) != len(names):
            raise ValueError("two steps share a name, ignoring case.")
        return self

    @property
    def reward_strategy(self) -> MultiStepRewardStrategy:
        return self.multi_step_reward_strategy or MultiStepRewardStrategy.MEAN

    @classmethod
    def parse(cls, text: str) -> Self:
        return cls.model_validate(tomllib.loads(text))
