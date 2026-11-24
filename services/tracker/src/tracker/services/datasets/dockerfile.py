import posixpath
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

HEREDOC_INSTRUCTIONS = frozenset({"RUN", "COPY", "ADD"})
HEREDOC = re.compile(r"\d*<<(-?)([\"']?)([^\s\"'<>|&;()]+)\2")
CONTINUATION = re.compile(r"\\[ \t]*$")
VARIABLE = re.compile(r"\$(?:\{([A-Za-z_]\w*)(?:(:?[-+])((?:[^{}]|\$\{[^{}]*\})*))?\}|([A-Za-z_]\w*))")


class UnresolvedVariableError(ValueError):
    pass


@dataclass
class DockerfileStage:
    base: str | None
    workdir: str | None = None
    workdir_known: bool = True
    user: str | None = None
    env: dict[str, str] = field(default_factory=dict[str, str])
    args: dict[str, str | None] = field(default_factory=dict[str, str | None])

    @property
    def scope(self) -> dict[str, str | None]:
        return {**self.args, **self.env}


def _skipped(line: str) -> bool:
    stripped = line.strip()
    return not stripped or stripped.startswith("#")


def dockerfile_instructions(text: str) -> Iterator[tuple[str, str]]:
    lines = iter(text.splitlines())
    for line in lines:
        if _skipped(line):
            continue
        while CONTINUATION.search(line):
            line = CONTINUATION.sub("", line)
            following = next((candidate for candidate in lines if not _skipped(candidate)), None)
            if following is None:
                break
            line += following
        if _skipped(line):
            continue
        keyword, *rest = line.split(None, 1)
        arguments = rest[0].strip() if rest else ""
        if keyword.upper() in HEREDOC_INSTRUCTIONS:
            for heredoc in filter(None, map(HEREDOC.fullmatch, _words(arguments))):
                for body in lines:
                    if (body.lstrip("\t") if heredoc[1] else body) == heredoc[3]:
                        break
        yield keyword.upper(), arguments


def _words(text: str) -> list[str]:
    words: list[str] = []
    current, quote, escaped = "", "", False
    for char in text:
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif quote:
            quote = "" if char == quote else quote
        elif char in "'\"":
            quote = char
        elif char.isspace():
            if current:
                words.append(current)
            current = ""
            continue
        current += char
    return [*words, current] if current else words


def _expand(word: str, scope: Mapping[str, str | None]) -> str:
    parts: list[str] = []
    index, quoted = 0, False
    while index < len(word):
        char = word[index]
        if char == "'" and not quoted:
            end = word.find("'", index + 1)
            end = len(word) if end < 0 else end
            parts.append(word[index + 1 : end])
            index = end + 1
        elif char == '"':
            quoted = not quoted
            index += 1
        elif char == "\\" and index + 1 < len(word):
            following = word[index + 1]
            parts.append(following if not quoted or following in '"$\\' else char + following)
            index += 2
        elif match := VARIABLE.match(word, index):
            parts.append(_substitute(match, scope))
            index = match.end()
        elif word.startswith("${", index):
            raise UnresolvedVariableError(word)
        else:
            parts.append(char)
            index += 1
    return "".join(parts)


def _substitute(match: re.Match[str], scope: Mapping[str, str | None]) -> str:
    name = match[1] or match[4]
    if name not in scope:
        raise UnresolvedVariableError(name)
    value, operator = scope[name], match[2]
    if operator is None:
        return value or ""
    is_set = bool(value) if operator.startswith(":") else value is not None
    if operator.endswith("-"):
        return (value or "") if is_set else _expand(match[3], scope)
    return _expand(match[3], scope) if is_set else ""


def _declare_args(
    arguments: str,
    target: dict[str, str | None],
    scope: Mapping[str, str | None],
    global_args: Mapping[str, str | None],
) -> None:
    for word in _words(arguments):
        name, has_default, default = word.partition("=")
        try:
            target[name] = _expand(default, scope) if has_default else global_args.get(name)
        except UnresolvedVariableError:
            target.pop(name, None)


def _set_env(stage: DockerfileStage, arguments: str) -> None:
    words = _words(arguments)
    if words and "=" not in words[0]:
        pairs = [(words[0], arguments[len(words[0]) :].strip())]
    else:
        pairs = [(word.partition("=")[0], word.partition("=")[2]) for word in words]
    scope = stage.scope
    for name, value in pairs:
        try:
            stage.env[name] = _expand(value, scope)
        except UnresolvedVariableError:
            stage.env.pop(name, None)
            stage.args.pop(name, None)


def _set_workdir(stage: DockerfileStage, arguments: str) -> None:
    try:
        path = _expand(arguments, stage.scope)
    except UnresolvedVariableError:
        stage.workdir, stage.workdir_known = None, False
        return
    if path and (stage.workdir_known or path.startswith("/")):
        stage.workdir = posixpath.normpath(posixpath.join(stage.workdir or "/", path))
        stage.workdir_known = True


def _set_user(stage: DockerfileStage, arguments: str) -> None:
    try:
        stage.user = _expand(arguments.strip(), stage.scope) or None
    except UnresolvedVariableError:
        stage.user = None


def _from_stage(
    arguments: str, global_args: Mapping[str, str | None], stages: dict[str, DockerfileStage]
) -> DockerfileStage:
    words = [word for word in _words(arguments) if not word.startswith("--")]
    try:
        image = _expand(words[0], global_args) if words else ""
    except UnresolvedVariableError:
        image = ""
    parent = stages.get(image.lower())
    stage = (
        DockerfileStage(
            parent.base, parent.workdir, parent.workdir_known, parent.user, dict(parent.env), dict(parent.args)
        )
        if parent
        else DockerfileStage(image or None, workdir_known=False)
    )
    if len(words) >= 3 and words[1].upper() == "AS":
        stages[words[2].lower()] = stage
    return stage


@lru_cache
def _final_stage(text: str) -> DockerfileStage:
    global_args: dict[str, str | None] = {}
    stages: dict[str, DockerfileStage] = {}
    stage = DockerfileStage(None, args=global_args)
    for keyword, arguments in dockerfile_instructions(text):
        if keyword == "FROM":
            stage = _from_stage(arguments, global_args, stages)
        elif keyword == "ARG":
            _declare_args(arguments, stage.args, stage.scope, global_args)
        elif keyword == "ENV":
            _set_env(stage, arguments)
        elif keyword == "WORKDIR":
            _set_workdir(stage, arguments)
        elif keyword == "USER":
            _set_user(stage, arguments)
    return stage


def final_stage(dockerfile: Path) -> DockerfileStage:
    return _final_stage(dockerfile.read_text()) if dockerfile.is_file() else DockerfileStage(None)
