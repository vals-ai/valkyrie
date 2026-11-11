"""The metric script a dataset folder may carry, checked before it is pushed."""

import ast
import hashlib
import re
import tomllib
from pathlib import Path

METRIC_FILE = "metric.py"
MAX_METRIC_BYTES = 256 * 1024
SCRIPT_BLOCK = re.compile(r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$")


class MetricScriptError(ValueError):
    pass


def metric_digest(content: bytes) -> str:
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def read_metric_script(root: Path) -> bytes | None:
    """The folder's metric.py, or None when it has none, after refusing a script the Tracker would refuse."""
    path = root / METRIC_FILE
    if not path.is_file():
        return None
    content = path.read_bytes()
    if len(content) > MAX_METRIC_BYTES:
        raise MetricScriptError(f"{path} is larger than {MAX_METRIC_BYTES} bytes")
    try:
        source = content.decode()
        ast.parse(source, filename=METRIC_FILE)
    except (UnicodeDecodeError, SyntaxError, ValueError) as error:
        raise MetricScriptError(f"{path} is not valid Python: {error}") from error
    blocks = [match for match in SCRIPT_BLOCK.finditer(source) if match.group("type") == "script"]
    if len(blocks) > 1:
        raise MetricScriptError(f"{path} has more than one `# /// script` block")
    if blocks:
        header = "".join(
            line[2:] if line.startswith("# ") else line[1:]
            for line in blocks[0].group("content").splitlines(keepends=True)
        )
        try:
            tomllib.loads(header)
        except tomllib.TOMLDecodeError as error:
            raise MetricScriptError(f"The `# /// script` block of {path} is not valid TOML: {error}") from error
    return content
