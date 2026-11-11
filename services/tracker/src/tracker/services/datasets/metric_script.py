import ast
import hashlib
import re
import tomllib

METRIC_FILE = "metric.py"
MAX_METRIC_BYTES = 256 * 1024
SCRIPT_BLOCK = re.compile(r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$")


class InvalidMetricScriptError(ValueError):
    pass


def file_digest(content: bytes) -> str:
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def check_metric_script(content: bytes) -> None:
    if len(content) > MAX_METRIC_BYTES:
        raise InvalidMetricScriptError(f"{METRIC_FILE} is larger than {MAX_METRIC_BYTES} bytes")
    try:
        source = content.decode()
        ast.parse(source, filename=METRIC_FILE)
    except (UnicodeDecodeError, SyntaxError, ValueError) as error:
        raise InvalidMetricScriptError(f"{METRIC_FILE} is not valid Python: {error}") from error
    blocks = [match for match in SCRIPT_BLOCK.finditer(source) if match.group("type") == "script"]
    if len(blocks) > 1:
        raise InvalidMetricScriptError(f"{METRIC_FILE} has more than one `# /// script` block")
    if blocks:
        header = "".join(
            line[2:] if line.startswith("# ") else line[1:]
            for line in blocks[0].group("content").splitlines(keepends=True)
        )
        try:
            tomllib.loads(header)
        except tomllib.TOMLDecodeError as error:
            raise InvalidMetricScriptError(
                f"The `# /// script` block of {METRIC_FILE} is not valid TOML: {error}"
            ) from error
