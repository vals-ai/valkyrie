import ast
import subprocess
import sys

SKIP = ("services/tracker/src/tracker/database/migrations/", "/vendor/", "infra/")
CAP = 200
OPTION_CAP = 120
OPTION_CALLS = ("option", "argument")
BANNED = {";": "semicolon", "\u2014": "em dash"}


def text(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def is_marker(node: ast.expr) -> bool:
    return isinstance(node, ast.Call) and ast.unparse(node.func) == "pytest.mark.description"


def is_fixture(node: ast.expr) -> bool:
    return "fixture" in ast.unparse(node)


def check_text(path: str, line: int, value: str, cap: int) -> list[str]:
    found = [f"{path}:{line}: {name}" for char, name in BANNED.items() if char in value]
    for paragraph in value.split("\n\n"):
        if paragraph.startswith(("Example:", " ")):
            continue
        if paragraph.startswith("Test cases:"):
            if any(len(bullet.strip()) > OPTION_CAP for bullet in paragraph.splitlines()):
                found.append(f"{path}:{line}: test case over {OPTION_CAP} characters")
        elif len(paragraph.strip()) > cap:
            found.append(f"{path}:{line}: paragraph over {cap} characters")
    return found


def missing_markers(path: str, body: list[ast.stmt]) -> list[str]:
    found = []
    for node in body:
        if isinstance(node, ast.ClassDef) and not any("TestCase" in ast.unparse(base) for base in node.bases):
            found.extend(missing_markers(path, node.body))
        elif (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test")
            and not any(is_marker(d) or is_fixture(d) for d in node.decorator_list)
        ):
            found.append(f"{path}:{node.lineno}: test without description marker")
    return found


def violations(path: str, source: str) -> list[str]:
    tree = ast.parse(source)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if is_marker(node) and node.args and (value := text(node.args[0])) is not None:
            found.extend(check_text(path, node.lineno, value, CAP))
        cap = OPTION_CAP if ast.unparse(node.func).endswith(OPTION_CALLS) else CAP
        for keyword in node.keywords:
            if keyword.arg in ("description", "summary", "help") and (value := text(keyword.value)) is not None:
                found.extend(check_text(path, keyword.value.lineno, value, cap))
    if path.rsplit("/", 1)[-1].startswith("test_"):
        found.extend(missing_markers(path, tree.body))
    return found


def main() -> int:
    files = subprocess.check_output(["git", "ls-files", "*.py"], text=True).split()
    found: list[str] = []
    for path in files:
        if not any(part in path for part in SKIP):
            with open(path, encoding="utf-8") as file:
                found.extend(violations(path, file.read()))
    for line in found:
        print(line)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
