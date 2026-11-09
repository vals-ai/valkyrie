import ast
import io
import re
import subprocess
import sys
import tokenize
from collections.abc import Iterator

PRAGMA = re.compile(r"#\s*(type:\s*ignore|noqa|pyright:|pragma:|fmt:)")
SKIP = ("services/tracker/src/tracker/database/migrations/", "/vendor/")
PUBLIC_DOCSTRINGS = "packages/valkyrie-sdk/src/"
GENERATED_MODELS = f"{PUBLIC_DOCSTRINGS}valkyrie/sdk/models/_generated/"
DEFINITIONS = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
DOCSTRING_OWNERS = (ast.Module, *DEFINITIONS)


def is_public(name: str) -> bool:
    return not name.startswith("_") or name.endswith("__")


def public_definitions(node: ast.AST) -> Iterator[ast.AST]:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, DEFINITIONS):
            if is_public(child.name):
                yield child
                if isinstance(child, ast.ClassDef):
                    yield from public_definitions(child)
        else:
            yield from public_definitions(child)


def public_owners(path: str, tree: ast.Module) -> set[ast.AST]:
    if path.startswith(GENERATED_MODELS):
        return set(public_definitions(tree))
    if not path.startswith(PUBLIC_DOCSTRINGS) or not all(
        is_public(part.removesuffix(".py")) for part in path.removeprefix(PUBLIC_DOCSTRINGS).split("/")
    ):
        return set()
    return {tree, *public_definitions(tree)}


def violations(path: str, source: str) -> list[str]:
    found: list[str] = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type != tokenize.COMMENT:
            continue
        line = token.start[0]
        if (line == 1 and token.string.startswith("#!")) or PRAGMA.search(token.string):
            continue
        found.append(f"{path}:{line}: comment")
    tree = ast.parse(source)
    public = public_owners(path, tree)
    for node in ast.walk(tree):
        if (
            isinstance(node, DOCSTRING_OWNERS)
            and node not in public
            and ast.get_docstring(node, clean=False) is not None
        ):
            found.append(f"{path}:{node.body[0].lineno}: docstring")
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
