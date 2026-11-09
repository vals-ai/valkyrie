import pytest
import ast
from pathlib import Path

TRACKER_ROOT = Path(__file__).resolve().parents[2]
ALLOWED = (
    "src/tracker/repositories/",
    "src/tracker/database/session.py",
    "src/tracker/database/migrations/",
)
QUERY_NAMES = {
    "aliased",
    "and_",
    "asc",
    "case",
    "col",
    "delete",
    "desc",
    "exists",
    "insert",
    "literal",
    "or_",
    "select",
    "selectinload",
    "tuple_",
    "type_coerce",
    "update",
}
SCHEMA_NAMES = {"func", "text"}
SCHEMA_FILE = "src/tracker/database/models.py"
ORM_CALLS = {"add_all", "execute", "merge", "query", "scalar", "scalars"}
SESSION_CALLS = {"add", "delete", "exec", "get", "refresh"}
ORM_MODULES = {"sqlalchemy", "sqlmodel"}


def _violations(path: Path, relative: str) -> list[str]:
    banned = QUERY_NAMES if relative == SCHEMA_FILE else QUERY_NAMES | SCHEMA_NAMES
    found: list[str] = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found += [
                f"{node.lineno}: imports {alias.name}"
                for alias in node.names
                if alias.name.split(".")[0] in ORM_MODULES
            ]
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] in ORM_MODULES:
            found += [f"{node.lineno}: imports {alias.name}" for alias in node.names if alias.name in banned]
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            receiver = node.func.value.func if isinstance(node.func.value, ast.Call) else node.func.value
            name = receiver.id if isinstance(receiver, ast.Name) else getattr(receiver, "attr", "")
            session_like = name == "db" or any(part in name.lower() for part in ("session", "conn"))
            if node.func.attr in ORM_CALLS or (node.func.attr in SESSION_CALLS and session_like):
                found.append(f"{node.lineno}: {name}.{node.func.attr}()")
    return found


@pytest.mark.description("ORM access stays inside the Tracker repositories.")
def test_orm_access_stays_in_repositories() -> None:
    paths = [TRACKER_ROOT / "main.py", *sorted((TRACKER_ROOT / "src/tracker").rglob("*.py"))]
    violations = [
        f"{relative}:{violation}"
        for path in paths
        if not (relative := path.relative_to(TRACKER_ROOT).as_posix()).startswith(ALLOWED)
        for violation in _violations(path, relative)
    ]
    assert not violations, "Move ORM access into src/tracker/repositories/:\n" + "\n".join(violations)
