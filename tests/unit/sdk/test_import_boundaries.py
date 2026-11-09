from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SDK_SOURCE = "packages/valkyrie-sdk/src"
SOURCE_ROOTS = (SDK_SOURCE, "src")
DYNAMIC_IMPORTERS = {"import_module", "__import__"}
SDK = "valkyrie.sdk"
PUBLIC_SDK = (SDK, f"{SDK}.models", f"{SDK}.errors")


@dataclass(frozen=True)
class Boundary:
    name: str
    paths: tuple[str, ...]
    forbidden: tuple[str, ...]
    lazy_allowed: dict[str, tuple[str, ...]] = field(default_factory=dict)
    sdk_allowed: tuple[str, ...] | None = None


BOUNDARIES = (
    Boundary(
        name="sdk",
        paths=("packages/valkyrie-sdk/src", "tests/unit/sdk"),
        forbidden=("tracker", "valkyrie.cli", "valkyrie.tui", "valkyrie.display"),
    ),
    Boundary(
        name="tui",
        paths=("src/valkyrie/tui",),
        forbidden=("tracker", "valkyrie.cli", "httpx"),
        sdk_allowed=PUBLIC_SDK,
    ),
    Boundary(
        name="cli",
        paths=("src/valkyrie/cli",),
        forbidden=("tracker", "valkyrie.tui", "httpx"),
        lazy_allowed={"src/valkyrie/cli/ui.py": ("valkyrie.tui",)},
        sdk_allowed=PUBLIC_SDK,
    ),
    Boundary(
        name="display",
        paths=("src/valkyrie/display.py",),
        forbidden=("tracker", "valkyrie.cli", "valkyrie.tui"),
    ),
)


@dataclass(frozen=True)
class Import:
    modules: tuple[str, ...]
    line: int
    in_function: bool


def _package(path: Path, root: Path) -> tuple[str, ...]:
    relative = path.relative_to(root)
    for source_root in SOURCE_ROOTS:
        if relative.is_relative_to(source_root):
            relative = relative.relative_to(source_root)
            break
    return relative.parent.parts


def _resolve(node: ast.ImportFrom, package: tuple[str, ...], path: Path) -> str:
    if node.level == 0:
        return node.module or ""
    if node.level - 1 > len(package):
        raise AssertionError(f"{path}:{node.lineno}: relative import escapes the source root")
    base = package[: len(package) - (node.level - 1)]
    return ".".join([*base, *([node.module] if node.module else [])])


def _imported_modules(node: ast.AST, package: tuple[str, ...], path: Path) -> list[tuple[str, ...]]:
    if isinstance(node, ast.Import):
        return [(alias.name,) for alias in node.names]
    if isinstance(node, ast.ImportFrom):
        module = _resolve(node, package, path)
        return [(module, f"{module}.{alias.name}".lstrip(".")) for alias in node.names]
    if isinstance(node, ast.Call) and node.args:
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        first = node.args[0]
        if name in DYNAMIC_IMPORTERS and isinstance(first, ast.Constant) and isinstance(first.value, str):
            return [(first.value,)]
    return []


def iter_imports(path: Path, root: Path) -> Iterator[Import]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package = _package(path, root)

    def visit(node: ast.AST, in_function: bool) -> Iterator[Import]:
        for modules in _imported_modules(node, package, path):
            yield Import(modules, getattr(node, "lineno", 0), in_function)
        nested = in_function or isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda)
        for child in ast.iter_child_nodes(node):
            yield from visit(child, nested)

    yield from visit(tree, in_function=False)


def _matches(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(f"{prefix}.") for prefix in prefixes)


def _python_files(boundary: Boundary, root: Path) -> list[Path]:
    files: list[Path] = []
    for entry in boundary.paths:
        path = root / entry
        assert path.exists(), f"{boundary.name} boundary path {entry} does not exist"
        found = [path] if path.is_file() else sorted(path.rglob("*.py"))
        assert found, f"{boundary.name} boundary path {entry} has no Python files"
        files.extend(found)
    return files


def sdk_modules(root: Path) -> frozenset[str]:
    source = root / SDK_SOURCE
    package = source / SDK.replace(".", "/")
    assert package.is_dir(), f"SDK package {package.relative_to(root)} does not exist"
    modules = set()
    for path in package.rglob("*.py"):
        parts = path.relative_to(source).with_suffix("").parts
        modules.add(".".join(parts[:-1] if parts[-1] == "__init__" else parts))
    return frozenset(modules)


def _private_sdk_module(found: Import, allowed: tuple[str, ...], modules: frozenset[str]) -> str | None:
    imported, *names = found.modules
    if _matches(imported, (SDK,)) and imported not in allowed:
        return imported
    return next((name for name in names if name in modules and name not in allowed), None)


def find_violations(boundary: Boundary, root: Path = REPO_ROOT) -> list[str]:
    violations: list[str] = []
    modules = sdk_modules(root) if boundary.sdk_allowed is not None else frozenset()
    for path in _python_files(boundary, root):
        relative = path.relative_to(root).as_posix()
        lazy_allowed = boundary.lazy_allowed.get(relative, ())
        for found in iter_imports(path, root):
            module = next((module for module in found.modules if _matches(module, boundary.forbidden)), None)
            if module is not None and not (found.in_function and _matches(module, lazy_allowed)):
                violations.append(f"{relative}:{found.line} imports {module}")
            if boundary.sdk_allowed is not None:
                private = _private_sdk_module(found, boundary.sdk_allowed, modules)
                if private is not None:
                    violations.append(f"{relative}:{found.line} imports non-public SDK module {private}")
    return sorted(set(violations))


@pytest.mark.description("Client packages import only what their boundary allows.")
@pytest.mark.parametrize("boundary", BOUNDARIES, ids=lambda boundary: boundary.name)
def test_import_boundary(boundary: Boundary) -> None:
    assert find_violations(boundary) == []


@pytest.mark.description("Every file listed in a boundary's `lazy_allowed` exists.")
def test_lazy_allowances_name_existing_files() -> None:
    for boundary in BOUNDARIES:
        for relative in boundary.lazy_allowed:
            assert (REPO_ROOT / relative).is_file(), f"{boundary.name}: {relative} does not exist"


@pytest.mark.description("A forbidden import planted in a boundary path shows up in `find_violations`.")
@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("from valkyrie.cli.display import format_table\n", "valkyrie.cli.display"),
        ("import tracker.models\n", "tracker.models"),
        ("from valkyrie import cli\n", "valkyrie.cli"),
        ("from ..cli import display\n", "valkyrie.cli"),
        ("from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import valkyrie.cli\n", "valkyrie.cli"),
        ("def f():\n    from valkyrie.cli import main\n", "valkyrie.cli"),
        ("import importlib\nimportlib.import_module('valkyrie.cli.main')\n", "valkyrie.cli.main"),
    ],
)
def test_planted_violation_is_reported(tmp_path: Path, source: str, expected: str) -> None:
    tui = tmp_path / "src/valkyrie/tui"
    tui.mkdir(parents=True)
    (tui / "planted.py").write_text(source)
    boundary = Boundary(name="tui", paths=("src/valkyrie/tui",), forbidden=("tracker", "valkyrie.cli"))

    assert any(expected in violation for violation in find_violations(boundary, tmp_path))


@pytest.mark.description("An SDK import outside the public modules shows up in `find_violations`.")
@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("from valkyrie.sdk.login import SessionAuth\n", "valkyrie.sdk.login"),
        ("import valkyrie.sdk.client\n", "valkyrie.sdk.client"),
        ("from valkyrie.sdk import login\n", "valkyrie.sdk.login"),
        ("from valkyrie.sdk.models.datasets import ResourceResponse\n", "valkyrie.sdk.models.datasets"),
        ("from valkyrie.sdk.models import datasets\n", "valkyrie.sdk.models.datasets"),
        ("def f():\n    from valkyrie.sdk.login import SessionAuth\n", "valkyrie.sdk.login"),
        ("import importlib\nimportlib.import_module('valkyrie.sdk.login')\n", "valkyrie.sdk.login"),
        ("import httpx\n", "httpx"),
    ],
)
def test_non_public_sdk_import_is_reported(tmp_path: Path, source: str, expected: str) -> None:
    sdk = tmp_path / SDK_SOURCE / "valkyrie/sdk"
    (sdk / "models").mkdir(parents=True)
    for module in ("__init__", "login", "client", "errors", "models/__init__", "models/datasets"):
        (sdk / f"{module}.py").write_text("")
    tui = tmp_path / "src/valkyrie/tui"
    tui.mkdir(parents=True)
    (tui / "planted.py").write_text(source)
    boundary = Boundary(name="tui", paths=("src/valkyrie/tui",), forbidden=("httpx",), sdk_allowed=PUBLIC_SDK)

    assert any(violation.endswith(expected) for violation in find_violations(boundary, tmp_path))


@pytest.mark.description("Names and modules from `valkyrie.sdk`, its `models` and its `errors` pass the SDK allowlist.")
def test_public_sdk_imports_pass(tmp_path: Path) -> None:
    sdk = tmp_path / SDK_SOURCE / "valkyrie/sdk"
    (sdk / "models").mkdir(parents=True)
    for module in ("__init__", "errors", "login", "models/__init__", "models/datasets"):
        (sdk / f"{module}.py").write_text("")
    cli = tmp_path / "src/valkyrie/cli"
    cli.mkdir(parents=True)
    (cli / "main.py").write_text(
        "from valkyrie.sdk import SessionAuth, ValkyrieClient, errors, models\n"
        "from valkyrie.sdk.models import ResourceResponse\n"
        "from valkyrie.sdk.errors import ValkyrieSDKError\n"
        "import valkyrie.sdk.models\n"
    )
    boundary = Boundary(name="cli", paths=("src/valkyrie/cli",), forbidden=(), sdk_allowed=PUBLIC_SDK)

    assert find_violations(boundary, tmp_path) == []


@pytest.mark.description("A lazy allowance covers only imports inside functions.")
def test_lazy_allowance_only_covers_function_scope(tmp_path: Path) -> None:
    cli = tmp_path / "src/valkyrie/cli"
    cli.mkdir(parents=True)
    (cli / "ui.py").write_text("def ui():\n    from valkyrie.tui.app import App\n")
    (cli / "main.py").write_text("from valkyrie.tui.app import App\n")
    boundary = Boundary(
        name="cli",
        paths=("src/valkyrie/cli",),
        forbidden=("valkyrie.tui",),
        lazy_allowed={"src/valkyrie/cli/ui.py": ("valkyrie.tui",)},
    )

    assert find_violations(boundary, tmp_path) == ["src/valkyrie/cli/main.py:1 imports valkyrie.tui.app"]

    (cli / "ui.py").write_text("from valkyrie.tui.app import App\n")
    assert "src/valkyrie/cli/ui.py:1 imports valkyrie.tui.app" in find_violations(boundary, tmp_path)


@pytest.mark.description("An SDK allowlist without the SDK source fails with `does not exist`.")
def test_missing_sdk_source_fails(tmp_path: Path) -> None:
    tui = tmp_path / "src/valkyrie/tui"
    tui.mkdir(parents=True)
    (tui / "app.py").write_text("from valkyrie.sdk import ValkyrieClient\n")
    boundary = Boundary(name="tui", paths=("src/valkyrie/tui",), forbidden=(), sdk_allowed=PUBLIC_SDK)

    with pytest.raises(AssertionError, match="does not exist"):
        find_violations(boundary, tmp_path)


@pytest.mark.description("A boundary that names a missing path fails with `does not exist`.")
def test_missing_boundary_path_fails(tmp_path: Path) -> None:
    boundary = Boundary(name="tui", paths=("src/valkyrie/tui",), forbidden=("valkyrie.cli",))

    with pytest.raises(AssertionError, match="does not exist"):
        find_violations(boundary, tmp_path)
