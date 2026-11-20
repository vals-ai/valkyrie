import argparse
import re
from pathlib import Path

_DEPENDENCY = re.compile(r'"create-benchmark-service(\[[^\]]*\])?[^"]*"')
_VALS_IMPORT = re.compile(r"\bbenchmark_service\.vals\b")
_VALS_PACKAGE = re.compile(r"\bvals_benchmark_service\b")
_SKIPPED_DIRECTORIES = {".venv", ".git", "node_modules"}
_DESCRIPTION = """Point a benchmark service at the create-benchmark-service package in vals-ai/Valkyrie.

Rewrites the service's `pyproject.toml` to install create-benchmark-service from
vals-ai/Valkyrie at a `cbs-v*` tag, and, when the service imports
`benchmark_service.vals`, renames those imports to `vals_benchmark_service` and adds
that package at a `vals-benchmark-service-v*` tag. Run `uv lock` in the service afterwards.

    uv run python packages/vals-benchmark-service/scripts/migrate_cbs_consumer.py SERVICE_DIR --tag cbs-v0.78.1 \
        --vals-tag vals-benchmark-service-v0.1.0
"""


def _python_files(service: Path) -> list[Path]:
    return [path for path in service.rglob("*.py") if not _SKIPPED_DIRECTORIES & set(path.relative_to(service).parts)]


def _set_source(pyproject: str, name: str, source: str) -> str:
    line = f"{name} = {source}"
    pattern = re.compile(rf"^{re.escape(name)}\s*=.*$", re.MULTILINE)
    if pattern.search(pyproject):
        return pattern.sub(line, pyproject)
    if "[tool.uv.sources]" in pyproject:
        return pyproject.replace("[tool.uv.sources]", f"[tool.uv.sources]\n{line}", 1)
    return f"{pyproject.rstrip()}\n\n[tool.uv.sources]\n{line}\n"


def migrate(
    service: Path,
    framework_source: str,
    vals_source: str | None,
    vals_grading_tag: str | None = None,
) -> list[Path]:
    pyproject_path = service / "pyproject.toml"
    pyproject = pyproject_path.read_text()
    if not _DEPENDENCY.search(pyproject):
        raise ValueError(f"{pyproject_path} does not depend on create-benchmark-service")

    renamed: dict[Path, str] = {}
    uses_vals = False
    for path in _python_files(service):
        text = path.read_text()
        migrated = _VALS_IMPORT.sub("vals_benchmark_service", text)
        if migrated != text:
            renamed[path] = migrated
        uses_vals = uses_vals or bool(_VALS_PACKAGE.search(migrated))

    updated = _DEPENDENCY.sub(lambda match: f'"create-benchmark-service{match.group(1) or ""}"', pyproject)
    updated = _set_source(updated, "create-benchmark-service", framework_source)
    if uses_vals:
        if vals_source is None:
            raise ValueError("The service imports benchmark_service.vals; pass --vals-tag or --vals-source")
        if '"vals-benchmark-service' not in updated:
            updated = _DEPENDENCY.sub(
                lambda match: f'{match.group(0)},\n    "vals-benchmark-service"', updated, count=1
            )
        updated = _set_source(updated, "vals-benchmark-service", vals_source)
    if re.search(r"^vals-grading\s*=.*create-benchmark-service\.git", updated, re.MULTILINE):
        if vals_grading_tag is None:
            raise ValueError("The service installs vals-grading from the old repository; pass --vals-grading-tag")
        updated = _set_source(
            updated,
            "vals-grading",
            f'{{ git = "https://github.com/vals-ai/Valkyrie.git", tag = "{vals_grading_tag}", '
            'subdirectory = "packages/vals-grading" }',
        )
    for path, text in renamed.items():
        path.write_text(text)
    changed = list(renamed)
    if updated != pyproject:
        pyproject_path.write_text(updated)
        changed.append(pyproject_path)
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("service", type=Path)
    parser.add_argument("--tag", help="cbs-v* tag of vals-ai/Valkyrie to install")
    parser.add_argument("--framework-source", help="uv source table for create-benchmark-service, instead of --tag")
    parser.add_argument(
        "--vals-grading-tag", help="vals-grading-v* tag of vals-ai/Valkyrie for services that use vals-grading"
    )
    parser.add_argument("--vals-tag", help="vals-benchmark-service-v* tag of vals-ai/Valkyrie to install")
    parser.add_argument("--vals-source", help="uv source table for vals-benchmark-service, instead of --vals-tag")
    arguments = parser.parse_args()
    if (arguments.tag is None) == (arguments.framework_source is None):
        parser.error("pass exactly one of --tag and --framework-source")
    framework_source = arguments.framework_source or (
        f'{{ git = "https://github.com/vals-ai/Valkyrie.git", tag = "{arguments.tag}", '
        'subdirectory = "packages/create-benchmark-service" }'
    )
    if arguments.vals_tag is not None and arguments.vals_source is not None:
        parser.error("pass at most one of --vals-tag and --vals-source")
    vals_source = arguments.vals_source
    if arguments.vals_tag is not None:
        vals_source = (
            f'{{ git = "https://github.com/vals-ai/Valkyrie.git", tag = "{arguments.vals_tag}", '
            'subdirectory = "packages/vals-benchmark-service" }'
        )
    for path in migrate(arguments.service, framework_source, vals_source, arguments.vals_grading_tag):
        print(f"updated {path}")


if __name__ == "__main__":
    main()
