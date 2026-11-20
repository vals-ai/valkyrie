"""Project generator for benchmark services."""

import importlib.metadata
import re
import shutil
import sys
from pathlib import Path
from typing import TypedDict

from jinja2 import Environment, FileSystemLoader

FRAMEWORK_VERSION = importlib.metadata.version("create-benchmark-service")
FRAMEWORK_SOURCE = f"git+https://github.com/vals-ai/Valkyrie.git@cbs-v{FRAMEWORK_VERSION}#subdirectory=packages/create-benchmark-service"


class BenchmarkNames(TypedDict):
    """Transformed benchmark name formats."""

    benchmark_name: str
    benchmark_package: str


def transform_name(name: str) -> BenchmarkNames:
    """Transform benchmark name into various formats.

    Args:
        name: Benchmark name (e.g., "swe-bench", "swebench")

    Returns:
        BenchmarkNames with:
        - benchmark_name: lowercase with hyphens (e.g., "swe-bench")
        - benchmark_package: lowercase with underscores (e.g., "swe_bench_benchmark_service")
    """

    benchmark_name = name.lower().replace("_", "-")
    benchmark_package = benchmark_name.replace("-", "_") + "_benchmark_service"

    return {
        "benchmark_name": benchmark_name,
        "benchmark_package": benchmark_package,
    }


def validate_name(name: str) -> None:
    """Validate benchmark name.

    Args:
        name: Benchmark name to validate

    Raises:
        ValueError: If name is invalid
    """
    if not name:
        raise ValueError("Benchmark name cannot be empty")

    # Allow alphanumeric, hyphens, and underscores
    if not re.match(r"^[a-zA-Z0-9_-]+$", name):
        raise ValueError("Benchmark name can only contain alphanumeric characters, hyphens, and underscores")

    # Must start with a letter
    if not name[0].isalpha():
        raise ValueError("Benchmark name must start with a letter")

    # Check for reserved names (Python standard library modules)
    normalized_name = name.lower().replace("-", "_")
    if normalized_name in sys.stdlib_module_names:
        raise ValueError(
            f"'{name}' conflicts with Python standard library module '{normalized_name}'. "
            f"Please choose a different name."
        )


def copy_file(source: Path, dest: Path) -> None:
    """Copy a file from source to destination.

    Args:
        source: Source file path
        dest: Destination file path
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)


def generate_project(benchmark_name: str, output_dir: Path) -> None:
    """Generate a new benchmark service project.

    Args:
        benchmark_name: Name of the benchmark (e.g., "swebench")
        output_dir: Output directory path

    Raises:
        ValueError: If benchmark name is invalid
        FileExistsError: If output directory exists
    """
    # Validate name
    validate_name(benchmark_name)

    # Check if output directory exists
    if output_dir.exists():
        raise FileExistsError(f"Directory {output_dir} already exists.")

    # Transform names
    names = transform_name(benchmark_name)

    output_dir.mkdir(parents=True, exist_ok=True)

    templates_dir = Path(__file__).parent / "templates"
    for file_name in ("Makefile", ".python-version"):
        copy_file(templates_dir / file_name, output_dir / file_name)

    env = Environment(loader=FileSystemLoader(templates_dir), keep_trailing_newline=True)

    copy_file(templates_dir / ".gitignore.scaffold", output_dir / ".gitignore")

    # Render Jinja2 templates
    template_files = {
        "main.py.jinja": "main.py",
        "pyproject.toml.jinja": "pyproject.toml",
        "README.md.jinja": "README.md",
    }

    template_context = {**names, "framework_source": FRAMEWORK_SOURCE}

    for template_name, output_name in template_files.items():
        project_template = env.get_template(template_name)
        content = project_template.render(template_context)
        (output_dir / output_name).write_text(content)

    shutil.copytree(templates_dir / ".github", output_dir / ".github")

    # Create empty tests directory
    (output_dir / "tests").mkdir(exist_ok=True)
    (output_dir / "tests" / "__init__.py").write_text("")

    # Create benchmark-specific package directory
    benchmark_package_dir = output_dir / "src" / names["benchmark_package"]
    benchmark_package_dir.mkdir(parents=True, exist_ok=True)

    # Create __init__.py in the package
    (benchmark_package_dir / "__init__.py").write_text(f'"""Utilities for {names["benchmark_name"]} benchmark."""\n')

    # Copy templates
    service_template = env.get_template("benchmark_service.py.jinja")
    (benchmark_package_dir / "benchmark_service.py").write_text(service_template.render(template_context))
    copy_file(templates_dir / "Dockerfile", output_dir / "Dockerfile")
    copy_file(templates_dir / ".dockerignore", output_dir / ".dockerignore")
