from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

from scripts.sdk.model_overlay import MODELS, ModelOverlay

OPENAPI_PATH = Path("services/tracker/openapi.json")
OUTPUT_DIR = Path("packages/valkyrie-sdk/src/valkyrie/sdk/models/_generated")
REF_PREFIX = "#/components/schemas/"
GENERATED_HEADER = re.compile(r"\A(#[^\n]*\n)+\n*")
SINGLE_LINE_DOCSTRING = re.compile(r'"""\n(\s+)([^\n]+)\n\s+"""')
TOP_LEVEL_KEYS = {"default", "description", "x-discriminator", "x-repr", "x-strict", "x-enum-field-as-literal"}
CONSTRAINT_KEYS = {"minLength", "maxLength", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "pattern"}
GENERATOR_ARGS = (
    "--input-file-type",
    "openapi",
    "--output-model-type",
    "pydantic_v2.BaseModel",
    "--target-python-version",
    "3.12",
    "--base-class",
    "valkyrie.sdk.models._base.ResponseModel",
    "--use-standard-collections",
    "--use-union-operator",
    "--use-schema-description",
    "--use-double-quotes",
    "--use-annotated",
    "--field-constraints",
    "--strict-nullable",
    "--collapse-root-models",
    "--use-type-alias",
    "--use-subclass-enum",
    "--no-use-specialized-enum",
    "--capitalise-enum-members",
    "--output-datetime-class",
    "datetime",
    "--field-extra-keys-without-x-prefix",
    "x-discriminator",
    "x-repr",
    "x-strict",
    "--formatters",
    "ruff-check",
    "ruff-format",
    "--disable-timestamp",
)


class OverlayError(RuntimeError):
    pass


def _qualified(name: str, overlay: ModelOverlay) -> str:
    return f"{overlay['module']}.{name}"


def _ref_targets() -> dict[str, str]:
    targets: dict[str, str] = {}
    for name, overlay in MODELS.items():
        if not overlay.get("referenced", True):
            continue
        for source in (overlay.get("schema", name), *overlay.get("aliases", ())):
            targets[source] = _qualified(name, overlay)
    return targets | {_qualified(name, overlay): _qualified(name, overlay) for name, overlay in MODELS.items()}


def _retarget(value: Any, targets: dict[str, str]) -> Any:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith(REF_PREFIX):
            source = ref.removeprefix(REF_PREFIX)
            if source not in targets:
                raise OverlayError(f"{source} is referenced by an SDK model but has no entry in MODELS")
            return {**value, "$ref": REF_PREFIX + targets[source]}
        return {key: _retarget(item, targets) for key, item in value.items()}
    if isinstance(value, list):
        return [_retarget(item, targets) for item in value]
    return value


def _value_branch(schema: dict[str, Any]) -> dict[str, Any]:
    branches = [branch for branch in schema.get("anyOf", []) if branch != {"type": "null"}]
    if len(branches) == 1 and len(schema["anyOf"]) == 2:
        return branches[0]
    return schema


def _collapse_constrained_nullable(schema: dict[str, Any]) -> dict[str, Any]:
    branch = _value_branch(schema)
    if branch is schema or not isinstance(branch.get("type"), str) or not CONSTRAINT_KEYS & branch.keys():
        return schema
    outer = {key: item for key, item in schema.items() if key != "anyOf"}
    return {**outer, **branch, "type": [branch["type"], "null"]}


def _patch_property(schema: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    if "replace" in patch:
        return deepcopy(patch["replace"])
    schema = deepcopy(schema)
    branch = _value_branch(schema)
    for key, item in patch.items():
        (schema if key in TOP_LEVEL_KEYS else branch)[key] = deepcopy(item)
    return schema


def _sdk_schema(name: str, overlay: ModelOverlay, source: dict[str, Any], built: dict[str, dict[str, Any]]) -> Any:
    schema = deepcopy(source[overlay.get("schema", name)])
    schema.pop("title", None)
    schema.pop("description", None)
    if "doc" in overlay:
        schema["description"] = overlay["doc"]
    if schema.get("additionalProperties") is False:
        del schema["additionalProperties"]
    if "properties" not in schema:
        return schema

    drop = set(overlay.get("drop", ()))
    parent = overlay.get("parent")
    patches = {**(MODELS[parent].get("fields", {}) if parent else {}), **overlay.get("fields", {})}
    unknown = (drop | patches.keys()) - schema["properties"].keys()
    if unknown:
        raise OverlayError(f"{name} names fields the spec does not have: {sorted(unknown)}")
    properties: dict[str, Any] = {}
    for field, field_schema in schema["properties"].items():
        if field in drop:
            continue
        field_schema = {key: item for key, item in field_schema.items() if key != "title"}
        properties[field] = _patch_property(_collapse_constrained_nullable(field_schema), patches.get(field, {}))
    required = [field for field in schema.get("required", []) if field in properties]

    if parent is None:
        schema["properties"] = properties
        schema["required"] = required
        return schema
    inherited = built[parent]["properties"]
    changed = [field for field in inherited if properties.get(field) != inherited[field]]
    if changed:
        raise OverlayError(f"{name} changes fields it inherits from {parent}: {changed}")
    own = {field: item for field, item in properties.items() if field not in inherited}
    child = {"type": "object", "properties": own, "required": [field for field in required if field in own]}
    return {
        key: item
        for key, item in {
            "description": schema.get("description"),
            "allOf": [{"$ref": REF_PREFIX + _qualified(parent, MODELS[parent])}, *([child] if own else [])],
        }.items()
        if item is not None
    }


def sdk_openapi(openapi: dict[str, Any]) -> dict[str, Any]:
    source = openapi["components"]["schemas"]
    targets = _ref_targets()
    built: dict[str, dict[str, Any]] = {}
    schemas: dict[str, Any] = {}
    for name, overlay in MODELS.items():
        schema = _sdk_schema(name, overlay, source, built)
        built[name] = schema
        schemas[_qualified(name, overlay)] = _retarget(schema, targets)
    return {
        "openapi": openapi["openapi"],
        "info": openapi["info"],
        "paths": {},
        "components": {"schemas": schemas},
    }


def _template_data() -> dict[str, Any]:
    return {
        _qualified(name, overlay): {"config": overlay["config"]}
        for name, overlay in MODELS.items()
        if "config" in overlay
    }


def _base_classes() -> dict[str, str]:
    return {_qualified(name, overlay): overlay["base"] for name, overlay in MODELS.items() if "base" in overlay}


def _format(source: str, path: Path) -> str:
    return subprocess.run(
        [sys.executable, "-m", "ruff", "format", "--stdin-filename", str(path), "-"],
        input=SINGLE_LINE_DOCSTRING.sub(r'"""\2"""', GENERATED_HEADER.sub("", source)),
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def render(openapi_path: Path = OPENAPI_PATH) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        spec = root / "openapi.json"
        template_data = root / "template_data.json"
        output = root / "models"
        spec.write_text(json.dumps(sdk_openapi(json.loads(openapi_path.read_text())), indent=2))
        template_data.write_text(json.dumps(_template_data()))
        subprocess.run(
            [
                sys.executable,
                "-m",
                "datamodel_code_generator",
                "--input",
                str(spec),
                "--output",
                str(output),
                "--extra-template-data",
                str(template_data),
                "--base-class-map",
                json.dumps(_base_classes()),
                *GENERATOR_ARGS,
            ],
            check=True,
            capture_output=True,
        )
        return {path.name: _format(path.read_text(), OUTPUT_DIR / path.name) for path in sorted(output.glob("*.py"))}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    rendered = render()
    if args.check:
        current = {path.name: path.read_text() for path in sorted(OUTPUT_DIR.glob("*.py"))}
        if current != rendered:
            print(f"{OUTPUT_DIR} is stale; run make sdk-models", file=sys.stderr)
            return 1
        return 0
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    OUTPUT_DIR.mkdir()
    for name, source in rendered.items():
        (OUTPUT_DIR / name).write_text(source)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
