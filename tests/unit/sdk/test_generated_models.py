from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from scripts.sdk import generate_models
from scripts.sdk.generate_models import OUTPUT_DIR, OverlayError, render, sdk_openapi


@pytest.mark.description("The committed generated SDK models match a fresh run of the generator on openapi.json.")
def test_generated_models_match_the_openapi_spec() -> None:
    committed = {path.name: path.read_text() for path in sorted(Path(OUTPUT_DIR).glob("*.py"))}
    assert committed == render()


@pytest.mark.description("A schema the SDK models reference without an overlay entry fails generation.")
def test_unmapped_reference_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_models, "MODELS", {"Outer": {"module": "runs"}})
    openapi: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "t", "version": "1"},
        "components": {
            "schemas": {
                "Outer": {"type": "object", "properties": {"inner": {"$ref": "#/components/schemas/Inner"}}},
                "Inner": {"type": "object", "properties": {}},
            }
        },
    }
    with pytest.raises(OverlayError, match="Inner is referenced"):
        sdk_openapi(deepcopy(openapi))


@pytest.mark.description("An overlay that patches a field the spec does not have fails generation.")
def test_unknown_overlay_field_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(generate_models, "MODELS", {"Outer": {"module": "runs", "drop": ("missing",)}})
    openapi: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": "t", "version": "1"},
        "components": {"schemas": {"Outer": {"type": "object", "properties": {"kept": {"type": "string"}}}}},
    }
    with pytest.raises(OverlayError, match="missing"):
        sdk_openapi(openapi)
