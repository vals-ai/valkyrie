import pytest
import json
from pathlib import Path

from generate_openapi import build_openapi


@pytest.mark.description("`openapi.json` equals the output of `build_openapi`.")
def test_openapi_snapshot_matches_generator() -> None:
    snapshot_path = Path(__file__).parents[2] / "openapi.json"

    expected = json.dumps(build_openapi(), indent=2) + "\n"
    assert snapshot_path.read_text() == expected


@pytest.mark.description("Project update and delete routes document 404, and delete documents 409.")
def test_openapi_documents_project_maintenance_missing_and_linked_states() -> None:
    operations = build_openapi()["paths"]["/projects/{handle}"]

    assert "404" in operations["put"]["responses"]
    assert "404" in operations["delete"]["responses"]
    assert "409" in operations["delete"]["responses"]


@pytest.mark.description("The schema marks `/auth/cli` as public and `/access-keys` as needing a bearer token.")
def test_openapi_declares_authentication() -> None:
    schema = build_openapi()

    assert schema["components"]["securitySchemes"] == {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        },
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "x-api-key",
        },
        "RemoteGraderWorkerKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-Remote-Grader-Worker-Key",
        },
    }
    assert schema["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert schema["paths"]["/health"]["get"]["security"] == []
    assert "security" not in schema["paths"]["/start-benchmark"]["post"]
    assert schema["paths"]["/auth/cli"]["get"]["security"] == []
    assert schema["paths"]["/access-keys"]["post"]["security"] == [{"BearerAuth": []}]


@pytest.mark.description("The schema has no storage override route and no `managed_s3_bucket` start field.")
def test_openapi_has_no_client_storage_override() -> None:
    schema = build_openapi()

    assert "/start-benchmark-with-storage" not in schema["paths"]
    assert "managed_s3_bucket" not in schema["components"]["schemas"]["StartBenchmarkRequest"]["properties"]


@pytest.mark.description(
    "Saved run arguments hide `priority` and `queue_pool_id`, while the start request still accepts `priority`."
)
def test_openapi_keeps_scheduler_storage_fields_internal() -> None:
    schemas = build_openapi()["components"]["schemas"]

    for model in ("AWSBenchmarkArguments", "LocalBenchmarkArguments"):
        benchmark_argument_properties = schemas[model]["properties"]
        assert "priority" not in benchmark_argument_properties
        assert "queue_pool_id" not in benchmark_argument_properties
    assert "priority" in schemas["StartBenchmarkRequest"]["properties"]


@pytest.mark.description(
    "The scheduler overview schema gives capacity `available` and `total` as non-negative numbers."
)
def test_openapi_includes_scheduler_overview_contract() -> None:
    operation = build_openapi()["paths"]["/scheduler/overview"]["get"]
    parameters = {parameter["name"]: parameter["schema"] for parameter in operation["parameters"]}

    assert parameters == {
        "waiting_limit": {
            "type": "integer",
            "maximum": 200,
            "minimum": 1,
            "default": 100,
            "title": "Waiting Limit",
        },
        "active_limit": {
            "type": "integer",
            "maximum": 200,
            "minimum": 1,
            "default": 100,
            "title": "Active Limit",
        },
        "waiting_offset": {"type": "integer", "minimum": 0, "default": 0, "title": "Waiting Offset"},
        "active_offset": {"type": "integer", "minimum": 0, "default": 0, "title": "Active Offset"},
        "include_capacity": {
            "type": "boolean",
            "default": False,
            "title": "Include Capacity",
        },
    }
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SchedulerOverviewResponse"
    }

    schemas = build_openapi()["components"]["schemas"]
    pool_schema = schemas["SchedulerPoolResponse"]
    assert pool_schema["required"] == ["pool_id", "waiting"]
    assert set(pool_schema["properties"]) == {"pool_id", "waiting", "provider", "capacity_domains"}
    assert schemas["SchedulerCapacityDomainResponse"]["required"] == ["target_id", "sandbox_class", "capacity"]
    capacity_schema = schemas["SchedulerCapacityResponse"]
    assert capacity_schema["required"] == ["cpu", "memory", "disk"]
    assert set(capacity_schema["properties"]) == {
        "cpu",
        "memory",
        "disk",
        "gpu",
        "allowed_gpu_types",
    }
    assert capacity_schema["properties"]["gpu"] == {
        "anyOf": [
            {"$ref": "#/components/schemas/SchedulerResourceCapacityResponse"},
            {"type": "null"},
        ]
    }
    assert capacity_schema["properties"]["allowed_gpu_types"] == {
        "anyOf": [
            {"type": "array", "items": {"type": "string"}},
            {"type": "null"},
        ],
        "title": "Allowed Gpu Types",
    }
    assert schemas["SchedulerResourceCapacityResponse"]["properties"] == {
        "available": {"type": "number", "minimum": 0.0, "title": "Available"},
        "total": {"type": "number", "minimum": 0.0, "title": "Total"},
    }
