"""Tests for the lab-facing /v1/ eval API surface."""

import pytest
from pydantic import ValidationError

from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalRequest,
    V1EvalResponse,
    V1EvalStatus,
    V1PayloadType,
    V1ScoreRequest,
    V1ScoreResponse,
    V1Task,
)


def test_eval_request_accepts_text_payload() -> None:
    req = V1EvalRequest.model_validate(
        {
            "run_id": "external-run-123",
            "task_id": "01-011",
            "dataset": "validation",
            "payload": {"type": "text", "schema": "fabv2.text.v1", "data": "42"},
            "versions": {"runner": "0.0.10-a1b2c3d"},
        }
    )
    assert req.payload.type == V1PayloadType.TEXT
    assert req.payload.data == "42"
    assert req.payload.schema_id == "fabv2.text.v1"
    raw = req.model_dump(mode="json", by_alias=True)
    assert raw["payload"]["schema"] == "fabv2.text.v1"
    assert "schema_id" not in raw["payload"]


def test_eval_request_rejects_unknown_payload_type() -> None:
    with pytest.raises(ValidationError):
        V1EvalRequest.model_validate(
            {
                "run_id": "r",
                "task_id": "t",
                "payload": {"type": "movie", "schema": "x.v1", "data": ""},
            }
        )


def test_eval_response_round_trips_evaluated_status() -> None:
    resp = V1EvalResponse(
        run_id="external-run-123",
        task_id="01-011",
        status=V1EvalStatus.EVALUATED,
        evaluator_version="0.0.2",
        result={"pass_percentage": 0.83},
        errors=[],
    )
    raw = resp.model_dump(mode="json")
    rehydrated = V1EvalResponse.model_validate(raw)
    assert rehydrated == resp
    assert raw["status"] == V1EvalStatus.EVALUATED


def test_score_request_carries_run_id_and_nullable_results() -> None:
    req = V1ScoreRequest.model_validate(
        {
            "run_id": "external-run-123",
            "dataset": "validation",
            "evaluation_results": {
                "01-011": {"status": "evaluated", "result": {"pass_percentage": 0.83}},
                "01-012": None,
            },
        }
    )
    assert req.run_id == "external-run-123"
    assert req.evaluation_results["01-012"] is None


def test_score_response_requires_final_score_and_tasks() -> None:
    resp = V1ScoreResponse(
        run_id="external-run-123",
        tasks_evaluated=["01-011", "01-012"],
        final_score=0.62,
        metadata={},
    )
    raw = resp.model_dump(mode="json")
    assert raw["final_score"] == 0.62
    assert raw["tasks_evaluated"] == ["01-011", "01-012"]


def test_v1_task_allows_benchmark_specific_extras() -> None:
    """V1Task accepts benchmark-specific per-task fields (e.g. SWE-bench's
    repo/base_commit, an artifact benchmark's docker image hints). Per-benchmark
    fields should be documented in each benchmark's README and validated on the
    runner side with a typed Task subclass."""
    t = V1Task.model_validate(
        {
            "id": "01-011",
            "question": "What is fair use?",
            "timeout": 600,
            "system_prompt": "be helpful",
        }
    )
    assert t.id == "01-011"
    assert t.timeout == 600
    raw = t.model_dump(mode="json")
    assert raw["system_prompt"] == "be helpful"


def test_v1_dataset_tasks_response_round_trip() -> None:
    resp = V1DatasetTasksResponse(
        dataset="validation",
        dataset_version="validation-2026-06-10",
        tasks=[
            V1Task(id="t1", question="q1"),
            V1Task(id="t2", question="q2", timeout=120),
        ],
    )
    raw = resp.model_dump(mode="json")
    rehydrated = V1DatasetTasksResponse.model_validate(raw)
    assert rehydrated == resp
    assert raw["dataset_version"] == "validation-2026-06-10"
    assert raw["tasks"][1]["timeout"] == 120
