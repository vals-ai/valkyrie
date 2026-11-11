import asyncio
import random
import time
from typing import TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.control_protocol import AUTHORITY_REVOKED_CODE, MAINTENANCE_ADMISSION_PAUSED


def _retry_pause(
    operation: str, error: httpx.TransportError | httpx.HTTPStatusError, deadline: float, backoff: float
) -> float:
    remaining = deadline - time.monotonic()
    if (
        operation not in RETRYABLE_OPERATIONS
        or remaining <= 0
        or (isinstance(error, httpx.HTTPStatusError) and error.response.status_code not in (502, 503, 504))
    ):
        raise error
    return min(random.uniform(0, backoff), remaining)


class _AdmissionPaused(Exception):
    pass


Response = TypeVar("Response", bound=BaseModel)

CONTROL_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
RETRY_BUDGET_SECONDS = 120.0
RETRYABLE_OPERATIONS = frozenset(
    {
        "admit_direct_task",
        "claim_task_evaluation",
        "clear_native_output",
        "ensure_task_deadline",
        "finish_dispatch",
        "monitor_tasks",
        "publish_native_output",
        "queue-abandon",
        "queue-accept",
        "queue-claim",
        "queue-clear",
        "queue-clock",
        "queue-eligibility",
        "queue-round",
        "queue-state",
        "queue-states",
        "read_dataset_version",
        "read_run_state",
        "read_task",
        "record_sandbox_cost",
        "record_sandbox_link",
        "revoke_egress_policy",
        "save_task_resume",
        "write_task_accounting",
        "write_task_search_costs",
    }
)


class ControlClient:
    def __init__(
        self, http: httpx.AsyncClient, dispatch_id: UUID, invocation_key: str, bootstrap_key: str | None = None
    ):
        self.dispatch_id = dispatch_id
        self.invocation_key = invocation_key
        self.bootstrap_key = bootstrap_key
        self._http = http

    async def call(self, operation: str, request: BaseModel, response_type: type[Response]) -> Response:
        delay = 0.1
        backoff = 0.5
        deadline = time.monotonic() + RETRY_BUDGET_SECONDS
        while True:
            try:
                return await self.send(operation, request, response_type)
            except (httpx.TransportError, httpx.HTTPStatusError) as error:
                pause = _retry_pause(operation, error, deadline, backoff)
                backoff = min(backoff * 2, 8.0)
            except _AdmissionPaused:
                pause = delay
                delay = min(delay * 2, 5.0)
            await asyncio.sleep(pause)

    async def send(self, operation: str, request: BaseModel, response_type: type[Response]) -> Response:
        headers = {
            "X-Executor-Dispatch-ID": str(self.dispatch_id),
            "Authorization": f"Bearer {self.invocation_key}",
        }
        if operation == "claim_dispatch" and self.bootstrap_key is not None:
            headers["X-Executor-Bootstrap"] = self.bootstrap_key
        response = await self._http.post(
            f"/executor-control/v1/{operation}", headers=headers, json=request.model_dump(mode="json")
        )
        if response.status_code in (409, 423):
            detail = response.json()["detail"]
            if isinstance(detail, dict):
                if response.status_code == 409 and detail.get("code") == AUTHORITY_REVOKED_CODE:
                    raise ExecutionAuthorityRevoked(detail["message"])
                if (
                    response.status_code == 423
                    and detail.get("code") == MAINTENANCE_ADMISSION_PAUSED
                    and operation
                    in (
                        "admit_direct_task",
                        "queue-prepare",
                        "queue-claim",
                        "queue-accept",
                        "queue-begin-readiness",
                        "queue-orphans",
                    )
                ):
                    raise _AdmissionPaused
        response.raise_for_status()
        return response_type.model_validate(response.json())

    async def close(self) -> None:
        await self._http.aclose()
