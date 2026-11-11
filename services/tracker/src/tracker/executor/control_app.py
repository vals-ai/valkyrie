from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor import (
    control_dispatch,
    control_queue_api,
    control_run_api,
    control_task_api,
)
from tracker.executor.control_protocol import AUTHORITY_REVOKED_CODE

app = FastAPI()
app.include_router(control_dispatch.router)
app.include_router(control_task_api.router)
app.include_router(control_run_api.router)
app.include_router(control_queue_api.router)


@app.get("/health", description="Return the Control API health status.")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(ExecutionAuthorityRevoked)
async def authority_revoked(_request: Request, error: ExecutionAuthorityRevoked) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": {"code": AUTHORITY_REVOKED_CODE, "message": str(error)}},
    )
