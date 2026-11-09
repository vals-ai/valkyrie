import logging
from collections.abc import Generator
from contextlib import contextmanager

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException

from tracker.exceptions import S3Error

logger = logging.getLogger(__name__)

NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}
DENIED_CODES = {"403", "AccessDenied", "Forbidden"}


@contextmanager
def storage_errors(noun: str) -> Generator[None]:
    try:
        yield
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail=f"{noun} not found") from error
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=f"{noun} storage permission denied") from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (BotoCoreError, ClientError, S3Error) as error:
        cause = error if isinstance(error, ClientError) else error.__cause__
        code = cause.response.get("Error", {}).get("Code") if isinstance(cause, ClientError) else None
        if code in NOT_FOUND_CODES:
            raise HTTPException(status_code=404, detail=f"{noun} not found") from error
        if code in DENIED_CODES:
            raise HTTPException(status_code=403, detail=f"{noun} storage permission denied") from error
        logger.exception("%s storage operation failed", noun)
        raise HTTPException(status_code=502, detail=f"{noun} storage operation failed") from error
