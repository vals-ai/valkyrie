import pytest
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import HTTPException

from tracker.api.storage_errors import storage_errors
from tracker.exceptions import S3Error


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "GetObject")


def _wrapped(code: str) -> S3Error:
    error = S3Error("Failed to delete object from S3")
    error.__cause__ = _client_error(code)
    return error


@pytest.mark.description(
    "`storage_errors` raises an `HTTPException` with a fixed status for each storage failure type."
)
@pytest.mark.parametrize(
    ("failure", "status"),
    [
        (FileNotFoundError(), 404),
        (PermissionError(), 403),
        (ValueError("bad cursor"), 400),
        (_client_error("NoSuchKey"), 404),
        (_client_error("AccessDenied"), 403),
        (_client_error("500"), 502),
        (_wrapped("AccessDenied"), 403),
        (_wrapped("404"), 404),
        (_wrapped("InternalError"), 502),
        (S3Error("unreachable"), 502),
        (BotoCoreError(), 502),
    ],
)
def test_each_storage_failure_maps_to_one_http_status(failure: BaseException, status: int) -> None:
    with pytest.raises(HTTPException) as raised, storage_errors("Task"):
        raise failure

    assert raised.value.status_code == status
