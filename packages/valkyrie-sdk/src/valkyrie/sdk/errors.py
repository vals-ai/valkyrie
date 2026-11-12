"""Exceptions raised by the Valkyrie SDK."""

from typing import Any

import httpx
from pydantic import ValidationError

_FIELD_ERROR_TEMPLATES = {
    "missing": "'{field}' is required but was not provided",
    "literal_error": "'{field}' has an invalid value. {msg}",
}


class ValkyrieSDKError(Exception):
    """Base class for all SDK errors."""


class ValkyrieConfigError(ValkyrieSDKError):
    """A saved Valkyrie login could not be read, or signing in or refreshing it failed."""


class ValkyrieInputError(ValkyrieSDKError, ValueError):
    """An SDK call received an invalid argument. It is also a `ValueError`."""


class ValkyrieRunError(ValkyrieInputError):
    """A run or run log operation received invalid input."""


class ValkyrieTransportError(ValkyrieSDKError):
    """A request could not reach the Valkyrie service."""


class ValkyrieAPIError(ValkyrieSDKError):
    """The Valkyrie API returned a non-success response."""

    def __init__(self, status_code: int, detail: Any, headers: httpx.Headers | None = None):
        self.status_code = status_code
        self.detail = detail
        self.headers = httpx.Headers(headers)
        super().__init__(f"Valkyrie API request failed ({status_code}): {detail}")

    @classmethod
    def from_response(cls, response: httpx.Response) -> "ValkyrieAPIError":
        """Build one from a read non-success response, using its JSON `detail` or body as the detail.

        `headers` keeps the response headers, such as `Retry-After` and `X-RateLimit-Limit` on a 429.
        """
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
        detail = body.get("detail", body) if isinstance(body, dict) else body
        return cls(response.status_code, detail, response.headers)


class ValkyrieDatasetPushError(ValkyrieSDKError):
    """Some tasks of a dataset push were not stored."""

    def __init__(self, stored: list[str], failed: dict[str, str]) -> None:
        self.stored = stored
        self.failed = failed
        super().__init__(f"{len(failed)} of {len(stored) + len(failed)} tasks were not stored: {', '.join(failed)}")


class ValkyrieStreamError(ValkyrieSDKError):
    """A Tracker stream or paginated listing returned an error, an invalid event, or a repeated cursor."""


class ValkyrieContractError(ValkyrieSDKError):
    """Raised when an agent contract, or the kwargs passed to it, fails validation."""

    @classmethod
    def from_validation_error(cls, error: ValidationError, context: str = "") -> "ValkyrieContractError":
        """Build one from a pydantic ValidationError: `context` first when given, then one line per invalid field.

        Fields are worded the way Tracker words them, so the CLI and the server report a bad contract alike.
        """
        lines: list[str] = []
        for detail in error.errors():
            field = ".".join(str(location) for location in detail["loc"])
            template = _FIELD_ERROR_TEMPLATES.get(detail["type"], "'{field}': {msg}")
            lines.append(f"  - {template.format(field=field, msg=detail['msg'])}")

        message = "\n".join(lines)
        return cls(f"{context}\n{message}" if context else message)
