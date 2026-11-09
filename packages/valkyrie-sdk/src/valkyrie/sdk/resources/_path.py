from urllib.parse import quote

from valkyrie.sdk.errors import ValkyrieInputError


def encode_path_segment(value: str, *, label: str = "name") -> str:
    if value in {"", ".", ".."}:
        raise ValkyrieInputError(f"Invalid {label}: {value!r}")
    return quote(value, safe="")
