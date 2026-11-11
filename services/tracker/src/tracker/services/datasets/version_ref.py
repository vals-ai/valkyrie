import re
from typing import Literal, NamedTuple

from tracker.services.datasets.digest import DIGEST_PREFIX

LATEST = "latest"
MAX_VERSION_TASKS = 100_000
MAX_VERSION_TAGS = 10
TAG = re.compile(r"[a-z0-9][a-z0-9.\-]*")
MIN_DIGEST_PREFIX_HEX = 8
HEX = re.compile(r"[0-9a-f]+")
TAG_RULE = (
    "a tag is lowercase letters, digits, hyphens and dots, starts with a letter or digit, and is not a whole number"
)


class VersionRef(NamedTuple):
    kind: Literal["tag", "revision", "digest"]
    value: str


def validate_tag(tag: str) -> str:
    if tag.isdigit() or tag.startswith(DIGEST_PREFIX) or not TAG.fullmatch(tag):
        raise ValueError(f"Invalid tag {tag!r}: {TAG_RULE}")
    return tag


def parse_version_ref(text: str) -> VersionRef:
    if text.isascii() and text.isdigit():
        return VersionRef("revision", text)
    if text.startswith(DIGEST_PREFIX):
        hex_part = text.removeprefix(DIGEST_PREFIX)
        if len(hex_part) < MIN_DIGEST_PREFIX_HEX or len(hex_part) > 64 or not HEX.fullmatch(hex_part):
            raise ValueError(
                f"Invalid digest {text!r}: write {DIGEST_PREFIX} and at least {MIN_DIGEST_PREFIX_HEX} hex characters"
            )
        return VersionRef("digest", text)
    return VersionRef("tag", validate_tag(text))
