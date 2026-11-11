import io
import re
import zipfile

import pytest

from tests.unit.datasets.support import TASK, archive_digest_of, zip_bytes
from tracker.services.datasets.digest import DIGEST_PATTERN, version_digest
from tracker.services.datasets.extract import UnsafeArchiveError
from tracker.services.datasets.version_ref import VersionRef, parse_version_ref, validate_tag


@pytest.mark.description("The digest names a task's files, not the archive that happens to hold them.")
class TestArchiveDigest:
    @pytest.mark.description("A digest ignores compression and file order.")
    def test_a_digest_ignores_compression_and_file_order(self) -> None:
        reversed_task = dict(reversed(list(TASK.items())))

        assert archive_digest_of(zip_bytes(TASK)) == archive_digest_of(zip_bytes(reversed_task, zipfile.ZIP_STORED))
        assert re.fullmatch(DIGEST_PATTERN, archive_digest_of(zip_bytes(TASK)))

    @pytest.mark.description("A digest follows each files name and content.")
    def test_a_digest_follows_each_files_name_and_content(self) -> None:
        digests = {
            archive_digest_of(zip_bytes(TASK)),
            archive_digest_of(zip_bytes({**TASK, "instruction.md": b"Another."})),
            archive_digest_of(zip_bytes({**TASK, "tests/test2.sh": b"#!/bin/bash"})),
            archive_digest_of(zip_bytes({"renamed.md": TASK["instruction.md"], **TASK})),
        }

        assert len(digests) == 4

    @pytest.mark.description("An archive that holds a file twice is refused.")
    def test_an_archive_that_holds_a_file_twice_is_refused(self) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as writer, pytest.warns(UserWarning, match="Duplicate name"):
            writer.writestr("a.txt", b"1")
            writer.writestr("a.txt", b"2")

        with pytest.raises(UnsafeArchiveError):
            archive_digest_of(buffer.getvalue())


@pytest.mark.description("The digest names a version by its tasks.")
class TestVersionDigest:
    @pytest.mark.description("A digest ignores the order of the tasks.")
    def test_a_digest_ignores_the_order_of_the_tasks(self) -> None:
        assert version_digest({"a": "sha256:1", "b": "sha256:2"}) == version_digest({"b": "sha256:2", "a": "sha256:1"})

    @pytest.mark.description("A digest follows each tasks id and digest.")
    @pytest.mark.parametrize(
        "other",
        [{"a": "sha256:1"}, {"a": "sha256:1", "b": "sha256:3"}, {"a": "sha256:1", "c": "sha256:2"}],
        ids=["task-removed", "task-changed", "task-renamed"],
    )
    def test_a_digest_follows_each_tasks_id_and_digest(self, other: dict[str, str]) -> None:
        assert version_digest(other) != version_digest({"a": "sha256:1", "b": "sha256:2"})


@pytest.mark.description("Reading what a request or a run says about a version.")
class TestVersionRef:
    @pytest.mark.description("References are read as tags revisions or digests.")
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("latest", VersionRef("tag", "latest")),
            ("stable-2.1", VersionRef("tag", "stable-2.1")),
            ("12", VersionRef("revision", "12")),
            ("sha256:3f2a9c1e", VersionRef("digest", "sha256:3f2a9c1e")),
        ],
    )
    def test_references_are_read_as_tags_revisions_or_digests(self, text: str | None, expected: VersionRef) -> None:
        assert parse_version_ref(text) == expected

    @pytest.mark.description("References that are none of these are refused.")
    @pytest.mark.parametrize(
        "text", ["sha256:3f2a", "sha256:" + "G" * 12, "sha256:" + "a" * 65, "Upper", "a b", "-a", ""]
    )
    def test_references_that_are_none_of_these_are_refused(self, text: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            parse_version_ref(text)

    @pytest.mark.description("Tags that read as other references are refused.")
    @pytest.mark.parametrize("tag", ["7", "sha256:abcdef12", "A", "", "a_b", "-x"])
    def test_tags_that_read_as_other_references_are_refused(self, tag: str) -> None:
        with pytest.raises(ValueError, match="Invalid tag"):
            validate_tag(tag)
