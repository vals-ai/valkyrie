import io
import os
import stat
import warnings
import zipfile
from pathlib import Path

import pytest

from tracker.services.datasets import extract as extraction
from tests.unit.datasets.support import TASK, zip_bytes
from tracker.services.datasets.extract import UnsafeArchiveError, check_archive, extract


def archive_of(files: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(zip_bytes(files, compression)))


def archive_with_mode(name: str, mode: int) -> zipfile.ZipFile:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as writer:
        info = zipfile.ZipInfo(name)
        info.create_system = 3
        info.external_attr = mode << 16
        writer.writestr(info, "target")
    return zipfile.ZipFile(buffer)


@pytest.mark.description("What an archive may hold.")
class TestCheckArchive:
    @pytest.mark.description("A task archive passes.")
    def test_a_task_archive_passes(self) -> None:
        check_archive(archive_of(TASK))

    @pytest.mark.description("Paths that could leave the destination are refused.")
    @pytest.mark.parametrize(
        "name",
        ["../escape.txt", "a/../../escape.txt", "/abs.txt", "a\\b.txt", "C:evil.txt"],
    )
    def test_paths_that_could_leave_the_destination_are_refused(self, name: str) -> None:
        with pytest.raises(UnsafeArchiveError, match="unsafe path"):
            check_archive(archive_of({name: b"x"}))

    @pytest.mark.description("Links and special files are refused.")
    @pytest.mark.parametrize(
        "mode",
        [stat.S_IFLNK | 0o777, stat.S_IFCHR | 0o666, stat.S_IFBLK | 0o660, stat.S_IFIFO | 0o644, stat.S_IFSOCK | 0o644],
    )
    def test_links_and_special_files_are_refused(self, mode: int) -> None:
        with pytest.raises(UnsafeArchiveError, match="link or special file"):
            check_archive(archive_with_mode("thing", mode))

    @pytest.mark.description("Regular files and directories are fine.")
    @pytest.mark.parametrize("mode", [0, stat.S_IFREG | 0o644, stat.S_IFDIR | 0o755])
    def test_regular_files_and_directories_are_fine(self, mode: int) -> None:
        check_archive(archive_with_mode("thing", mode))

    @pytest.mark.description("Unaccepted compression is refused.")
    def test_unaccepted_compression_is_refused(self) -> None:
        with pytest.raises(UnsafeArchiveError, match="compresses 'a.txt'"):
            check_archive(archive_of({"a.txt": b"x" * 100}, zipfile.ZIP_BZIP2))

    @pytest.mark.description("Too many entries are refused.")
    def test_too_many_entries_are_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(extraction, "MAX_ENTRIES", 2)

        with pytest.raises(UnsafeArchiveError, match="over the 2 allowed"):
            check_archive(archive_of({"a": b"1", "b": b"2", "c": b"3"}))


@pytest.mark.description("Writing out only what a phase needs.")
class TestExtract:
    @pytest.mark.description("Only the named parts are written.")
    def test_only_the_named_parts_are_written(self, tmp_path: Path) -> None:
        extract(archive_of(TASK), ["task.toml", "tests"], tmp_path)

        written = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*") if path.is_file())

        assert written == ["task.toml", "tests/cases/one.txt", "tests/test.sh"]
        assert (tmp_path / "tests" / "cases" / "one.txt").read_bytes() == b"1"

    @pytest.mark.description("A prefix does not match a sibling with the same start.")
    def test_a_prefix_does_not_match_a_sibling_with_the_same_start(self, tmp_path: Path) -> None:
        extract(archive_of({"tests/a": b"1", "tests-extra/b": b"2"}), ["tests"], tmp_path)

        assert sorted(path.name for path in tmp_path.rglob("*") if path.is_file()) == ["a"]

    @pytest.mark.description("A part the archive lacks is skipped.")
    def test_a_part_the_archive_lacks_is_skipped(self, tmp_path: Path) -> None:
        extract(archive_of(TASK), ["environment/docker-compose.yaml"], tmp_path)

        assert list(tmp_path.iterdir()) == []

    @pytest.mark.description("Empty directories are created.")
    def test_empty_directories_are_created(self, tmp_path: Path) -> None:
        extract(archive_of({"tests/": b"", "tests/test.sh": b"x"}), ["tests"], tmp_path)

        assert (tmp_path / "tests").is_dir()

    @pytest.mark.description("Files over the size cap stop the extraction.")
    def test_files_over_the_size_cap_stop_the_extraction(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(extraction, "MAX_EXTRACTED_BYTES", 100)

        with pytest.raises(UnsafeArchiveError, match="exceed 100 bytes"):
            extract(archive_of(TASK), ["environment"], tmp_path)

    @pytest.mark.description("A name that appears twice is refused.")
    def test_a_name_that_appears_twice_is_refused(self, tmp_path: Path) -> None:
        buffer = io.BytesIO()
        with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w") as writer:
            warnings.simplefilter("ignore")
            writer.writestr("tests/a", b"1")
            writer.writestr("tests/a", b"2")

        with pytest.raises(UnsafeArchiveError, match="more than once"):
            extract(zipfile.ZipFile(buffer), ["tests"], tmp_path)

    @pytest.mark.description("Executable bits survive and a file stays readable by its owner.")
    def test_executable_bits_survive_and_a_file_stays_readable_by_its_owner(self, tmp_path: Path) -> None:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as writer:
            for name, mode in (("tests/run.sh", 0o755), ("tests/secret.txt", 0o000), ("tests/plain.txt", 0)):
                info = zipfile.ZipInfo(name)
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | mode) << 16 if mode else 0
                writer.writestr(info, "x")

        extract(zipfile.ZipFile(buffer), ["tests"], tmp_path)

        assert os.access(tmp_path / "tests" / "run.sh", os.X_OK)
        assert (tmp_path / "tests" / "secret.txt").read_text() == "x"
        assert not os.access(tmp_path / "tests" / "secret.txt", os.X_OK)
