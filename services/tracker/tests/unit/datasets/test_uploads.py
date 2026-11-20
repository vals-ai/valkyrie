import hashlib
import io
import stat
import zipfile
from pathlib import Path

import pytest
from valkyrie.sdk.task_content import task_digest

from tests.unit.datasets.support import TASK, archive_digest_of, zip_bytes
from tracker import config
from tracker.services.datasets.extract import UnsafeArchiveError
from tracker.services.datasets.publish import InvalidUploadError, RejectedUploadError, UploadTooLargeError
from tracker.services.datasets.uploads import PreparedDataset, prepare_dataset_archive


def _prepare(tmp_path: Path, content: bytes) -> PreparedDataset:
    source = tmp_path / "upload.zip"
    source.write_bytes(content)
    return prepare_dataset_archive(
        source,
        tmp_path / "prepared",
        expected_size=len(content),
        expected_sha256=hashlib.sha256(content).hexdigest(),
        target_name="example/validation",
    )


@pytest.mark.description("Dataset ZIP uploads reuse native folder selection, manifest locks, and task validation.")
class TestPrepareDataset:
    @pytest.mark.description("Damaged compressed ZIP data is rejected as an invalid upload.")
    def test_damaged_compressed_entry(self, tmp_path: Path) -> None:
        content = bytearray(zip_bytes({"task-a/task.toml": TASK["task.toml"]}))
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entry = archive.infolist()[0]
            offset = entry.header_offset + 30 + len(entry.filename.encode()) + len(entry.extra)
        content[offset] = 7
        with pytest.raises(InvalidUploadError, match="damaged compressed data"):
            _prepare(tmp_path, bytes(content))
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("A file in place of a task directory is rejected with the path to correct.")
    @pytest.mark.parametrize("directory", ["solution", "steps", "environment", "tests"])
    def test_task_directory_is_a_file(self, tmp_path: Path, directory: str) -> None:
        files = {name: value for name, value in TASK.items() if not name.startswith(f"{directory}/")}
        files[directory] = b"not a directory"
        with pytest.raises(RejectedUploadError, match=f"Task task-a: {directory} must be a directory"):
            _prepare(tmp_path, zip_bytes({f"task-a/{name}": value for name, value in files.items()}))

    @pytest.mark.description(
        "A wrapped dataset preserves executable scripts and produces the same task content as the SDK."
    )
    def test_native_content_and_modes(self, tmp_path: Path) -> None:
        files = {f"bundle/task-a/{name}": content for name, content in TASK.items()}
        files.update(
            {
                "bundle/task-a/environment/ignored.txt": b"excluded",
                "bundle/task-a/.gitignore": b"environment/ignored.txt\n",
                "bundle/task-a/tests/.harborignore": b"hidden.txt\n",
                "bundle/task-a/tests/hidden.txt": b"excluded",
                "bundle/task-a/unused.txt": b"not task content",
            }
        )
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("bundle/", b"")
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr("bundle/", b"")
            for name, content in files.items():
                entry = zipfile.ZipInfo(name)
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | (0o755 if name.endswith(".sh") else 0o644)) << 16
                archive.writestr(entry, content)
        prepared = _prepare(tmp_path, data.getvalue())
        assert prepared.request.tasks == {"task-a": archive_digest_of(zip_bytes(TASK))}
        assert prepared.request.metric_digest is None
        assert prepared.request.manifest is None
        archive_path = prepared.archives[prepared.request.tasks["task-a"]]
        with zipfile.ZipFile(archive_path) as archive:
            assert set(archive.namelist()) == set(TASK)
            assert archive.getinfo("tests/test.sh").external_attr >> 16 & 0o777 == 0o755
            assert archive.read("environment/data/big.bin") == TASK["environment/data/big.bin"]
        assert any("unused.txt" in warning for warning in prepared.warnings)
        assert not (tmp_path / "prepared" / "unpacked").exists()

    @pytest.mark.description(
        "A manifest selects its exact tasks, locks their digests, and retains its metric script and tag."
    )
    @pytest.mark.parametrize("draft_layout", [False, True], ids=["valid-unlisted-task", "unfinished-unlisted-task"])
    def test_manifest_selection(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, draft_layout: bool) -> None:
        monkeypatch.setattr(config, "METRIC_RUNNER_FUNCTION", "metric-runner")
        task = {**TASK, "task.toml": TASK["task.toml"] + b"\n[task]\nname = 'example/task-a'\n"}
        other = {**TASK, "task.toml": TASK["task.toml"] + b"\n[task]\nname = 'example/task-b'\n"}
        if draft_layout:
            other = {name: content for name, content in other.items() if not name.startswith("solution/")}
            other["solution"] = b"unfinished draft"
        digest = archive_digest_of(zip_bytes(task))
        metric = b"def score(rewards):\n    return sum(rewards)\n"
        manifest = (
            "[dataset]\nname = 'example/validation'\nversion = 'release-1'\n"
            f"[[tasks]]\nname = 'example/task-a'\ndigest = '{digest}'\n"
            "[[files]]\npath = 'metric.py'\n"
            f"digest = 'sha256:{hashlib.sha256(metric).hexdigest()}'\n"
        )
        files = {
            **{f"task-a/{name}": content for name, content in task.items()},
            **{f"task-b/{name}": content for name, content in other.items()},
            "dataset.toml": manifest.encode(),
            "metric.py": metric,
        }
        prepared = _prepare(tmp_path, zip_bytes(files))
        assert prepared.request.tasks == {"task-a": digest}
        assert prepared.request.tags == ["release-1"]
        assert prepared.request.manifest is not None
        assert prepared.request.manifest.tasks["task-a"].name == "example/task-a"
        assert prepared.request.metric_digest == "sha256:" + hashlib.sha256(metric).hexdigest()
        assert prepared.metric == metric
        assert any("excludes 1 task" in warning and "task-b" in warning for warning in prepared.warnings)

    @pytest.mark.description(
        "A manifest upload rejects an oversized task configuration even when it excludes that task."
    )
    def test_unlisted_task_configuration_limit(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        task = {**TASK, "task.toml": TASK["task.toml"] + b"\n[task]\nname = 'example/task-a'\n"}
        monkeypatch.setattr("tracker.services.datasets.uploads._MAX_CONFIG_BYTES", len(task["task.toml"]))
        content = zip_bytes(
            {
                "dataset.toml": "[dataset]\nname = 'example/validation'\n[[tasks]]\nname = 'example/task-a'\n"
                f"digest = '{archive_digest_of(zip_bytes(task))}'\n",
                **{f"task-a/{name}": value for name, value in task.items()},
                "task-b/task.toml": b"[task]\nname = 'example/task-b'\n" + b" " * len(task["task.toml"]),
            }
        )

        with pytest.raises(UploadTooLargeError, match="task.toml"):
            _prepare(tmp_path, content)

    @pytest.mark.description("A task changed after its manifest lock is rejected and leaves no prepared output.")
    def test_manifest_mismatch(self, tmp_path: Path) -> None:
        task = {**TASK, "task.toml": TASK["task.toml"] + b"\n[task]\nname = 'example/task-a'\n"}
        content = zip_bytes(
            {
                **{f"task-a/{name}": value for name, value in task.items()},
                "dataset.toml": "[dataset]\nname = 'example/validation'\n[[tasks]]\nname = 'example/task-a'\ndigest = 'sha256:"
                + "0" * 64
                + "'\n",
            }
        )
        with pytest.raises(ValueError, match="changed since"):
            _prepare(tmp_path, content)
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("Valid folder structure does not bypass the Tracker's task configuration checks.")
    def test_native_validation(self, tmp_path: Path) -> None:
        content = zip_bytes(
            {
                f"task-a/{name}": value
                for name, value in {
                    **TASK,
                    "task.toml": b"[environment]\ncpus = 0\n",
                }.items()
            }
        )
        with pytest.raises(RejectedUploadError, match="cpus"):
            _prepare(tmp_path, content)
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("Unsafe and ambiguous archive paths fail before any task is prepared.")
    @pytest.mark.parametrize("name", ["../escape", "/absolute", "task-a/./alias", "task-a//alias", "task-a\\alias"])
    def test_unsafe_paths(self, tmp_path: Path, name: str) -> None:
        with pytest.raises(UnsafeArchiveError):
            _prepare(tmp_path, zip_bytes({name: b"unsafe"}))
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("Duplicate files, links, and file-directory collisions cannot replace extracted content.")
    @pytest.mark.parametrize("kind", ["duplicate", "symlink", "file-directory"])
    def test_ambiguous_members(self, tmp_path: Path, kind: str) -> None:
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("task-a/file", b"first")
            if kind == "duplicate":
                with pytest.warns(UserWarning, match="Duplicate name"):
                    archive.writestr("task-a/file", b"second")
            elif kind == "symlink":
                entry = zipfile.ZipInfo("task-a/link")
                entry.create_system = 3
                entry.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(entry, b"../../outside")
            else:
                archive.writestr("task-a/file/child", b"conflict")
        with pytest.raises(UnsafeArchiveError):
            _prepare(tmp_path, data.getvalue())
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("Archive inspection and extraction stop at their configured byte limits.")
    @pytest.mark.parametrize(
        "limit", ["uploads._DIRECTORY_READ_BYTES", "extract.MAX_EXTRACTED_BYTES", "uploads._MAX_CONFIG_BYTES"]
    )
    def test_processing_limits(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: str) -> None:
        monkeypatch.setattr(f"tracker.services.datasets.{limit}", 10)
        content = zip_bytes({f"task-a/{name}": value for name, value in TASK.items()})
        with pytest.raises(UploadTooLargeError):
            _prepare(tmp_path, content)
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("The processor rejects replaced bytes even when the replacement has the same size.")
    def test_raw_checksum(self, tmp_path: Path) -> None:
        source = tmp_path / "upload.zip"
        source.write_bytes(b"replaced")
        with pytest.raises(InvalidUploadError, match="checksum"):
            prepare_dataset_archive(
                source,
                tmp_path / "prepared",
                expected_size=8,
                expected_sha256=hashlib.sha256(b"original").hexdigest(),
                target_name="example/validation",
            )
        assert not (tmp_path / "prepared").exists()

    @pytest.mark.description("Task digests match the SDK when nested ignore rules change the selected files.")
    def test_sdk_digest_parity(self, tmp_path: Path) -> None:
        folder = tmp_path / "source" / "task-a"
        files = {**TASK, "tests/.harborignore": b"private/\n", "tests/private/data.txt": b"excluded"}
        for name, content in files.items():
            path = folder / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        expected = task_digest(folder)
        prepared = _prepare(tmp_path, zip_bytes({f"task-a/{name}": content for name, content in files.items()}))
        assert prepared.request.tasks == {"task-a": expected}
