from pathlib import Path

import pytest

from valkyrie.sdk.dataset_manifest import (
    DatasetToml,
    ManifestError,
    check_digests,
    check_script,
    lock_folders,
    read_manifest,
    wire_manifest,
)
from valkyrie.sdk.metric_script import metric_digest
from valkyrie.sdk.task_archive import find_task_folders
from valkyrie.sdk.task_content import task_digest


def write_task(root: Path, folder: str, name: str | None = None, text: str = "Do it.") -> None:
    section = f'[task]\nname = "{name}"\n' if name else ""
    files = {
        "task.toml": f"{section}[agent]\ntimeout_sec = 60\n",
        "instruction.md": text,
        "tests/test.sh": "#!/bin/bash\n",
    }
    for relative, content in files.items():
        (root / folder / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / folder / relative).write_text(content)


def write_manifest(root: Path, tasks: dict[str, str], dataset: str = 'name = "acme/verified"', extra: str = "") -> None:
    entries = "".join(f'[[tasks]]\nname = "{name}"\ndigest = "{digest}"\n' for name, digest in tasks.items())
    (root / "dataset.toml").write_text(f"[dataset]\n{dataset}\n\n{entries}{extra}")


def listed(root: Path, *folders: str) -> dict[str, str]:
    return {f"acme/{folder}": task_digest(root / folder) for folder in folders}


@pytest.mark.description("What a dataset.toml must say.")
class TestReadManifest:
    @pytest.mark.description("A folder without a manifest has none.")
    def test_a_folder_without_a_manifest_has_none(self, tmp_path: Path) -> None:
        assert read_manifest(tmp_path) is None

    @pytest.mark.description("The dataset tasks and metadata are read.")
    def test_the_dataset_tasks_and_metadata_are_read(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(
            tmp_path,
            listed(tmp_path, "a"),
            dataset='name = "acme/verified"\nversion = "1.0"\ndescription = "Checked"\nkeywords = ["code"]\n'
            'authors = [{ name = "Ada", email = "ada@example.com" }]',
            extra='\n[metadata]\nsource = "https://example.test"\n',
        )

        manifest = read_manifest(tmp_path)

        assert manifest is not None
        assert manifest.dataset.version == "1.0" and manifest.dataset.keywords == ["code"]
        assert manifest.dataset.authors[0].email == "ada@example.com"
        assert manifest.metadata == {"source": "https://example.test"}
        assert [task.name for task in manifest.tasks] == ["acme/a"]

    @pytest.mark.description("A manifest harbor would refuse is refused.")
    @pytest.mark.parametrize(
        ("dataset", "tasks", "complaint"),
        [
            ('name = "verified"', {"acme/a": "sha256:" + "0" * 64}, "'org/name' format"),
            ('name = "acme/verified"', {"a": "sha256:" + "0" * 64}, "'org/name' format"),
            ('name = "acme/verified"', {"acme/a": "sha256:abc"}, "sha256:<64 hex chars>"),
            ('name = "acme/verified"\nversion = ""', {"acme/a": "sha256:" + "0" * 64}, "at least 1 character"),
            ('name = "acme/verified"', {}, "lists no tasks"),
        ],
    )
    def test_a_manifest_harbor_would_refuse_is_refused(
        self, tmp_path: Path, dataset: str, tasks: dict[str, str], complaint: str
    ) -> None:
        write_manifest(tmp_path, tasks, dataset=dataset)

        with pytest.raises(ManifestError, match=complaint):
            read_manifest(tmp_path)

    @pytest.mark.description("A manifest that is not toml is refused.")
    def test_a_manifest_that_is_not_toml_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "dataset.toml").write_text("[dataset\n")

        with pytest.raises(ManifestError, match="not a valid dataset.toml"):
            read_manifest(tmp_path)

    @pytest.mark.description("A manifest that pins a file other than metric py is refused.")
    def test_a_manifest_that_pins_a_file_other_than_metric_py_is_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(tmp_path, listed(tmp_path, "a"), extra='\n[[files]]\npath = "other.py"\n')

        with pytest.raises(ManifestError, match=r"pins dataset files \(other.py\), but only metric.py is supported"):
            read_manifest(tmp_path)

    @pytest.mark.description("A manifest may pin metric py with or without its digest.")
    def test_a_manifest_may_pin_metric_py_with_or_without_its_digest(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        digest = metric_digest(b"print(1)\n")
        write_manifest(tmp_path, listed(tmp_path, "a"), extra=f'\n[[files]]\npath = "metric.py"\ndigest = "{digest}"\n')

        manifest = read_manifest(tmp_path)

        assert manifest is not None
        assert [(file.path, file.digest) for file in manifest.files] == [("metric.py", digest)]
        write_manifest(tmp_path, listed(tmp_path, "a"), extra='\n[[files]]\npath = "metric.py"\n')
        assert read_manifest(tmp_path) is not None

    @pytest.mark.description("A file digest that is not a digest is refused.")
    def test_a_file_digest_that_is_not_a_digest_is_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(
            tmp_path, listed(tmp_path, "a"), extra='\n[[files]]\npath = "metric.py"\ndigest = "sha256:abc"\n'
        )

        with pytest.raises(ManifestError, match="sha256:<64 hex chars>"):
            read_manifest(tmp_path)


@pytest.mark.description("A manifest that pins metric.py must match the folder's script.")
class TestCheckScript:
    def manifest_pinning(self, tmp_path: Path, digest: str = "") -> DatasetToml:
        write_task(tmp_path, "a", "acme/a")
        entry = f'digest = "{digest}"\n' if digest else ""
        write_manifest(tmp_path, listed(tmp_path, "a"), extra=f'\n[[files]]\npath = "metric.py"\n{entry}')
        manifest = read_manifest(tmp_path)
        assert manifest is not None
        return manifest

    @pytest.mark.description("A matching or unpinned digest passes and no manifest needs nothing.")
    def test_a_matching_or_unpinned_digest_passes_and_no_manifest_needs_nothing(self, tmp_path: Path) -> None:
        script = b"print(1)\n"

        check_script(self.manifest_pinning(tmp_path, metric_digest(script)), script)
        check_script(self.manifest_pinning(tmp_path), script)
        check_script(None, None)

    @pytest.mark.description("A pinned script the folder lacks or has changed is refused.")
    def test_a_pinned_script_the_folder_lacks_or_has_changed_is_refused(self, tmp_path: Path) -> None:
        manifest = self.manifest_pinning(tmp_path, metric_digest(b"print(1)\n"))

        with pytest.raises(ManifestError, match="pins metric.py but the folder has none"):
            check_script(manifest, None)
        with pytest.raises(ManifestError, match="metric.py changed since dataset.toml was written"):
            check_script(manifest, b"print(2)\n")


@pytest.mark.description("Pairing listed tasks with folders and holding them to their digests.")
class TestLockFolders:
    def _locked(self, root: Path):
        manifest = read_manifest(root)
        assert manifest is not None
        return lock_folders(manifest, find_task_folders(root), root)

    @pytest.mark.description("Each listed task is paired with the folder that has its name.")
    def test_each_listed_task_is_paired_with_the_folder_that_has_its_name(self, tmp_path: Path) -> None:
        write_task(tmp_path, "folder-one", "acme/a")
        write_task(tmp_path, "folder-two", "acme/b", text="Another.")
        write_manifest(
            tmp_path, {"acme/b": task_digest(tmp_path / "folder-two"), "acme/a": task_digest(tmp_path / "folder-one")}
        )

        locked = self._locked(tmp_path)

        assert list(locked.folders) == ["folder-one", "folder-two"]
        assert locked.source_names == {"folder-one": "acme/a", "folder-two": "acme/b"}
        assert locked.unlisted == []

    @pytest.mark.description("Folders the manifest does not list are left out.")
    def test_folders_the_manifest_does_not_list_are_left_out(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_task(tmp_path, "extra", "acme/extra", text="Not listed.")
        write_manifest(tmp_path, listed(tmp_path, "a"))

        locked = self._locked(tmp_path)

        assert list(locked.folders) == ["a"]
        assert locked.unlisted == ["extra"]

    @pytest.mark.description("A listed task no folder has is refused by name.")
    def test_a_listed_task_no_folder_has_is_refused_by_name(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(tmp_path, {**listed(tmp_path, "a"), "acme/gone": "sha256:" + "1" * 64})

        with pytest.raises(ManifestError, match="no folder under .* has a \\[task\\] name for: acme/gone"):
            self._locked(tmp_path)

    @pytest.mark.description("A folder without a task name is refused.")
    def test_a_folder_without_a_task_name_is_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_task(tmp_path, "plain")
        write_manifest(tmp_path, listed(tmp_path, "a"))

        with pytest.raises(ManifestError, match="no \\[task\\] name in task.toml: plain"):
            self._locked(tmp_path)

    @pytest.mark.description("Two folders with the same task name are refused.")
    def test_two_folders_with_the_same_task_name_are_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_task(tmp_path, "b", "acme/a", text="Copy.")
        write_manifest(tmp_path, listed(tmp_path, "a"))

        with pytest.raises(ManifestError, match="acme/a in a, b"):
            self._locked(tmp_path)

    @pytest.mark.description("A task listed twice with different digests is refused.")
    def test_a_task_listed_twice_with_different_digests_is_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path, "a", "acme/a")
        digest = task_digest(tmp_path / "a")
        (tmp_path / "dataset.toml").write_text(
            '[dataset]\nname = "acme/verified"\n'
            f'[[tasks]]\nname = "acme/a"\ndigest = "{digest}"\n'
            f'[[tasks]]\nname = "acme/a"\ndigest = "sha256:{"2" * 64}"\n'
        )

        with pytest.raises(ManifestError, match="acme/a with two different digests"):
            self._locked(tmp_path)

    @pytest.mark.description("The dataset version becomes the tag.")
    @pytest.mark.parametrize(("version", "tag"), [("1.0", "1.0"), ("v2", "v2"), (None, None)])
    def test_the_dataset_version_becomes_the_tag(self, tmp_path: Path, version: str | None, tag: str | None) -> None:
        write_task(tmp_path, "a", "acme/a")
        label = "" if version is None else f'\nversion = "{version}"'
        write_manifest(tmp_path, listed(tmp_path, "a"), dataset=f'name = "acme/verified"{label}')

        assert self._locked(tmp_path).tag == tag

    @pytest.mark.description("A version that cannot be a tag is refused with the rule.")
    @pytest.mark.parametrize("version", ["1", "V1", "a_b", "sha256:abcdef12"])
    def test_a_version_that_cannot_be_a_tag_is_refused_with_the_rule(self, tmp_path: Path, version: str) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(tmp_path, listed(tmp_path, "a"), dataset=f'name = "acme/verified"\nversion = "{version}"')

        with pytest.raises(ManifestError, match="can't be a tag: Invalid tag .* a tag is lowercase letters"):
            self._locked(tmp_path)

    @pytest.mark.description("A task that changed since the manifest was written is refused naming both digests.")
    def test_a_task_that_changed_since_the_manifest_was_written_is_refused_naming_both_digests(
        self, tmp_path: Path
    ) -> None:
        write_task(tmp_path, "a", "acme/a")
        write_manifest(tmp_path, listed(tmp_path, "a"))
        expected = task_digest(tmp_path / "a")
        (tmp_path / "a" / "instruction.md").write_text("Changed.")
        locked = self._locked(tmp_path)
        found = task_digest(tmp_path / "a")

        with pytest.raises(ManifestError, match=f"Task a \\(acme/a\\) changed.*{expected}.*{found}"):
            check_digests(locked, {"a": found})

    @pytest.mark.description("The wire manifest keys tasks by folder name.")
    def test_the_wire_manifest_keys_tasks_by_folder_name(self, tmp_path: Path) -> None:
        write_task(tmp_path, "folder", "acme/a")
        write_manifest(tmp_path, {"acme/a": task_digest(tmp_path / "folder")})
        locked = self._locked(tmp_path)
        digests = {"folder": task_digest(tmp_path / "folder")}

        check_digests(locked, digests)
        wire = wire_manifest(locked, digests)

        assert wire.name == "acme/verified"
        assert {task_id: (task.name, task.digest) for task_id, task in wire.tasks.items()} == {
            "folder": ("acme/a", digests["folder"])
        }
