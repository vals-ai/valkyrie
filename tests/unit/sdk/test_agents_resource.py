import io
import os
import stat
import zipfile
from pathlib import Path

import httpx
import pytest

from tests.unit.sdk.conftest import ClientFactory
from valkyrie.sdk import ValkyrieAPIError, github_download
from valkyrie.sdk.agent_bundle import extract_agent_archive, get_agent_zip_stream

_CONTRACT = "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n"


def _archive(member: str = "demo/run.py", *, symlink: bool = False) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("demo/contract.yaml", _CONTRACT)
        info = zipfile.ZipInfo(member)
        if symlink:
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "agent content")

    return stream.getvalue()


def _unexpected_request(_request: httpx.Request) -> httpx.Response:
    pytest.fail("invalid input reached tracker")


@pytest.mark.description("SDK library requests and external archive transfers.")
class TestAgentsResource:
    @pytest.mark.description("`client.agents.list` returns agent models with their names.")
    async def test_list_returns_typed_agents(self, make_client: ClientFactory) -> None:
        def listing(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"agents": [{"name": "demo"}]})

        async with make_client(listing) as client:
            response = await client.agents.list()

        assert response.agents[0].name == "demo"

    @pytest.mark.description("Agent methods raise `ValueError` for an invalid name before any request.")
    @pytest.mark.parametrize("name", ["", " ", "agent one", ".", "..", "team/agent", "a\\b", "agent\n"])
    async def test_invalid_names_never_reach_tracker(self, make_client: ClientFactory, name: str) -> None:
        async with make_client(_unexpected_request) as client:
            for operation in (client.agents.download_url, client.agents.remove):
                with pytest.raises(ValueError, match="Invalid agent name"):
                    await operation(name)

    @pytest.mark.description(
        "`agents.download` fetches the archive URL without Tracker credentials and replaces the old folder."
    )
    @pytest.mark.parametrize("download_host", ["download.test", "tracker.test"])
    async def test_download_never_forwards_tracker_credentials(
        self,
        make_client: ClientFactory,
        tmp_path: Path,
        download_host: str,
    ) -> None:
        def download_url(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/demo.zip":
                assert request.url.host == download_host
                assert ("x-api-key" in request.headers) == (download_host == "tracker.test")
                assert "authorization" not in request.headers

                return httpx.Response(200, content=_archive())
            assert request.headers["X-Api-Key"] == "vals-key"
            return httpx.Response(
                200,
                json={
                    "name": "demo",
                    "download_url": f"https://{download_host}/demo.zip",
                    "expires_in": 300,
                },
            )

        async with make_client(download_url) as client:
            with pytest.raises(ValueError, match="max_archive_bytes"):
                await client.agents.download("demo", tmp_path, max_archive_bytes=1)
            assert not (tmp_path / "demo").exists()
            path = await client.agents.download("demo", tmp_path)

            assert (path / "run.py").read_text() == "agent content"
            with pytest.raises(FileExistsError):
                await client.agents.download("demo", tmp_path)
            (path / "obsolete").touch()
            await client.agents.download("demo", tmp_path, overwrite=True)

        assert not (path / "obsolete").exists()

    @pytest.mark.description(
        "A storage error on the archive URL raises `ValkyrieAPIError` with its status and body and writes no folder."
    )
    async def test_download_reports_the_storage_error_status(self, make_client: ClientFactory, tmp_path: Path) -> None:
        def download_url(request: httpx.Request) -> httpx.Response:
            if request.url.host == "download.test":
                return httpx.Response(403, text="<Error><Code>AccessDenied</Code></Error>")
            return httpx.Response(
                200, json={"name": "demo", "download_url": "https://download.test/demo.zip", "expires_in": 300}
            )

        async with make_client(download_url) as client:
            with pytest.raises(ValkyrieAPIError) as raised:
                await client.agents.download("demo", tmp_path)

        assert (raised.value.status_code, raised.value.detail) == (403, "<Error><Code>AccessDenied</Code></Error>")
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.description(
        "Installing a repo subfolder pushes it under the given alias and leaves its contract file unchanged."
    )
    async def test_install_subfolder_pushes_override_without_rewriting_contract(
        self,
        make_client: ClientFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def git(_repository: Path | None, *arguments: str) -> str:
            if arguments[0] == "clone":
                folder = Path(arguments[-1]) / "agents" / "demo"
                folder.mkdir(parents=True)
                (folder / "contract.yaml").write_text(_CONTRACT)
            return "refs/remotes/origin/main\n" if arguments[0] == "for-each-ref" else ""

        def upload(request: httpx.Request) -> httpx.Response:
            assert request.method == "PUT"
            assert request.url.path == "/agents/alias"
            assert request.headers["content-type"] == "application/zip"
            with zipfile.ZipFile(io.BytesIO(request.content)) as archive:
                assert archive.read("alias/contract.yaml").decode() == _CONTRACT

            return httpx.Response(200, json={"name": "alias"})

        monkeypatch.setattr(github_download, "_run_git_command", git)
        async with make_client(upload) as client:
            response = await client.agents.install(
                "https://github.com/example/agents/tree/main/agents/demo", name="alias"
            )

        assert response.name == "alias"

    @pytest.mark.description(
        "A `/tree/` URL checks out the longest matching branch or tag and treats the rest as the subfolder."
    )
    @pytest.mark.parametrize(
        ("tree_path", "reference", "subfolder"),
        [
            ("main", "main", ""),
            ("feature/demo/", "feature/demo", ""),
            ("feature/demo/agents/demo", "feature/demo", "agents/demo"),
            ("v1/agents", "v1", "agents"),
            ("0a1b2c3/agents", "0a1b2c3", "agents"),
        ],
    )
    async def test_tree_url_resolves_reference_and_subfolder(
        self, monkeypatch: pytest.MonkeyPatch, tree_path: str, reference: str, subfolder: str
    ) -> None:
        commands: list[tuple[str, ...]] = []

        async def git(_repository: Path | None, *arguments: str) -> str:
            commands.append(arguments)
            if arguments[0] == "clone":
                (Path(arguments[-1]) / "agents" / "demo").mkdir(parents=True)
            if arguments[0] == "for-each-ref":
                return "refs/remotes/origin/HEAD\nrefs/remotes/origin/main\nrefs/remotes/origin/feature/demo\nrefs/tags/v1\n"
            return ""

        monkeypatch.setattr(github_download, "_run_git_command", git)
        async with github_download.download_github(f"https://github.com/example/agents/tree/{tree_path}") as path:
            checked_out = path

        assert ("checkout", reference, "--") in commands
        assert checked_out == Path(commands[0][-1]) / subfolder
        assert (("sparse-checkout", "set", "--", subfolder) in commands) == bool(subfolder)

    @pytest.mark.description("Push controls whether an alias is overwritten.")
    @pytest.mark.parametrize("overwrite", [False, True])
    async def test_push_controls_alias_overwrite(
        self, make_client: ClientFactory, tmp_path: Path, overwrite: bool
    ) -> None:
        (tmp_path / "contract.yaml").write_text(_CONTRACT)

        def upload(request: httpx.Request) -> httpx.Response:
            assert dict(request.url.params) == {"overwrite": str(overwrite).lower()}
            return httpx.Response(200, json={"name": "demo"})

        async with make_client(upload) as client:
            await client.agents.push(tmp_path, overwrite=overwrite)

    @pytest.mark.description("Submodules validate each level before fetching.")
    @pytest.mark.parametrize("nested_url", ["https://github.com/example/nested", "https://example.test/nested"])
    async def test_submodules_validate_each_level_before_fetch(
        self, monkeypatch: pytest.MonkeyPatch, nested_url: str
    ) -> None:
        repositories: list[Path] = []
        fetched: list[str] = []

        async def git(repository: Path | None, *arguments: str) -> str:
            if arguments[0] == "clone":
                root = Path(arguments[-1])
                root.mkdir()
                (root / ".gitmodules").touch()
                repositories.append(root)
                return ""
            assert repository is not None
            if arguments[:3] == ("config", "--file", ".gitmodules"):
                url = "../child" if repository == repositories[0] else nested_url
                return f"submodule.child.path\nchild\0submodule.child.url\n{url}\0"
            if arguments[0] == "submodule":
                fetched.append(str(repository))
                child = repository / "child"
                child.mkdir()
                if repository == repositories[0]:
                    (child / ".gitmodules").touch()
            return ""

        monkeypatch.setattr(github_download, "_run_git_command", git)
        if nested_url.startswith("https://github.com/"):
            async with github_download.download_github("https://github.com/example/parent") as path:
                assert (path / "child" / "child").is_dir()
            assert len(fetched) == 2
        else:
            with pytest.raises(ValueError, match="HTTPS GitHub"):
                async with github_download.download_github("https://github.com/example/parent"):
                    pytest.fail("unvalidated submodule accepted")
            assert fetched == [str(repositories[0])]

    @pytest.mark.description(
        "`agents.install` raises `ValueError` for non-GitHub URLs, `..` paths, bad refs, and newlines before any request."
    )
    @pytest.mark.parametrize(
        "url",
        [
            "https://gitlab.com/example/agent",
            "https://github.com/example/repo/tree/main/../escape",
            "https://github.com/example/repo/tree/-bad/agents",
            "https://github.com/example/repo\n",
        ],
    )
    async def test_install_rejects_unsafe_urls(self, make_client: ClientFactory, url: str) -> None:
        async with make_client(_unexpected_request) as client:
            with pytest.raises(ValueError):
                await client.agents.install(url)


@pytest.mark.description("Bundle exclusions and extraction without filesystem escapes.")
class TestAgentArchive:
    @pytest.mark.description("The agent bundle leaves out caches and rejects symlinks.")
    def test_bundle_excludes_caches_and_rejects_symlinks(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        source.mkdir()
        (source / "run.py").write_text("run")
        (source / ".env").write_text("SECRET=excluded")
        (source / "__pycache__").mkdir()
        (source / "__pycache__" / "run.pyc").touch()
        with get_agent_zip_stream("alias", source) as stream, zipfile.ZipFile(stream) as archive:
            assert archive.namelist() == ["alias/run.py"]

        (source / "link").symlink_to(tmp_path)

        with pytest.raises(ValueError, match="symlinks that leave the agent folder"):
            with get_agent_zip_stream("alias", source):
                pytest.fail("symlink accepted")

    @pytest.mark.description("A symlink inside the agent folder is bundled as a copy of its target.")
    def test_a_link_inside_the_agent_folder_is_bundled_as_a_copy_of_its_target(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        (source / "assets").mkdir(parents=True)
        (source / "assets" / "icon.png").write_bytes(b"icon")
        (source / "AGENTS.md").write_text("notes")
        (source / "CLAUDE.md").symlink_to("AGENTS.md")
        (source / "public").symlink_to("assets", target_is_directory=True)

        with get_agent_zip_stream("alias", source) as stream, zipfile.ZipFile(stream) as archive:
            assert sorted(archive.namelist()) == [
                "alias/AGENTS.md",
                "alias/CLAUDE.md",
                "alias/assets/icon.png",
                "alias/public/icon.png",
            ]
            assert archive.read("alias/CLAUDE.md") == b"notes"
            assert all(info.external_attr >> 16 & 0o170000 != 0o120000 for info in archive.infolist())

    @pytest.mark.description("A broken symlink or a symlink loop is refused.")
    @pytest.mark.parametrize(
        ("link", "target", "complaint"),
        [("missing", "nowhere.txt", "broken symlinks"), ("loop", ".", "symlink loops")],
    )
    def test_a_broken_link_or_a_loop_is_refused(self, tmp_path: Path, link: str, target: str, complaint: str) -> None:
        source = tmp_path / "source"
        source.mkdir()
        (source / "run.py").write_text("run")
        (source / link).symlink_to(target)

        with pytest.raises(ValueError, match=complaint):
            with get_agent_zip_stream("alias", source):
                pytest.fail("link accepted")

    @pytest.mark.description(
        "Exclusions match whole names, so excluded suffixes are dropped and lookalike names are kept."
    )
    def test_bundle_exclusions_match_whole_names(self, tmp_path: Path) -> None:
        source = tmp_path / "source"
        (source / "demo.egg-info").mkdir(parents=True)
        (source / "demo.egg-info" / "PKG-INFO").touch()
        for name in ("data.sources.json", ".gitignore", ".env.local", "prod.env", ".envrc", "native.so", "libfoo.so.1"):
            (source / name).touch()
        with get_agent_zip_stream("alias", source) as stream, zipfile.ZipFile(stream) as archive:
            assert sorted(archive.namelist()) == ["alias/.gitignore", "alias/data.sources.json"]

    @pytest.mark.description("An unsafe archive leaves the existing target alone.")
    @pytest.mark.parametrize(
        "member",
        [
            "../escape",
            "/escape",
            "other/file",
            "demo/../escape",
            "demo\\escape",
            "demo/C:escape",
            "demo/contract.yaml",
            "demo/CONTRACT.yaml",
            "demo/./file",
            "demo//file",
        ],
    )
    def test_unsafe_archive_preserves_existing_target(self, tmp_path: Path, member: str) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")

        with pytest.raises(ValueError, match="Unsafe"):
            extract_agent_archive(io.BytesIO(_archive(member)), "demo", tmp_path, overwrite=True)

        assert (target / "keep").read_text() == "original"
        assert sorted(path.name for path in tmp_path.iterdir()) == ["demo"]

    @pytest.mark.description("Symlinks and corrupt archives are rejected.")
    def test_symlinks_and_corruption_are_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            extract_agent_archive(io.BytesIO(_archive(symlink=True)), "demo", tmp_path, overwrite=False)

        corrupted = _archive().replace(b"agent content", b"wrong content")

        with pytest.raises(zipfile.BadZipFile, match="CRC"):
            extract_agent_archive(io.BytesIO(corrupted), "demo", tmp_path, overwrite=False)

        assert not (tmp_path / "demo").exists()

    @pytest.mark.description(
        "Extracting over a target that is a symlink raises `FileExistsError` and writes nothing through it."
    )
    def test_existing_target_symlink_is_never_followed(self, tmp_path: Path) -> None:
        (tmp_path / "demo").symlink_to(tmp_path / "elsewhere", target_is_directory=True)

        with pytest.raises(FileExistsError):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True)

    @pytest.mark.description("An archive over the limits leaves the existing target alone.")
    @pytest.mark.parametrize("limit", ["max_archive_bytes", "max_expanded_bytes", "max_entries"])
    def test_limits_preserve_existing_target(self, tmp_path: Path, limit: str) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")

        with pytest.raises(ValueError, match=limit):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True, **{limit: 1})

        assert (target / "keep").read_text() == "original"
        assert list(tmp_path.iterdir()) == [target]

    @pytest.mark.description("A failed overwrite keeps the previous copy.")
    @pytest.mark.parametrize("rollback_fails", [False, True])
    def test_failed_overwrite_preserves_previous_copy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rollback_fails: bool
    ) -> None:
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")
        rename = Path.rename

        def fail_replacement(path: Path, destination: str | Path) -> Path:
            if path.name == "demo" and path.parent != tmp_path:
                if rollback_fails or not path.parent.name.startswith(".demo-backup-"):
                    raise OSError("rename failed")
            return rename(path, destination)

        monkeypatch.setattr(Path, "rename", fail_replacement)
        with pytest.raises(OSError):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True)

        if rollback_fails:
            backup = next(tmp_path.glob(".demo-backup-*/demo"))
            assert (backup / "keep").read_text() == "original"
        else:
            assert (target / "keep").read_text() == "original"
            assert list(tmp_path.iterdir()) == [target]

    @pytest.mark.description("The actual expanded size is bounded.")
    def test_actual_expanded_bytes_are_bounded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def expanded(_source: zipfile.ZipExtFile, _size: int = -1) -> bytes:
            return b"x" * 1001

        monkeypatch.setattr(zipfile.ZipExtFile, "read", expanded)

        with pytest.raises(ValueError, match="max_expanded_bytes"):
            extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=False, max_expanded_bytes=1000)

        assert not (tmp_path / "demo").exists()

    @pytest.mark.description(
        "If the old copy cannot be deleted after replacement, the new agent stays and a warning names the backup to remove."
    )
    def test_backup_cleanup_failure_keeps_successful_replacement(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level("WARNING", logger="valkyrie.sdk.agent_bundle")
        target = tmp_path / "demo"
        target.mkdir()
        (target / "keep").write_text("original")
        os_unlink = os.unlink

        def fail_unlink(path: str, *, dir_fd: int | None = None) -> None:
            if Path(path).name == "keep":
                raise PermissionError("backup cleanup denied")
            os_unlink(path, dir_fd=dir_fd)

        monkeypatch.setattr(os, "unlink", fail_unlink)
        result = extract_agent_archive(io.BytesIO(_archive()), "demo", tmp_path, overwrite=True)

        assert result == target
        assert (target / "run.py").read_text() == "agent content"
        assert not (target / "keep").exists()
        backup = next(tmp_path.glob(".demo-backup-*"))
        assert (backup / "demo" / "keep").read_text() == "original"
        assert str(backup) in caplog.text
        assert "manually" in caplog.text
