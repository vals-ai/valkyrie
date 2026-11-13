import io
import os
import shutil
import stat
import subprocess
import zipfile
from collections.abc import Awaitable, Callable
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


def _fake_registry_git(
    submodules: list[str],
    calls: list[tuple[str, ...]],
    *,
    subfolder: str | None = None,
    reject_shallow: bool = False,
) -> Callable[..., Awaitable[str]]:

    async def git(repository: Path | None, *arguments: str) -> str:
        if arguments[0] == "clone":
            root = Path(arguments[-1])
            (root / (subfolder or "")).mkdir(parents=True)
            (root / ".gitmodules").touch()
        elif arguments[:3] == ("config", "--file", ".gitmodules"):
            return "".join(
                f"submodule.{p}.path\n{p}\0submodule.{p}.url\nhttps://github.com/example/{i}\0"
                for i, p in enumerate(submodules)
            )
        elif arguments == ("rev-parse", "--absolute-git-dir"):
            return f"{repository}/.git\n"
        elif arguments[0] == "submodule":
            calls.append(arguments)
            if reject_shallow and "--depth" in arguments:
                raise ChildProcessError("not our ref")
        return ""

    return git


def _git(repository: Path, *arguments: str) -> str:
    command = ["git", "-c", "user.name=test", "-c", "user.email=test@example.test", "-c", "commit.gpgsign=false"]
    result = subprocess.run([*command, "-C", str(repository), *arguments], check=True, capture_output=True, text=True)

    return result.stdout.strip()


def _isolate_git(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for index, (key, value) in enumerate([("protocol.file.allow", "always"), ("protocol.version", "0")]):
        monkeypatch.setenv(f"GIT_CONFIG_KEY_{index}", key)
        monkeypatch.setenv(f"GIT_CONFIG_VALUE_{index}", value)
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")


def _refusing_upstream(path: Path) -> str:
    path.mkdir()
    _git(path, "init", "-q")
    (path / "version").write_text("1")
    _git(path, "add", "version")
    _git(path, "commit", "-q", "-m", "first")
    first = _git(path, "rev-parse", "HEAD")
    (path / "version").write_text("2")
    _git(path, "commit", "-q", "-a", "-m", "second")
    for option in ("allowReachableSHA1InWant", "allowAnySHA1InWant", "allowTipSHA1InWant"):
        _git(path, "config", f"uploadpack.{option}", "false")

    return first


def _repository_pinning(path: Path, remote: Path, name: str, commit: str, *, shallow: bool = False) -> None:
    path.mkdir()
    _git(path, "init", "-q")
    _git(path, "submodule", "add", "-q", remote.as_uri(), name)
    _git(path / name, "checkout", "-q", commit)
    if shallow:
        _git(path, "config", "--file", ".gitmodules", f"submodule.{name}.shallow", "true")
    _git(path, "add", "-A")
    _git(path, "commit", "-q", "-m", f"pin {name}")


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
        "A folder install limits submodule fetches to overlapping paths.\n"
        "\n"
        "Test cases:\n"
        "- Submodules inside the selected folder are fetched.\n"
        "- A submodule equal to or containing the selected folder is fetched.\n"
        "- Sibling paths with a shared name prefix are not fetched."
    )
    @pytest.mark.parametrize(
        ("subfolder", "submodules", "expected"),
        [
            ("agents/a", ["agents/a/upstream", "agents/b/upstream"], ["agents/a/upstream"]),
            ("agents/a", ["agents/a"], ["agents/a"]),
            ("agents/a/inner", ["agents/a"], ["agents/a"]),
            ("agents/a", ["agents/ab/upstream"], []),
        ],
    )
    async def test_subfolder_install_fetches_only_overlapping_submodules(
        self, monkeypatch: pytest.MonkeyPatch, subfolder: str, submodules: list[str], expected: list[str]
    ) -> None:
        calls: list[tuple[str, ...]] = []

        monkeypatch.setattr(
            github_download, "_run_git_command", _fake_registry_git(submodules, calls, subfolder=subfolder)
        )
        async with github_download.download_github(f"https://github.com/example/registry/tree/main/{subfolder}"):
            pass

        assert [call[-1] for call in calls] == expected

    @pytest.mark.description(
        "An agent install validates only the submodules selected by its folder.\n"
        "\n"
        "Test cases:\n"
        "- A disallowed remote outside the selected folder is ignored and the agent uploads.\n"
        "- A disallowed remote inside, equal to, or containing the selected folder fails before upload.\n"
        "- A whole repository install rejects every disallowed submodule remote."
    )
    @pytest.mark.parametrize("remote", ["git@github.com:example/child.git", "https://gitlab.com/example/child"])
    @pytest.mark.parametrize(
        ("folder", "submodule", "allowed"),
        [
            ("agents/a", "agents/b", True),
            ("agents/a", "agents/a/child", False),
            ("agents/a", "agents/a", False),
            ("agents/a/inner", "agents/a", False),
            (None, "agents/b", False),
        ],
    )
    async def test_install_skips_excluded_submodule_urls(
        self,
        make_client: ClientFactory,
        monkeypatch: pytest.MonkeyPatch,
        folder: str | None,
        submodule: str,
        allowed: bool,
        remote: str,
    ) -> None:
        configured: list[str] = []
        fetched: list[str] = []
        uploads: list[list[str]] = []

        async def git(repository: Path | None, *arguments: str) -> str:
            if arguments[0] == "clone":
                root = Path(arguments[-1])
                agent = root / (folder or "")
                agent.mkdir(parents=True)
                (agent / "contract.yaml").write_text(_CONTRACT)
                (root / ".gitmodules").touch()
            elif arguments[:3] == ("config", "--file", ".gitmodules"):
                return f"submodule.child.path\n{submodule}\0submodule.child.url\n{remote}\0"
            elif arguments[:2] == ("config", "--local"):
                configured.append(arguments[-1])
            elif arguments[0] == "submodule":
                fetched.append(arguments[-1])
            return ""

        def upload(request: httpx.Request) -> httpx.Response:
            with zipfile.ZipFile(io.BytesIO(request.content)) as archive:
                uploads.append(archive.namelist())
            return httpx.Response(200, json={"name": "demo"})

        monkeypatch.setattr(github_download, "_run_git_command", git)
        url = "https://github.com/example/registry"
        if folder:
            url += f"/tree/main/{folder}"

        async with make_client(upload) as client:
            if allowed:
                await client.agents.install(url)
            else:
                with pytest.raises(ValueError, match="Submodules must use HTTPS GitHub repository URLs"):
                    await client.agents.install(url)

        assert uploads == ([["demo/contract.yaml"]] if allowed else [])
        assert configured == []
        assert fetched == []

    @pytest.mark.description(
        "Recursive checkout applies the selected folder at each submodule level.\n"
        "\n"
        "Test cases:\n"
        "- A folder inside an ancestor submodule excludes unrelated nested siblings.\n"
        "- A deeply nested folder preserves its remaining path through each ancestor.\n"
        "- A submodule inside the selected folder fetches all its descendants.\n"
        "- A folder with no overlapping nested submodule fetches only its ancestor."
    )
    @pytest.mark.parametrize(
        ("within", "expected"),
        [
            ("agents/a/inner", ["agents/a", "inner/x", "deeper"]),
            ("agents/a/inner/x/deeper", ["agents/a", "inner/x", "deeper"]),
            ("agents", ["agents/a", "inner/x", "deeper", "sibling/y"]),
            ("agents/a/missing", ["agents/a"]),
        ],
    )
    async def test_recursive_submodule_scope(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, within: str, expected: list[str]
    ) -> None:
        root = tmp_path / "repository"
        declarations = {
            root: ["agents/a"],
            root / "agents/a": ["inner/x", "sibling/y"],
            root / "agents/a/inner/x": ["deeper"],
        }
        for repository in declarations:
            repository.mkdir(parents=True, exist_ok=True)
            (repository / ".gitmodules").touch()
        fetched: list[str] = []

        async def git(repository: Path | None, *arguments: str) -> str:
            if arguments[:3] == ("config", "--file", ".gitmodules"):
                assert repository is not None
                return "".join(
                    f"submodule.{path}.path\n{path}\0submodule.{path}.url\nhttps://github.com/example/child\0"
                    for path in declarations.get(repository, [])
                )
            if arguments[0] == "submodule":
                fetched.append(arguments[-1])
            return ""

        monkeypatch.setattr(github_download, "_run_git_command", git)

        await github_download._checkout_submodules(root, "https://github.com/example/root", within=within)  # pyright: ignore[reportPrivateUsage]

        assert fetched == expected

    @pytest.mark.description(
        "A real Git install fetches nested submodules according to the selected folder.\n"
        "\n"
        "Test cases:\n"
        "- A folder inside an ancestor submodule installs without touching a broken sibling remote.\n"
        "- Selecting a nested submodule directly installs its files and contract.\n"
        "- Selecting a parent folder fetches every nested submodule and fails on a broken remote before uploading."
    )
    @pytest.mark.parametrize("folder", ["agents/a/inner", "agents/a/inner/x", "agents"])
    async def test_scoped_install_with_real_git(
        self, make_client: ClientFactory, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, folder: str
    ) -> None:
        _isolate_git(monkeypatch)
        leaf = tmp_path / "leaf"
        leaf.mkdir()
        _git(leaf, "init", "-q")
        (leaf / "run.py").write_text("run")
        _git(leaf, "add", ".")
        _git(leaf, "commit", "-qm", "add files")
        middle = tmp_path / "middle"
        middle.mkdir()
        _git(middle, "init", "-q")
        _git(middle, "submodule", "add", "-q", leaf.as_uri(), "inner/x")
        _git(middle, "submodule", "add", "-q", leaf.as_uri(), "sibling/y")
        for name in ("inner", "inner/x"):
            (middle / name / "contract.yaml").write_text(_CONTRACT)
        _git(middle, "config", "--file", ".gitmodules", "submodule.inner/x.url", "https://github.com/example/leaf")
        _git(middle, "config", "--file", ".gitmodules", "submodule.sibling/y.url", "https://github.com/example/missing")
        _git(middle, "add", ".")
        _git(middle, "commit", "-qm", "add nested modules")
        _git(middle / "inner/x", "add", ".")
        _git(middle / "inner/x", "commit", "-qm", "add contract")
        _git(middle, "add", ".")
        _git(middle, "commit", "-qm", "update inner")
        registry = tmp_path / "registry"
        _repository_pinning(registry, middle, "agents/a", _git(middle, "rev-parse", "HEAD"))
        _git(registry, "branch", "-M", "main")
        _git(registry, "config", "--file", ".gitmodules", "submodule.agents/a.url", "https://github.com/example/middle")
        _git(registry, "add", ".")
        _git(registry, "commit", "-qm", "set remote")
        for index, (name, remote) in enumerate(
            [
                ("registry", registry),
                ("middle", middle),
                ("leaf", middle / "inner/x"),
                ("missing", tmp_path / "nonexistent"),
            ],
            start=2,
        ):
            monkeypatch.setenv(f"GIT_CONFIG_KEY_{index}", f"url.{remote.as_uri()}.insteadOf")
            monkeypatch.setenv(f"GIT_CONFIG_VALUE_{index}", f"https://github.com/example/{name}")
        monkeypatch.setenv("GIT_CONFIG_COUNT", "6")
        run_git = github_download._run_git_command  # pyright: ignore[reportPrivateUsage]
        fetched: list[str] = []

        async def git(repository: Path | None, *arguments: str) -> str:
            if arguments[0] == "submodule":
                fetched.append(arguments[-1])
            return await run_git(repository, *arguments)

        uploads: list[list[str]] = []

        def upload(request: httpx.Request) -> httpx.Response:
            with zipfile.ZipFile(io.BytesIO(request.content)) as archive:
                uploads.append(archive.namelist())
            return httpx.Response(200, json={"name": "demo"})

        monkeypatch.setattr(github_download, "_run_git_command", git)

        async with make_client(upload) as client:
            if folder == "agents":
                with pytest.raises(ChildProcessError, match="nonexistent"):
                    await client.agents.install(f"https://github.com/example/registry/tree/main/{folder}")
                assert fetched == ["agents/a", "inner/x", "sibling/y", "sibling/y"]
                assert uploads == []

                return
            await client.agents.install(f"https://github.com/example/registry/tree/main/{folder}")

        assert fetched == ["agents/a", "inner/x"]
        assert len(uploads) == 1
        assert "demo/contract.yaml" in uploads[0]
        assert any(name.endswith("/run.py") for name in uploads[0])
        print(f"Installed {folder}: fetched {fetched}, uploaded {uploads[0]}, sibling/y untouched")

    @pytest.mark.description("A whole repository install fetches every submodule at depth one.")
    async def test_whole_repository_install_fetches_every_submodule_at_depth_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, ...]] = []

        monkeypatch.setattr(github_download, "_run_git_command", _fake_registry_git(["first", "agents/second"], calls))
        async with github_download.download_github("https://github.com/example/registry"):
            pass

        assert calls == [
            ("submodule", "update", "--init", "--checkout", "--depth", "1", "--", "first"),
            ("submodule", "update", "--init", "--checkout", "--depth", "1", "--", "agents/second"),
        ]

    @pytest.mark.description("A failed shallow submodule fetch retries with full history.")
    async def test_shallow_submodule_failure_falls_back_to_full_fetch(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level("WARNING", logger="valkyrie.sdk.github_download")
        calls: list[tuple[str, ...]] = []

        monkeypatch.setattr(
            github_download, "_run_git_command", _fake_registry_git(["child"], calls, reject_shallow=True)
        )
        async with github_download.download_github("https://github.com/example/registry"):
            pass

        assert calls == [
            ("submodule", "update", "--init", "--checkout", "--depth", "1", "--", "child"),
            ("submodule", "update", "--init", "--checkout", "--no-recommend-shallow", "--", "child"),
        ]
        assert "Shallow fetch of submodule child failed" in caplog.text

    @pytest.mark.description("Fallback cleanup preserves files outside the partial submodule.")
    @pytest.mark.parametrize("name", ["../../../victim", "../../victim", "..", "."])
    async def test_fallback_cleanup_never_leaves_the_checkout(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
    ) -> None:
        repository = tmp_path / "work" / "repository"
        (repository / ".git" / "modules").mkdir(parents=True)
        (repository / ".git" / "modules" / "keep").touch()
        (repository / ".git" / "HEAD").touch()
        survivors = [repository / ".git" / "modules" / "keep", repository / ".git" / "HEAD"]
        for victim in (tmp_path / "work" / "victim", repository / "victim"):
            victim.mkdir()
            (victim / "file").touch()
            survivors.append(victim / "file")

        monkeypatch.setattr(github_download, "_run_git_command", _fake_registry_git([], [], reject_shallow=True))
        await github_download._fetch_submodule(repository, name, "child")  # pyright: ignore[reportPrivateUsage]

        assert all(survivor.exists() for survivor in survivors)

    @pytest.mark.description("Fallback cleanup never follows a symlink to delete its target.")
    @pytest.mark.parametrize("link", ["child", ".git/modules/child"])
    async def test_fallback_cleanup_never_follows_symlinks(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, link: str
    ) -> None:
        repository = tmp_path / "repository"
        (repository / ".git" / "modules").mkdir(parents=True)
        sibling = repository / "sibling"
        sibling.mkdir()
        (sibling / "file").touch()
        (repository / link).symlink_to(sibling, target_is_directory=True)

        monkeypatch.setattr(github_download, "_run_git_command", _fake_registry_git([], [], reject_shallow=True))
        await github_download._fetch_submodule(repository, "child", "child")  # pyright: ignore[reportPrivateUsage]

        assert (sibling / "file").exists()

    @pytest.mark.description("A shallow fetch rejection recovers the pinned commit with full history.")
    @pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
    @pytest.mark.parametrize("shallow", [False, True], ids=["default", "gitmodules-shallow"])
    async def test_shallow_fetch_fallback_recovers_with_real_git(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture, shallow: bool
    ) -> None:
        caplog.set_level("WARNING", logger="valkyrie.sdk.github_download")
        _isolate_git(monkeypatch)
        upstream = tmp_path / "upstream"
        pinned = _refusing_upstream(upstream)
        _repository_pinning(tmp_path / "registry", upstream, "child", pinned, shallow=shallow)
        checkout = tmp_path / "checkout"
        _git(tmp_path, "clone", "-q", "--no-recurse-submodules", str(tmp_path / "registry"), str(checkout))

        await github_download._fetch_submodule(checkout, "child", "child")  # pyright: ignore[reportPrivateUsage]

        assert _git(checkout / "child", "rev-parse", "HEAD") == pinned
        assert (checkout / "child" / "version").read_text() == "1"
        assert "Shallow fetch of submodule child failed" in caplog.text

    @pytest.mark.description("A nested shallow fetch rejection recovers through the parent Git directory.")
    @pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
    async def test_shallow_fetch_fallback_recovers_for_nested_submodule_with_real_git(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level("WARNING", logger="valkyrie.sdk.github_download")
        _isolate_git(monkeypatch)
        upstream = tmp_path / "upstream"
        pinned = _refusing_upstream(upstream)
        _repository_pinning(tmp_path / "middle", upstream, "child", pinned)
        _repository_pinning(
            tmp_path / "registry", tmp_path / "middle", "middle", _git(tmp_path / "middle", "rev-parse", "HEAD")
        )
        checkout = tmp_path / "checkout"
        _git(tmp_path, "clone", "-q", "--no-recurse-submodules", str(tmp_path / "registry"), str(checkout))
        _git(checkout, "submodule", "update", "--init", "--", "middle")
        assert (checkout / "middle" / ".git").is_file()

        await github_download._fetch_submodule(checkout / "middle", "child", "child")  # pyright: ignore[reportPrivateUsage]

        assert _git(checkout / "middle" / "child", "rev-parse", "HEAD") == pinned
        assert (checkout / "middle" / "child" / "version").read_text() == "1"
        assert "Shallow fetch of submodule child failed" in caplog.text

    @pytest.mark.description("An unsafe GitHub URL fails before any upload.")
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
