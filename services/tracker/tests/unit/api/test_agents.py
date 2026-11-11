import io
import stat
import struct
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from botocore.exceptions import ClientError
from tracker import auth, config
from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.exceptions import S3Error

import tracker.api.agents as agents_api
from main import app
from tracker.aws import s3 as aws_s3
from tracker.aws.s3 import S3ObjectStore

_client = TestClient(app)


def _agent_archive(
    member: str = "demo/run.py",
    contract: str | None = None,
    *,
    symlink: bool = False,
    unsupported_compression: bool = False,
) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "demo/contract.yaml",
            contract
            if contract is not None
            else "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path}'\n",
        )
        info = zipfile.ZipInfo(member)
        if symlink:
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "agent content")

    body = bytearray(stream.getvalue())
    if unsupported_compression:
        struct.pack_into("<H", body, 8, 99)
        struct.pack_into("<H", body, body.index(b"PK\x01\x02") + 10, 99)

    return bytes(body)


@pytest.mark.description("Write validation and clean storage permission failures.")
class TestAgentWrites:
    @pytest.mark.description("A conditional agent upload that loses the race returns 409.")
    def test_conditional_upload_returns_conflict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        upload = AsyncMock(side_effect=FileExistsError("agents/demo.zip"))
        monkeypatch.setattr(aws_s3, "upload_stream_to_s3", upload)
        response = _client.put(
            "/agents/demo?overwrite=false",
            headers={"Content-Type": "application/zip"},
            content=_agent_archive(),
        )
        assert response.status_code == 409
        assert upload.call_args.kwargs["overwrite"] is False

    @pytest.mark.description("An agent upload with unsafe archive members is rejected.")
    @pytest.mark.parametrize(
        "member", ["../escape", "/escape", "demo/../escape", "other/file", "demo\\file", "demo/C:file"]
    )
    def test_upload_rejects_unsafe_members(self, member: str) -> None:
        response = _client.put(
            "/agents/demo",
            headers={"Content-Type": "application/zip"},
            content=_agent_archive(member),
        )

        assert response.status_code == 400

    @pytest.mark.description("An agent upload with an invalid contract, a symlink, or a bad CRC is rejected.")
    @pytest.mark.parametrize(
        "body",
        [
            b"not a zip",
            _agent_archive(contract="name: demo"),
            _agent_archive(symlink=True),
            _agent_archive(unsupported_compression=True),
            _agent_archive().replace(b"agent content", b"wrong content"),
        ],
        ids=["not-zip", "invalid-contract", "symlink", "unsupported-compression", "bad-crc"],
    )
    def test_upload_rejects_invalid_contract_symlink_and_crc(self, body: bytes) -> None:
        response = _client.put("/agents/demo", headers={"Content-Type": "application/zip"}, content=body)

        assert response.status_code == 400

    @pytest.mark.description("An agent upload over the size limits returns 413.")
    @pytest.mark.parametrize(
        "setting", ["AGENT_UPLOAD_MAX_BYTES", "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", "AGENT_ARCHIVE_MAX_ENTRIES"]
    )
    def test_upload_limits_return_413(self, monkeypatch: pytest.MonkeyPatch, setting: str) -> None:
        monkeypatch.setattr(config, setting, 1)
        response = _client.put("/agents/demo", headers={"Content-Type": "application/zip"}, content=_agent_archive())

        assert response.status_code == 413
        assert setting in response.json()["detail"]

    @pytest.mark.description("The bytes actually uploaded are bounded, whatever the headers say.")
    def test_actual_upload_bytes_are_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(config, "AGENT_UPLOAD_MAX_BYTES", 3)
        response = _client.put(
            "/agents/demo",
            headers={"Content-Type": "application/zip", "Content-Length": "1"},
            content=b"oversized body",
        )

        assert response.status_code == 413

    @pytest.mark.description("Archive metadata limits are checked before decompression.")
    def test_metadata_limits_precede_decompression(self, monkeypatch: pytest.MonkeyPatch) -> None:
        body = _agent_archive()
        monkeypatch.setattr(config, "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", 1)
        monkeypatch.setattr(zipfile.ZipFile, "open", MagicMock(side_effect=AssertionError("archive was decompressed")))

        response = _client.put("/agents/demo", headers={"Content-Type": "application/zip"}, content=body)

        assert response.status_code == 413

    @pytest.mark.description("The bytes actually decompressed are bounded.")
    def test_actual_decompressed_bytes_are_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        body = _agent_archive()
        monkeypatch.setattr(config, "AGENT_ARCHIVE_MAX_EXPANDED_BYTES", 1000)
        monkeypatch.setattr(zipfile.ZipExtFile, "read", MagicMock(return_value=b"x" * 1001))

        response = _client.put("/agents/demo", headers={"Content-Type": "application/zip"}, content=body)

        assert response.status_code == 413

    @pytest.mark.description("A storage permission denial returns an actionable error.")
    @pytest.mark.parametrize("operation", ["put", "delete"])
    @pytest.mark.parametrize(
        "code, status, detail",
        [("AccessDenied", 403, "permission denied"), ("InternalError", 502, "storage operation failed")],
    )
    def test_storage_denial_is_actionable(
        self,
        monkeypatch: pytest.MonkeyPatch,
        operation: str,
        code: str,
        status: int,
        detail: str,
    ) -> None:
        error = S3Error("storage failed")
        error.__cause__ = ClientError({"Error": {"Code": code}}, "PutObject")
        monkeypatch.setattr(aws_s3, "s3_object_exists", AsyncMock(return_value=True))
        monkeypatch.setattr(aws_s3, "upload_stream_to_s3", AsyncMock(side_effect=error))
        monkeypatch.setattr(aws_s3, "delete_from_s3", AsyncMock(side_effect=error))

        response = _client.request(
            operation,
            "/agents/demo",
            headers={"Content-Type": "application/zip"},
            content=_agent_archive(),
        )

        assert response.status_code == status
        assert detail in response.json()["detail"]

    @pytest.mark.description("An agent upload with invalid headers is rejected.")
    @pytest.mark.parametrize(
        "name, headers, status",
        [
            ("invalid name", {"Content-Type": "application/zip"}, 400),
            ("oracle", {"Content-Type": "application/zip"}, 400),
            ("demo", {"Content-Type": "application/json"}, 415),
            ("demo", {"Content-Type": "application/zip", "Content-Length": "invalid"}, 400),
            ("demo", {"Content-Type": "application/zip", "Content-Length": "-1"}, 400),
            ("demo", {"Content-Type": "application/zip", "Content-Length": "0" * 5000 + "1"}, 400),
        ],
    )
    def test_upload_rejects_invalid_headers(self, name: str, headers: dict[str, str], status: int) -> None:
        response = _client.put(f"/agents/{name}", headers=headers, content=b"archive")

        assert response.status_code == status

    @pytest.mark.description("A zero or negative `AGENT_UPLOAD_MAX_BYTES` raises `ValueError`.")
    @pytest.mark.parametrize("value", ["0", "-1", "invalid"])
    def test_operator_limits_must_be_positive(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("AGENT_UPLOAD_MAX_BYTES", value)

        with pytest.raises(ValueError):
            getattr(config, "_positive_int_setting")("AGENT_UPLOAD_MAX_BYTES", 1073741824)


@pytest.mark.description("Agent catalog and download route behavior.")
class TestAgentRoutes:
    @pytest.mark.description("A denied HEAD on an agent returns a permission error.")
    @pytest.mark.parametrize("method, path", [("DELETE", "/agents/demo"), ("GET", "/agents/demo/download-url")])
    def test_denied_head_returns_permission_error(
        self, monkeypatch: pytest.MonkeyPatch, method: str, path: str
    ) -> None:
        error = S3Error("storage failed")
        error.__cause__ = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
        operation = "delete" if method == "DELETE" else "exists"
        monkeypatch.setattr(S3ObjectStore, operation, AsyncMock(side_effect=error))

        response = _client.request(method, path)

        assert response.status_code == 403
        assert "permission denied" in response.json()["detail"]

    @pytest.mark.description(
        (
            "Agent listing must expose the names and timestamps returned by storage.\n"
            "\n"
            "Test cases:\n"
            "- The route serializes one stored agent and its last-modified timestamp."
        )
    )
    def test_list_agents_returns_storage_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        list_agents = AsyncMock(return_value=[("agent-a", datetime(2026, 1, 2, tzinfo=timezone.utc))])
        monkeypatch.setattr(agents_api, "list_agents", list_agents)

        response = _client.get(
            "/agents",
        )

        assert response.status_code == 200
        assert response.json() == {"agents": [{"name": "agent-a", "last_modified": "2026-01-02 00:00:00+00:00"}]}
        list_agents.assert_awaited_once()
        assert isinstance(list_agents.call_args.args[0], S3ObjectStore)

    @pytest.mark.description(
        (
            "Download links must use the route's configured expiration in both the signer and response.\n"
            "\n"
            "Test cases:\n"
            "- An existing agent receives a URL signed with the configured TTL."
        )
    )
    def test_agent_download_url_uses_route_expiration(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        exists = AsyncMock(return_value=True)
        presigned_url = AsyncMock(return_value="https://example.test/agent-a.zip")
        monkeypatch.setattr(S3ObjectStore, "exists", exists)
        monkeypatch.setattr(S3ObjectStore, "temporary_download_url", presigned_url)

        response = _client.get("/agents/agent-a/download-url")

        assert response.status_code == 200
        assert response.json() == {
            "name": "agent-a",
            "download_url": "https://example.test/agent-a.zip",
            "expires_in": agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        }
        exists.assert_awaited_once_with("agents/agent-a.zip")
        presigned_url.assert_awaited_once_with(
            "agents/agent-a.zip",
            expires_in=agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
        )

    @pytest.mark.description(
        (
            "Missing agent artifacts must return not found instead of a useless signed URL.\n"
            "\n"
            "Test cases:\n"
            "- A missing storage object receives a stable 404 response."
        )
    )
    def test_agent_download_url_returns_not_found_for_missing_agent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        exists = AsyncMock(return_value=False)
        monkeypatch.setattr(S3ObjectStore, "exists", exists)

        response = _client.get("/agents/missing/download-url")

        assert response.status_code == 404
        assert response.json()["detail"] == "Agent 'missing' not found in S3"
        exists.assert_awaited_once_with("agents/missing.zip")

    @pytest.mark.description(
        (
            "Download local bundles through the authenticated organization-scoped route.\n"
            "\n"
            "Test cases:\n"
            "- Download links return agent bytes without requiring a shared filesystem.\n"
            "- Other organizations and missing credentials cannot download the bundle.\n"
            "- Invalid agent names and missing files cannot download."
        )
    )
    def test_local_agent_download_is_scoped_and_requires_authentication(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        org = app.dependency_overrides[get_current_org]()
        monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path))
        local_client = TestClient(app, base_url="http://localhost")
        archive = tmp_path / "orgs" / str(org.id) / "objects/agents/demo.zip"
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b"agent bundle")

        response = local_client.get("/agents/demo/download-url")

        assert response.status_code == 200, response.text
        assert response.json()["expires_in"] == 0
        download_url = response.json()["download_url"]
        assert download_url.startswith("http://localhost/agents/demo/download-url?")
        download = local_client.get(download_url)
        assert download.status_code == 200
        assert download.content == b"agent bundle"
        assert local_client.get("/agents/invalid%3Aname/download-url", params={"download": "true"}).status_code == 400

        with monkeypatch.context() as scoped:
            scoped.setitem(app.dependency_overrides, get_current_org, lambda: Org(id=uuid4(), name="other"))
            assert local_client.get(download_url).status_code == 404
        with monkeypatch.context() as scoped:
            scoped.delitem(app.dependency_overrides, get_current_org)
            scoped.setattr(auth, "AUTH_REQUIRED", True)
            assert local_client.get(download_url).status_code == 401

        archive.unlink()
        assert local_client.get(download_url).status_code == 404
