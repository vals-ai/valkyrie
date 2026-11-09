# pyright: reportPrivateUsage=false


from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from typing import Any
import time
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from descope.exceptions import AuthException
from fastapi import HTTPException
from jwt import InvalidAudienceError, InvalidSignatureError
from requests.exceptions import ReadTimeout
from sqlmodel import Session, select
from tenacity import wait_none

import tracker.auth as auth_module
from tracker.auth import (
    _exchange_and_normalize_access_key,
    LOGIN_REQUIRED,
    NO_ORGANIZATION,
    NOT_A_MEMBER,
    SEVERAL_ORGANIZATIONS,
    LoginIdentity,
    RequestIdentity,
    get_current_identity,
    get_current_login,
    get_current_runner,
    get_current_starter,
)
from tracker.aws.tenant_storage import TenantBucketError
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.repositories.organization import OrganizationRepository


@pytest.fixture
def test_org(empty_database_session: Session) -> Org:
    org = Org(id=uuid4(), name="test-tenant")
    empty_database_session.add(org)
    empty_database_session.commit()
    return org


@pytest.fixture(autouse=True)
def reset_access_key_state() -> Generator[None, None, None]:
    auth_module._exchange_and_normalize_access_key.cache_clear()
    yield
    auth_module._exchange_and_normalize_access_key.cache_clear()


@pytest.fixture
def mock_descope() -> Generator[MagicMock, None, None]:
    mock_client = MagicMock()
    with patch("tracker.auth._descope_client", mock_client):
        yield mock_client


def descope_access_key_response(
    *,
    tenant: str = "test-tenant",
    key_id: str = "K2abc",
    email: str | None = None,
    name: str | None = None,
    user_id: str | None = None,
    expires_at: int | float | None = None,
) -> dict[str, object]:
    session_token: dict[str, object] = {
        "sub": key_id,
        "tenants": {tenant: {}},
        "exp": time.time() + 60 if expires_at is None else expires_at,
    }
    if email is not None:
        session_token["email"] = email
    if name is not None:
        session_token["name"] = name
    if user_id is not None:
        session_token["customClaims"] = {"user_id": user_id}

    return {
        "tenants": {tenant: {}},
        "keyId": key_id,
        "sessionToken": session_token,
    }


def disable_auth_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    exchange_access_key = getattr(auth_module, "_exchange_access_key")
    monkeypatch.setattr(exchange_access_key.retry, "wait", wait_none())


_RUN_KEY_CLAIM_PLACEMENTS = [
    {"valkyrie_run_key": True},
    {"customClaims": {"valkyrie_run_key": True}},
    {"nsec": {"valkyrie_run_key": True}},
]


class TestDescopeIdentityResolution:
    @pytest.mark.description("Run keys are not accepted as API credentials.")
    @pytest.mark.parametrize("placement", _RUN_KEY_CLAIM_PLACEMENTS)
    def test_run_keys_are_not_api_credentials(self, mock_descope: MagicMock, placement: dict[str, object]) -> None:
        response = descope_access_key_response()
        session_token = response["sessionToken"]
        assert isinstance(session_token, dict)
        session_token.update(placement)
        mock_descope.exchange_access_key.return_value = response

        with pytest.raises(HTTPException) as exc_info:
            _exchange_and_normalize_access_key("run-key")

        assert exc_info.value.status_code == 401

    @pytest.mark.description("A key created at login needs its creator to still be in the tenant.")
    @pytest.mark.parametrize(
        ("user", "status"),
        [
            ({"userTenants": [{"tenantId": "test-tenant"}]}, None),
            ({"userTenants": [{"tenantId": "other"}]}, 401),
            (AuthException(404, "E", "missing"), 401),
            (AuthException(503, "E", "down"), 503),
            (None, 401),
        ],
    )
    def test_login_created_keys_need_their_creator_in_the_tenant(
        self, mock_descope: MagicMock, user: object, status: int | None
    ) -> None:
        response = descope_access_key_response(user_id="U-alice")
        session_token = response["sessionToken"]
        assert isinstance(session_token, dict)
        session_token["customClaims"] = {"user_id": "U-alice", "valkyrie_user_key": True}
        mock_descope.exchange_access_key.return_value = response
        load = mock_descope.mgmt.user.load_by_user_id
        load.side_effect = user if isinstance(user, Exception) else None
        load.return_value = {"user": user}

        with patch("tracker.auth.DESCOPE_MANAGEMENT_KEY", "" if user is None else "mgmt-key"):
            if status is None:
                assert auth_module._exchange_and_normalize_access_key("user-key").user_id == "U-alice"
                return
            with pytest.raises(HTTPException) as exc_info:
                auth_module._exchange_and_normalize_access_key("user-key")
        assert exc_info.value.status_code == status

    @pytest.mark.description("Exchanging a valid access key returns its tenant name and access key ID.")
    def test_valid_api_key_resolves_identity(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = _exchange_and_normalize_access_key("valid-key")
        assert identity.tenant_name == "test-tenant"
        assert identity.access_key_id == "K2abc"

    @pytest.mark.description("The tenant name from a valid access key matches an existing org.")
    def test_valid_api_key_finds_org(
        self, mock_descope: MagicMock, empty_database_session: Session, test_org: Org
    ) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = _exchange_and_normalize_access_key("valid-key")
        org = OrganizationRepository(empty_database_session).get_by_name(identity.tenant_name)
        assert org is not None
        assert org.id == test_org.id

    @pytest.mark.description("An invalid access key returns 401.")
    def test_access_key_invalid_api_key_raises_401(
        self, mock_descope: MagicMock, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        secret = "rejected-access-key-secret"
        mock_descope.exchange_access_key.side_effect = AuthException(
            status_code=401, error_message=f"Rejected {secret}"
        )
        monkeypatch.setattr(auth_module.logger, "handlers", [caplog.handler])

        with pytest.raises(HTTPException) as exc_info:
            _exchange_and_normalize_access_key(secret)

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Invalid API key"
        assert secret not in str(exc_info.value.detail)

        assert "API key validation failed" in caplog.text
        assert secret not in caplog.text

    @pytest.mark.description("A request must carry exactly one credential.")
    @pytest.mark.parametrize(
        ("headers", "detail"),
        [
            (
                {"authorization": "Bearer session-token", "x-api-key": "valid-key"},
                "Send Authorization OR x-api-key, not both",
            ),
            ({}, "Missing Authorization or x-api-key header"),
        ],
    )
    def test_requires_exactly_one_credential(
        self, empty_database_session: Session, headers: dict[str, str], detail: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        request = MagicMock()
        request.headers = headers

        with pytest.raises(HTTPException) as exc_info:
            get_current_identity(request, empty_database_session)

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == detail

    @pytest.mark.description("An org missing from the database resolves to no identity.")
    def test_org_not_in_db_returns_none(self, empty_database_session: Session) -> None:
        org = OrganizationRepository(empty_database_session).get_by_name("nonexistent-org")
        assert org is None

    @pytest.mark.description("An access key with full claims resolves every identity field.")
    def test_access_key_full_claims(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(
            email="Alice@Vals.AI",
            name="Alice Smith",
        )

        identity = _exchange_and_normalize_access_key("valid-key")
        assert identity.tenant_name == "test-tenant"
        assert identity.access_key_id == "K2abc"
        assert identity.email == "alice@vals.ai"
        assert identity.name == "Alice Smith"

    @pytest.mark.description("An access key without an email resolves to no identity.")
    def test_access_key_missing_email_returns_none(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()

        identity = _exchange_and_normalize_access_key("valid-key")
        assert identity.access_key_id == "K2abc"
        assert identity.email is None
        assert identity.name is None

    @pytest.mark.description("An access key without a name resolves to no identity.")
    def test_access_key_missing_name_returns_none(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(email="alice@vals.ai")

        identity = _exchange_and_normalize_access_key("valid-key")
        assert identity.email == "alice@vals.ai"
        assert identity.name is None

    @pytest.mark.description("A whitespace-only email claim is treated identically to a missing one.")
    def test_access_key_whitespace_only_email_treated_as_missing(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response(email="   ")

        identity = _exchange_and_normalize_access_key("valid-key")
        assert identity.email is None

    @pytest.mark.description("An access key must belong to exactly one tenant.")
    @pytest.mark.parametrize("tenants", [{}, {"a": {}, "b": {}}])
    def test_access_key_requires_one_tenant(
        self, mock_descope: MagicMock, tenants: dict[str, dict[str, object]]
    ) -> None:
        mock_descope.exchange_access_key.return_value = {
            "tenants": tenants,
            "keyId": "K2abc",
            "sessionToken": {"sub": "K2abc", "email": "alice@vals.ai"},
        }

        with pytest.raises(HTTPException) as exc_info:
            _exchange_and_normalize_access_key("invalid-tenant-key")

        assert exc_info.value.status_code == 400

    @pytest.mark.description(
        (
            "Transient Descope timeouts must retry and return the eventual identity.\n"
            "\n"
            "Test cases:\n"
            "- The first exchange times out.\n"
            "- The second exchange succeeds without a real retry delay."
        )
    )
    def test_access_key_retries_read_timeout(self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        disable_auth_retry_wait(monkeypatch)
        mock_descope.exchange_access_key.side_effect = [
            ReadTimeout("HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"),
            descope_access_key_response(),
        ]

        identity = _exchange_and_normalize_access_key("some-key")

        assert identity.tenant_name == "test-tenant"
        assert mock_descope.exchange_access_key.call_count == 2

    @pytest.mark.description(
        (
            "Exhausted Descope retries must return a stable service-unavailable error.\n"
            "\n"
            "Test cases:\n"
            "- All three exchanges time out.\n"
            "- The client receives a safe 503 response after immediate test retries."
        )
    )
    def test_access_key_returns_503_when_retries_exhausted(
        self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        disable_auth_retry_wait(monkeypatch)
        mock_descope.exchange_access_key.side_effect = ReadTimeout(
            "HTTPSConnectionPool(host='api.descope.com', port=443): Read timed out. (read timeout=60)"
        )

        with patch("tracker.auth.logger.warning") as log_warning:
            with pytest.raises(HTTPException) as exc_info:
                _exchange_and_normalize_access_key("some-key")

        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == "Auth service unavailable"
        log_warning.assert_called_once_with(
            "Descope API key validation failed because the provider is unavailable", exc_info=True
        )
        assert mock_descope.exchange_access_key.call_count == 3


class TestAccessKeyCache:
    def setup_method(self) -> None:
        auth_module._exchange_and_normalize_access_key.cache_clear()

    def teardown_method(self) -> None:
        auth_module._exchange_and_normalize_access_key.cache_clear()

    @pytest.mark.description(
        "A cached key expires at the earlier of the cache TTL or its own expiry minus skew, measured on the monotonic clock."
    )
    def test_cache_deadline_uses_monotonic_clock_ttl_and_expiry_skew(self) -> None:
        assert auth_module._cache_deadline(10000.0, wall_now=100.0, monotonic_now=500.0) == 560.0
        assert auth_module._cache_deadline(150.0, wall_now=100.0, monotonic_now=500.0) == 549.0
        assert auth_module._cache_deadline(105.0, wall_now=100.0, monotonic_now=500.0) == 504.0
        assert auth_module._cache_deadline(101.0, wall_now=100.0, monotonic_now=500.0) == 500.0

    @pytest.mark.description("A key whose expiry is unsafe is not cached.")
    @pytest.mark.parametrize(
        "expires_at", [None, float("nan"), float("inf"), 0.0, pytest.param(10**400, id="overflowing-int")]
    )
    def test_unsafe_expiry_is_not_cached(self, mock_descope: MagicMock, expires_at: int | float | None) -> None:
        response = descope_access_key_response(expires_at=expires_at)
        if expires_at is None:
            del response["sessionToken"]["exp"]  # type: ignore[index]
        mock_descope.exchange_access_key.return_value = response
        _exchange_and_normalize_access_key("uncached-key")
        _exchange_and_normalize_access_key("uncached-key")
        assert mock_descope.exchange_access_key.call_count == 2

    @pytest.mark.description("A cached key needs only one exchange.")
    def test_warm_key_uses_one_exchange(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()
        assert _exchange_and_normalize_access_key("warm-key") == _exchange_and_normalize_access_key("warm-key")
        assert mock_descope.exchange_access_key.call_count == 1

    @pytest.mark.description("Concurrent requests with the same key share one exchange.")
    def test_concurrent_same_key_uses_one_exchange(self, mock_descope: MagicMock) -> None:
        started, release = Event(), Event()

        def exchange(_api_key: str) -> dict[str, object]:
            started.set()
            assert release.wait(timeout=2)
            return descope_access_key_response()

        mock_descope.exchange_access_key.side_effect = exchange
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = [pool.submit(_exchange_and_normalize_access_key, "shared-key") for _ in range(8)]
            assert started.wait(timeout=2)
            release.set()
            identities = [result.result(timeout=2) for result in results]
        assert {identity.access_key_id for identity in identities} == {"K2abc"}
        assert mock_descope.exchange_access_key.call_count == 1

    @pytest.mark.description("Two different access keys are exchanged at the same time without waiting on each other.")
    def test_different_keys_exchange_concurrently(self, mock_descope: MagicMock) -> None:
        entered = Barrier(2, timeout=2)

        def exchange(api_key: str) -> dict[str, object]:
            entered.wait()
            return descope_access_key_response(key_id=f"id-{api_key}")

        mock_descope.exchange_access_key.side_effect = exchange
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(_exchange_and_normalize_access_key, key) for key in ("a", "b")]
            identities = [future.result(timeout=2) for future in futures]
        assert {identity.access_key_id for identity in identities} == {"id-a", "id-b"}

    @pytest.mark.description("Failed exchanges are not cached.")
    def test_failures_are_not_cached(self, mock_descope: MagicMock) -> None:
        mock_descope.exchange_access_key.side_effect = [
            AuthException(status_code=401, error_message="invalid"),
            descope_access_key_response(),
        ]
        with pytest.raises(HTTPException) as exc_info:
            _exchange_and_normalize_access_key("retry-key")
        assert exc_info.value.status_code == 401
        assert _exchange_and_normalize_access_key("retry-key").access_key_id == "K2abc"
        assert mock_descope.exchange_access_key.call_count == 2

    @pytest.mark.description("The key cache does not keep raw credentials.")
    def test_cache_keys_do_not_retain_raw_credentials(self, mock_descope: MagicMock) -> None:
        raw_key = "raw-access-key-secret"
        mock_descope.exchange_access_key.return_value = descope_access_key_response()
        _exchange_and_normalize_access_key(raw_key)
        assert raw_key not in repr(auth_module._access_key_cache)
        assert auth_module._access_key_digest(raw_key) != raw_key.encode()


@pytest.mark.description("Starter and organization dependencies across hosting modes.")
class TestCurrentStarterResolution:
    @pytest.mark.description("A self hosted Tracker resolves the run starter locally.")
    def test_get_current_starter_self_hosted(
        self, monkeypatch: pytest.MonkeyPatch, empty_database_session: Session
    ) -> None:
        empty_database_session.add(Org(id=uuid4(), name=DEFAULT_ORG_NAME))
        empty_database_session.commit()

        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", False)

        identity = get_current_starter(get_current_identity(MagicMock(), empty_database_session))

        assert isinstance(identity, RequestIdentity)
        assert identity.org.name == DEFAULT_ORG_NAME
        assert identity.access_key_id is None
        assert identity.email is None
        assert identity.name is None

    @pytest.mark.description("A hosted Tracker resolves the run starter from the identity.")
    @pytest.mark.parametrize(
        "token_profile", [{"email": "Alice@vals.ai", "name": "Alice"}, {}], ids=["token", "profile"]
    )
    @pytest.mark.parametrize(
        "headers", [{"x-api-key": "valid-key"}, {"authorization": "Bearer session-token"}], ids=["api-key", "session"]
    )
    def test_get_current_starter_hosted(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
        token_profile: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U-alice", **token_profile)
        mock_descope.validate_session.return_value = {"sub": "U-alice", "tenants": {test_org.name: {}}, **token_profile}
        mock_descope.mgmt.user.load_by_user_id.return_value = {"user": {"email": "alice@vals.ai", "name": "Alice"}}
        mock_request = MagicMock()
        mock_request.headers = headers

        identity = get_current_identity(mock_request, empty_database_session)
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()
        starter = get_current_starter(identity)

        assert (starter.org.id, starter.user_id) == (test_org.id, "U-alice")
        assert (starter.email, starter.name) == ("alice@vals.ai", "Alice")
        assert starter.access_key_id == ("K2abc" if "x-api-key" in headers else None)
        assert mock_descope.mgmt.user.load_by_user_id.call_count == (0 if token_profile else 1)

    @pytest.mark.description("A failed Descope profile lookup leaves the starter email empty and logs the traceback.")
    def test_get_current_starter_logs_profile_lookup_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        caplog: pytest.LogCaptureFixture,
        test_org: Org,
    ) -> None:
        mock_descope.mgmt.user.load_by_user_id.side_effect = ConnectionError("descope down")
        monkeypatch.setattr(auth_module.logger, "handlers", [caplog.handler])
        identity = RequestIdentity(org=test_org, user_id="U-alice")

        starter = get_current_starter(identity)

        assert (starter.email, starter.name) == (None, None)

        record = next(
            record for record in caplog.records if record.getMessage() == "Failed to load Descope user profile"
        )
        assert record.exc_info is not None
        assert isinstance(record.exc_info[1], ConnectionError)


class TestCurrentRunnerResolution:
    @pytest.fixture(autouse=True)
    def hosted(self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(auth_module, "AUTH_REQUIRED", True)
        monkeypatch.setattr(auth_module, "DESCOPE_MANAGEMENT_KEY", "management-key")
        mock_descope.validate_session.return_value = {"sub": "U-alice", "tenants": {"test-tenant": {}}}
        response = descope_access_key_response(user_id="U-alice")
        token = response["sessionToken"]
        assert isinstance(token, dict)
        token["customClaims"] = {"user_id": "U-alice", "valkyrie_user_key": True}
        mock_descope.exchange_access_key.return_value = response
        mock_descope.mgmt.user.load_by_user_id.return_value = {
            "user": {"email": "alice@vals.ai", "userTenants": [{"tenantId": "test-tenant", "roleNames": []}]}
        }

    @pytest.mark.description("Bound credentials resolve to the same live user.")
    @pytest.mark.parametrize(
        ("headers", "access_key_id"),
        [
            ({"authorization": "Bearer login-token"}, None),
            ({"x-api-key": "bound-key"}, "K2abc"),
            ({"authorization": "Bearer exchanged-key"}, "K2abc"),
            ({"authorization": "Bearer console-key-token"}, "K2abc"),
        ],
        ids=["login", "bound-key", "key-derived-bearer", "console-bound-bearer"],
    )
    def test_bound_credentials_use_the_same_live_user(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
        headers: dict[str, str],
        access_key_id: str | None,
    ) -> None:
        if headers.get("authorization") in {"Bearer exchanged-key", "Bearer console-key-token"}:
            mock_descope.validate_session.return_value = {
                "sub": "K2abc",
                "tenants": {test_org.name: {}},
                **(
                    {"user_id": "U-alice", "valkyrie_user_key": True}
                    if headers["authorization"] == "Bearer exchanged-key"
                    else {"user_id": "U-alice"}
                ),
            }
        identity = get_current_identity(MagicMock(headers=headers), empty_database_session)
        runner = get_current_runner(identity)

        assert (runner.org.id, runner.user_id, runner.access_key_id) == (test_org.id, "U-alice", access_key_id)
        mock_descope.mgmt.user.load_by_user_id.assert_called_with("U-alice")

    @pytest.mark.description("Only a project role or the selected tenant role can deny a runner.")
    @pytest.mark.parametrize(
        ("project_roles", "tenant_roles", "denied"),
        [
            (["valkyrie-no-start"], [], True),
            ([], ["valkyrie-no-start"], True),
            ([], [], False),
        ],
        ids=["project-role", "selected-tenant-role", "unrelated-tenant-role"],
    )
    def test_only_project_or_selected_tenant_role_denies(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
        project_roles: list[str],
        tenant_roles: list[str],
        denied: bool,
    ) -> None:
        mock_descope.mgmt.user.load_by_user_id.return_value = {
            "user": {
                "roleNames": project_roles,
                "userTenants": [
                    {"tenantId": test_org.name, "roleNames": tenant_roles},
                    {"tenantId": "elsewhere", "roleNames": ["valkyrie-no-start"]},
                ],
            }
        }
        identity = get_current_identity(
            MagicMock(headers={"authorization": "Bearer login-token"}), empty_database_session
        )
        if denied:
            with pytest.raises(HTTPException) as exc_info:
                get_current_runner(identity)
            assert (exc_info.value.status_code, exc_info.value.detail) == (
                403,
                "This user cannot start or retry benchmarks",
            )
        else:
            assert get_current_runner(identity).user_id == "U-alice"
        mock_descope.mgmt.user.load_by_user_id.assert_called_once_with("U-alice")

    @pytest.mark.description(
        "A cached access key is exchanged once, while the user's roles are reloaded on every request."
    )
    def test_cached_key_exchange_does_not_cache_role_membership(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        user = {"userTenants": [{"tenantId": test_org.name, "roleNames": []}]}
        mock_descope.mgmt.user.load_by_user_id.return_value = {"user": user}
        headers = {"x-api-key": "bound-key"}

        assert (
            get_current_runner(get_current_identity(MagicMock(headers=headers), empty_database_session)).user_id
            == "U-alice"
        )
        user["userTenants"][0]["roleNames"] = ["valkyrie-no-start"]
        with pytest.raises(HTTPException) as exc_info:
            get_current_runner(get_current_identity(MagicMock(headers=headers), empty_database_session))
        assert exc_info.value.status_code == 403
        user["userTenants"][0]["roleNames"] = []
        assert (
            get_current_runner(get_current_identity(MagicMock(headers=headers), empty_database_session)).user_id
            == "U-alice"
        )
        mock_descope.exchange_access_key.assert_called_once_with("bound-key")
        assert mock_descope.mgmt.user.load_by_user_id.call_count >= 3

    @pytest.mark.description("A console key with no bound user keeps its access.")
    def test_console_key_without_bound_user_is_not_newly_prohibited(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()
        mock_descope.mgmt.access_key.load.return_value = {"key": {"id": "K2abc"}}
        identity = get_current_identity(MagicMock(headers={"x-api-key": "console-key"}), empty_database_session)

        runner = get_current_runner(identity)
        assert (runner.org.id, runner.access_key_id, runner.user_id) == (test_org.id, "K2abc", None)
        mock_descope.mgmt.access_key.load.assert_called_once_with("K2abc")
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()

    @pytest.mark.description("A key whose user is unclear is loaded by its access key ID before the run is admitted.")
    @pytest.mark.parametrize(
        "headers",
        [{"x-api-key": "console-key"}, {"authorization": "Bearer key-jwt"}],
        ids=["raw-key", "key-derived-bearer"],
    )
    @pytest.mark.parametrize(
        ("key_lookup", "expected_status"),
        [
            ({"key": {"id": "K2abc"}}, None),
            ({"key": {"id": "K2abc", "boundUserId": "U-alice"}}, 403),
            (ReadTimeout(), 503),
            (AuthException(404, "E", "missing"), 403),
        ],
        ids=["unbound", "missing-owner", "outage", "missing-key"],
    )
    def test_ambiguous_key_needs_exact_lookup_before_admission(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
        key_lookup: object,
        expected_status: int | None,
        headers: dict[str, str],
    ) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()
        mock_descope.validate_session.return_value = {"sub": "K2abc", "tenants": {test_org.name: {}}}
        mock_descope.mgmt.user.load_by_user_id.side_effect = AuthException(404, "E", "not found")
        if isinstance(key_lookup, Exception):
            mock_descope.mgmt.access_key.load.side_effect = key_lookup
        else:
            mock_descope.mgmt.access_key.load.return_value = key_lookup
        identity = get_current_identity(MagicMock(headers=headers), empty_database_session)

        if expected_status is not None:
            with pytest.raises(HTTPException) as exc_info:
                get_current_runner(identity)
            assert exc_info.value.status_code == expected_status
        else:
            runner = get_current_runner(identity)
            assert (runner.org.id, runner.access_key_id, runner.user_id) == (test_org.id, "K2abc", None)
        looked_up_users = [call.args for call in mock_descope.mgmt.user.load_by_user_id.call_args_list]
        assert looked_up_users == ([("K2abc",)] if "authorization" in headers else []) + (
            [("U-alice",)] if isinstance(key_lookup, dict) and key_lookup["key"].get("boundUserId") else []
        )
        mock_descope.mgmt.access_key.load.assert_called_once_with("K2abc")

    @pytest.mark.description("A bound key without a marker uses the live user's roles.")
    @pytest.mark.parametrize(
        "headers",
        [{"x-api-key": "bound-key"}, {"authorization": "Bearer key-jwt"}],
        ids=["raw-key", "key-derived-bearer"],
    )
    def test_markerless_bound_key_uses_live_user_roles(
        self, mock_descope: MagicMock, empty_database_session: Session, test_org: Org, headers: dict[str, str]
    ) -> None:
        mock_descope.exchange_access_key.return_value = descope_access_key_response()
        mock_descope.validate_session.return_value = {"sub": "K2abc", "tenants": {test_org.name: {}}}
        denied_user = {"user": {"roleNames": ["valkyrie-no-start"], "userTenants": [{"tenantId": test_org.name}]}}
        allowed_user = {"user": {"roleNames": [], "userTenants": [{"tenantId": test_org.name}]}}
        users: list[object] = [denied_user, allowed_user]
        if "authorization" in headers:
            users = [
                AuthException(404, "E", "not found"),
                denied_user,
                AuthException(404, "E", "not found"),
                allowed_user,
            ]
        mock_descope.mgmt.user.load_by_user_id.side_effect = users
        mock_descope.mgmt.access_key.load.return_value = {"key": {"id": "K2abc", "boundUserId": "U-alice"}}
        identity = get_current_identity(MagicMock(headers=headers), empty_database_session)

        with pytest.raises(HTTPException) as exc_info:
            get_current_runner(identity)
        assert exc_info.value.status_code == 403
        if "authorization" in headers:
            assert mock_descope.mgmt.user.load_by_user_id.call_args_list[0].args == ("K2abc",)
            assert mock_descope.mgmt.user.load_by_user_id.call_args_list[1].args == ("U-alice",)
        else:
            assert mock_descope.mgmt.user.load_by_user_id.call_args_list[0].args == ("U-alice",)

        runner = get_current_runner(identity)
        assert (runner.org.id, runner.access_key_id, runner.user_id) == (test_org.id, "K2abc", "U-alice")
        assert [call.args for call in mock_descope.mgmt.user.load_by_user_id.call_args_list] == (
            [("K2abc",), ("U-alice",), ("K2abc",), ("U-alice",)]
            if "authorization" in headers
            else [("U-alice",), ("U-alice",)]
        )
        assert mock_descope.mgmt.access_key.load.call_count == 2

    @pytest.mark.description("A bound user is not admitted when Descope management is unavailable.")
    def test_management_unavailable_does_not_admit_bound_user(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mock_descope: MagicMock,
        empty_database_session: Session,
        test_org: Org,
    ) -> None:
        monkeypatch.setattr(auth_module, "DESCOPE_MANAGEMENT_KEY", "")
        identity = get_current_identity(
            MagicMock(headers={"authorization": "Bearer login-token"}), empty_database_session
        )
        assert identity.org.id == test_org.id
        with pytest.raises(HTTPException) as exc_info:
            get_current_runner(identity)
        assert exc_info.value.status_code == 503
        mock_descope.mgmt.user.load_by_user_id.assert_not_called()


@pytest.mark.description("Every org lookup path creates the organization of a tenant it hasn't seen before.")
class TestMissingOrganization:
    @pytest.fixture(autouse=True)
    def hosted(self, monkeypatch: pytest.MonkeyPatch, mock_descope: MagicMock) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(tenant="unknown-tenant")
        mock_descope.validate_session.return_value = {"tenants": {"unknown-tenant": {}}}

    @pytest.mark.description("An unknown tenant gets an organization.")
    @pytest.mark.parametrize(
        "headers", [{"x-api-key": "valid-key"}, {"authorization": "Bearer session-token"}], ids=["api-key", "session"]
    )
    def test_unknown_tenants_get_an_organization(
        self, headers: dict[str, str], empty_database_session: Session
    ) -> None:
        request = MagicMock()
        request.headers = headers

        org = get_current_identity(request, empty_database_session).org

        assert org.name == "unknown-tenant"
        assert get_current_identity(request, empty_database_session).org.id == org.id
        assert len(empty_database_session.exec(select(Org).where(Org.name == "unknown-tenant")).all()) == 1


@pytest.mark.description("Signing in prepares the bucket of an organization that may use managed AWS.")
class TestOrganizationStorage:
    @pytest.fixture(autouse=True)
    def hosted(self, monkeypatch: pytest.MonkeyPatch, mock_descope: MagicMock, test_org: Org) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        mock_descope.exchange_access_key.return_value = descope_access_key_response(tenant=test_org.name)
        mock_descope.validate_session.return_value = {"tenants": {test_org.name: {}}}

    @pytest.mark.description("Resolving the current identity makes sure the org's bucket exists.")
    @pytest.mark.parametrize(
        "headers", [{"x-api-key": "valid-key"}, {"authorization": "Bearer session-token"}], ids=["api-key", "session"]
    )
    def test_signed_in_org_gets_its_bucket(
        self,
        monkeypatch: pytest.MonkeyPatch,
        empty_database_session: Session,
        test_org: Org,
        headers: dict[str, str],
    ) -> None:
        ensure = MagicMock()
        monkeypatch.setattr("tracker.auth.ensure_managed_org_bucket", ensure)
        request = MagicMock()
        request.headers = headers

        org = get_current_identity(request, empty_database_session).org

        assert org.id == test_org.id
        ensure.assert_called_once_with(empty_database_session, org)

    @pytest.mark.description("A bucket setup failure during sign in returns 503 without AWS details in the message.")
    def test_bucket_failure_is_retryable_and_hides_aws_detail(
        self, monkeypatch: pytest.MonkeyPatch, empty_database_session: Session
    ) -> None:
        monkeypatch.setattr(
            "tracker.auth.ensure_managed_org_bucket",
            MagicMock(side_effect=TenantBucketError("Could not create the organization bucket valkyrie-secret")),
        )
        request = MagicMock()
        request.headers = {"x-api-key": "valid-key"}

        with pytest.raises(HTTPException) as exc_info:
            get_current_identity(request, empty_database_session)

        assert exc_info.value.status_code == 503
        assert "valkyrie-secret" not in exc_info.value.detail


@pytest.mark.description(
    "A read-only access key, or a session token exchanged from one, only makes GET and HEAD requests."
)
class TestReadOnlyAccessKeys:
    @pytest.fixture(autouse=True)
    def hosted(self, monkeypatch: pytest.MonkeyPatch, mock_descope: MagicMock, test_org: Org) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        response = descope_access_key_response(tenant=test_org.name)
        token = response["sessionToken"]
        assert isinstance(token, dict)
        token["customClaims"] = {"valkyrie_read_only": True}
        mock_descope.exchange_access_key.return_value = response
        mock_descope.validate_session.return_value = {
            "sub": "K2abc",
            "tenants": {test_org.name: {}},
            "user_id": "U-alice",
            "valkyrie_read_only": True,
        }

    @pytest.mark.description("GET and HEAD requests are allowed.")
    @pytest.mark.parametrize(
        "headers", [{"x-api-key": "read-key"}, {"authorization": "Bearer exchanged-key"}], ids=["api-key", "bearer"]
    )
    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_reads_are_allowed(
        self, empty_database_session: Session, test_org: Org, headers: dict[str, str], method: str
    ) -> None:
        identity = get_current_identity(MagicMock(headers=headers, method=method), empty_database_session)

        assert identity.org.id == test_org.id

    @pytest.mark.description("Write requests are refused with 403.")
    @pytest.mark.parametrize(
        "headers", [{"x-api-key": "read-key"}, {"authorization": "Bearer exchanged-key"}], ids=["api-key", "bearer"]
    )
    @pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
    def test_writes_are_refused(self, empty_database_session: Session, headers: dict[str, str], method: str) -> None:
        with pytest.raises(HTTPException) as exc_info:
            get_current_identity(MagicMock(headers=headers, method=method), empty_database_session)

        assert (exc_info.value.status_code, exc_info.value.detail) == (403, "This access key is read-only")

    @pytest.mark.description("A session token with the read-only claim nested in its custom claims cannot write.")
    def test_nested_claims_are_read_only(
        self, mock_descope: MagicMock, empty_database_session: Session, test_org: Org
    ) -> None:
        mock_descope.validate_session.return_value = {
            "sub": "K2abc",
            "tenants": {test_org.name: {}},
            "customClaims": {"user_id": "U-alice", "valkyrie_read_only": True},
        }
        request = MagicMock(headers={"authorization": "Bearer exchanged-key"}, method="POST")

        with pytest.raises(HTTPException) as exc_info:
            get_current_identity(request, empty_database_session)

        assert exc_info.value.status_code == 403


class TestLoginSessions:
    @pytest.fixture(autouse=True)
    def hosted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)

    def _request(self, **headers: str) -> MagicMock:
        request = MagicMock()
        request.headers = {"authorization": "Bearer session-token", **headers}
        return request

    @pytest.mark.description("Invalid sessions and run keys are refused at login.")
    @pytest.mark.parametrize(
        "validate_session",
        [
            AuthException(status_code=401, error_message="Sensitive provider detail"),
            InvalidSignatureError("Sensitive provider detail"),
            {"sub": "K-run", "valkyrie_run_key": True},
        ],
        ids=["rejected", "bad-signature", "run-key"],
    )
    def test_invalid_sessions_and_run_keys_are_refused(
        self, mock_descope: MagicMock, empty_database_session: Session, validate_session: object
    ) -> None:
        mock_descope.validate_session.side_effect = [validate_session]

        with pytest.raises(HTTPException) as exc_info:
            get_current_identity(self._request(), empty_database_session)

        assert (exc_info.value.status_code, exc_info.value.detail) == (401, "Invalid session")

    @pytest.mark.description("A login session selects its tenant.")
    @pytest.mark.parametrize(
        ("claims", "headers", "expected"),
        [
            ({"tenants": {"only": {}}}, {}, "only"),
            ({"tenants": {"first": {}, "second": {}}}, {"x-valkyrie-tenant": "second"}, "second"),
            ({"tenants": {"first": {}, "second": {}}, "dct": "second"}, {}, "second"),
            ({"tenants": {"first": {}, "second": {}}, "dct": "elsewhere"}, {}, (400, SEVERAL_ORGANIZATIONS)),
            ({"tenants": {"first": {}}}, {"x-valkyrie-tenant": "other"}, (403, NOT_A_MEMBER)),
            ({}, {}, (400, NO_ORGANIZATION)),
        ],
    )
    def test_tenant_selection(
        self,
        mock_descope: MagicMock,
        empty_database_session: Session,
        claims: dict[str, Any],
        headers: dict[str, str],
        expected: str | tuple[int, str],
    ) -> None:
        mock_descope.validate_session.return_value = {"sub": "U-alice", **claims}

        if isinstance(expected, str):
            assert get_current_identity(self._request(**headers), empty_database_session).org.name == expected
        else:
            with pytest.raises(HTTPException) as exc_info:
                get_current_identity(self._request(**headers), empty_database_session)
            assert (exc_info.value.status_code, exc_info.value.detail) == expected
            assert empty_database_session.exec(select(Org)).all() == []
        mock_descope.validate_session.assert_called_once_with("session-token", audience=None)


class TestCliLoginTokens:
    @pytest.mark.description("A bearer token issued to the CLI client resolves to the signed in user.")
    def test_tokens_issued_to_the_cli_client_are_accepted(
        self, mock_descope: MagicMock, test_org: Org, empty_database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("tracker.auth.DESCOPE_CLI_CLIENT_ID", "cli-client")
        claims = {"sub": "U-alice", "tenants": {test_org.name: {}}}

        def validate(token: str, audience: str | None = None) -> dict[str, Any]:
            if audience != "cli-client":
                raise InvalidAudienceError("Audience doesn't match")
            return claims

        mock_descope.validate_session.side_effect = validate
        request = MagicMock()
        request.headers = {"authorization": "Bearer cli-token"}

        assert get_current_identity(request, empty_database_session).user_id == "U-alice"

    @pytest.mark.description("A bearer token issued to any other audience returns 401.")
    def test_tokens_for_neither_the_project_nor_the_cli_client_are_rejected(
        self, mock_descope: MagicMock, empty_database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.auth.DESCOPE_CLI_CLIENT_ID", "cli-client")
        mock_descope.validate_session.side_effect = InvalidAudienceError("Audience doesn't match")
        warning = MagicMock()
        monkeypatch.setattr(auth_module.logger, "warning", warning)

        with pytest.raises(HTTPException) as exc_info:
            auth_module._validate_bearer("other-token")

        assert exc_info.value.status_code == 401
        warning.assert_called_once_with("Descope session validation failed: %s", "InvalidAudienceError")

    @pytest.mark.description("A rejected bearer token logs the provider's reason, not the token.")
    def test_rejected_session_logs_the_descope_reason(
        self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.auth.DESCOPE_CLI_CLIENT_ID", None)
        mock_descope.validate_session.side_effect = AuthException(401, "invalid token", "Signature has expired")
        warning = MagicMock()
        monkeypatch.setattr(auth_module.logger, "warning", warning)

        with pytest.raises(HTTPException):
            auth_module._validate_bearer("expired-token")

        warning.assert_called_once_with("Descope session validation failed: %s", "Signature has expired")


class TestCurrentLogin:
    @pytest.fixture(autouse=True)
    def management(self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("tracker.auth.DESCOPE_MANAGEMENT_KEY", "management-key")
        mock_descope.validate_session.return_value = {"sub": "U-alice", "dct": "second"}

    @pytest.mark.description("A login needs a Descope user.")
    @pytest.mark.parametrize(
        ("headers", "lookup", "expected"),
        [
            ({"x-api-key": "K-ci"}, None, (403, LOGIN_REQUIRED)),
            ({"authorization": "Bearer exchanged-key"}, AuthException(400, "E", "not found"), (403, LOGIN_REQUIRED)),
            ({"authorization": "Bearer session"}, ReadTimeout(), (503, "Auth service unavailable")),
        ],
        ids=["access-key", "not-a-user", "outage"],
    )
    def test_logins_need_a_descope_user(
        self, mock_descope: MagicMock, headers: dict[str, str], lookup: Exception | None, expected: tuple[int, str]
    ) -> None:
        mock_descope.mgmt.user.load_by_user_id.side_effect = lookup

        with pytest.raises(HTTPException) as exc_info:
            get_current_login(MagicMock(headers=headers))

        assert (exc_info.value.status_code, exc_info.value.detail) == expected

    @pytest.mark.description("The login's orgs, roles, and current org are read from a fresh Descope user load.")
    def test_organizations_come_from_the_live_user_record(self, mock_descope: MagicMock) -> None:
        tenants = [{"tenantId": "first", "roleNames": ["admin"]}, {"tenantId": "second"}]
        user = {"loginIds": ["a@x.com"], "email": "a@x.com", "userTenants": tenants}
        mock_descope.mgmt.user.load_by_user_id.return_value = {"user": user}

        login = get_current_login(MagicMock(headers={"authorization": "Bearer session"}))

        assert login == LoginIdentity("U-alice", "a@x.com", None, {"first": ("admin",), "second": ()}, "second")
        assert login.current_organization == "second"
        mock_descope.mgmt.user.load_by_user_id.assert_called_once_with("U-alice")

    @pytest.mark.description("Without a Descope management key, login lookups return 404 before any session check.")
    def test_a_tracker_without_a_management_key_has_no_logins(
        self, mock_descope: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.auth.DESCOPE_MANAGEMENT_KEY", "")

        with pytest.raises(HTTPException) as exc_info:
            get_current_login(MagicMock(headers={"authorization": "Bearer session"}))

        assert exc_info.value.status_code == 404
        mock_descope.validate_session.assert_not_called()


@pytest.mark.description("Runs in a passkey org need a passkey session.")
@pytest.mark.parametrize(
    ("headers", "claims", "allowed"),
    [
        ({"authorization": "Bearer session"}, {"sub": "U-alice", "amr": ["webauthn"]}, True),
        ({"authorization": "Bearer session"}, {"sub": "U-alice", "amr": ["oauth"]}, False),
        ({"x-api-key": "bound-key"}, {}, False),
    ],
    ids=["passkey", "google", "access-key"],
)
def test_passkey_org_runs_need_a_passkey_session(
    monkeypatch: pytest.MonkeyPatch,
    mock_descope: MagicMock,
    empty_database_session: Session,
    headers: dict[str, str],
    claims: dict[str, Any],
    allowed: bool,
) -> None:
    monkeypatch.setattr(auth_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr(auth_module, "DESCOPE_MANAGEMENT_KEY", "management-key")
    monkeypatch.setattr(auth_module, "PASSKEY_ORGS", frozenset({"passkey-org"}))
    org = Org(id=uuid4(), name="passkey-org")
    empty_database_session.add(org)
    empty_database_session.commit()
    mock_descope.validate_session.return_value = {"tenants": {org.name: {}}, **claims}
    mock_descope.exchange_access_key.return_value = descope_access_key_response(user_id="U-alice", tenant=org.name)
    mock_descope.mgmt.user.load_by_user_id.return_value = {"user": {"email": "alice@vals.ai"}}
    identity = get_current_identity(MagicMock(headers=headers), empty_database_session)

    if allowed:
        assert get_current_runner(identity).user_id == "U-alice"
        return
    with pytest.raises(HTTPException) as exc_info:
        get_current_runner(identity)
    assert exc_info.value.status_code == 403
