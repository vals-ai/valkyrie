import hashlib
import json
import logging
from typing import Literal
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.exceptions import ConnectionClosedError, EndpointConnectionError, SSLError
from botocore.stub import Stubber
from fastapi.testclient import TestClient
from projects import gateway as gateway_module
from projects import main
from projects.gateway import ANOTHER_VERIFIER, GatewayClient, KeyRequestStatus, KeyStatus, SecretStore
from tests.conftest import ORG, ROUTER_BENCH, TOKEN

KEY = UUID("00000000-0000-0000-0000-0000000000c1")
SECRET_NAME = f"valkyrie/tenants/{ORG}/projects/gateway-keys/{KEY}"
COPY_NAME = f"valkyrie/tenants/{ORG}/model-gateway-{KEY}"


@pytest.fixture(autouse=True)
def offline_aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


class FakeGateway:
    def __init__(self, idempotent: bool = True) -> None:
        self.status = "approved"
        self.state = "pending_collection"
        self.active_hash: str | None = None
        self.idempotent = idempotent
        self.verifiers: list[tuple[UUID, int, str]] = []

    def request_status(self, request_id: UUID) -> tuple[str, UUID | None]:
        return self.status, KEY if self.status == "approved" else None

    def key_state(self, key_id: UUID) -> tuple[str, int]:
        return self.state, 3

    def set_verifier(self, key_id: UUID, revision: int, verifier_hash: str) -> Literal["set", "another", "changed"]:
        self.verifiers.append((key_id, revision, verifier_hash))
        if self.state == "pending_collection":
            self.state, self.active_hash = "active", verifier_hash
            return "set"
        if not self.idempotent:
            return "changed"
        return "set" if verifier_hash == self.active_hash else "another"


class RefusingGateway(FakeGateway):
    def set_verifier(self, key_id: UUID, revision: int, verifier_hash: str) -> Literal["set", "another", "changed"]:
        self.verifiers.append((key_id, revision, verifier_hash))
        return "changed"


class FakeStore:
    def __init__(self) -> None:
        self.secrets: dict[str, str] = {}
        self.orgs: dict[str, UUID] = {}

    def keep_first(self, name: str, org_id: UUID, secret: str) -> str:
        _ = self.orgs.setdefault(name, org_id)
        return self.secrets.setdefault(name, secret)

    def get(self, name: str) -> str | None:
        return self.secrets.get(name)

    def replace(self, name: str, secret: str) -> None:
        self.secrets[name] = secret


class DroppedStore(FakeStore):
    def keep_first(self, name: str, org_id: UUID, secret: str) -> str:
        raise ConnectionClosedError(endpoint_url="https://secretsmanager")


class UnreachableStore(FakeStore):
    def keep_first(self, name: str, org_id: UUID, secret: str) -> str:
        raise EndpointConnectionError(endpoint_url="https://secretsmanager")


class CertificateStore(FakeStore):
    def keep_first(self, name: str, org_id: UUID, secret: str) -> str:
        raise SSLError(endpoint_url="https://secretsmanager", error=Exception("certificate verify failed"))


def _wire(gateway: FakeGateway, store: FakeStore) -> None:
    main.app.dependency_overrides[main.gateway] = lambda: gateway
    main.app.dependency_overrides[main.secret_store] = lambda: store


def _project(client: TestClient, **changes: object) -> str:
    project_id = uuid4()
    row = (
        {"id": str(project_id)} | ROUTER_BENCH | {"handle": f"p-{project_id.hex}", "name": f"P {project_id}"} | changes
    )
    response = client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN)
    assert response.status_code == 201
    return str(row["id"])


def _pending(client: TestClient) -> str:
    return _project(client, gateway_key_id=None)


def _path(project_id: str, suffix: str = "") -> str:
    return f"/orgs/{ORG}/projects/{project_id}/gateway-key{suffix}"


class TestMintGatewayKey:
    @pytest.mark.description(
        "Minting sends the Gateway only the hash and stores the secret under the org's tenant prefix and Tracker's name."
    )
    def test_mint_sends_hash_and_stores_secret(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        project_id = _pending(client)

        response = client.post(_path(project_id), headers=TOKEN)

        assert response.status_code == 200
        assert response.json() == {"gateway_key_id": str(KEY)}
        secret = store.secrets[SECRET_NAME]
        assert secret.startswith("mgwk_")
        assert gateway.verifiers == [(KEY, 3, hashlib.sha256(secret.encode()).hexdigest())]
        assert secret not in response.text
        assert store.secrets[COPY_NAME] == secret
        assert store.orgs == {SECRET_NAME: ORG, COPY_NAME: ORG}
        assert client.get(f"/orgs/{ORG}/projects/{project_id}", headers=TOKEN).json()["gateway_key_id"] == str(KEY)

    @pytest.mark.description("A mint that loses a race to another mint activates the secret that was stored first.")
    def test_mint_uses_the_secret_stored_first(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        store.secrets[SECRET_NAME] = "mgwk_first"

        assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 200

        assert gateway.verifiers == [(KEY, 3, hashlib.sha256(b"mgwk_first").hexdigest())]

    @pytest.mark.description(
        "A retry after the Gateway took the hash finishes and writes Tracker's copy, and an active key without our secret gets 409."
    )
    def test_mint_retry_after_activation(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        gateway.state, gateway.active_hash = "active", _hash("mgwk_live")
        stale = _pending(client)
        assert client.post(_path(stale), headers=TOKEN).status_code == 409
        assert client.get(f"/orgs/{ORG}/projects/{stale}", headers=TOKEN).json()["gateway_key_id"] is None

        store.secrets[SECRET_NAME] = "mgwk_live"
        project_id = _pending(client)

        assert client.post(_path(project_id), headers=TOKEN).json() == {"gateway_key_id": str(KEY)}
        assert store.secrets[SECRET_NAME] == "mgwk_live"
        assert store.secrets[COPY_NAME] == "mgwk_live"
        assert set(gateway.verifiers) == {(KEY, 3, _hash("mgwk_live"))}

    @pytest.mark.description(
        "A second mint returns the same key once the Gateway confirms it, and mints no new secret."
    )
    def test_mint_is_one_shot(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        project_id = _pending(client)
        assert client.post(_path(project_id), headers=TOKEN).status_code == 200

        again = client.post(_path(project_id), headers=TOKEN)

        assert again.json() == {"gateway_key_id": str(KEY)}
        assert {hash_ for _, _, hash_ in gateway.verifiers} == {gateway.active_hash}
        assert len(store.secrets) == 2

    @pytest.mark.description("An unapproved request, or a key collected elsewhere, gets 409 and stores nothing.")
    def test_mint_refuses_without_an_approved_pending_key(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        gateway.status = "pending"
        assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 409

        gateway.status = "approved"
        legacy = _project(client, gateway_key_id=str(KEY))
        assert client.post(_path(legacy), headers=TOKEN).status_code == 409

        no_request = _project(client, gateway_key_id=None, gateway_request_id=None)
        assert client.post(_path(no_request), headers=TOKEN).status_code == 409

        assert gateway.verifiers == []
        assert store.secrets == {}

    @pytest.mark.description("A key that is neither pending nor active gets 409, and nothing is stored or bound.")
    def test_mint_refuses_a_revoked_key(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        gateway.state = "revoked"
        project_id = _pending(client)

        response = client.post(_path(project_id), headers=TOKEN)

        assert response.status_code == 409
        assert response.json()["detail"] == "Gateway key was collected elsewhere"
        assert client.get(f"/orgs/{ORG}/projects/{project_id}", headers=TOKEN).json()["gateway_key_id"] is None
        assert gateway.verifiers == []
        assert store.secrets == {}

    @pytest.mark.description(
        "A mint whose verifier the Gateway refuses gets 409 and leaves the project without a key or a Tracker copy."
    )
    def test_mint_refused_verifier(self, client: TestClient) -> None:
        gateway, store = RefusingGateway(), FakeStore()
        _wire(gateway, store)
        project_id = _pending(client)

        refused = client.post(_path(project_id), headers=TOKEN)

        assert (refused.status_code, refused.json()["detail"]) == (409, "Gateway key changed while minting; retry")
        assert client.get(f"/orgs/{ORG}/projects/{project_id}", headers=TOKEN).json()["gateway_key_id"] is None
        assert COPY_NAME not in store.secrets

    @pytest.mark.description("The service never serves a stored secret, so a read of the secret path gets 405.")
    def test_secret_is_never_served(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        project_id = _pending(client)
        assert client.post(_path(project_id), headers=TOKEN).status_code == 200

        response = client.get(_path(project_id, "/secret"), headers=TOKEN)

        assert response.status_code == 405
        assert store.secrets[SECRET_NAME] not in response.text

    @pytest.mark.description("Unknown projects get 404 and callers without the machine token get 401.")
    def test_unknown_project_and_missing_token(self, client: TestClient) -> None:
        _wire(FakeGateway(), FakeStore())
        project_id = _pending(client)

        assert client.post(_path(str(uuid4())), headers=TOKEN).status_code == 404
        assert client.post(_path(project_id)).status_code == 401


class TestHandOverGatewayKey:
    @pytest.mark.description(
        "Tracker hands over its existing secret once, a repeat of that secret is accepted, and the reply carries no secret."
    )
    def test_hand_over_stores_once(self, client: TestClient) -> None:
        store = FakeStore()
        _wire(FakeGateway(), store)
        path = _path(_project(client, gateway_key_id=str(KEY)), "/secret")
        assert store.secrets == {}

        response = client.put(path, json={"gateway_key_id": str(KEY), "secret": "mgwk_old"}, headers=TOKEN)

        assert response.status_code == 204
        assert response.content == b""
        assert store.secrets == {SECRET_NAME: "mgwk_old"}
        assert (
            client.put(path, json={"gateway_key_id": str(KEY), "secret": "mgwk_old"}, headers=TOKEN).status_code == 204
        )
        assert (
            client.put(path, json={"gateway_key_id": str(KEY), "secret": "mgwk_new"}, headers=TOKEN).status_code == 409
        )
        assert store.secrets == {SECRET_NAME: "mgwk_old"}

    @pytest.mark.description("A hand-over for another key ID, or without the machine token, is refused.")
    def test_hand_over_only_for_own_key(self, client: TestClient) -> None:
        store = FakeStore()
        _wire(FakeGateway(), store)
        path = _path(_project(client, gateway_key_id=str(KEY)), "/secret")

        assert (
            client.put(path, json={"gateway_key_id": str(uuid4()), "secret": "mgwk_x"}, headers=TOKEN).status_code
            == 409
        )
        assert client.put(path, json={"gateway_key_id": str(KEY), "secret": "mgwk_x"}).status_code == 401
        assert store.secrets == {}


class TestSecretStore:
    @pytest.mark.description(
        "The store writes today's run secret JSON, tagged with the org, keeps an existing one, "
        "and replaces it when asked."
    )
    def test_store_writes_run_json_tagged_with_org(self) -> None:
        client = boto3.client("secretsmanager", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]
        value = json.dumps({"MODEL_GATEWAY_URL": "https://gw", "MODEL_GATEWAY_API_KEY": "mgwk_a"})
        tags = [{"Key": "valkyrie:org-id", "Value": str(ORG)}]
        with Stubber(client) as stub:
            stub.add_response("create_secret", {}, {"Name": SECRET_NAME, "SecretString": value, "Tags": tags})
            stub.add_client_error("create_secret", service_error_code="ResourceExistsException")
            stub.add_response("get_secret_value", {"SecretString": value}, {"SecretId": SECRET_NAME})

            assert SecretStore(client, "https://gw").keep_first(SECRET_NAME, ORG, "mgwk_a") == "mgwk_a"
            assert SecretStore(client, "https://gw").keep_first(SECRET_NAME, ORG, "mgwk_b") == "mgwk_a"

            replaced = json.dumps({"MODEL_GATEWAY_URL": "https://gw", "MODEL_GATEWAY_API_KEY": "mgwk_b"})
            stub.add_response("put_secret_value", {}, {"SecretId": SECRET_NAME, "SecretString": replaced})
            SecretStore(client, "https://gw").replace(SECRET_NAME, "mgwk_b")
            stub.assert_no_pending_responses()

    @pytest.mark.description("A read of a secret that does not exist yet answers no secret.")
    def test_store_reads_missing_secret_as_none(self) -> None:
        client = boto3.client("secretsmanager", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]
        with Stubber(client) as stub:
            stub.add_client_error("get_secret_value", service_error_code="ResourceNotFoundException")

            assert SecretStore(client, "https://gw").get(SECRET_NAME) is None


class DownGateway(FakeGateway):
    def request_status(self, request_id: UUID) -> tuple[str, UUID | None]:
        raise httpx.ConnectTimeout("down")


class StatusGateway(FakeGateway):
    def __init__(self, code: int) -> None:
        super().__init__()
        self.code = code

    def request_status(self, request_id: UUID) -> tuple[str, UUID | None]:
        response = httpx.Response(self.code, request=httpx.Request("GET", "https://gw"))
        raise httpx.HTTPStatusError("refused", request=response.request, response=response)


class DropsVerifierOnce(FakeGateway):
    def set_verifier(self, key_id: UUID, revision: int, verifier_hash: str) -> Literal["set", "another", "changed"]:
        if not self.verifiers:
            self.verifiers.append((key_id, revision, "dropped"))
            raise httpx.ReadTimeout("dropped")
        return super().set_verifier(key_id, revision, verifier_hash)


class TestUpstreamUnavailable:
    @pytest.mark.description(
        "An upstream failure answers a clear status instead of 500.\n"
        "\n"
        "Test cases:\n"
        "- An unreachable Gateway answers 503 to retry.\n"
        "- A Gateway 429 or 503 answers 503 to retry.\n"
        "- A Gateway refusal answers 500 with the Gateway status.\n"
        "- A failing Secrets Manager answers 503 to retry.\n"
        "- A Secrets Manager refusal answers 500."
    )
    def test_upstream_failure_status(self, client: TestClient) -> None:
        _wire(DownGateway(), FakeStore())
        response = client.post(_path(_pending(client)), headers=TOKEN)
        assert (response.status_code, response.json()) == (503, {"detail": "Gateway unavailable, try again"})
        for code in (429, 503):
            _wire(StatusGateway(code), FakeStore())
            response = client.post(_path(_pending(client)), headers=TOKEN)
            assert (response.status_code, response.json()) == (503, {"detail": "Gateway unavailable, try again"})
        _wire(StatusGateway(401), FakeStore())
        response = client.post(_path(_pending(client)), headers=TOKEN)
        assert (response.status_code, response.json()) == (
            500,
            {"detail": "Gateway refused the request with HTTP 401"},
        )

        secrets_client = boto3.client("secretsmanager", region_name="us-east-1")  # pyright: ignore[reportUnknownMemberType]
        main.app.dependency_overrides[main.secret_store] = lambda: SecretStore(secrets_client, "https://gw")
        with Stubber(secrets_client) as stub:
            stub.add_client_error("get_secret_value", service_error_code="InternalServiceError")
            response = client.post(_path(_project(client, gateway_key_id=str(KEY))), headers=TOKEN)
            stub.add_client_error("get_secret_value", service_error_code="ThrottlingException")
            throttled = client.post(_path(_project(client, gateway_key_id=str(KEY))), headers=TOKEN)
        assert (response.status_code, response.json()) == (503, {"detail": "Secrets Manager unavailable, try again"})
        assert throttled.status_code == 503

        with Stubber(secrets_client) as stub:
            stub.add_client_error("get_secret_value", service_error_code="AccessDeniedException")
            refused = client.post(_path(_project(client, gateway_key_id=str(KEY))), headers=TOKEN)
        assert refused.status_code == 500

    @pytest.mark.description(
        "A Gateway drop after the secret is stored answers 503, and the next mint activates the same secret."
    )
    def test_drop_between_store_and_verifier_finishes_with_same_secret(self, client: TestClient) -> None:
        gateway, store = DropsVerifierOnce(), FakeStore()
        _wire(gateway, store)
        project_id = _pending(client)

        assert client.post(_path(project_id), headers=TOKEN).status_code == 503
        stored = store.secrets[SECRET_NAME]
        assert client.post(_path(project_id), headers=TOKEN).json() == {"gateway_key_id": str(KEY)}

        assert store.secrets == {SECRET_NAME: stored, COPY_NAME: stored}
        assert gateway.verifiers[-1] == (KEY, 3, hashlib.sha256(stored.encode()).hexdigest())

    @pytest.mark.description("A Secrets Manager connection that drops mid-call or cannot open answers 503.")
    def test_dropped_secrets_connection_answers_503(self, client: TestClient) -> None:
        for store in (DroppedStore(), UnreachableStore()):
            _wire(FakeGateway(), store)
            assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 503

    @pytest.mark.description("Each Gateway or Secrets Manager failure logs one warning that carries no key secret.")
    def test_upstream_failure_logged_without_secret(self, client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
        store = FakeStore()
        with caplog.at_level(logging.WARNING, logger="projects.main"):
            _wire(DownGateway(), store)
            assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 503
            _wire(FakeGateway(), DroppedStore())
            assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 503
        messages = [record.getMessage() for record in caplog.records if record.name == "projects.main"]

        assert [message.split(":")[0] for message in messages] == ["Gateway call failed", "Secrets Manager call failed"]
        assert not any("mgwk_" in message for message in messages)

    @pytest.mark.description("A Secrets Manager certificate failure answers 500 instead of a retryable 503.")
    def test_secrets_certificate_failure_answers_500(self, client: TestClient) -> None:
        _wire(FakeGateway(), CertificateStore())
        assert client.post(_path(_pending(client)), headers=TOKEN).status_code == 500

    @pytest.mark.description("A Secrets Manager call that cannot connect is sent twice and then fails.")
    def test_secret_store_retries_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for name, value in {
            "MODEL_GATEWAY_URL": "https://gw",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_ENDPOINT_URL_SECRETS_MANAGER": "http://127.0.0.1:9",
        }.items():
            monkeypatch.setenv(name, value)
        sends: list[str] = []
        real_client = boto3.client  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]

        def counted(*args: object, **kwargs: object) -> object:
            built = real_client(*args, **kwargs)  # pyright: ignore[reportUnknownVariableType, reportArgumentType, reportCallIssue]
            built.meta.events.register("before-send", lambda **_: sends.append("send"))  # pyright: ignore[reportUnknownMemberType, reportUnknownLambdaType, reportUnknownArgumentType]
            return built  # pyright: ignore[reportUnknownVariableType]

        monkeypatch.setattr(main.boto3, "client", counted)
        main.secret_store.cache_clear()
        try:
            with pytest.raises(EndpointConnectionError):
                _ = main.secret_store().get(SECRET_NAME)
        finally:
            main.secret_store.cache_clear()
        assert len(sends) == 2

    @pytest.mark.description(
        "Each Secrets Manager call waits at most 5 seconds, and each Gateway call 5 seconds with a 2 second connect."
    )
    def test_upstream_calls_are_time_limited(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gw")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        configs: list[object] = []
        timeouts: list[object] = []
        real_client = boto3.client  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        real_http = httpx.Client

        def recorded(*args: object, **kwargs: object) -> object:
            configs.append(kwargs["config"])
            return real_client(*args, **kwargs)  # pyright: ignore[reportUnknownVariableType, reportArgumentType, reportCallIssue]

        def recorded_http(*args: object, **kwargs: object) -> httpx.Client:
            timeouts.append(kwargs["timeout"])
            return real_http(*args, **kwargs)  # pyright: ignore[reportArgumentType]

        monkeypatch.setattr(main.boto3, "client", recorded)
        monkeypatch.setattr(gateway_module.httpx, "Client", recorded_http)
        main.secret_store.cache_clear()
        try:
            _ = main.secret_store()
        finally:
            main.secret_store.cache_clear()
        _ = GatewayClient("https://gw", "machine")

        assert [(config.connect_timeout, config.read_timeout) for config in configs] == [(5.0, 5.0)]  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        assert timeouts == [httpx.Timeout(5.0, connect=2.0)]

    @pytest.mark.description("The Gateway client retries a read once after a connect or timeout error.")
    def test_gateway_client_retries_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        def answer(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if len(calls) == 1:
                raise httpx.ConnectError("refused")
            return httpx.Response(200, json={"status": "active", "revision": 4})

        transport = httpx.MockTransport(answer)
        real_client = httpx.Client
        monkeypatch.setattr(gateway_module.httpx, "Client", lambda **kwargs: real_client(transport=transport, **kwargs))  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]

        assert GatewayClient("https://gw", "machine").key_state(KEY) == ("active", 4)
        assert len(calls) == 2

    @pytest.mark.description("The Gateway client sends the verifier POST once, even when its reply is lost.")
    def test_gateway_client_never_retries_the_verifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        def answer(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            raise httpx.ReadTimeout("lost")

        transport = httpx.MockTransport(answer)
        real_client = httpx.Client
        monkeypatch.setattr(gateway_module.httpx, "Client", lambda **kwargs: real_client(transport=transport, **kwargs))  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]

        with pytest.raises(httpx.ReadTimeout):
            _ = GatewayClient("https://gw", "machine").set_verifier(KEY, 3, "h")
        assert len(calls) == 1


class RacedGateway(FakeGateway):
    def __init__(self, winner: str, idempotent: bool = True) -> None:
        super().__init__(idempotent)
        self.state, self.active_hash, self.raced = "active", _hash(winner), False

    def key_state(self, key_id: UUID) -> tuple[str, int]:
        if self.raced:
            return super().key_state(key_id)
        self.raced = True
        return "pending_collection", 3


class TestLostRace:
    @pytest.mark.description(
        "A mint that loses to an old Tracker collect answers 503, "
        "then Tracker's hand-over replaces the unused secret because the Gateway holds Tracker's hash."
    )
    def test_lost_to_old_collect_hands_over(self, client: TestClient) -> None:
        store = FakeStore()
        _wire(RacedGateway("mgwk_tracker"), store)
        project_id = _pending(client)

        assert client.post(_path(project_id), headers=TOKEN).status_code == 503

        assert list(store.secrets) == [SECRET_NAME]
        hand_over = {"gateway_key_id": str(KEY), "secret": "mgwk_tracker"}
        assert client.put(_path(project_id, "/secret"), json=hand_over, headers=TOKEN).status_code == 204
        assert client.post(_path(project_id), headers=TOKEN).json() == {"gateway_key_id": str(KEY)}
        assert store.secrets[SECRET_NAME] == "mgwk_tracker"

    @pytest.mark.description(
        "Before the hand-over, a mint on the unused secret answers the same as on a missing one and never returns it."
    )
    def test_unconfirmed_secret_mints_as_missing(self, client: TestClient) -> None:
        store = FakeStore()
        _wire(RacedGateway("mgwk_tracker"), store)
        project_id = _pending(client)
        assert client.post(_path(project_id), headers=TOKEN).status_code == 503
        unconfirmed = client.post(_path(project_id), headers=TOKEN)

        candidate = store.secrets.pop(SECRET_NAME)
        missing = client.post(_path(project_id), headers=TOKEN)

        assert (unconfirmed.status_code, unconfirmed.json()) == (missing.status_code, missing.json())
        assert missing.status_code == 409
        assert candidate not in unconfirmed.text

    @pytest.mark.description("A hand-over whose hash the Gateway does not hold never replaces a stored secret.")
    def test_hand_over_needs_the_gateway_hash(self, client: TestClient) -> None:
        gateway, store = FakeGateway(), FakeStore()
        gateway.state, gateway.active_hash = "active", _hash("mgwk_twin")
        _wire(gateway, store)
        store.secrets[SECRET_NAME] = "mgwk_twin"
        path = _path(_project(client, gateway_key_id=str(KEY)), "/secret")

        hand_over = {"gateway_key_id": str(KEY), "secret": "mgwk_stale"}
        assert client.put(path, json=hand_over, headers=TOKEN).status_code == 409
        assert store.secrets == {SECRET_NAME: "mgwk_twin"}

    @pytest.mark.description(
        "On a Gateway without the idempotent route, a mint that loses to its own twin answers 409 and keeps the secret."
    )
    def test_lost_to_twin_keeps_the_secret(self, client: TestClient) -> None:
        gateway, store = RacedGateway("mgwk_twin", idempotent=False), FakeStore()
        _wire(gateway, store)
        store.secrets[SECRET_NAME] = "mgwk_twin"
        project_id = _pending(client)

        assert client.post(_path(project_id), headers=TOKEN).status_code == 409
        assert client.post(_path(project_id), headers=TOKEN).status_code == 409

        assert store.secrets == {SECRET_NAME: "mgwk_twin"}
        assert gateway.active_hash == _hash("mgwk_twin")

    @pytest.mark.description(
        "After the Gateway change, a twin whose hash the Gateway holds gets 200 and keeps the secret."
    )
    def test_twin_after_gateway_change_gets_200(self, client: TestClient) -> None:
        gateway, store = RacedGateway("mgwk_twin"), FakeStore()
        _wire(gateway, store)
        store.secrets[SECRET_NAME] = "mgwk_twin"
        project_id = _pending(client)

        assert client.post(_path(project_id), headers=TOKEN).json() == {"gateway_key_id": str(KEY)}

        assert store.secrets == {SECRET_NAME: "mgwk_twin", COPY_NAME: "mgwk_twin"}
        assert gateway.verifiers == [(KEY, 3, _hash("mgwk_twin"))]

    @pytest.mark.description(
        "The client reads the Gateway's 409 detail, and only the new route's marker means another secret."
    )
    def test_gateway_client_reads_the_marker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        answers = [
            httpx.Response(200, json={}),
            httpx.Response(409, json={"detail": ANOTHER_VERIFIER}),
            httpx.Response(409, json={"detail": "Key state or revision changed"}),
        ]

        transport = httpx.MockTransport(lambda request: answers.pop(0))
        real_client = httpx.Client
        monkeypatch.setattr(gateway_module.httpx, "Client", lambda **kwargs: real_client(transport=transport, **kwargs))  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]
        gateway = GatewayClient("https://gw", "machine")

        assert [gateway.set_verifier(KEY, 3, "h") for _ in range(3)] == ["set", "another", "changed"]


class TestGatewayKeyOrgScope:
    @pytest.mark.parametrize(
        ("method", "suffix", "body"),
        [
            ("POST", "", None),
            ("PUT", "/secret", {"gateway_key_id": str(KEY), "secret": "mgwk_x"}),
        ],
    )
    @pytest.mark.description(
        "A Gateway key route called under another org's path gets 404 and touches neither the Gateway nor the store."
    )
    def test_other_org_gets_404(
        self, client: TestClient, method: str, suffix: str, body: dict[str, object] | None
    ) -> None:
        gateway, store = FakeGateway(), FakeStore()
        _wire(gateway, store)
        project_id = _project(client, gateway_key_id=str(KEY))
        store.secrets[SECRET_NAME] = "mgwk_live"

        path = f"/orgs/{uuid4()}/projects/{project_id}/gateway-key{suffix}"
        response = client.request(method, path, json=body, headers=TOKEN)

        assert response.status_code == 404
        assert gateway.verifiers == []
        assert store.secrets == {SECRET_NAME: "mgwk_live"}


@pytest.mark.description(
    "The Gateway client posts the revision and verifier, and reads a 200 as set and an unmarked 409 as changed."
)
def test_gateway_client_reads_the_verifier_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, object]] = []
    answers = iter([200, 409])

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append((request.url.path, json.loads(request.content)))
        return httpx.Response(next(answers), json={})

    real_client = httpx.Client

    def mock_client(base_url: str, headers: dict[str, str], timeout: httpx.Timeout) -> httpx.Client:
        return real_client(base_url=base_url, headers=headers, timeout=timeout, transport=httpx.MockTransport(handle))

    monkeypatch.setattr(gateway_module.httpx, "Client", mock_client)
    client = GatewayClient("http://gateway.test", "machine-key")
    key_id = uuid4()

    assert [client.set_verifier(key_id, 3, "ab"), client.set_verifier(key_id, 3, "ab")] == ["set", "changed"]
    assert (
        sent
        == [(f"/admin/managed-auth/machine/keys/{key_id}/verifier", {"expected_revision": 3, "verifier_hash": "ab"})]
        * 2
    )


@pytest.mark.description("The Gateway client reads a request's status and key ID, and a key's status and revision.")
def test_gateway_client_reads_request_and_key_state(monkeypatch: pytest.MonkeyPatch) -> None:
    key_id = uuid4()
    approved, pending = uuid4(), uuid4()
    bodies = {
        f"/admin/managed-auth/machine/requests/{approved}": {"status": "approved", "key_id": str(key_id)},
        f"/admin/managed-auth/machine/requests/{pending}": {"status": "pending", "key_id": None},
        f"/admin/managed-auth/machine/keys/{key_id}": {"status": "pending_collection", "revision": 4},
    }
    real_client = httpx.Client

    def mock_client(base_url: str, headers: dict[str, str], timeout: httpx.Timeout) -> httpx.Client:
        transport = httpx.MockTransport(lambda request: httpx.Response(200, json=bodies[request.url.path]))
        return real_client(base_url=base_url, headers=headers, timeout=timeout, transport=transport)

    monkeypatch.setattr(gateway_module.httpx, "Client", mock_client)
    client = GatewayClient("http://gateway.test", "machine-key")

    assert client.request_status(approved) == (KeyRequestStatus.APPROVED, key_id)
    assert client.request_status(pending) == (KeyRequestStatus.PENDING, None)
    assert client.key_state(key_id) == (KeyStatus.PENDING_COLLECTION, 4)
