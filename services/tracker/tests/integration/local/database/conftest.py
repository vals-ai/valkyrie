from collections.abc import Callable, Generator
from types import SimpleNamespace
from time import monotonic
from uuid import uuid4

import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

import tracker.remote_grader_crypt as grader_crypt
from tracker.api.remote_grader import router as remote_grader_router
from tracker.database.models import ExecutorAdmission
from tracker.database.session import get_session


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def postgres_engine(postgres_container: PostgresContainer) -> Generator[Engine, None, None]:
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(ExecutorAdmission())
        session.commit()

    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def postgres_session(postgres_engine: Engine) -> Generator[Session, None, None]:
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture
def remote_grader_app(postgres_engine: Engine) -> FastAPI:
    app = FastAPI()
    app.include_router(remote_grader_router)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    app.dependency_overrides[get_session] = request_session

    return app


@pytest.fixture
def kms_grader_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    ciphertexts: dict[bytes, tuple[bytes, dict[str, str]]] = {}

    class KmsClient:
        def encrypt(self, *, KeyId: str, Plaintext: bytes, EncryptionContext: dict[str, str]) -> dict[str, bytes]:
            assert KeyId == "test-grader-key"
            assert len(EncryptionContext) == 1
            assert next(iter(EncryptionContext)) in {"remote_grader_grant_id", "remote_grader_registration_id"}
            ciphertext = uuid4().bytes
            ciphertexts[ciphertext] = (Plaintext, EncryptionContext)
            return {"CiphertextBlob": ciphertext}

        def decrypt(self, *, KeyId: str, CiphertextBlob: bytes, EncryptionContext: dict[str, str]) -> dict[str, bytes]:
            assert KeyId == "test-grader-key"
            plaintext, context = ciphertexts[CiphertextBlob]
            assert EncryptionContext == context
            return {"Plaintext": plaintext}

    monkeypatch.setenv("REMOTE_GRADER_KMS_KEY_ID", "test-grader-key")

    def kms_client(service: str, **kwargs: object) -> KmsClient:
        assert service == "kms"
        return KmsClient()

    monkeypatch.setattr(grader_crypt, "boto3", SimpleNamespace(client=kms_client))


@pytest.fixture
def wait_for_database_lock(postgres_engine: Engine) -> Callable[[], None]:
    def wait() -> None:
        deadline = monotonic() + 5
        with Session(postgres_engine) as observer:
            while monotonic() < deadline:
                blocked = (
                    observer.connection()
                    .execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                            "AND pid <> pg_backend_pid())"
                        )
                    )
                    .scalar_one()
                )
                if blocked:
                    return
                observer.rollback()
        raise AssertionError("the second transaction never waited on a PostgreSQL lock")

    return wait
