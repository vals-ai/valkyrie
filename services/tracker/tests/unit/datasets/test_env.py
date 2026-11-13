from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from tracker import config
from tracker.services.runs import start
from tracker.types import OrgContext
from tracker.services.datasets.env import SecretReader, resolve_env, secret_reader
from tracker.exceptions import SecretNotFoundError, SecretsError
from tracker.runtime.secrets import SecretValue


def reader_of(values: dict[str, str]) -> SecretReader:
    async def read(name: str) -> str | None:
        return values.get(name)

    return read


class StubStore:
    def __init__(self, values: dict[str, SecretValue]) -> None:
        self.values = values

    async def get(self, name: str) -> SecretValue:
        if name not in self.values:
            raise SecretNotFoundError(name)
        return self.values[name]


@pytest.mark.description("Values become literals, secrets or defaults.")
class TestResolveEnv:
    @pytest.mark.description("Explicit JSON fields resolve without changing the stored secret name.")
    @pytest.mark.parametrize("field_value", ["token", "", 0])
    async def test_explicit_field(self, field_value: str | int) -> None:
        read = secret_reader(StubStore({"tenant/Config": {"API_KEY": field_value}}), lambda name: [f"tenant/{name}"])

        resolved, from_secrets = await resolve_env({"KEY": "${Config#API_KEY}"}, read, "[verifier].env")

        assert resolved == {"KEY": str(field_value)}
        assert from_secrets == ["KEY"]

    @pytest.mark.description("Unreadable JSON fields use the default or report the missing reference.")
    @pytest.mark.parametrize("secret_value", [{"OTHER": "token"}, "plain"])
    async def test_unreadable_field(self, secret_value: SecretValue) -> None:
        read = secret_reader(StubStore({"Config": secret_value}), lambda name: [name])

        resolved, from_secrets = await resolve_env({"KEY": "${Config#API_KEY:-fallback}"}, read, "[verifier].env")

        assert resolved == {"KEY": "fallback"}
        assert from_secrets == []
        with pytest.raises(SecretsError, match="Config#API_KEY"):
            await resolve_env({"KEY": "${Config#API_KEY}"}, read, "[verifier].env")

    @pytest.mark.description("Literals pass through and templates read secrets.")
    async def test_literals_pass_through_and_templates_read_secrets(self) -> None:
        resolved, from_secrets = await resolve_env(
            {"MODE": "fast", "KEY": "${API_KEY}"}, reader_of({"API_KEY": "s3cret"}), "env"
        )

        assert resolved == {"MODE": "fast", "KEY": "s3cret"}
        assert from_secrets == ["KEY"]

    @pytest.mark.description("A default applies only without the secret.")
    @pytest.mark.parametrize(
        ("secrets", "expected"),
        [({}, "fallback"), ({"NAME": "from-secret"}, "from-secret")],
    )
    async def test_a_default_applies_only_without_the_secret(self, secrets: dict[str, str], expected: str) -> None:
        resolved, _ = await resolve_env({"KEY": "${NAME:-fallback}"}, reader_of(secrets), "env")

        assert resolved == {"KEY": expected}

    @pytest.mark.description("A missing secret without a default names the setting and secret.")
    async def test_a_missing_secret_without_a_default_names_the_setting_and_secret(self) -> None:
        with pytest.raises(SecretsError, match=r"\[verifier\]\.env sets KEY from the secret 'NAME'"):
            await resolve_env({"KEY": "${NAME}"}, reader_of({}), "[verifier].env")

    @pytest.mark.description("Only a whole value is a template.")
    async def test_only_a_whole_value_is_a_template(self) -> None:
        resolved, from_secrets = await resolve_env({"HEADER": "Bearer ${TOKEN}"}, reader_of({"TOKEN": "x"}), "env")

        assert resolved == {"HEADER": "Bearer ${TOKEN}"}
        assert from_secrets == []

    @pytest.mark.description("Without a reader only defaults resolve.")
    async def test_without_a_reader_only_defaults_resolve(self) -> None:
        resolved, _ = await resolve_env({"KEY": "${NAME:-fallback}"}, None, "env")

        assert resolved == {"KEY": "fallback"}


@pytest.mark.description("Reading a secret under the name the run's store knows it by.")
class TestSecretReader:
    @pytest.mark.description("The stored name is looked up and json secrets are read by variable.")
    async def test_the_stored_name_is_looked_up_and_json_secrets_are_read_by_variable(self) -> None:
        store = StubStore({"tenant/TOKEN": "plain", "tenant/DB": {"DB": "pg", "OTHER": "x"}})
        read = secret_reader(store, lambda name: [f"tenant/{name}"])

        assert await read("TOKEN") == "plain"
        assert await read("DB") == "pg"

    @pytest.mark.description("A name the run may not use reads as missing.")
    async def test_a_name_the_run_may_not_use_reads_as_missing(self) -> None:
        def stored_name(name: str) -> list[str]:
            raise ValueError(f"Secret '{name}' does not exist")

        read = secret_reader(StubStore({"TOKEN": "plain"}), stored_name)

        assert await read("TOKEN") is None

    @pytest.mark.description("Stored names are tried in order and the first secret found is read.")
    async def test_stored_names_are_tried_in_order(self) -> None:
        store = StubStore({"tenant/TOKEN": "mine", "TOKEN": "shared", "SHARED": "fallback"})
        read = secret_reader(store, lambda name: [f"tenant/{name}", name])

        assert await read("TOKEN") == "mine"
        assert await read("SHARED") == "fallback"

    @pytest.mark.description("An unknown secret or json key reads as missing.")
    async def test_an_unknown_secret_or_json_key_reads_as_missing(self) -> None:
        read = secret_reader(StubStore({"JSON": {"OTHER": "x"}}), lambda name: [name])

        assert await read("NOPE") is None
        assert await read("JSON") is None

    @pytest.mark.description("A json secret without the variable falls through to the next stored name.")
    async def test_a_json_secret_without_the_variable_falls_through(self) -> None:
        store = StubStore({"tenant/TOKEN": {"OTHER": "x"}, "TOKEN": "shared"})
        read = secret_reader(store, lambda name: [f"tenant/{name}", name])

        assert await read("TOKEN") == "shared"


@pytest.mark.description(
    "A run reads dataset secrets from its own tenant first and shared tenants fall back to the shared name."
)
@pytest.mark.parametrize(
    ("org_name", "values", "expected"),
    [
        (
            "internal.example",
            {"valkyrie/tenants/00000000-0000-0000-0000-000000000001/TOKEN": "mine", "TOKEN": "shared"},
            "mine",
        ),
        ("internal.example", {"TOKEN": "shared"}, "shared"),
        ("acme.example", {"TOKEN": "shared"}, None),
    ],
    ids=["shared-tenant-own-secret", "shared-tenant-fallback", "customer-never-reads-shared"],
)
async def test_run_secret_reader_tries_the_run_org_first(
    monkeypatch: pytest.MonkeyPatch, org_name: str, values: dict[str, SecretValue], expected: str | None
) -> None:
    monkeypatch.setattr(start, "AUTH_REQUIRED", True)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example")
    org = OrgContext(id=UUID("00000000-0000-0000-0000-000000000001"), name=org_name, storage_key=None)
    request = cast(Any, SimpleNamespace(properties=None))
    runtime = cast(Any, SimpleNamespace(secrets=StubStore(values)))

    read = start._task_secret_reader(request, runtime, org)

    assert await read("TOKEN") == expected
