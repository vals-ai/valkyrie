import pytest

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
        read = secret_reader(store, lambda name: f"tenant/{name}")

        assert await read("TOKEN") == "plain"
        assert await read("DB") == "pg"

    @pytest.mark.description("A name the run may not use reads as missing.")
    async def test_a_name_the_run_may_not_use_reads_as_missing(self) -> None:
        def stored_name(name: str) -> str:
            raise ValueError(f"Secret '{name}' does not exist")

        read = secret_reader(StubStore({"TOKEN": "plain"}), stored_name)

        assert await read("TOKEN") is None

    @pytest.mark.description("An unknown secret or json key reads as missing.")
    async def test_an_unknown_secret_or_json_key_reads_as_missing(self) -> None:
        read = secret_reader(StubStore({"JSON": {"OTHER": "x"}}), lambda name: name)

        assert await read("NOPE") is None
        assert await read("JSON") is None
