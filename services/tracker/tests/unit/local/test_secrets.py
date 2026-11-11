from pathlib import Path

import pytest

from tracker.exceptions import SecretNotFoundError, SecretsError
from tracker.local.secrets import InMemorySecretStore, SecretsFile, load_execution_secrets
from tracker.runtime.secrets import resolve_secrets


@pytest.mark.description("Only declared keys enter the transient per-run store.")
class TestLocalSecrets:
    @pytest.mark.description("Grouped secret references leave out undeclared values.")
    async def test_grouped_references_exclude_undeclared_values(self) -> None:
        references = {"FIRST_KEY": "shared", "SECOND_KEY": "shared", "THIRD_KEY": "other"}
        declared = {"FIRST_KEY": "first", "SECOND_KEY": "second", "THIRD_KEY": "third"}
        store = InMemorySecretStore(references, {**declared, "UNDECLARED": "private"})

        assert await resolve_secrets(references, store) == declared
        assert await store.get("shared") == {"FIRST_KEY": "first", "SECOND_KEY": "second"}
        assert await store.get("other") == {"THIRD_KEY": "third"}
        with pytest.raises(SecretNotFoundError, match="no values for secret reference 'UNDECLARED'"):
            await store.get("UNDECLARED")

    @pytest.mark.description("A secret store missing declared keys fails to build and names every missing key.")
    def test_missing_declared_keys_fail_before_store_is_available(self) -> None:
        with pytest.raises(SecretsError, match="missing keys: FIRST_KEY, SECOND_KEY"):
            InMemorySecretStore({"SECOND_KEY": "shared", "FIRST_KEY": "shared"}, {"UNDECLARED": "private"})

    @pytest.mark.description("A `${VAR}` in a secrets file stays literal, and undeclared keys are dropped.")
    def test_file_values_do_not_expand_host_credentials(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOST_ONLY_SECRET", "must-not-leak")
        source = tmp_path / "run.env"
        source.write_text("MODEL_KEY=${HOST_ONLY_SECRET}\nUNDECLARED=private\n", encoding="utf-8")

        values = load_execution_secrets(source, {"MODEL_KEY": "model-reference"})

        assert values == {"MODEL_KEY": "${HOST_ONLY_SECRET}"}

    @pytest.mark.description("Optional secret files may be missing, and required ones may not.")
    def test_optional_and_missing_secret_files(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.env"

        assert load_execution_secrets(missing, {}) == {}
        assert load_execution_secrets(None, {"MODEL_KEY": "model-reference"}) == {}
        with pytest.raises(SecretsError, match="Configured local secrets file does not exist"):
            load_execution_secrets(missing, {"MODEL_KEY": "model-reference"})


@pytest.mark.description("A task reads a named value from the file, except the settings that run the platform.")
class TestSecretsFile:
    @pytest.mark.description("A task reads a value by name.")
    async def test_a_task_reads_a_value_by_name(self, tmp_path: Path) -> None:
        source = tmp_path / "run.env"
        source.write_text("JUDGE_KEY=sk-judge\n", encoding="utf-8")

        assert await SecretsFile(source).get("JUDGE_KEY") == "sk-judge"

    @pytest.mark.description("A name the file lacks is not found.")
    async def test_a_name_the_file_lacks_is_not_found(self, tmp_path: Path) -> None:
        source = tmp_path / "run.env"
        source.write_text("OTHER=1\n", encoding="utf-8")

        with pytest.raises(SecretNotFoundError, match="no value for 'JUDGE_KEY'"):
            await SecretsFile(source).get("JUDGE_KEY")

    @pytest.mark.description("Provider and gateway settings are off limits.")
    @pytest.mark.parametrize(
        "name", ["DAYTONA_API_KEY", "DAYTONA_V2_API_KEY", "MODAL_TOKEN_SECRET", "MODEL_GATEWAY_API_KEY"]
    )
    async def test_provider_and_gateway_settings_are_off_limits(self, tmp_path: Path, name: str) -> None:
        source = tmp_path / "run.env"
        source.write_text(f"{name}=secret\n", encoding="utf-8")

        with pytest.raises(SecretsError, match="cannot read sandbox provider or model gateway settings"):
            await SecretsFile(source).get(name)
