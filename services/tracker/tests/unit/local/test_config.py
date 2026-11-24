from pathlib import Path

import pytest

from tracker.local import config


@pytest.mark.description("Relative paths in the local config resolve from its folder.")
def test_relative_paths_resolve_from_config_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("KEY=value\n")
    config_path = tmp_path / "local-server.yaml"
    config_path.write_text("data_root: .local/data\nsecrets_file: .env\n")
    monkeypatch.chdir("/")
    monkeypatch.setattr(config, "resources", None)

    config.configure(config_path)

    assert config.resources is not None
    assert config.resources.data_root == tmp_path / ".local/data"
    assert config.resources.secrets_file == tmp_path / ".env"


@pytest.mark.description("A `.env` beside the config is the secrets file unless the config names a secrets source.")
@pytest.mark.parametrize(
    ("settings", "has_env", "expected"),
    [
        ("", True, ".env"),
        ("", False, None),
        ("secrets_file: other.env\n", True, "other.env"),
        ("secrets_manager_region: us-west-2\n", True, None),
    ],
)
def test_dotenv_is_the_default_secrets_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, settings: str, has_env: bool, expected: str | None
) -> None:
    if has_env:
        (tmp_path / ".env").write_text("KEY=value\n")
    (tmp_path / "other.env").write_text("KEY=value\n")
    config_path = tmp_path / "local-server.yaml"
    config_path.write_text("data_root: .local/data\n" + settings)
    monkeypatch.setattr(config, "resources", None)

    config.configure(config_path)

    assert config.resources is not None
    assert config.resources.secrets_file == (tmp_path / expected if expected else None)
