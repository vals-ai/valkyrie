import pytest

import json
import os
from pathlib import Path
import subprocess
import sys

from tracker.clients.model_gateway import machine


@pytest.mark.parametrize(
    ("mode", "url", "key", "stage", "invalid_setting"),
    [
        ("manual", "", "", "", None),
        ("manual", "https://gateway.example", "machine-token", "dev", None),
        ("automatic", "", "", "dev", "MODEL_GATEWAY_URL"),
        ("provision", "https://gateway.example:bad", "machine-token", "dev", "MODEL_GATEWAY_URL"),
        ("provision", "https://gateway.example:65536", "machine-token", "dev", "MODEL_GATEWAY_URL"),
        ("invalid", "", "", "dev", "MODEL_GATEWAY_PROJECT_KEYS_MODE"),
        ("manual", "https://gateway.example", "", "dev", "MODEL_GATEWAY_MACHINE_API_KEY"),
        ("manual", "https://gateway.example", "machine-token", "", "VALKYRIE_STAGE"),
    ],
)
@pytest.mark.description(
    "Startup allows a disabled Gateway and names the setting to fix when it cannot use the Gateway."
)
def test_project_gateway_configuration_at_startup(
    monkeypatch: pytest.MonkeyPatch, mode: str, url: str, key: str, stage: str, invalid_setting: str | None
) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", mode)
    monkeypatch.setenv("MODEL_GATEWAY_URL", url)
    monkeypatch.setenv("MODEL_GATEWAY_MACHINE_API_KEY", key)
    monkeypatch.setattr(machine, "VALKYRIE_STAGE", stage)

    if invalid_setting is None:
        machine.validate_enabled_project_gateway()
    else:
        with pytest.raises(ValueError, match=f"Set {invalid_setting} "):
            machine.validate_enabled_project_gateway()


@pytest.mark.description("`TRACKER_PASSKEY_ORGS` parsing into `PASSKEY_ORGS` and `GATEWAY_KEY_ORGS`.")
class TestPasskeyOrgs:
    @pytest.mark.description(
        "Passkey orgs are trimmed and sorted, and each also gets a gateway key along with `vals.ai` and `smith-research`."
    )
    def test_listed_orgs_are_trimmed_and_get_gateway_keys(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json; from tracker.config import GATEWAY_KEY_ORGS, PASSKEY_ORGS; "
                "print(json.dumps([sorted(PASSKEY_ORGS), sorted(GATEWAY_KEY_ORGS)]))",
            ],
            env={**os.environ, "TRACKER_PASSKEY_ORGS": " passkey-org ,other,"},
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=True,
        )

        passkey_orgs, gateway_key_orgs = json.loads(result.stdout.splitlines()[-1])
        assert passkey_orgs == ["other", "passkey-org"]
        assert gateway_key_orgs == ["other", "passkey-org", "smith-research", "vals.ai"]
