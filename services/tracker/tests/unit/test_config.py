import pytest

import json
import os
from pathlib import Path
import subprocess
import sys


@pytest.mark.description("`TRACKER_PASSKEY_ORGS` parsing into `PASSKEY_ORGS` and `GATEWAY_KEY_ORGS`.")
class TestPasskeyOrgs:
    @pytest.mark.description(
        "Passkey orgs are trimmed and sorted, and each also gets a gateway key along with `vals.ai`."
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
        assert gateway_key_orgs == ["other", "passkey-org", "vals.ai"]
