from pathlib import Path

import pytest
from click.testing import CliRunner

from valkyrie.cli.main import cli
from valkyrie.sdk.harbor_dataset import ScoreType, write_dataset

from tests.unit.sdk.test_harbor_dataset import make_task


@pytest.mark.description("`valkyrie dataset check` passes a standard dataset and fails with each problem otherwise.")
def test_check_passes_a_standard_dataset_and_fails_with_each_problem(tmp_path: Path) -> None:
    root = tmp_path / "a2j"
    write_dataset(root, [make_task()], metric="print(1)\n", score_types={"accuracy": ScoreType(unit="percent")})

    passed = CliRunner().invoke(cli, ["dataset", "check", str(root)])
    (root / "metric.py").unlink()
    failed = CliRunner().invoke(cli, ["dataset", "check", str(root)])

    assert passed.exit_code == 0, passed.output
    assert failed.exit_code == 1
    assert "metric.py" in failed.output
