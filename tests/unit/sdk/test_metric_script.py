from pathlib import Path

import pytest

from valkyrie.sdk.metric_script import (
    MAX_METRIC_BYTES,
    MetricScriptError,
    metric_digest,
    read_metric_script,
)

HEADER = "# /// script\n# dependencies = [\n#   'numpy',\n# ]\n# ///\n"


@pytest.mark.description("A folder without a script has none.")
def test_a_folder_without_a_script_has_none(tmp_path: Path) -> None:
    assert read_metric_script(tmp_path) is None


@pytest.mark.description("A valid script with or without an inline header is returned as bytes.")
def test_a_valid_script_with_or_without_an_inline_header_is_returned_as_bytes(tmp_path: Path) -> None:
    (tmp_path / "metric.py").write_text("print('ok')\n")
    assert read_metric_script(tmp_path) == b"print('ok')\n"

    (tmp_path / "metric.py").write_text(HEADER + "import numpy\n")
    assert read_metric_script(tmp_path) == (HEADER + "import numpy\n").encode()


@pytest.mark.description("A script the tracker would refuse is refused.")
@pytest.mark.parametrize(
    ("content", "complaint"),
    [
        pytest.param(b"def broken(:\n", "not valid Python", id="syntax"),
        pytest.param(b"\xff\xfe", "not valid Python", id="not-utf8"),
        pytest.param(b"#" * (MAX_METRIC_BYTES + 1), "larger than", id="too-large"),
        pytest.param(b"# /// script\n# dependencies = [\n# ///\nx = 1\n", "not valid TOML", id="bad-header"),
        pytest.param((HEADER + "x = 1\n" + HEADER).encode(), "more than one", id="two-headers"),
    ],
)
def test_a_script_the_tracker_would_refuse_is_refused(tmp_path: Path, content: bytes, complaint: str) -> None:
    (tmp_path / "metric.py").write_bytes(content)

    with pytest.raises(MetricScriptError, match=complaint):
        read_metric_script(tmp_path)


@pytest.mark.description("The digest is the sha256 of the bytes.")
def test_the_digest_is_the_sha256_of_the_bytes() -> None:
    assert metric_digest(b"") == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
