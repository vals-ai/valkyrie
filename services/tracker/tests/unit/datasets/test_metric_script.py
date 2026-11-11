import pytest

from tracker.services.datasets.metric_script import (
    MAX_METRIC_BYTES,
    InvalidMetricScriptError,
    check_metric_script,
    file_digest,
)

HEADER = "# /// script\n# dependencies = [\n#   'numpy',\n# ]\n# ///\n"


@pytest.mark.description("What a script must be before it is stored.")
class TestCheckMetricScript:
    @pytest.mark.description("A python script with or without an inline header passes.")
    def test_a_python_script_with_or_without_an_inline_header_passes(self) -> None:
        check_metric_script(b"print('ok')\n")
        check_metric_script((HEADER + "import numpy\n").encode())

    @pytest.mark.description("A script that would fail for every run is refused.")
    @pytest.mark.parametrize(
        ("content", "complaint"),
        [
            pytest.param(b"def broken(:\n", "not valid Python", id="syntax"),
            pytest.param(b"\xff\xfe", "not valid Python", id="not-utf8"),
            pytest.param(b"x = 1\n" + b"#" * MAX_METRIC_BYTES, "larger than", id="too-large"),
            pytest.param(b"# /// script\n# dependencies = [\n# ///\nx = 1\n", "not valid TOML", id="bad-header"),
            pytest.param((HEADER + "x = 1\n" + HEADER).encode(), "more than one", id="two-headers"),
        ],
    )
    def test_a_script_that_would_fail_for_every_run_is_refused(self, content: bytes, complaint: str) -> None:
        with pytest.raises(InvalidMetricScriptError, match=complaint):
            check_metric_script(content)

    @pytest.mark.description("Other inline blocks are left alone.")
    def test_other_inline_blocks_are_left_alone(self) -> None:
        check_metric_script(b"# /// notes\n# anything at all {\n# ///\nx = 1\n")


@pytest.mark.description("The digest is the sha256 of the bytes.")
def test_the_digest_is_the_sha256_of_the_bytes() -> None:
    assert file_digest(b"") == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
