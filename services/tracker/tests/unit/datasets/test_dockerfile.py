from pathlib import Path

import pytest

from tracker.services.datasets.dockerfile import final_stage

DRUID_DOCKERFILE = """FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV WORKSPACE=/root

RUN mkdir -p ~/.m2/repository && \\
    cd ~/.m2/repository && \\
    cat > apache-jar-resource-bundle-1.5-SNAPSHOT.pom <<'POMEOF'
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
</project>
POMEOF

# Set working directory
WORKDIR ${WORKSPACE}

CMD ["/bin/bash"]
"""

LATEX_DOCKERFILE = """FROM ubuntu:24.04

COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /uvx /usr/local/bin/

ENV HF_HOME=/opt/huggingface

# Create working directory
WORKDIR /root
RUN mkdir -p /app

# Pre-download model weights to avoid timeouts
RUN python3 - <<'PY'
from huggingface_hub import snapshot_download

for repo, revision in repos.items():
    snapshot_download(repo_id=repo, revision=revision, local_files_only=False)
PY

RUN chmod -R a+rX "$HF_HOME"
"""


def write(tmp_path: Path, dockerfile: str) -> Path:
    path = tmp_path / "Dockerfile"
    path.write_text(dockerfile)

    return path


@pytest.mark.description("A dockerfile's working directory is the one its last build stage would run in.")
class TestDockerfileWorkdir:
    @pytest.mark.description("A `WORKDIR` that names an `ENV` variable runs in the variable's value.")
    def test_a_workdir_naming_an_env_variable_takes_its_value(self, tmp_path: Path) -> None:
        assert final_stage(write(tmp_path, DRUID_DOCKERFILE)).workdir == "/root"

    @pytest.mark.description("A heredoc line that starts with `from` leaves the stage and its workdir in place.")
    def test_a_heredoc_line_starting_with_from_is_not_an_instruction(self, tmp_path: Path) -> None:
        stage = final_stage(write(tmp_path, LATEX_DOCKERFILE))

        assert (stage.workdir, stage.base) == ("/root", "ubuntu:24.04")

    @pytest.mark.description(
        "The workdir follows build arguments, stages, continuations and comments as a build would."
    )
    @pytest.mark.parametrize(
        ("dockerfile", "expected"),
        [
            ("FROM x\nworkdir /lower\n", "/lower"),
            ("FROM x\nWORKDIR /a\nWORKDIR b\n", "/a/b"),
            ("FROM x\nWORKDIR b\n", None),
            ("FROM x\nWORKDIR /app/\n", "/app"),
            ("FROM build\nWORKDIR /build\nFROM runtime\nRUN true\n", None),
            ("FROM build\nWORKDIR /build\nFROM runtime\nWORKDIR /app\n", "/app"),
            ("FROM x AS build\nWORKDIR /build\nFROM build\nWORKDIR out\n", "/build/out"),
            ("# WORKDIR /commented\nFROM x\n", None),
            ("FROM x\nWORKDIR\n", None),
            ("FROM x\nARG APP=/srv\nWORKDIR $APP/web\n", "/srv/web"),
            ("ARG APP=/srv\nFROM x\nARG APP\nWORKDIR ${APP}\n", "/srv"),
            ("FROM x\nARG APP\nWORKDIR ${APP:-/default}\n", "/default"),
            ("FROM x\nENV ROOT=/opt BASE=/srv\nENV APP=${ROOT}/app\nWORKDIR $APP\n", "/opt/app"),
            ("FROM x\nARG DIR=/arg\nENV DIR=/env\nWORKDIR $DIR\n", "/env"),
            ("FROM x\nWORKDIR $HOME/app\n", None),
            ("FROM x\nWORKDIR /a\nWORKDIR $HOME\nWORKDIR b\n", None),
            ("FROM x\nWORKDIR \\\n  # a comment\n\n  /continued\n", "/continued"),
            ("FROM x\nRUN cat <<-EOF > /f\n\tFROM y\n\tEOF\nWORKDIR /kept\n", "/kept"),
            ("FROM x\nRUN python3 -c 'print(1<<20)'\nWORKDIR /kept\n", "/kept"),
            ("FROM x\nRUN echo $((1<<2))\nWORKDIR /kept\n", "/kept"),
            ('FROM x\nRUN echo "a<<b"\nWORKDIR /kept\n', "/kept"),
        ],
        ids=[
            "lowercase",
            "relative",
            "relative to the base image",
            "trailing slash",
            "external last stage",
            "last stage",
            "earlier stage",
            "comment",
            "empty",
            "stage argument",
            "global argument",
            "default value",
            "environment",
            "env over arg",
            "unknown variable",
            "relative to unknown",
            "continuation",
            "tab heredoc",
            "quoted shift",
            "arithmetic shift",
            "quoted marker",
        ],
    )
    def test_the_workdir_follows_the_build(self, tmp_path: Path, dockerfile: str, expected: str | None) -> None:
        assert final_stage(write(tmp_path, dockerfile)).workdir == expected

    @pytest.mark.description("A missing dockerfile names no workdir.")
    def test_a_missing_dockerfile_names_no_workdir(self, tmp_path: Path) -> None:
        assert final_stage(tmp_path / "Dockerfile").workdir is None


@pytest.mark.description("A dockerfile starts from the image its last stage is built on.")
@pytest.mark.parametrize(
    ("dockerfile", "base"),
    [
        ("FROM golang:1.23\nRUN go version\n", "golang:1.23"),
        ("FROM --platform=linux/amd64 python:3.12 AS build\nFROM golang:1.23\n", "golang:1.23"),
        ("FROM golang:1.23 AS build\nRUN true\nFROM build\n", "golang:1.23"),
        ("ARG BASE=golang\nFROM ${BASE}:1.23\n", "golang:1.23"),
        ("FROM ${BASE}:1.23\n", None),
    ],
    ids=["one stage", "last stage", "earlier stage", "build argument", "unknown argument"],
)
def test_a_dockerfile_starts_from_the_image_of_its_last_stage(
    tmp_path: Path, dockerfile: str, base: str | None
) -> None:
    assert final_stage(write(tmp_path, dockerfile)).base == base


@pytest.mark.description("A dockerfile's user is the last `USER` of its final stage, inherited from an earlier stage.")
@pytest.mark.parametrize(
    ("dockerfile", "user"),
    [
        ("FROM ubuntu:24.04\nRUN true\n", None),
        ("FROM ubuntu:24.04\nUSER agent\n", "agent"),
        ("FROM ubuntu:24.04 AS base\nUSER agent\nFROM base\nRUN true\n", "agent"),
        ("FROM ubuntu:24.04\nUSER agent\nFROM python:3.12\n", None),
        ("FROM ubuntu:24.04\nARG NAME=nobody\nUSER ${NAME}:nogroup\n", "nobody:nogroup"),
        ("FROM ubuntu:24.04\nRUN <<EOF\nUSER agent\nEOF\n", None),
        ("FROM ubuntu:24.04 AS base\nARG WHO=nobody\nFROM base\nUSER $WHO\n", "nobody"),
    ],
    ids=["none", "declared", "earlier stage", "new stage resets", "build argument", "heredoc", "inherited argument"],
)
def test_a_dockerfiles_user_comes_from_its_final_stage(tmp_path: Path, dockerfile: str, user: str | None) -> None:
    assert final_stage(write(tmp_path, dockerfile)).user == user
