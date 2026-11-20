import json
import os
import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from valkyrie.sdk.harbor_dataset import HarborTask, MCPServer, ScoreType, Verifier, dataset_problems, write_dataset

GRADER = "ghcr.io/vals-ai/a2j-grader@sha256:" + "a" * 64
SCORE_TYPES = {"accuracy": ScoreType(unit="percent")}
SEARCH = MCPServer(name="search", transport="streamable-http", url="http://search:8000/mcp")
TASK_TOML = """schema_version = "1.3"

[task]
name = "vals/a2j-both-case-001"

[metadata]
benchmark = "a2j"
category = "housing"
tags = ["eviction"]

[metadata.valkyrie]
verifier_result_file = "details.json"

[agent]
timeout_sec = 10800.0

[verifier]
environment_mode = "separate"
timeout_sec = 1800.0

[verifier.environment]
cpus = 1.0
memory_mb = 1024
storage_mb = 1024

[verifier.env]
MODEL_GATEWAY_URL = "${MODEL_GATEWAY_URL:-}"
"""


def make_task(**overrides: object) -> HarborTask:
    fields: dict[str, object] = {
        "task_id": "both__case-001",
        "name": "vals/a2j-both-case-001",
        "instruction": "Help the client.\n",
        "category": "housing",
        "tags": ["eviction"],
        "environment_dockerfile": "FROM python:3.12-slim\nCOPY conductor /task/agent/conductor\n",
        "environment_files": {"conductor/conductor.json": '{"condition": "both"}\n'},
        "metadata": {"benchmark": "a2j"},
        "agent_timeout_sec": 10800.0,
        "verifier": Verifier(
            image=GRADER,
            command="a2j-grade --generation /logs/artifacts/generation.json",
            files={"rubric.json": "{}\n"},
            env={"MODEL_GATEWAY_URL": "${MODEL_GATEWAY_URL:-}"},
        ),
    }
    return HarborTask.model_validate(fields | overrides)


def write(root: Path, *tasks: HarborTask) -> Path:
    write_dataset(root, tasks or (make_task(),), metric="print(1)\n", score_types=SCORE_TYPES)
    return root


@pytest.mark.description("Writing a Harbor dataset.")
class TestWriteDataset:
    @pytest.mark.description("A written dataset has the Harbor layout and passes its own check.")
    def test_a_written_dataset_has_the_harbor_layout_and_passes_its_own_check(self, tmp_path: Path) -> None:
        root = write(tmp_path / "a2j")
        task_dir = root / "both__case-001"

        assert sorted(str(p.relative_to(task_dir)) for p in task_dir.rglob("*") if p.is_file()) == [
            "environment/Dockerfile",
            "environment/conductor/conductor.json",
            "instruction.md",
            "task.toml",
            "tests/Dockerfile",
            "tests/rubric.json",
            "tests/test.sh",
        ]
        assert os.access(task_dir / "tests/test.sh", os.X_OK)
        assert json.loads((root / "score_types.json").read_text()) == {"accuracy": {"unit": "percent"}}
        config = tomllib.loads((task_dir / "task.toml").read_text())
        assert config["task"] == {"name": "vals/a2j-both-case-001"}
        assert config["metadata"] == {
            "benchmark": "a2j",
            "category": "housing",
            "tags": ["eviction"],
            "valkyrie": {"verifier_result_file": "details.json"},
        }
        assert config["verifier"]["environment_mode"] == "separate"
        assert dataset_problems(root) == []

    @pytest.mark.description("A task without MCP servers or a skills folder writes the same `task.toml` as before.")
    def test_a_task_without_agent_inputs_writes_the_same_task_toml(self) -> None:
        assert make_task().task_toml() == TASK_TOML

    @pytest.mark.description("MCP servers and a skills folder are written where Harbor and the Tracker read them.")
    def test_mcp_servers_and_skills_are_written_where_harbor_and_the_tracker_read_them(self, tmp_path: Path) -> None:
        task = make_task(
            environment_dockerfile="FROM python:3.12-slim\nWORKDIR /skills\nCOPY skills/cite/SKILL.md cite/SKILL.md\n",
            environment_files={"skills/cite/SKILL.md": "# Cite\n"},
            mcp_servers=[
                SEARCH.model_copy(update={"headers": {"Authorization": "Bearer ${SEARCH_TOKEN}"}}),
                MCPServer(name="calc", transport="stdio", command="calc-mcp", args=["--exact"]),
            ],
            skills_dir="/skills",
        )
        root = write(tmp_path / "a2j", task)

        config = tomllib.loads((root / "both__case-001/task.toml").read_text())
        assert config["environment"] == {
            "skills_dir": "/skills",
            "mcp_servers": [
                {
                    "name": "search",
                    "transport": "streamable-http",
                    "url": "http://search:8000/mcp",
                    "headers": {"Authorization": "Bearer ${SEARCH_TOKEN}"},
                },
                {"name": "calc", "transport": "stdio", "command": "calc-mcp", "args": ["--exact"]},
            ],
        }
        assert dataset_problems(root) == []

    @pytest.mark.description("A task that Harbor or the Tracker would refuse is refused when it is built.")
    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"name": "A2J/Case 1"}, "lowercase <org>/<task>"),
            ({"name": "vals/case..001"}, "lowercase <org>/<task>"),
            ({"task_id": "../escape"}, "Not a valid task ID"),
            ({"task_id": "metric.py"}, "reserved for a dataset file"),
            ({"instruction": " \n"}, "must not be empty"),
            ({"environment_image": GRADER}, "exactly one of"),
            ({"environment_dockerfile": None, "environment_image": "python:3.12"}, "Pin the image"),
            ({"files": {"tests/answer.json": "{}"}}, "verifier.files"),
            ({"metadata": {"category": "x"}}, "written by HarborTask"),
            ({"metadata": {"score": float("nan")}}, "round-trip"),
            ({"skills_dir": "skills"}, "absolute path"),
            ({"mcp_servers": [SEARCH, SEARCH]}, "Duplicate MCP server names"),
            ({"mcp_servers": [{"name": "calc", "transport": "stdio"}]}, "needs a command"),
            ({"mcp_servers": [{"name": "search", "transport": "sse", "command": "search-mcp"}]}, "needs a url"),
            ({"mcp_servers": [SEARCH.model_dump() | {"headers": {"Authorization": "Bearer sk-1"}}]}, "credential"),
        ],
    )
    def test_a_task_that_harbor_or_the_tracker_would_refuse_is_refused_when_built(
        self, overrides: dict[str, object], message: str
    ) -> None:
        with pytest.raises(ValidationError, match=message):
            make_task(**overrides)

    @pytest.mark.description("A grader image without a digest is refused.")
    def test_a_grader_image_without_a_digest_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="Pin the image"):
            Verifier(image="ghcr.io/vals-ai/a2j-grader:latest", command="a2j-grade")

    @pytest.mark.description(
        "Task IDs that differ only in case and repeated names are refused before anything is written."
    )
    def test_duplicate_ids_and_names_are_refused_before_anything_is_written(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Duplicate task IDs"):
            write(tmp_path / "ids", make_task(), make_task(task_id="BOTH__case-001", name="vals/other"))
        with pytest.raises(ValueError, match="Duplicate task names"):
            write(tmp_path / "names", make_task(), make_task(task_id="both__case-002"))

        assert not (tmp_path / "ids").exists()
        assert not (tmp_path / "names").exists()


@pytest.mark.description("Checking a Harbor dataset.")
class TestDatasetProblems:
    @pytest.mark.description("Each departure from the layout is reported.")
    def test_each_departure_from_the_layout_is_reported(self, tmp_path: Path) -> None:
        root = write(tmp_path / "a2j")
        task_dir = root / "both__case-001"
        (task_dir / "tests/Dockerfile").write_text("FROM ghcr.io/vals-ai/a2j-grader:latest\n")
        (task_dir / "tests/test.sh").chmod(0o644)
        toml = (task_dir / "task.toml").read_text()
        (task_dir / "task.toml").write_text(
            toml.replace('environment_mode = "separate"', 'environment_mode = "shared"').replace(
                "[verifier.environment]\n",
                '[verifier.environment]\ndocker_image = "ghcr.io/vals-ai/a2j-grader:latest"\n',
            )
        )
        (root / "metric.py").unlink()

        problems = dataset_problems(root)

        assert len(problems) == 5
        assert any("[verifier.environment].docker_image" in p for p in problems)
        assert any("metric.py" in p for p in problems)
        assert any("environment_mode" in p for p in problems)
        assert any("test.sh" in p for p in problems)
        assert any("tests/Dockerfile" in p for p in problems)

    @pytest.mark.description("MCP servers and skills folders that the Tracker would refuse or ignore are reported.")
    def test_mcp_servers_and_skills_the_tracker_would_refuse_or_ignore_are_reported(self, tmp_path: Path) -> None:
        root = write(tmp_path / "a2j", make_task(mcp_servers=[SEARCH], skills_dir="/skills"))
        toml_path = root / "both__case-001/task.toml"
        toml_path.write_text(
            toml_path.read_text().replace(
                "[verifier.environment]\n", '[verifier.environment]\nskills_dir = "/skills"\n'
            )
            + '\n[[environment.mcp_servers]]\nname = "calc"\ntransport = "stdio"\n'
            + '\n[[environment.mcp_servers]]\nname = "search"\nurl = "http://search:8000/mcp"\n'
        )

        problems = dataset_problems(root)

        assert len(problems) == 3
        assert any("[verifier.environment].skills_dir is ignored" in p for p in problems)
        assert any("mcp_servers[1]" in p and "needs a command" in p for p in problems)
        assert any("declares 'search' more than once" in p for p in problems)

    @pytest.mark.description("An MCP server entry that the readers refuse is reported with the field at fault.")
    @pytest.mark.parametrize(
        ("entry", "message"),
        [
            ('[environment.mcp_servers]\nname = "s"\nurl = "http://s/mcp"\n', "must be an array of tables"),
            (
                '[[environment.mcp_servers]]\nname = "s"\nurl = "http://s/mcp"\nenv = "x"\n',
                "mcp_servers[0]: env: Extra",
            ),
            ('[[environment.mcp_servers]]\nurl = "http://s/mcp"\n', "mcp_servers[0]: name: Field required"),
            ('[[environment.mcp_servers]]\nname = "s"\ntransport = "ws"\nurl = "http://s/mcp"\n', "transport: Input"),
        ],
    )
    def test_an_mcp_server_entry_the_readers_refuse_is_reported_with_its_field(
        self, tmp_path: Path, entry: str, message: str
    ) -> None:
        root = write(tmp_path / "a2j")
        toml_path = root / "both__case-001/task.toml"
        toml_path.write_text(toml_path.read_text() + "\n" + entry)

        problems = dataset_problems(root)

        assert len(problems) == 1
        assert message in problems[0]

    @pytest.mark.description("Malformed task files are reported without stopping the check.")
    def test_malformed_task_files_are_reported_without_stopping_the_check(self, tmp_path: Path) -> None:
        root = write(tmp_path / "a2j")
        task_dir = root / "both__case-001"
        (task_dir / "tests/Dockerfile").write_text("")
        (task_dir / "instruction.md").write_bytes(b"\xff\xfe")
        toml = (task_dir / "task.toml").read_text()
        (task_dir / "task.toml").write_text('task = "x"\n' + toml.split("\n", 1)[1].replace("[task]\n", "[old_task]\n"))

        problems = dataset_problems(root)

        assert any("instruction.md" in p for p in problems)
        assert any("[task].name" in p for p in problems)
        assert any("tests/Dockerfile" in p for p in problems)

    @pytest.mark.description("Grader details must be a `TaskEvaluation` that uses only declared score types.")
    @pytest.mark.parametrize(
        ("details", "message"),
        [
            ({"status": "success", "scores": {"accuracy": {"value": 1.0}}, "metadata": {"judge": "pass"}}, None),
            ({"status": "success", "scores": {"recall": {"value": 1.0}}}, "not declared"),
            ({"status": "done", "scores": {}}, "not a TaskEvaluation"),
            ({"status": "success", "score": 1.0}, "not a TaskEvaluation"),
        ],
    )
    def test_grader_details_must_be_a_task_evaluation_with_declared_scores(
        self, tmp_path: Path, details: dict[str, object], message: str | None
    ) -> None:
        root = write(tmp_path / "a2j")
        path = tmp_path / "details.json"
        path.write_text(json.dumps(details))

        problems = dataset_problems(root, [path])

        assert (problems == []) if message is None else (len(problems) == 1 and message in problems[0])
