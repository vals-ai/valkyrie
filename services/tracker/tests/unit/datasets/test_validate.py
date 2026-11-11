from pathlib import Path

import pytest

from tracker.services.datasets.config import TaskConfig
from tracker.services.datasets.folder import SandboxKind, sandbox_kind
from tracker.services.datasets.validate import TaskReport, validate_task

MINIMAL = """
schema_version = "1.4"

[agent]
timeout_sec = 600

[verifier]
timeout_sec = 60

[environment]
cpus = 1
memory_mb = 2048
workdir = "/app"
"""
WITH_IMAGE = MINIMAL.replace('workdir = "/app"', 'workdir = "/app"\ndocker_image = "ubuntu:24.04"')
TEST_SCRIPT = "#!/bin/bash\nmkdir -p /logs/verifier\necho 1 > /logs/verifier/reward.txt\n"


def write_task(
    root: Path,
    toml: str = MINIMAL,
    *,
    name: str = "hello-file",
    files: dict[str, str] | None = None,
    skip: tuple[str, ...] = (),
) -> Path:
    task = root / name
    contents = {
        "task.toml": toml,
        "instruction.md": "Create hello.txt.\n",
        "tests/test.sh": TEST_SCRIPT,
        "environment/Dockerfile": "FROM ubuntu:24.04\nWORKDIR /app\n",
        **(files or {}),
    }
    for relative, text in contents.items():
        if relative not in skip:
            (task / relative).parent.mkdir(parents=True, exist_ok=True)
            (task / relative).write_text(text)
    return task


def problems(report: TaskReport) -> str:
    return "\n".join(report.errors)


@pytest.mark.description("Tasks that are ready to store and run.")
class TestValidTasks:
    @pytest.mark.description("A minimal task with a dockerfile is valid.")
    def test_a_minimal_task_with_a_dockerfile_is_valid(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path))

        assert report.ok
        assert report.warnings == ()
        assert report.config == TaskConfig.parse(MINIMAL)

    @pytest.mark.description("A task with a prebuilt image needs no environment folder.")
    def test_a_task_with_a_prebuilt_image_needs_no_environment_folder(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, WITH_IMAGE, skip=("environment/Dockerfile",)))

        assert report.ok

    @pytest.mark.description("A multimodal style task is valid.")
    def test_a_multimodal_style_task_is_valid(self, tmp_path: Path) -> None:
        toml = """
schema_version = "1.3"

[task]
name = "swe-bench/swebench-multimodal__django-1"
keywords = ["swe-bench"]

[metadata]
category = "debugging"

[agent]
timeout_sec = 3000.0

[verifier]
timeout_sec = 1800.0

[environment]
network_mode = "public"
build_timeout_sec = 1800.0
cpus = 4
memory_mb = 8192
storage_mb = 20480
"""

        report = validate_task(write_task(tmp_path, toml, name="swebench-multimodal__django-1"))

        assert report.ok

    @pytest.mark.description("Allowlisted stages and gpu requests are valid.")
    def test_allowlisted_stages_and_gpu_requests_are_valid(self, tmp_path: Path) -> None:
        toml = (
            '[agent]\nnetwork_mode = "allowlist"\nallowed_hosts = ["pypi.org"]\n'
            '[verifier]\nnetwork_mode = "no-network"\n[environment]\ngpus = 1\ngpu_types = ["H100", "A100"]\n'
        )

        assert validate_task(write_task(tmp_path, toml)).ok


@pytest.mark.description("The report carries the settings it checked, so callers don't read the file again.")
class TestParsedConfig:
    @pytest.mark.description("A task refused for what it asks of the run still returns its settings.")
    def test_a_task_refused_for_what_it_asks_of_the_run_still_returns_its_settings(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, MINIMAL + "\n[environment.tpu]\ntype = 'v6e'\ntopology = '2x4'"))

        assert not report.ok
        assert report.config is not None

    @pytest.mark.description("A task that does not parse returns none.")
    @pytest.mark.parametrize("toml", ["[environment\ncpus = ", '[environment]\ncpus = "many"'])
    def test_a_task_that_does_not_parse_returns_none(self, tmp_path: Path, toml: str) -> None:
        assert validate_task(write_task(tmp_path, toml)).config is None


STEPS_TOML = (
    MINIMAL
    + """
[[steps]]
name = "first"
[[steps]]
name = "second"
"""
)


def steps_task(root: Path, toml: str = STEPS_TOML, *, files: dict[str, str] | None = None) -> Path:
    return write_task(
        root,
        toml,
        skip=("instruction.md",),
        files={
            "steps/first/instruction.md": "First.\n",
            "steps/second/instruction.md": "Second.\n",
            **(files or {}),
        },
    )


@pytest.mark.description("A task with steps keeps an instruction of its own for each and needs a test script for each.")
class TestSteps:
    @pytest.mark.description("A task with steps needs no instruction of its own.")
    def test_a_task_with_steps_needs_no_instruction_of_its_own(self, tmp_path: Path) -> None:
        report = validate_task(steps_task(tmp_path))

        assert report.ok
        assert report.config is not None
        assert [step.name for step in report.config.steps] == ["first", "second"]

    @pytest.mark.description("Every step needs its instruction.")
    def test_every_step_needs_its_instruction(self, tmp_path: Path) -> None:
        task = steps_task(tmp_path)
        (task / "steps" / "second" / "instruction.md").unlink()

        assert validate_task(task).errors == ("steps/second/instruction.md is missing",)

    @pytest.mark.description("A step may bring its own tests or share the tasks.")
    def test_a_step_may_bring_its_own_tests_or_share_the_tasks(self, tmp_path: Path) -> None:
        shared = validate_task(steps_task(tmp_path / "shared"))
        own = validate_task(
            steps_task(
                tmp_path / "own",
                files={"steps/first/tests/test.sh": "#!/bin/bash\n"},
            )
        )
        neither = validate_task(
            write_task(
                tmp_path / "neither",
                STEPS_TOML,
                skip=("instruction.md", "tests/test.sh"),
                files={"steps/first/instruction.md": "x", "steps/second/instruction.md": "x"},
            )
        )

        assert shared.ok and own.ok
        assert neither.errors == (
            "step 'first' has no tests/test.sh of its own or in the task's tests",
            "step 'second' has no tests/test.sh of its own or in the task's tests",
        )

    @pytest.mark.description("A step may not name a service a task without compose does not have.")
    def test_a_step_may_not_name_a_service_a_task_without_compose_does_not_have(self, tmp_path: Path) -> None:
        toml = (
            STEPS_TOML
            + "\n[[steps.verifier.collect]]\ncommand = 'dump'\nservice = 'db'"
            + "\n[[steps]]\nname = 'third'\nartifacts = [{ source = '/data', service = 'cache' }]"
        )
        files = {"steps/third/instruction.md": "Third.\n"}

        report = validate_task(steps_task(tmp_path, toml, files=files))

        assert report.errors == (
            "the service 'cache' is named, but only a compose task has services",
            "the service 'db' is named, but only a compose task has services",
        )

    @pytest.mark.description("A compose verifier of a step may not ask for gpus.")
    def test_a_compose_verifier_of_a_step_may_not_ask_for_gpus(self, tmp_path: Path) -> None:
        toml = (
            MINIMAL.replace('workdir = "/app"', 'workdir = "/app"\ndocker_image = "x"\ngpus = 1')
            + "[[steps]]\nname = 'first'\n[steps.verifier]\nenvironment_mode = 'separate'\n"
            + "[[steps]]\nname = 'second'\n"
        )
        files = {"steps/first/tests/docker-compose.yaml": "services:\n  main: {}\n"}

        report = validate_task(steps_task(tmp_path, toml, files=files))

        assert report.errors == ("the compose verifier of step 'first' can't request GPUs",)

    @pytest.mark.description("A task with too many steps or step artifacts is refused.")
    def test_a_task_with_too_many_steps_or_step_artifacts_is_refused(self, tmp_path: Path) -> None:
        many_steps = MINIMAL + "".join(f"[[steps]]\nname = 's{index}'\n" for index in range(101))
        crowded = (
            "artifacts = ["
            + ", ".join(f"'/task/{index}'" for index in range(15))
            + "]\n"
            + MINIMAL
            + "[[steps]]\nname = 'first'\nartifacts = ["
            + ", ".join(f"'/step/{index}'" for index in range(10))
            + "]\n"
        )

        assert "at most 100 items" in problems(validate_task(write_task(tmp_path / "many", many_steps)))
        assert "would collect more than 20 artifacts" in problems(validate_task(write_task(tmp_path / "full", crowded)))

    @pytest.mark.description("A reward strategy without steps is ignored with a warning.")
    def test_a_reward_strategy_without_steps_is_ignored_with_a_warning(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, "multi_step_reward_strategy = 'mean'\n" + MINIMAL))

        assert report.ok
        assert report.warnings == ("multi_step_reward_strategy only applies to a task with steps, so it is ignored",)

    @pytest.mark.description("Unknown keys in a step and its tables warn.")
    def test_unknown_keys_in_a_step_and_its_tables_warn(self, tmp_path: Path) -> None:
        toml = (
            MINIMAL
            + "[[steps]]\nname = 'first'\nminimum = 1\n[steps.agent]\ntimeout = 5\n[steps.verifier]\nretries = 2\n"
        )

        report = validate_task(steps_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == (
            "steps[0].minimum is not a field this version knows, so it is ignored",
            "steps[0].agent.timeout is not a field this version knows, so it is ignored",
            "steps[0].verifier.retries is not a field this version knows, so it is ignored",
        )

    @pytest.mark.description("A step verifiers settings that only apply to the agent are ignored with a warning.")
    def test_a_step_verifiers_settings_that_only_apply_to_the_agent_are_ignored_with_a_warning(
        self, tmp_path: Path
    ) -> None:
        toml = STEPS_TOML.replace(
            'name = "first"', 'name = "first"\n[steps.verifier.environment]\ndocker_image = "g"\nskills_dir = "/s"\n'
        )

        report = validate_task(steps_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == (
            "[steps.first.verifier.environment].skills_dir only applies to the agent's environment, so it is ignored",
        )


@pytest.mark.description("What every task folder holds.")
class TestRequiredFiles:
    @pytest.mark.description("A missing file is named.")
    @pytest.mark.parametrize("missing", ["task.toml", "instruction.md", "tests/test.sh"])
    def test_a_missing_file_is_named(self, tmp_path: Path, missing: str) -> None:
        report = validate_task(write_task(tmp_path, skip=(missing,)))

        assert f"{missing} is missing" in report.errors

    @pytest.mark.description("Without a config the files a single step task needs are still named.")
    def test_without_a_config_the_files_a_single_step_task_needs_are_still_named(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, skip=("task.toml", "instruction.md", "tests/test.sh")))

        assert report.errors == ("task.toml is missing", "instruction.md is missing", "tests/test.sh is missing")

    @pytest.mark.description("The folder name is the task id.")
    @pytest.mark.parametrize("name", ["-leading", "has space", "x" * 129])
    def test_the_folder_name_is_the_task_id(self, tmp_path: Path, name: str) -> None:
        report = validate_task(write_task(tmp_path, name=name))

        assert "is not a valid task ID" in problems(report)

    @pytest.mark.description("Invalid toml is reported without other checks crashing.")
    def test_invalid_toml_is_reported_without_other_checks_crashing(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, "[environment\ncpus = "))

        assert "task.toml is not valid TOML" in problems(report)

    @pytest.mark.description("A field with the wrong type is named with its table.")
    def test_a_field_with_the_wrong_type_is_named_with_its_table(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, '[environment]\ncpus = "many"'))

        assert "[environment].cpus" in problems(report)

    @pytest.mark.description("Every problem is reported together.")
    def test_every_problem_is_reported_together(self, tmp_path: Path) -> None:
        folder = write_task(tmp_path, '[environment]\ncpus = "many"', skip=("instruction.md", "tests/test.sh"))

        report = validate_task(folder)

        assert len(report.errors) == 3
        assert "instruction.md is missing" in report.errors
        assert "tests/test.sh is missing" in report.errors
        assert "[environment].cpus" in problems(report)


@pytest.mark.description(
    ("A task builds its sandbox from one source: a compose file, else its image, else its Dockerfile.")
)
class TestSandboxSource:
    @pytest.mark.description("No source is an error.")
    def test_no_source_is_an_error(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, skip=("environment/Dockerfile",)))

        assert "needs a sandbox source" in problems(report)

    @pytest.mark.description("A task with an image and a dockerfile uses the image.")
    def test_a_task_with_an_image_and_a_dockerfile_uses_the_image(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, WITH_IMAGE)

        assert validate_task(task).ok
        assert sandbox_kind(task, TaskConfig.parse(WITH_IMAGE)) == SandboxKind.IMAGE

    @pytest.mark.description("The source is picked in order of precedence.")
    @pytest.mark.parametrize(
        ("toml", "files", "kind"),
        [
            (MINIMAL, {}, SandboxKind.DOCKERFILE),
            (WITH_IMAGE, {}, SandboxKind.IMAGE),
            (MINIMAL, {"environment/docker-compose.yaml": ""}, SandboxKind.COMPOSE),
            (WITH_IMAGE, {"environment/docker-compose.yaml": ""}, SandboxKind.COMPOSE),
        ],
        ids=["dockerfile", "image-over-dockerfile", "compose-over-dockerfile", "compose-over-image"],
    )
    def test_the_source_is_picked_in_order_of_precedence(
        self, tmp_path: Path, toml: str, files: dict[str, str], kind: SandboxKind
    ) -> None:
        task = write_task(tmp_path, toml, files=files)

        assert sandbox_kind(task, TaskConfig.parse(toml)) == kind


@pytest.mark.description(
    ("Settings a task can name but this version can't honor, so it refuses the task rather than ignore them.")
)
class TestNotSupportedYet:
    @pytest.mark.description("A setting the run would ignore is refused by name.")
    @pytest.mark.parametrize(
        ("toml", "named"),
        [
            ("[environment.tpu]\ntype = 'v6e'\ntopology = '2x4'", "[environment].tpu"),
            ("[environment]\nos = 'windows'", "[environment].os"),
        ],
    )
    def test_a_setting_the_run_would_ignore_is_refused_by_name(self, tmp_path: Path, toml: str, named: str) -> None:
        report = validate_task(write_task(tmp_path, toml))

        assert not report.ok
        assert named in problems(report)

    @pytest.mark.description("Artifacts and collect hooks on the main service are fine.")
    def test_artifacts_and_collect_hooks_on_the_main_service_are_fine(self, tmp_path: Path) -> None:
        toml = (
            "artifacts = ['/app/out', { source = 'logs', destination = 'run-logs', exclude = ['*.tmp'] }]\n"
            + MINIMAL
            + "\n[[verifier.collect]]\ncommand = 'sync'\nuser = 'root'"
        )

        assert validate_task(write_task(tmp_path, toml)).ok

    @pytest.mark.description("A service other than main needs a compose task.")
    def test_a_service_other_than_main_needs_a_compose_task(self, tmp_path: Path) -> None:
        toml = (
            "artifacts = [{ source = '/var/lib/db', service = 'db' }]\n"
            + MINIMAL
            + "\n[[verifier.collect]]\ncommand = 'dump'\nservice = 'cache'"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert report.errors == (
            "the service 'cache' is named, but only a compose task has services",
            "the service 'db' is named, but only a compose task has services",
        )

    @pytest.mark.description("Literal and whole value template environment values are fine.")
    def test_literal_and_whole_value_template_environment_values_are_fine(self, tmp_path: Path) -> None:
        toml = "[environment]\nenv = { MODE = 'test', KEY = '${API_KEY}', REGION = '${REGION:-us}' }"

        assert validate_task(write_task(tmp_path, toml)).ok

    @pytest.mark.description("Env values that would silently misbehave are refused.")
    @pytest.mark.parametrize(
        ("toml", "problem"),
        [
            ("[verifier]\nenv = { AUTH = 'Bearer ${TOKEN}' }", "[verifier].env AUTH has a partial ${...}"),
            ("[solution]\nenv = { AUTH = 'Bearer ${TOKEN}' }", "[solution].env AUTH has a partial ${...}"),
            ("[environment]\nenv = { BROKEN = '${NAME' }", "[environment].env BROKEN has a partial ${...}"),
            ("[environment]\nenv = { RUN_ID = 'mine' }", "[environment].env can't set RUN_ID"),
            ("[environment]\nenv = { MODEL_GATEWAY_URL = 'https://x' }", "can't set MODEL_GATEWAY_URL"),
            ("[environment]\nenv = { VALKYRIE_AGENT_MODEL = 'x' }", "can't set VALKYRIE_AGENT_MODEL"),
        ],
    )
    def test_env_values_that_would_silently_misbehave_are_refused(
        self, tmp_path: Path, toml: str, problem: str
    ) -> None:
        report = validate_task(write_task(tmp_path, toml))

        assert not report.ok
        assert problem in problems(report)

    @pytest.mark.description(
        "Settings that only apply to the agents environment are ignored on the verifiers with a warning."
    )
    def test_settings_that_only_apply_to_the_agents_environment_are_ignored_on_the_verifiers_with_a_warning(
        self, tmp_path: Path
    ) -> None:
        toml = (
            "[verifier.environment]\ndocker_image = 'grader:1'\nworkdir = '/x'\nskills_dir = '/skills'\n"
            "[verifier.environment.healthcheck]\ncommand = 'true'\n"
            "[[verifier.environment.mcp_servers]]\nname = 'a'\nurl = 'https://x'"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == (
            "[verifier.environment].healthcheck only applies to the agent's environment, so it is ignored",
            "[verifier.environment].mcp_servers only applies to the agent's environment, so it is ignored",
            "[verifier.environment].skills_dir only applies to the agent's environment, so it is ignored",
        )

    @pytest.mark.description("Users a healthcheck mcp servers and skills are fine.")
    def test_users_a_healthcheck_mcp_servers_and_skills_are_fine(self, tmp_path: Path) -> None:
        toml = (
            "[agent]\nuser = 'agent'\n[verifier]\nuser = 1000\n[environment]\nskills_dir = '/skills'\n"
            "[environment.healthcheck]\ncommand = 'true'\n[[environment.mcp_servers]]\nname = 'a'\nurl = 'https://x'"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == ()

    @pytest.mark.description("A header variable nothing sets gets one note per header and the others get none.")
    def test_a_header_variable_nothing_sets_gets_one_note_per_header_and_the_others_get_none(
        self, tmp_path: Path
    ) -> None:
        toml = (
            "[environment]\nenv = { TOKEN = '${MCP_TOKEN}' }\n[[environment.mcp_servers]]\nname = 'a'\n"
            "url = 'https://x'\nheaders = { Authorization = 'Bearer ${TOKEN}', X-Org = '${ORG}-${ORG}-${TEAM}', "
            "X-Region = '${REGION:-eu}', X-Gateway = '${MODEL_GATEWAY_URL}' }"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == (
            "The MCP server 'a' header 'X-Org' uses ${ORG}, ${TEAM}, which [environment].env doesn't set; "
            "the agent resolves it from its own environment",
        )

    @pytest.mark.description("An unknown key inside an mcp server or a healthcheck is a warning.")
    def test_an_unknown_key_inside_an_mcp_server_or_a_healthcheck_is_a_warning(self, tmp_path: Path) -> None:
        toml = (
            "[environment]\n[[environment.mcp_servers]]\nname = 'a'\nurl = 'https://x'\nheader = { X = 'y' }\n"
            "[environment.healthcheck]\ncommand = 'true'\nretry = 2"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings[:2] == (
            "[environment].healthcheck.retry is not a field this version knows, so it is ignored",
            "[environment].mcp_servers[0].header is not a field this version knows, so it is ignored",
        )

    @pytest.mark.description("An mcp server declared twice is a warning that the last one is used.")
    def test_an_mcp_server_declared_twice_is_a_warning_that_the_last_one_is_used(self, tmp_path: Path) -> None:
        toml = (
            "[environment]\n"
            "[[environment.mcp_servers]]\nname = 'b'\nurl = 'https://x'\n"
            "[[environment.mcp_servers]]\nname = 'a'\nurl = 'https://x'\n"
            "[[environment.mcp_servers]]\nname = 'b'\nurl = 'https://y'\n"
            "[[environment.mcp_servers]]\nname = 'b'\nurl = 'https://z'\n"
            "[verifier.environment]\n"
            "[[verifier.environment.mcp_servers]]\nname = 'c'\nurl = 'https://x'\n"
            "[[verifier.environment.mcp_servers]]\nname = 'c'\nurl = 'https://y'"
        )

        report = validate_task(write_task(tmp_path, toml))

        assert [warning for warning in report.warnings if "more than once" in warning] == [
            "[environment].mcp_servers declares 'b' more than once, so the last one is used",
            "[verifier.environment].mcp_servers declares 'c' more than once, so the last one is used",
        ]

    @pytest.mark.description("A compose task is valid with a separate verifier but may not ask for gpus.")
    def test_a_compose_task_is_valid_with_a_separate_verifier_but_may_not_ask_for_gpus(self, tmp_path: Path) -> None:
        files = {"environment/docker-compose.yaml": "services:\n  main: {}\n"}

        plain = validate_task(write_task(tmp_path / "plain", files=files))
        gpus = validate_task(write_task(tmp_path / "gpus", "[environment]\ngpus = 1", files=files))
        separate = validate_task(
            write_task(tmp_path / "separate", "[verifier]\nenvironment_mode = 'separate'", files=files)
        )

        assert plain.ok and separate.ok
        assert gpus.errors == ("a compose task can't request GPUs",)

    @pytest.mark.description("A separate verifier that runs compose may not ask for gpus.")
    @pytest.mark.parametrize(
        ("verifier", "files", "refused"),
        [
            ("[verifier]\nenvironment_mode = 'separate'", {"tests/docker-compose.yaml": "x"}, True),
            (
                "[verifier]\nenvironment_mode = 'separate'",
                {"tests/docker-compose.yaml": "x", "tests/Dockerfile": "x"},
                True,
            ),
            ("[verifier.environment]\ndocker_image = 'grader:1'", {"tests/docker-compose.yaml": "x"}, False),
            (
                "[verifier.environment]\ndocker_image = 'grader:1'\ngpus = 1",
                {"tests/docker-compose.yaml": "x"},
                True,
            ),
            ("", {"tests/docker-compose.yaml": "x"}, False),
            ("[verifier]\nenvironment_mode = 'separate'", {"tests/Dockerfile": "FROM x"}, False),
            ("[verifier]\nenvironment_mode = 'separate'", {}, False),
        ],
    )
    def test_a_separate_verifier_that_runs_compose_may_not_ask_for_gpus(
        self, tmp_path: Path, verifier: str, files: dict[str, str], refused: bool
    ) -> None:
        toml = f"[environment]\ndocker_image = 'x'\ngpus = 1\n{verifier}"

        report = validate_task(write_task(tmp_path, toml, files=files))

        assert ("a compose verifier can't request GPUs" in report.errors) is refused

    @pytest.mark.description("A copy of a compose agent environment may not ask for gpus either.")
    def test_a_copy_of_a_compose_agent_environment_may_not_ask_for_gpus_either(self, tmp_path: Path) -> None:
        files = {"environment/docker-compose.yaml": "services:\n  main: {}\n"}
        toml = "[verifier.environment]\ngpus = 1"

        report = validate_task(write_task(tmp_path, toml, files=files))

        assert report.errors == ("a compose verifier can't request GPUs",)


@pytest.mark.description("Keys nothing reads are ignored with a warning, since a typo would otherwise do nothing.")
class TestWarnings:
    @pytest.mark.description("Unknown keys in the stage tables warn.")
    def test_unknown_keys_in_the_stage_tables_warn(self, tmp_path: Path) -> None:
        toml = "[environment]\ncpuss = 2\n[agent]\ntimeout = 5\n[verifier]\nretries = 3\n[solution]\nenvs = {}\n"

        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == (
            "[environment].cpuss is not a field this version knows, so it is ignored",
            "[agent].timeout is not a field this version knows, so it is ignored",
            "[verifier].retries is not a field this version knows, so it is ignored",
            "[solution].envs is not a field this version knows, so it is ignored",
        )

    @pytest.mark.description("A setting refused as not supported is not also called unknown.")
    @pytest.mark.parametrize(
        "toml",
        [
            "[environment.tpu]\ntype = 'v6e'\ntopology = '2x4'",
        ],
    )
    def test_a_setting_refused_as_not_supported_is_not_also_called_unknown(self, tmp_path: Path, toml: str) -> None:
        report = validate_task(write_task(tmp_path, toml))

        assert not report.ok
        assert report.warnings == ()

    @pytest.mark.description("Older field names and unknown top level tables do not warn.")
    @pytest.mark.parametrize(
        "toml",
        [
            "[environment]\nmemory = '2G'\nstorage = '1G'\nallow_internet = true",
            "[solution]\nenv = {}\n[extras]\nnote = 1",
        ],
        ids=["older-field-names", "unknown-top-level-tables"],
    )
    def test_accepted_settings_do_not_warn(self, tmp_path: Path, toml: str) -> None:
        report = validate_task(write_task(tmp_path, toml))

        assert report.ok
        assert report.warnings == ()

    @pytest.mark.description("A verifier image that hides the tests dockerfile is a warning.")
    @pytest.mark.parametrize(
        ("verifier", "files", "warned"),
        [
            ("[verifier.environment]\ndocker_image = 'grader:1'", {"tests/Dockerfile": "FROM x"}, True),
            (
                "[verifier.environment]\ndocker_image = 'grader:1'",
                {"tests/Dockerfile": "FROM x", "tests/docker-compose.yaml": "x"},
                True,
            ),
            ("[verifier.environment]\ndocker_image = 'grader:1'", {"tests/docker-compose.yaml": "x"}, False),
            ("[verifier.environment]\ndocker_image = 'grader:1'", {}, False),
            ("[verifier.environment]\nmemory_mb = 2048", {"tests/Dockerfile": "FROM x"}, False),
            ("", {"tests/Dockerfile": "FROM x"}, False),
        ],
        ids=["dockerfile", "dockerfile and compose", "compose", "image alone", "no image", "no environment"],
    )
    def test_a_verifier_image_that_hides_the_tests_dockerfile_is_a_warning(
        self, tmp_path: Path, verifier: str, files: dict[str, str], warned: bool
    ) -> None:
        report = validate_task(write_task(tmp_path, f"[verifier]\n{verifier}", files=files))

        assert report.warnings == (
            ("tests/Dockerfile is ignored because [verifier.environment].docker_image takes precedence",)
            if warned
            else ()
        )


@pytest.mark.description("The schema version is kept as written, only a newer major version is worth a warning.")
class TestSchemaVersion:
    @pytest.mark.description("Versions this one reads do not warn.")
    @pytest.mark.parametrize("version", ["0.0", "1.1", "1.4"])
    def test_versions_this_one_reads_do_not_warn(self, tmp_path: Path, version: str) -> None:
        report = validate_task(write_task(tmp_path, f'schema_version = "{version}"'))

        assert report.ok
        assert report.warnings == ()

    @pytest.mark.description("A newer major version warns.")
    def test_a_newer_major_version_warns(self, tmp_path: Path) -> None:
        report = validate_task(write_task(tmp_path, 'schema_version = "2.0"'))

        assert report.ok
        assert report.warnings == ("schema_version '2.0' is newer than this version reads, so settings may be ignored",)
