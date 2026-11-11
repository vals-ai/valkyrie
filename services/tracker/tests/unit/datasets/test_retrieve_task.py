import json
import shlex
from pathlib import Path

import pytest
from benchmark_service import ComposeSource, DockerfileSource, ImageSource, Resources
from benchmark_service.sandbox.egress import resolve_allowed_addresses

from tracker.services.datasets.config import TaskConfig
from tracker.services.datasets.retrieve_task import (
    DEFAULT_CWD,
    TaskFolderResponse,
    UnsupportedTaskError,
    dockerfile_workdir,
    retrieve_task_response,
)

IMAGE_TASK = """
schema_version = "1.4"

[agent]
timeout_sec = 1800

[verifier]
timeout_sec = 600

[environment]
docker_image = "ubuntu:24.04"
cpus = 2
memory_mb = 4096
storage_mb = 20480
workdir = "/app"
"""


def mapped(tmp_path: Path, toml: str = IMAGE_TASK, *, dockerfile: str | None = None) -> TaskFolderResponse:
    task = tmp_path / "hello-file"
    (task / "environment").mkdir(parents=True)
    if dockerfile is not None:
        (task / "environment" / "Dockerfile").write_text(dockerfile)
    return retrieve_task_response(task, TaskConfig.parse(toml))


@pytest.mark.description("The fields every task gets.")
class TestSandboxContract:
    @pytest.mark.description("An image task maps every field.")
    def test_an_image_task_maps_every_field(self, tmp_path: Path) -> None:
        response = mapped(tmp_path)

        assert response.source == ImageSource(image="ubuntu:24.04")
        assert response.cwd == "/app"
        assert response.problem_path == "/app/instruction.md"
        assert response.agent_timeout == 1800
        assert response.resources == Resources(vcpu=2, memory=4, disk=20)

    @pytest.mark.description("A task without settings gets the defaults.")
    def test_a_task_without_settings_gets_the_defaults(self, tmp_path: Path) -> None:
        response = mapped(tmp_path, '[environment]\ndocker_image = "ubuntu:24.04"')

        assert response.cwd == DEFAULT_CWD
        assert response.problem_path == "/workspace/instruction.md"
        assert response.agent_timeout is None
        assert response.resources == Resources(vcpu=1, memory=1, disk=15)

    @pytest.mark.description("Runtime settings reach the agent through its env user and healthcheck.")
    def test_runtime_settings_reach_the_agent_through_its_env_user_and_healthcheck(self, tmp_path: Path) -> None:
        toml = (
            "[agent]\nuser = 1000\n[environment]\ndocker_image = 'x'\nskills_dir = '/skills'\n"
            "env = { MODE = 'fast' }\n[environment.healthcheck]\ncommand = 'ready'\n"
            "[[environment.mcp_servers]]\nname = 'web'\nurl = 'https://x'"
        )

        response = mapped(tmp_path, toml)

        assert response.env == {"MODE": "fast"}
        assert response.mcp_servers == {"web": {"type": "sse", "url": "https://x"}}
        assert response.skills_dir == "/skills"
        assert response.agent_user == "1000"
        assert response.healthcheck is not None and response.healthcheck.command == "ready"

    @pytest.mark.description("Mcp servers take the mcp config shape with header references unresolved.")
    def test_mcp_servers_take_the_mcp_config_shape_with_header_references_unresolved(self, tmp_path: Path) -> None:
        toml = (
            "[environment]\ndocker_image = 'x'\n"
            "[[environment.mcp_servers]]\nname = 'web'\ntransport = 'streamable-http'\nurl = 'https://x'\n"
            "headers = { Authorization = 'Bearer ${TOKEN}' }\n"
            "[[environment.mcp_servers]]\nname = 'files'\ntransport = 'stdio'\ncommand = 'fs-mcp'\nargs = ['/data']"
        )

        assert mapped(tmp_path, toml).mcp_servers == {
            "web": {"type": "http", "url": "https://x", "headers": {"Authorization": "Bearer ${TOKEN}"}},
            "files": {"type": "stdio", "command": "fs-mcp", "args": ["/data"]},
        }

    @pytest.mark.description("A gpu request names its acceptable types in order.")
    def test_a_gpu_request_names_its_acceptable_types_in_order(self, tmp_path: Path) -> None:
        toml = '[environment]\ndocker_image = "x"\ngpus = 2\ngpu_types = ["H100", "A100"]'

        assert mapped(tmp_path, toml).resources == Resources(
            vcpu=1, memory=1, disk=15, gpu=2, gpu_type=["H100", "A100"]
        )

    @pytest.mark.description("A gpu type without a gpu count asks for no gpu.")
    def test_a_gpu_type_without_a_gpu_count_asks_for_no_gpu(self, tmp_path: Path) -> None:
        toml = '[environment]\ndocker_image = "x"\ngpu_types = ["H100"]'

        assert mapped(tmp_path, toml).resources == Resources(vcpu=1, memory=1, disk=15)

    @pytest.mark.description("Cpus pass through and sizes round up to whole gigabytes without a cap or floor.")
    @pytest.mark.parametrize(
        ("sizes", "resources"),
        [
            ("cpus = 0.5\nmemory_mb = 512\nstorage_mb = 2048", Resources(vcpu=0.5, memory=1, disk=2)),
            ("cpus = 2.5\nmemory_mb = 65537\nstorage_mb = 204800", Resources(vcpu=2.5, memory=65, disk=200)),
        ],
        ids=["below a whole unit", "beyond any former cap"],
    )
    def test_cpus_pass_through_and_sizes_round_up_to_whole_gigabytes_without_a_cap_or_floor(
        self, tmp_path: Path, sizes: str, resources: Resources
    ) -> None:
        assert mapped(tmp_path, f'[environment]\ndocker_image = "x"\n{sizes}').resources == resources


@pytest.mark.description("Where the agent starts and where the instruction lands.")
class TestWorkingDirectory:
    @pytest.mark.description("The declared workdir wins over the dockerfile, whose workdir is the fallback.")
    def test_the_declared_workdir_wins_and_the_dockerfile_is_the_fallback(self, tmp_path: Path) -> None:
        declared = mapped(tmp_path / "declared", IMAGE_TASK, dockerfile="FROM x\nWORKDIR /other\n")
        fallback = mapped(
            tmp_path / "fallback", '[environment]\ndocker_image = "x"', dockerfile="FROM x\nWORKDIR /testbed\n"
        )

        assert declared.cwd == "/app"
        assert (fallback.cwd, fallback.problem_path) == ("/testbed", "/testbed/instruction.md")
        assert dockerfile_workdir(tmp_path / "missing" / "Dockerfile") is None

    @pytest.mark.description("The workdir is that of the last build stage.")
    @pytest.mark.parametrize(
        ("dockerfile", "expected"),
        [
            ("FROM x\nworkdir /lower\n", "/lower"),
            ("FROM x\nWORKDIR /a\nWORKDIR b\n", "/a/b"),
            ("FROM x\nWORKDIR b\n", "/b"),
            ("FROM build\nWORKDIR /build\nFROM runtime\nRUN true\n", None),
            ("FROM build\nWORKDIR /build\nFROM runtime\nWORKDIR /app\n", "/app"),
            ("# WORKDIR /commented\nFROM x\n", None),
            ("FROM x\nWORKDIR\n", None),
        ],
    )
    def test_the_workdir_is_that_of_the_last_build_stage(
        self, tmp_path: Path, dockerfile: str, expected: str | None
    ) -> None:
        path = tmp_path / "Dockerfile"
        path.write_text(dockerfile)

        assert dockerfile_workdir(path) == expected


@pytest.mark.description("Each stage's egress policy, falling back to the environment's.")
class TestNetworkRules:
    @pytest.mark.description("Public everywhere by default.")
    def test_public_everywhere_by_default(self, tmp_path: Path) -> None:
        egress = mapped(tmp_path).egress

        assert (egress.setup_task, egress.run, egress.evaluation) == ("*", "*", "*")

    @pytest.mark.description("Stages inherit the environment unless they set their own.")
    def test_stages_inherit_the_environment_unless_they_set_their_own(self, tmp_path: Path) -> None:
        toml = (
            '[environment]\ndocker_image = "x"\nnetwork_mode = "allowlist"\nallowed_hosts = ["pypi.org"]\n'
            '[verifier]\nnetwork_mode = "no-network"\n'
        )

        egress = mapped(tmp_path, toml).egress

        assert egress.setup_task == ["pypi.org"]
        assert egress.run == ["pypi.org"]
        assert egress.evaluation == []

    @pytest.mark.description("An ipv4 range reaches the provider as a range.")
    def test_an_ipv4_range_reaches_the_provider_as_a_range(self, tmp_path: Path) -> None:
        toml = '[environment]\ndocker_image = "x"\n[agent]\nnetwork_mode = "allowlist"\nallowed_hosts = ["1.1.1.0/24", "10.0.0.1"]'

        assert resolve_allowed_addresses(mapped(tmp_path, toml).egress.run) == (["1.1.1.0/24", "10.0.0.1/32"], [])

    @pytest.mark.description("Each stage can have its own allowlist.")
    def test_each_stage_can_have_its_own_allowlist(self, tmp_path: Path) -> None:
        toml = (
            '[agent]\nnetwork_mode = "allowlist"\nallowed_hosts = ["pypi.org", "files.pythonhosted.org"]\n'
            '[verifier]\nnetwork_mode = "allowlist"\nallowed_hosts = ["api.openai.com"]\n'
            '[environment]\ndocker_image = "x"\n'
        )

        egress = mapped(tmp_path, toml).egress

        assert (egress.setup_task, egress.run, egress.evaluation) == (
            "*",
            ["pypi.org", "files.pythonhosted.org"],
            ["api.openai.com"],
        )

    @pytest.mark.description("No network is the empty policy, and an allowlist without hosts reaches nothing.")
    def test_no_network_and_an_empty_allowlist_reach_nothing(self, tmp_path: Path) -> None:
        no_network = mapped(tmp_path / "none", '[environment]\ndocker_image = "x"\nnetwork_mode = "no-network"').egress
        empty_allowlist = mapped(
            tmp_path / "empty", '[agent]\nnetwork_mode = "allowlist"\n[environment]\ndocker_image = "x"'
        )

        assert (no_network.setup_task, no_network.run, no_network.evaluation) == ([], [], [])
        assert empty_allowlist.egress.run == []


@pytest.mark.description("A task with no image builds its sandbox from its Dockerfile.")
class TestDockerfileTasks:
    @pytest.mark.description("A task without an image is built from its dockerfile.")
    def test_a_task_without_an_image_is_built_from_its_dockerfile(self, tmp_path: Path) -> None:
        response = mapped(tmp_path, "[environment]\ncpus = 1", dockerfile="FROM ubuntu:24.04\nWORKDIR /testbed\n")

        assert response.source == DockerfileSource(
            dockerfile=str(tmp_path / "hello-file" / "environment" / "Dockerfile"), build_timeout=600
        )
        assert response.cwd == "/testbed"

    @pytest.mark.description("The build timeout comes from the task.")
    @pytest.mark.parametrize(("setting", "expected"), [("", 600), ("build_timeout_sec = 1800\n", 1800)])
    def test_the_build_timeout_comes_from_the_task(self, tmp_path: Path, setting: str, expected: float) -> None:
        response = mapped(tmp_path, f"[environment]\n{setting}", dockerfile="FROM ubuntu:24.04\n")

        assert isinstance(response.source, DockerfileSource)
        assert response.source.build_timeout == expected

    @pytest.mark.description("A symlinked dockerfile builds from the folder the link is in.")
    def test_a_symlinked_dockerfile_builds_from_the_folder_the_link_is_in(self, tmp_path: Path) -> None:
        shared = tmp_path / "shared"
        shared.mkdir()
        (shared / "Dockerfile").write_text("FROM ubuntu:24.04\n")
        task = tmp_path / "hello-file"
        (task / "environment").mkdir(parents=True)
        (task / "environment" / "Dockerfile").symlink_to(shared / "Dockerfile")

        response = retrieve_task_response(task, TaskConfig.parse("[environment]\ncpus = 1"))

        assert response.source == DockerfileSource(
            dockerfile=str(task / "environment" / "Dockerfile"), build_timeout=600
        )


@pytest.mark.description("Tasks whose source this version can't run.")
class TestUnmappableTasks:
    @pytest.mark.description("A task without an image or a dockerfile is refused.")
    def test_a_task_without_an_image_or_a_dockerfile_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(
            UnsupportedTaskError, match="no docker_image, environment/Dockerfile or environment/docker-compose.yaml"
        ):
            mapped(tmp_path, "[environment]\ncpus = 1")


@pytest.mark.description(
    ("A compose task runs in a Docker-in-Docker sandbox with its services layered over the main one.")
)
class TestComposeTasks:
    def compose_task(self, tmp_path: Path, toml: str = IMAGE_TASK) -> TaskFolderResponse:
        task = tmp_path / "db-task"
        (task / "environment").mkdir(parents=True)
        (task / "environment" / "docker-compose.yaml").write_text("services:\n  main: {}\n")
        return retrieve_task_response(task, TaskConfig.parse(toml))

    @pytest.mark.description("It maps to a compose source over a docker in docker image.")
    def test_it_maps_to_a_compose_source_over_a_docker_in_docker_image(self, tmp_path: Path) -> None:
        response = self.compose_task(tmp_path)

        assert isinstance(response.source, ComposeSource)
        assert response.source.outer == ImageSource(image="docker:28.3.3-dind")
        assert response.source.service == "main"
        assert response.egress.setup_task == "*"

    @pytest.mark.description("The command layers the overlays before the tasks own file and names the prebuilt image.")
    def test_the_command_layers_the_overlays_before_the_tasks_own_file_and_names_the_prebuilt_image(
        self, tmp_path: Path
    ) -> None:
        response = self.compose_task(tmp_path)
        assert isinstance(response.source, ComposeSource)

        command = shlex.split(response.source.compose_command)

        variables = [word for word in command if "=" in word and not word.startswith("/")][:12]
        assert variables == [
            "MAIN_IMAGE_NAME=valkyrie__db-task",
            "CONTEXT_DIR=/valkyrie/environment",
            "PREBUILT_IMAGE_NAME=ubuntu:24.04",
            "CPUS=2",
            "MEMORY=4096M",
            "HOST_AGENT_LOGS_PATH=/valkyrie/logs/agent",
            "ENV_AGENT_LOGS_PATH=/logs/agent",
            "HOST_VERIFIER_LOGS_PATH=/valkyrie/logs/verifier",
            "ENV_VERIFIER_LOGS_PATH=/logs/verifier",
            "HOST_ARTIFACTS_PATH=/valkyrie/logs/artifacts",
            "ENV_ARTIFACTS_PATH=/logs/artifacts",
        ]
        assert command[command.index("docker") : command.index("docker") + 4] == [
            "docker",
            "--log-level",
            "error",
            "compose",
        ]
        assert command[command.index("--env-file") + 1] == "/valkyrie/compose/task.env"
        files = [command[index + 1] for index, word in enumerate(command) if word == "-f"]
        assert files == [
            "/valkyrie/compose/resources.json",
            "/valkyrie/compose/main.yaml",
            "/valkyrie/compose/bundle.json",
            "/valkyrie/compose/main-env.json",
            "/valkyrie/environment/docker-compose.yaml",
        ]

    @pytest.mark.description("The resource variables are the settings exactly.")
    @pytest.mark.parametrize(
        ("limits", "variables"),
        [("", []), ("cpus = 1.5\nmemory_mb = 1536", ["CPUS=1.5", "MEMORY=1536M"])],
    )
    def test_the_resource_variables_are_the_settings_exactly(
        self, tmp_path: Path, limits: str, variables: list[str]
    ) -> None:
        response = self.compose_task(tmp_path, f"[environment]\ndocker_image = 'ubuntu:24.04'\n{limits}")
        assert isinstance(response.source, ComposeSource)

        command = shlex.split(response.source.compose_command)

        assert [word for word in command if word.startswith(("CPUS=", "MEMORY="))] == variables

    @pytest.mark.description("The main service mounts the three log folders from the host.")
    def test_the_main_service_mounts_the_three_log_folders_from_the_host(self, tmp_path: Path) -> None:
        response = self.compose_task(tmp_path)
        assert response.compose is not None

        mounts = json.loads(response.compose.overlays["bundle.json"])["services"]["main"]["volumes"]

        assert [(mount["type"], mount["source"], mount["target"]) for mount in mounts[1:]] == [
            ("bind", "/valkyrie/logs/agent", "/logs/agent"),
            ("bind", "/valkyrie/logs/verifier", "/logs/verifier"),
            ("bind", "/valkyrie/logs/artifacts", "/logs/artifacts"),
        ]

    @pytest.mark.description("A task without an image builds main and a task without network isolates it.")
    def test_a_task_without_an_image_builds_main_and_a_task_without_network_isolates_it(self, tmp_path: Path) -> None:
        toml = "[environment]\ncpus = 2\nmemory_mb = 2048\nnetwork_mode = 'no-network'"

        response = self.compose_task(tmp_path, toml)

        assert response.compose is not None
        assert "build:" in response.compose.overlays["main.yaml"]
        assert "network_mode: none" in response.compose.overlays["no-network.yaml"]
        assert json.loads(response.compose.overlays["resources.json"]) == {
            "services": {
                "main": {
                    "deploy": {
                        "resources": {
                            "limits": {"cpus": "2", "memory": "2048M"},
                            "reservations": {"cpus": "2", "memory": "2048M"},
                        }
                    }
                }
            }
        }
        assert response.compose.environment_dir == str((tmp_path / "db-task" / "environment").absolute())
