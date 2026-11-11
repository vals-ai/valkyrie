import asyncio
import json
from pathlib import Path
from typing import cast

import pytest
from benchmark_service import ComposeSource, ImageSource, Sandbox

from tests.unit.datasets.support import OuterSandbox
from tracker.services.datasets import compose
from tracker.services.datasets.compose import (
    DOCKERD_RUNNING,
    START_DOCKERD,
    ComposeRuntime,
    ComposeRuntimeError,
    start_compose_runtime,
)

SOURCE = ComposeSource(outer=ImageSource(image="docker:dind"), compose_command="docker compose -p demo")


@pytest.fixture
def runtime(tmp_path: Path) -> ComposeRuntime:
    environment = tmp_path / "environment"
    (environment / "app").mkdir(parents=True)
    (environment / "docker-compose.yaml").write_text("services:\n  main: {}\n")
    (environment / "app" / "Dockerfile").write_text("FROM ubuntu\n")
    return ComposeRuntime(
        environment_dir=str(environment),
        overlays={"resources.json": "{}", "main.yaml": "services: {}"},
        build_timeout_sec=900,
    )


async def start(outer: OuterSandbox, runtime: ComposeRuntime, env: dict[str, str] | None = None) -> None:
    await start_compose_runtime(cast(Sandbox, outer), SOURCE, runtime, env or {})


@pytest.mark.description("The daemon starts, the files arrive, and the services build and come up.")
class TestStartComposeRuntime:
    @pytest.mark.description("The daemon files and services are set up in order.")
    async def test_the_daemon_files_and_services_are_set_up_in_order(self, runtime: ComposeRuntime) -> None:
        outer = OuterSandbox()

        await start(outer, runtime, {"MODE": "fast"})

        assert outer.commands[:3] == [DOCKERD_RUNNING, START_DOCKERD, "docker info"]
        assert outer.commands[-3:] == [
            "docker compose -p demo build",
            "docker compose -p demo up --detach --wait",
            "docker compose -p demo exec -T main true",
        ]
        assert (
            "mkdir -p /valkyrie/compose /bundle /valkyrie/logs/agent /valkyrie/logs/verifier /valkyrie/logs/artifacts"
            in outer.commands
        )
        assert "chmod 777 /valkyrie/logs/agent /valkyrie/logs/verifier /valkyrie/logs/artifacts" in outer.commands
        assert set(outer.files) == {
            "/valkyrie/compose/resources.json",
            "/valkyrie/compose/main.yaml",
            "/valkyrie/compose/main-env.json",
            "/valkyrie/compose/task.env",
            "/valkyrie/environment/docker-compose.yaml",
            "/valkyrie/environment/app/Dockerfile",
        }
        assert outer.files["/valkyrie/compose/task.env"] == b'MODE="fast"\n'

        assert json.loads(outer.files["/valkyrie/compose/main-env.json"]) == {
            "services": {"main": {"environment": {"MODE": "fast"}}}
        }
        assert outer.timeouts_by_command["docker compose -p demo build"] == 900

    @pytest.mark.description("A dollar in a value reaches the service as itself.")
    async def test_a_dollar_in_a_value_reaches_the_service_as_itself(self, runtime: ComposeRuntime) -> None:
        outer = OuterSandbox()

        await start(outer, runtime, {"PASSWORD": "pa$word", "TEMPLATE": "${HOME}"})

        environment = json.loads(outer.files["/valkyrie/compose/main-env.json"])["services"]["main"]["environment"]
        assert environment == {"PASSWORD": "pa$$word", "TEMPLATE": "$${HOME}"}
        assert outer.files["/valkyrie/compose/task.env"] == b'PASSWORD="pa\\$word"\nTEMPLATE="\\${HOME}"\n'

    @pytest.mark.description("A failing step reports its command and a bounded output.")
    async def test_a_failing_step_reports_its_command_and_a_bounded_output(self, runtime: ComposeRuntime) -> None:
        outer = OuterSandbox(failing="build", output="x" * 10_000 + "the real error")

        with pytest.raises(ComposeRuntimeError, match="Command failed: docker compose -p demo build\n") as error:
            await start(outer, runtime)

        assert len(str(error.value)) < 2_200
        assert str(error.value).endswith("the real error")

    @pytest.mark.description("A failure that may go away is marked transient so the task is given a new sandbox.")
    @pytest.mark.parametrize(
        ("output", "transient"),
        [
            ("toomanyrequests: You have reached your pull rate limit", True),
            ("net/http: TLS handshake timeout", True),
            ("failed to solve: dockerfile parse error", False),
        ],
    )
    async def test_a_failure_that_may_go_away_is_marked_transient_so_the_task_is_given_a_new_sandbox(
        self, runtime: ComposeRuntime, output: str, transient: bool
    ) -> None:
        with pytest.raises(ComposeRuntimeError) as error:
            await start(OuterSandbox(failing="build", output=output), runtime)

        assert error.value.transient is transient

    @pytest.mark.description("The daemon the image already started is left alone.")
    async def test_the_daemon_the_image_already_started_is_left_alone(self, runtime: ComposeRuntime) -> None:
        outer = OuterSandbox(daemon_running=True)

        await start(outer, runtime)

        assert outer.commands[:2] == [DOCKERD_RUNNING, "docker info"]
        assert START_DOCKERD not in outer.commands

    @pytest.mark.description("A daemon that never starts is reported.")
    async def test_a_daemon_that_never_starts_is_reported(
        self, runtime: ComposeRuntime, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def no_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr(asyncio, "sleep", no_sleep)
        outer = OuterSandbox(docker_ready=False, failing="tail -c", output="failed to start daemon: cgroups")

        with pytest.raises(
            ComposeRuntimeError, match="Docker daemon did not become ready inside the compose sandbox"
        ) as error:
            await start(outer, runtime)

        assert error.value.transient is True
        assert "failed to start daemon: cgroups" in str(error.value)
        assert outer.commands.count("docker info") == compose.DOCKER_READY_ATTEMPTS
