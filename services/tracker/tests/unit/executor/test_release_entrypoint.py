import builtins
import json
import os
import subprocess
import sys

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError
from pytest import MonkeyPatch
from sqlmodel import Session

from tracker.executor import release_entrypoint
from tracker.database import session as tracker_session
from tracker.database.models import ExecutorAdmission, ExecutorRelease
from tracker.domain import ExecutorReleaseStatus
from tracker.executor.release_control import get_executor_admission


@pytest.mark.description(
    "The release entrypoint starts cleanly when the database config is only available after the sealed environment loads."
)
def test_release_entrypoint_loads_database_config_after_sealed_environment() -> None:
    environment = os.environ.copy()
    for name in ("DATABASE_URL", "DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"):
        environment.pop(name, None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os

import tracker.executor.release_entrypoint

os.environ.update(
    DB_USERNAME="sealed-user",
    DB_PASSWORD="sealed-password",
    DB_HOST="database.internal",
    DB_PORT="5433",
    DB_NAME="sealed-database",
)
from tracker.database.session import engine

assert engine.url.username == "sealed-user"
assert engine.url.password == "sealed-password"
assert engine.url.host == "database.internal"
assert engine.url.port == 5433
assert engine.url.database == "sealed-database"
""",
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.description("The release database engine keeps no idle connections.")
def test_release_engine_does_not_retain_idle_connections(tmp_path: os.PathLike[str]) -> None:
    environment = {**os.environ, "DATABASE_URL": f"sqlite:///{tmp_path}/release.sqlite3"}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from sqlalchemy import event
from sqlmodel import Session, select
from tracker.database.session import engine

connections = []
event.listen(engine, "connect", lambda connection, _record: connections.append(connection))
for _ in range(2):
    with Session(engine) as session:
        assert session.exec(select(1)).one() == 1
assert len(connections) == 2, "the engine retained an idle database connection"
""",
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr


class FakeSecretsManager:
    def __init__(self) -> None:
        self.secret_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, object]:
        self.secret_ids.append(SecretId)
        return {
            "SecretString": json.dumps(
                {
                    "username": "tracker",
                    "password": "secret",
                    "host": "ignored.invalid",
                    "port": 9999,
                }
            )
        }


IMAGE_REPOSITORY = "123456789012.dkr.ecr.us-east-1.amazonaws.com/valkyrie-executor"
REGISTERED_TASK_DEFINITION = "arn:aws:ecs:us-east-1:123456789012:task-definition/ValkyrieExecutorRunner:8"


class FakeEcrClient:
    def __init__(self, *, published: bool = True) -> None:
        self.published = published
        self.describe_calls: list[dict[str, object]] = []

    def describe_images(self, **kwargs: object) -> dict[str, object]:
        self.describe_calls.append(kwargs)
        if not self.published:
            raise ClientError({"Error": {"Code": "ImageNotFoundException"}}, "DescribeImages")
        return {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]}


class FakeEcsRegistry:
    def __init__(self) -> None:
        self.registered: list[dict[str, object]] = []

    def describe_task_definition(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"taskDefinition": "ValkyrieExecutorRunner"}
        return {
            "taskDefinition": {
                "taskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/ValkyrieExecutorRunner:7",
                "family": "ValkyrieExecutorRunner",
                "revision": 7,
                "status": "ACTIVE",
                "cpu": "1024",
                "memory": "4096",
                "runtimePlatform": {"cpuArchitecture": "ARM64"},
                "taskRoleArn": "arn:aws:iam::123456789012:role/runner-task",
                "executionRoleArn": "arn:aws:iam::123456789012:role/runner-execution",
                "requiresAttributes": [{"name": "ecs.capability.execution-role-ecr-pull"}],
                "compatibilities": ["FARGATE"],
                "registeredAt": "2026-01-01T00:00:00Z",
                "containerDefinitions": [
                    {"name": "ExecutorRunnerContainer", "image": "tracker:deployed", "stopTimeout": 120}
                ],
            }
        }

    def register_task_definition(self, **kwargs: object) -> dict[str, object]:
        self.registered.append(kwargs)
        return {"taskDefinition": {"taskDefinitionArn": REGISTERED_TASK_DEFINITION}}


class FakeEcsClient:
    def __init__(self) -> None:
        self.service_updates: list[dict[str, object]] = []
        self.waited_for: list[dict[str, object]] = []
        self.stopped: list[str] = []
        self.already_stopping: list[str] = []
        self.list_calls: list[dict[str, object]] = []
        self.tracker_polls = 0
        self.tracker_running_counts: list[int] = []
        self.running = ["arn:aws:ecs:us-east-1:123456789012:task/runner-1"]
        self.pending = ["arn:aws:ecs:us-east-1:123456789012:task/runner-2"]

    def get_waiter(self, name: str) -> "FakeEcsClient":
        assert name == "services_stable"
        return self

    def wait(self, **kwargs: object) -> None:
        self.waited_for.append(kwargs)

    def update_service(self, **kwargs: object) -> dict[str, object]:
        self.service_updates.append(kwargs)
        return {}

    def list_tasks(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["family"] == "ValkyrieExecutorRunner"
        self.list_calls.append(kwargs)
        status = kwargs["desiredStatus"]
        if status == "RUNNING":
            running = [task for task in self.running if task not in self.stopped]
            if len(running) > 1 and "nextToken" not in kwargs:
                return {"taskArns": running[:1], "nextToken": "page-2"}
            return {"taskArns": running[1:] if "nextToken" in kwargs else running}
        if status == "PENDING":
            return {"taskArns": [task for task in self.pending if task not in self.stopped]}
        return {"taskArns": [*self.stopped, *self.already_stopping]}

    def stop_task(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["reason"] == "Deployment maintenance"
        self.stopped.append(str(kwargs["task"]))
        return {}

    def describe_tasks(self, **kwargs: object) -> dict[str, object]:
        return {"tasks": [{"taskArn": task, "lastStatus": "STOPPED"} for task in kwargs["tasks"]]}

    def describe_services(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["services"] == ["Tracker"]
        self.tracker_polls += 1
        running_count = 1 if self.tracker_polls == 1 else 0
        self.tracker_running_counts.append(running_count)
        return {"services": [{"runningCount": running_count}]}


def _release_arguments(monkeypatch: MonkeyPatch, *, artifact_digest: str = "a" * 64) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "tracker.executor.release_entrypoint",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker",
            "database.internal",
            "5432",
            "tracker",
            IMAGE_REPOSITORY,
            "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "Tracker",
            "2",
            "--runner-task-family",
            "ValkyrieExecutorRunner",
            "activate",
            "git-abc123-def456",
            f"{IMAGE_REPOSITORY}@sha256:{artifact_digest}",
            "1",
        ],
    )


@pytest.mark.description("The release entrypoint uses the sealed configuration and saves the active release.")
def test_release_entrypoint_uses_sealed_configuration_and_persists_active_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "caller-credential")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SECRETSMANAGER", "https://caller.invalid")
    monkeypatch.setenv("HTTPS_PROXY", "https://caller.invalid")
    monkeypatch.setenv("DB_HOST", "caller.invalid")
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecr = FakeEcrClient()
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: ecr)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    real_import = builtins.__import__

    def assert_sealed_environment_before_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "tracker.database.session":
            assert os.environ["DB_HOST"] == "database.internal"
            assert os.environ["DB_USERNAME"] == "tracker"
            assert os.environ["DB_PASSWORD"] == "secret"
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", assert_sealed_environment_before_import)

    release_entrypoint.main()

    database_session.expire_all()
    stored_release = database_session.get(ExecutorRelease, "git-abc123-def456")
    admission = database_session.get(ExecutorAdmission, 1)
    assert client.secret_ids == ["arn:aws:secretsmanager:us-east-1:123456789012:secret:tracker"]
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_ENDPOINT_URL_SECRETSMANAGER" not in os.environ
    assert "HTTPS_PROXY" not in os.environ
    assert stored_release is not None
    assert stored_release.status == ExecutorReleaseStatus.ACTIVE
    assert stored_release.task_definition_arn == REGISTERED_TASK_DEFINITION
    assert admission is not None
    assert admission.release_id == stored_release.id
    assert ecr.describe_calls == [
        {"repositoryName": "valkyrie-executor", "imageIds": [{"imageDigest": "sha256:" + "a" * 64}]}
    ]
    assert ecs.registered == [
        {
            "family": "ValkyrieExecutorRunner",
            "cpu": "1024",
            "memory": "4096",
            "runtimePlatform": {"cpuArchitecture": "ARM64"},
            "taskRoleArn": "arn:aws:iam::123456789012:role/runner-task",
            "executionRoleArn": "arn:aws:iam::123456789012:role/runner-execution",
            "containerDefinitions": [
                {
                    "name": "ExecutorRunnerContainer",
                    "image": f"{IMAGE_REPOSITORY}@sha256:{'a' * 64}",
                    "environment": [{"name": "SENTRY_RELEASE", "value": "git-abc123-def456"}],
                    "stopTimeout": 120,
                }
            ],
        }
    ]


@pytest.mark.description("Running the release entrypoint again reuses the registered task definition.")
def test_release_entrypoint_rerun_reuses_the_registered_task_definition(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: FakeSecretsManager())
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: FakeEcrClient())
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)

    release_entrypoint.main()
    release_entrypoint.main()

    database_session.expire_all()
    stored_release = database_session.get(ExecutorRelease, "git-abc123-def456")
    assert stored_release is not None
    assert stored_release.task_definition_arn == REGISTERED_TASK_DEFINITION
    assert len(ecs.registered) == 1


@pytest.mark.description("Maintenance drains paginated and pending runners and waits for Tracker before resuming.")
def test_maintenance_begin_drains_runner_tasks_before_finish_resumes_service(
    monkeypatch: MonkeyPatch, control_postgres_session: Session
) -> None:
    target_sha = "b" * 40
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", target_sha]
    ecs = FakeEcsClient()
    ecs.running.append("arn:aws:ecs:us-east-1:123456789012:task/runner-3")
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: FakeSecretsManager())
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", control_postgres_session.get_bind())

    release_entrypoint.main()
    admission = get_executor_admission(control_postgres_session)
    control_postgres_session.refresh(admission)
    assert admission.maintenance_target_sha == target_sha
    assert ecs.stopped == [*ecs.running, *ecs.pending]
    assert any(call.get("nextToken") == "page-2" for call in ecs.list_calls)
    assert ecs.tracker_running_counts[0] == 1
    assert ecs.tracker_running_counts[-1] == 0
    assert ecs.waited_for == []
    assert ecs.service_updates == [
        {
            "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
            "service": "Tracker",
            "desiredCount": 0,
        }
    ]

    sys.argv[11] = "maintenance-finish"
    release_entrypoint.main()

    control_postgres_session.refresh(admission)
    assert admission.maintenance_target_sha is None
    assert ecs.service_updates[-1] == {
        "cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie",
        "service": "Tracker",
        "desiredCount": 2,
    }
    assert ecs.waited_for == [
        {"cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/Valkyrie", "services": ["Tracker"]}
    ]


@pytest.mark.description(
    "Maintenance waits for an already-stopping runner, including a temporarily missing description."
)
@pytest.mark.parametrize(
    "described_statuses",
    [("RUNNING", "STOPPED"), (None, "RUNNING", "STOPPED")],
    ids=["already-stopping", "missing-from-describe"],
)
def test_maintenance_begin_waits_for_already_stopping_runner(
    monkeypatch: MonkeyPatch,
    control_postgres_session: Session,
    described_statuses: tuple[str | None, ...],
) -> None:
    target_sha = "b" * 40
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", target_sha]
    ecs = FakeEcsClient()
    ecs.tracker_polls = 1
    ecs.running.clear()
    ecs.pending.clear()
    ecs.already_stopping.append("arn:aws:ecs:us-east-1:123456789012:task/previously-stopping")
    statuses = list(described_statuses)
    observed_statuses: list[str | None] = []

    def describe_tasks(**kwargs: object) -> dict[str, object]:
        status = statuses.pop(0)
        observed_statuses.append(status)
        return {
            "tasks": [] if status is None else [{"taskArn": task, "lastStatus": status} for task in kwargs["tasks"]]
        }

    monkeypatch.setattr(ecs, "describe_tasks", describe_tasks)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(release_entrypoint.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(tracker_session, "engine", control_postgres_session.get_bind())

    release_entrypoint.main()

    admission = get_executor_admission(control_postgres_session)
    control_postgres_session.refresh(admission)
    assert admission.maintenance_target_sha == target_sha
    assert observed_statuses == list(described_statuses)
    assert ecs.stopped == []
    assert [update["desiredCount"] for update in ecs.service_updates] == [0]


@pytest.mark.description("Maintenance drain failures leave admission fenced and Tracker scaled down.")
@pytest.mark.parametrize(
    "condition",
    ["running", "stopping", "missing-description", "tracker"],
    ids=["running-discovery", "stopping-runner", "missing-description", "tracker-shutdown"],
)
def test_maintenance_begin_timeout_keeps_maintenance_fenced(
    monkeypatch: MonkeyPatch,
    control_postgres_session: Session,
    condition: str,
) -> None:
    target_sha = "b" * 40
    _release_arguments(monkeypatch)
    sys.argv = sys.argv[:11] + ["maintenance-begin", target_sha]
    ecs = FakeEcsClient()
    ecs.tracker_polls = 1
    if condition == "running":
        ecs.pending.clear()
        list_tasks = ecs.list_tasks

        def list_running(**kwargs: object) -> dict[str, object]:
            if kwargs["desiredStatus"] == "RUNNING":
                return {"taskArns": ecs.running}
            return list_tasks(**kwargs)

        monkeypatch.setattr(ecs, "list_tasks", list_running)
    elif condition == "stopping":
        ecs.running.clear()
        ecs.pending.clear()
        ecs.already_stopping.append("arn:aws:ecs:us-east-1:123456789012:task/previously-stopping")
        monkeypatch.setattr(
            ecs,
            "describe_tasks",
            lambda **kwargs: {"tasks": [{"taskArn": task, "lastStatus": "RUNNING"} for task in kwargs["tasks"]]},
        )
    elif condition == "missing-description":
        ecs.pending.clear()
        monkeypatch.setattr(ecs, "describe_tasks", lambda **_kwargs: {"tasks": []})
    else:
        ecs.running.clear()
        ecs.pending.clear()
        monkeypatch.setattr(ecs, "describe_services", lambda **_kwargs: {"services": [{"runningCount": 1}]})

    monkeypatch.setattr(release_entrypoint, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", FakeSecretsManager)
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)
    monkeypatch.setattr(tracker_session, "engine", control_postgres_session.get_bind())

    with pytest.raises(TimeoutError, match="Maintenance drain timed out"):
        release_entrypoint.main()

    admission = get_executor_admission(control_postgres_session)
    control_postgres_session.refresh(admission)
    assert admission.maintenance_target_sha == target_sha
    assert [update["desiredCount"] for update in ecs.service_updates] == [0]
    assert ecs.waited_for == []


@pytest.mark.description(
    "An image that was never published registers no task definition and saves no `ExecutorRelease`."
)
def test_release_entrypoint_unpublished_image_registers_nothing_and_commits_no_release(
    monkeypatch: MonkeyPatch,
    database_session: Session,
) -> None:
    _release_arguments(monkeypatch)
    client = FakeSecretsManager()
    monkeypatch.setattr(release_entrypoint, "create_secrets_manager_client", lambda: client)
    monkeypatch.setattr(tracker_session, "engine", database_session.get_bind())
    ecs = FakeEcsRegistry()
    monkeypatch.setattr(release_entrypoint, "create_ecr_client", lambda: FakeEcrClient(published=False))
    monkeypatch.setattr(release_entrypoint, "create_ecs_client", lambda: ecs)

    with pytest.raises(ClientError, match="ImageNotFoundException"):
        release_entrypoint.main()

    database_session.expire_all()
    assert ecs.registered == []
    assert database_session.get(ExecutorRelease, "git-abc123-def456") is None


@pytest.mark.description("An invalid release ID is rejected before any secret is read.")
def test_release_entrypoint_rejects_invalid_release_id_before_reading_secret(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv[12] = "invalid release"
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid release input must not read Secrets Manager"),
    )

    with pytest.raises(ValidationError, match="release_id"):
        release_entrypoint.main()


@pytest.mark.description("An invalid artifact identity is rejected before any secret is read.")
@pytest.mark.parametrize(
    ("argument_index", "invalid_value", "error"),
    [
        (13, f"{IMAGE_REPOSITORY}:git-abc123", "configured ECR repository"),
        (13, f"{IMAGE_REPOSITORY}-other@sha256:{'a' * 64}", "configured ECR repository"),
        (14, "999", "Unsupported executor protocol"),
    ],
)
def test_release_entrypoint_rejects_invalid_artifact_identity_before_reading_secret(
    monkeypatch: MonkeyPatch,
    argument_index: int,
    invalid_value: str,
    error: str,
) -> None:
    _release_arguments(monkeypatch)
    sys.argv[argument_index] = invalid_value
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid artifact identity must not read Secrets Manager"),
    )

    with pytest.raises(ValueError, match=error):
        release_entrypoint.main()


@pytest.mark.description(
    "Extra command line arguments make the release entrypoint exit with a sealed release task error."
)
def test_release_entrypoint_rejects_arbitrary_extra_command_arguments(monkeypatch: MonkeyPatch) -> None:
    _release_arguments(monkeypatch)
    sys.argv.append("python -c 'malicious'")
    monkeypatch.setattr(
        release_entrypoint,
        "create_secrets_manager_client",
        lambda: pytest.fail("invalid command shape must not read Secrets Manager"),
    )

    with pytest.raises(SystemExit, match="sealed release task"):
        release_entrypoint.main()
