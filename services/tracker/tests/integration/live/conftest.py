from collections.abc import Generator
from typing import Any

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID, inline_database_calls_in
from tracker.auth import get_current_org
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker import config
from tracker.aws import cloudwatch_logs
from tracker.aws.cloudwatch_logs import benchmark_log_group_name
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark
from tracker.types import AWSBenchmarkArguments, AgentContractRequest
from tests.factories import make_benchmark


@pytest.fixture
def tracker_database(tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> Session:
    monkeypatch.setattr("tracker.sandbox_cost.engine", tracker_database.bind)
    monkeypatch.setattr("tracker.services.sandboxes.engine", tracker_database.bind)
    monkeypatch.setattr("tracker.sandbox_cost.run_sandbox_cost_loop", lambda stop_event, **_: stop_event.wait())
    inline_database_calls_in(monkeypatch)
    return tracker_database


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {"X-Api-Key": "test-api-key"}


@pytest.fixture
def database_session(database_session: Session, live_aws_runtime: AWSRuntime) -> Session:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.storage_key = live_aws_runtime.resources.s3_bucket
    database_session.add(org)
    database_session.commit()
    return database_session


@pytest.fixture
def live_deployment(live_aws_runtime: AWSRuntime, monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    resources = live_aws_runtime.resources
    for name, value in {
        "AWS_DEPLOYMENT_ACCOUNT_ID": live_aws_runtime.expected_bucket_owner,
        "AWS_DEPLOYMENT_REGION": resources.region,
        "AWS_DEPLOYMENT_S3_BUCKET": resources.s3_bucket,
        "AWS_DEPLOYMENT_LOG_GROUP": resources.log_group,
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": str(resources.log_retention_days),
    }.items():
        monkeypatch.setattr(config, name, value)

    log_group_names: set[str] = set()

    def record_log_group_name(log_group: str, benchmark_id: str) -> str:
        name = benchmark_log_group_name(log_group, benchmark_id)
        log_group_names.add(name)
        return name

    monkeypatch.setattr(cloudwatch_logs, "benchmark_log_group_name", record_log_group_name)
    yield
    logs_client: Any = live_aws_runtime.clients.cloudwatch_logs_client()
    for log_group_name in log_group_names:
        try:
            logs_client.delete_log_group(logGroupName=log_group_name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                raise


@pytest.fixture
def example_benchmark_object(
    contract: AgentContractRequest, live_aws_runtime: AWSRuntime, daytona_secret_name: str, live_deployment: None
) -> Benchmark:
    benchmark = make_benchmark(contract=contract, concurrency=5)
    assert isinstance(benchmark.arguments, AWSBenchmarkArguments)
    benchmark.arguments.properties = live_aws_runtime.resources
    benchmark.arguments.sandbox_provider = "daytona"
    benchmark.arguments.sandbox_provider_secret_name = daytona_secret_name
    return benchmark


@pytest.fixture
def live_api_client(
    tracker_database: Session,
    live_aws_runtime: AWSRuntime,
    live_deployment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[TestClient, None, None]:

    def get_test_session() -> Generator[Session, None, None]:
        yield tracker_database

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setattr("tracker.database.session.engine", tracker_database.bind)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_org,
        lambda: Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME, storage_key=live_aws_runtime.resources.s3_bucket),
    )

    with TestClient(app) as client:
        yield client
