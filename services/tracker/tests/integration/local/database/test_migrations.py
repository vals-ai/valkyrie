import os
import subprocess
import traceback
from collections.abc import Generator
from typing import Protocol
from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

import tracker.config as tracker_config
from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import (
    Benchmark,
    ExecutorDispatch,
    ExecutorRelease,
    Project,
    RunProject,
    Task,
)
from tracker.domain import BenchmarkStatus, ExecutorDispatchKind, SpendType, TaskStatus
from tracker.local.resources import LocalResources
from tracker.types import AgentContractRequest, AWSBenchmarkArguments, LocalBenchmarkArguments
from tracker.repositories.project import ProjectRepository

_TRACKER_ROOT = Path(__file__).resolve().parents[4]
_ALEMBIC_INI = _TRACKER_ROOT / "alembic.ini"
_EXECUTOR_RELEASE_OWNERSHIP_REVISION = "c7d8e9f0a1b2"
_EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR = "6f3c2d9a8b10"
_CURRENT_OWNERSHIP_REVISION = "e9f0a1b2c3d4"
_PREVIOUS_REVISION = "d8e9f0a1b2c3"
_PROJECT_GATEWAY_REVISION = "a6e7d8f9b0c1"
_MAINTENANCE_REVISION = "f0a1b2c3d4e5"
_ERROR_RESULT_PROVENANCE_REVISION = "a3f4b5c6d7e8"
_CONTROL_BOUNDARY_REVISION = "c1e2f3a4b5d6"
_DISPATCH_LEASE_REVISION = "6a7b8c9d0e1f"
_DROP_DISPATCH_LEASE_REVISION = "5b6c7d8e9f0a"
_DROP_DISPATCH_LEASE_PREDECESSOR = "4a5b6c7d8e9f"
_DROP_RELEASE_READINESS_REVISION = "6c7d8e9f0a1b"
_MIGRATION_ADVISORY_LOCK_ID = 0x56414C4B59524945
_TASK_LISTING_REVISION = "2d3e4f5a6b7c"
_TASK_LISTING_PREDECESSOR = "1c2d3e4f5a6b"
_SANDBOX_BUILD_RESERVATION_REVISION = "4f5a6b7c8d9e"
_SANDBOX_BUILD_RESERVATION_PREDECESSOR = "3e4f5a6b7c8d"
_RESERVED_ADMISSION_REVISION = "d9e0f1a2b3c4"
_RESERVED_ADMISSION_PREDECESSOR = "c1d2e3f4a5b6"
_SHARED_POOL_REVISION = "c82e1f7a9d03"
_SHARED_POOL_PREDECESSOR = "5a6b7c8d9e0f"
_ORG_BUCKET_REVISION = "a7b8c9d0e1f2"
_ORG_BUCKET_PREDECESSOR = "b3c4d5e6f7a8"
_DROP_AWS_MANAGED_REVISION = "c4d5e6f7a8b9"
_PROJECT_PREDECESSOR = "b7e4c1a9d2f0"
_UNIQUE_PROJECT_NAME_REVISION = "4c9d2e7f1a3b"
_UNIQUE_PROJECT_NAME_PREDECESSOR = "3222f9e0a6ce"


@pytest.mark.description("Alembic migrations resolve to exactly one head revision.")
def test_migration_graph_has_single_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(_ALEMBIC_INI)))
    heads = scripts.get_heads()

    assert len(heads) == 1, f"Expected one Alembic head, found {heads}"
    assert scripts.get_revision(_CONTROL_BOUNDARY_REVISION) is not None


@pytest.mark.description("The dispatch lease migration adds the lease columns and the status and lease expiry index.")
def test_dispatch_lease_migration_adds_recovery_state(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _DISPATCH_LEASE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("executordispatch")}
    assert {
        "assigned_task_ids",
        "claim_deadline_at",
        "heartbeat_at",
        "lease_expires_at",
        "failure_reason",
    } <= columns
    assert any(
        index["name"] == "ix_executordispatch_status_lease_expires"
        for index in inspector.get_indexes("executordispatch")
    )
    engine.dispose()


@pytest.mark.description(
    "Upgrading keeps only `lease_expires_at` and drops release readiness columns. Downgrading restores both."
)
def test_dispatch_migration_retains_only_local_lease_state_and_drops_release_readiness(
    migration_database_url: str,
) -> None:
    lease_columns = {"claim_deadline_at", "heartbeat_at", "lease_expires_at"}
    readiness_columns = {"readiness_verified", "readiness_metadata"}

    def lease_state() -> tuple[set[str], set[str], bool]:
        engine = create_engine(migration_database_url)
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("executordispatch")}
        release_columns = {column["name"] for column in inspector.get_columns("executorrelease")}
        indexed = any(
            index["name"] == "ix_executordispatch_status_lease_expires"
            for index in inspector.get_indexes("executordispatch")
        )
        engine.dispose()
        return columns & lease_columns, release_columns & readiness_columns, indexed

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_DISPATCH_LEASE_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == (lease_columns, readiness_columns, True)

    engine = create_engine(migration_database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO executorrelease"
                " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                " readiness_metadata, created_at)"
                " VALUES ('lease-drop-release', 'source:///src', :digest, '5', 'ACTIVE', true, '{}', now())"
            ),
            {"digest": "a" * 64},
        )
    engine.dispose()

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_DISPATCH_LEASE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == ({"lease_expires_at"}, readiness_columns, True)

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_RELEASE_READINESS_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == ({"lease_expires_at"}, set(), True)

    downgrade = _run_alembic(migration_database_url, "downgrade", _DROP_DISPATCH_LEASE_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    assert lease_state() == (lease_columns, readiness_columns, True)


@pytest.fixture(scope="module")
def migration_postgres() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def migration_database_url(migration_postgres: PostgresContainer) -> Generator[str, None, None]:
    name = f"migration_{uuid4().hex}"
    admin = create_engine(migration_postgres.get_connection_url(), isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {name}"))
    yield make_url(migration_postgres.get_connection_url()).set(database=name).render_as_string(hide_password=False)
    with admin.connect() as connection:
        connection.execute(text(f"DROP DATABASE {name} WITH (FORCE)"))
    admin.dispose()


def _run_alembic(database_url: str, action: str, revision: str) -> subprocess.CompletedProcess[str]:
    try:
        with patch.dict(os.environ, {"DATABASE_URL": database_url}), patch("logging.config.fileConfig"):
            getattr(command, action)(Config(str(_ALEMBIC_INI)), revision)
    except Exception:
        return subprocess.CompletedProcess([action, revision], 1, "", traceback.format_exc())
    return subprocess.CompletedProcess([action, revision], 0, "", "")


@pytest.mark.description("The failure category migration gives up on a busy table and retries.")
def test_failure_category_migration_aborts_on_busy_table_and_retries(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    try:
        with engine.connect() as blocker:
            blocker.execute(text("SELECT id FROM errorresult"))
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_alembic, migration_database_url, "upgrade", "head")
                try:
                    blocked = future.result(timeout=10)
                finally:
                    blocker.rollback()
            assert blocked.returncode != 0
            assert "lock timeout" in blocked.stderr

        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                _TASK_LISTING_REVISION
            )
            assert (
                connection.execute(text("SELECT count(*) FROM pg_type WHERE typname = 'failurecategory'")).scalar_one()
                == 0
            )
            assert "category" not in {column["name"] for column in inspect(connection).get_columns("errorresult")}

        retry = _run_alembic(migration_database_url, "upgrade", "head")
        assert retry.returncode == 0, retry.stderr
        with engine.connect() as connection:
            columns = {column["name"]: column for column in inspect(connection).get_columns("errorresult")}
            assert columns["category"]["nullable"] is True
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()
            )
    finally:
        engine.dispose()


@pytest.mark.description("The headline score migration aborts on a busy table and succeeds on retry.")
def test_headline_score_migration_aborts_on_busy_table_and_retries(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _PROJECT_GATEWAY_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    try:
        with engine.connect() as blocker:
            blocker.execute(text("SELECT id FROM finalevaluation"))
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_alembic, migration_database_url, "upgrade", "head")
                try:
                    blocked = future.result(timeout=10)
                finally:
                    blocker.rollback()
        assert blocked.returncode != 0
        assert "lock timeout" in blocked.stderr

        retry = _run_alembic(migration_database_url, "upgrade", "head")
        assert retry.returncode == 0, retry.stderr
        with engine.connect() as connection:
            columns = {column["name"]: column for column in inspect(connection).get_columns("finalevaluation")}
            assert columns["final_score"]["nullable"] is True
    finally:
        engine.dispose()


@pytest.mark.description(
    "Upgrading through both branches creates `executor_dispatch_payload` and a nullable `errorresult.category`."
)
@pytest.mark.parametrize("predecessor", ["3e4f5a6b7c8d", "b4c5d6e7f8a9"])
def test_dispatch_and_failure_category_heads_upgrade_together(migration_database_url: str, predecessor: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", predecessor)
    assert upgrade.returncode == 0, upgrade.stderr
    merge = _run_alembic(migration_database_url, "upgrade", "1f12bae14561")
    assert merge.returncode == 0, merge.stderr
    engine = create_engine(migration_database_url)
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "1f12bae14561"
            inspector = inspect(connection)
            assert "executor_dispatch_payload" in inspector.get_table_names()
            columns = {column["name"]: column for column in inspector.get_columns("errorresult")}
            assert columns["category"]["nullable"] is True
    finally:
        engine.dispose()


class _TaskListingIndexRow(Protocol):
    index_oid: int
    indisvalid: bool
    indisunique: bool
    indnkeyatts: int
    indnatts: int
    has_no_expressions: bool
    has_no_predicate: bool
    amname: str
    table_schema: str
    table_name: str
    key_columns: list[str]
    key_1_asc: bool
    key_1_nulls_last: bool
    key_2_asc: bool
    key_2_nulls_last: bool
    key_3_desc: bool
    key_3_nulls_first: bool


def _assert_canonical_task_listing_index(index: _TaskListingIndexRow) -> int:
    assert index.indisvalid is True
    assert index.indisunique is False
    assert index.indnkeyatts == 3
    assert index.indnatts == 3
    assert index.has_no_expressions is True
    assert index.has_no_predicate is True
    assert index.amname == "btree"
    assert index.table_schema == "public"
    assert index.table_name == "task"
    assert index.key_columns == ["benchmark", "org_id", "started_at"]
    assert index.key_1_asc is True
    assert index.key_1_nulls_last is True
    assert index.key_2_asc is True
    assert index.key_2_nulls_last is True
    assert index.key_3_desc is True
    assert index.key_3_nulls_first is True
    return index.index_oid


@pytest.mark.description("The task listing index migration can be retried safely.")
def test_task_listing_index_migration_is_retry_safe(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    index_query = text(
        """
        SELECT
            i.indexrelid AS index_oid,
            i.indisvalid,
            i.indisunique,
            i.indnkeyatts,
            i.indnatts,
            i.indexprs IS NULL AS has_no_expressions,
            i.indpred IS NULL AS has_no_predicate,
            am.amname,
            tn.nspname AS table_schema,
            t.relname AS table_name,
            (
                SELECT array_agg(a.attname ORDER BY key.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
                JOIN pg_attribute AS a
                  ON a.attrelid = i.indrelid
                 AND a.attnum = key.attnum
            ) AS key_columns,
            pg_index_column_has_property(i.indexrelid, 1, 'asc') AS key_1_asc,
            pg_index_column_has_property(i.indexrelid, 1, 'nulls_last') AS key_1_nulls_last,
            pg_index_column_has_property(i.indexrelid, 2, 'asc') AS key_2_asc,
            pg_index_column_has_property(i.indexrelid, 2, 'nulls_last') AS key_2_nulls_last,
            pg_index_column_has_property(i.indexrelid, 3, 'desc') AS key_3_desc,
            pg_index_column_has_property(i.indexrelid, 3, 'nulls_first') AS key_3_nulls_first
        FROM pg_index AS i
        JOIN pg_class AS c ON c.oid = i.indexrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_class AS t ON t.oid = i.indrelid
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace
        JOIN pg_am AS am ON am.oid = c.relam
        WHERE c.relname = :index_name AND n.nspname = current_schema()
        """
    )
    revision_query = text("SELECT version_num FROM alembic_version")
    index_params = {"index_name": "ix_task_benchmark_org_started_at"}

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        _assert_canonical_task_listing_index(index)
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(
                'CREATE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" '
                "ON task (benchmark, org_id, started_at DESC)"
            )
        )
        matching_index_oid = connection.execute(index_query, index_params).one().index_oid
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        assert _assert_canonical_task_listing_index(index) == matching_index_oid
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION
    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        assert connection.execute(index_query, index_params).one_or_none() is None
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(
                'CREATE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" '
                "ON task (org_id, benchmark, started_at ASC)"
            )
        )
        wrong_index = connection.execute(index_query, index_params).one()
        assert wrong_index.indisvalid is True
        assert wrong_index.indisunique is False
        assert wrong_index.key_columns == ["org_id", "benchmark", "started_at"]
        assert wrong_index.key_1_asc is True
        assert wrong_index.key_2_asc is True
        assert wrong_index.key_3_desc is False

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        _assert_canonical_task_listing_index(index)
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    org_id = uuid4()
    benchmark_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "task-listing-retry-org"},
        )
        connection.execute(
            text(
                "INSERT INTO benchmark "
                "(id, org_id, name, started_at, status) "
                "VALUES (:id, :org_id, :name, :started_at, :status)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "task-listing-retry-benchmark",
                "started_at": datetime(2026, 9, 21, tzinfo=UTC),
                "status": "IN_PROGRESS",
            },
        )
        for task_id in (uuid4(), uuid4()):
            connection.execute(
                text(
                    "INSERT INTO task "
                    "(id, org_id, task_id, status, started_at, benchmark) "
                    "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
                ),
                {
                    "id": task_id,
                    "org_id": org_id,
                    "task_id": str(task_id),
                    "status": "PENDING",
                    "started_at": datetime(2026, 9, 21, tzinfo=UTC),
                    "benchmark": benchmark_id,
                },
            )

    with engine.connect() as connection:
        with pytest.raises(DBAPIError):
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                text('CREATE UNIQUE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" ON task (org_id)')
            )
        invalid_index = connection.execute(index_query, index_params).one()
        assert invalid_index.indisvalid is False
        assert invalid_index.indisunique is True

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        assert index.indisvalid is True
        assert index.indisunique is False
        assert index.key_columns == ["benchmark", "org_id", "started_at"]
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text('DROP INDEX CONCURRENTLY IF EXISTS "ix_task_benchmark_org_started_at"')
        )
    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        assert connection.execute(index_query, index_params).one_or_none() is None
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_PREDECESSOR
    engine.dispose()


@pytest.mark.description("Downgrading executor release ownership restores the previous schema.")
def test_executor_release_ownership_downgrade_restores_predecessor_schema(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _EXECUTOR_RELEASE_OWNERSHIP_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    downgrade = _run_alembic(migration_database_url, "downgrade", _EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr

    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    assert "executorrelease" not in inspector.get_table_names()
    assert "executoradmission" not in inspector.get_table_names()
    benchmark_columns = {column["name"] for column in inspector.get_columns("benchmark")}
    assert (
        not {
            "executor_release_id",
            "executor_artifact_uri",
            "executor_artifact_digest",
            "executor_protocol_version",
        }
        & benchmark_columns
    )
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        release_status_enum_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'executorreleasestatus')")
        ).scalar_one()
    assert revision == _EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR
    assert release_status_enum_exists is False
    engine.dispose()


@pytest.mark.description(
    "The error result provenance migration upgrades existing error rows to the maintenance revision without losing them."
)
def test_error_result_provenance_migration_preserves_legacy_rows(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _MAINTENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    org_id = uuid4()
    benchmark_id = uuid4()
    task_id = uuid4()
    raw_insert_id = uuid4()
    legacy_failures = [
        (uuid4(), datetime(2026, 8, 10, 12, 0, tzinfo=UTC), "first legacy failure"),
        (uuid4(), datetime(2026, 8, 11, 12, 0, tzinfo=UTC), "second legacy failure"),
    ]
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "legacy-error-result-migration-org"},
        )
        connection.execute(
            text(
                "INSERT INTO benchmark (id, org_id, name, started_at, status) "
                "VALUES (:id, :org_id, :name, :started_at, :status)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "legacy-error-result-migration-benchmark",
                "started_at": legacy_failures[0][1],
                "status": "IN_PROGRESS",
            },
        )
        connection.execute(
            text(
                "INSERT INTO task "
                "(id, org_id, task_id, status, started_at, finished_at, benchmark) "
                "VALUES (:id, :org_id, :task_id, :status, :started_at, :finished_at, :benchmark)"
            ),
            {
                "id": task_id,
                "org_id": org_id,
                "task_id": "legacy-task",
                "status": "ERROR",
                "started_at": legacy_failures[0][1],
                "finished_at": legacy_failures[1][1],
                "benchmark": benchmark_id,
            },
        )
        connection.execute(
            text(
                "INSERT INTO errorresult (id, org_id, task, created_at, error_message) VALUES "
                "(:id, :org_id, :task, :created_at, :error_message)"
            ),
            [
                {
                    "id": failure_id,
                    "org_id": org_id,
                    "task": task_id,
                    "created_at": created_at,
                    "error_message": message,
                }
                for failure_id, created_at, message in legacy_failures
            ],
        )

    upgrade = _run_alembic(migration_database_url, "upgrade", _ERROR_RESULT_PROVENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    with engine.begin() as connection:
        migrated_rows = (
            connection.execute(
                text(
                    "SELECT id, org_id, task, created_at, error_message, producer, operation, "
                    "error_type, cause_code, retry_scheduled, failed_attempt_number "
                    "FROM errorresult WHERE task = :task ORDER BY created_at"
                ),
                {"task": task_id},
            )
            .mappings()
            .all()
        )
        connection.execute(
            text(
                "INSERT INTO errorresult (id, org_id, task, created_at, error_message) "
                "VALUES (:id, :org_id, :task, :created_at, :error_message)"
            ),
            {
                "id": raw_insert_id,
                "org_id": org_id,
                "task": task_id,
                "created_at": datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
                "error_message": "terminal row using database default",
            },
        )
        raw_retry_scheduled = connection.execute(
            text("SELECT retry_scheduled FROM errorresult WHERE id = :id"),
            {"id": raw_insert_id},
        ).scalar_one()

        inspector = inspect(connection)
        error_result_columns = {column["name"]: column for column in inspector.get_columns("errorresult")}
        error_result_indexes = {
            (index["name"], tuple(index["column_names"]), index["unique"])
            for index in inspector.get_indexes("errorresult")
        }

    assert len(migrated_rows) == len(legacy_failures)
    for migrated, (failure_id, created_at, message) in zip(migrated_rows, legacy_failures, strict=True):
        assert migrated["id"] == failure_id
        assert migrated["org_id"] == org_id
        assert migrated["task"] == task_id
        assert migrated["created_at"] == created_at.replace(tzinfo=None)
        assert migrated["error_message"] == message
        assert migrated["retry_scheduled"] is False
        assert all(
            migrated[field] is None
            for field in (
                "producer",
                "operation",
                "error_type",
                "cause_code",
                "failed_attempt_number",
            )
        )

    assert raw_retry_scheduled is False
    assert set(error_result_columns) == {
        "id",
        "org_id",
        "task",
        "created_at",
        "error_message",
        "producer",
        "operation",
        "error_type",
        "cause_code",
        "retry_scheduled",
        "failed_attempt_number",
    }
    assert error_result_columns["retry_scheduled"]["nullable"] is False
    assert error_result_indexes == {
        ("ix_errorresult_org_task_created_at", ("org_id", "task", "created_at"), False),
    }

    downgrade = _run_alembic(migration_database_url, "downgrade", _MAINTENANCE_REVISION)
    assert downgrade.returncode == 0, downgrade.stderr

    with engine.connect() as connection:
        inspector = inspect(connection)
        downgraded_columns = {column["name"] for column in inspector.get_columns("errorresult")}
        downgraded_indexes = {index["name"] for index in inspector.get_indexes("errorresult")}
        downgraded_rows = (
            connection.execute(
                text("SELECT id, org_id, task, created_at, error_message FROM errorresult ORDER BY created_at")
            )
            .mappings()
            .all()
        )
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()

    assert downgraded_columns == {"id", "org_id", "task", "created_at", "error_message"}
    assert "ix_errorresult_org_task_created_at" not in downgraded_indexes
    assert [dict(row) for row in downgraded_rows] == [
        {
            "id": legacy_failures[0][0],
            "org_id": org_id,
            "task": task_id,
            "created_at": legacy_failures[0][1].replace(tzinfo=None),
            "error_message": legacy_failures[0][2],
        },
        {
            "id": legacy_failures[1][0],
            "org_id": org_id,
            "task": task_id,
            "created_at": legacy_failures[1][1].replace(tzinfo=None),
            "error_message": legacy_failures[1][2],
        },
        {
            "id": raw_insert_id,
            "org_id": org_id,
            "task": task_id,
            "created_at": datetime(2026, 8, 12, 12, 0),
            "error_message": "terminal row using database default",
        },
    ]
    assert revision == _MAINTENANCE_REVISION
    engine.dispose()


@pytest.mark.description(
    "The maintenance fence and control boundary migrations add their schema without removing existing columns."
)
def test_maintenance_fence_and_control_boundary_migrations_are_additive(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _MAINTENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    admission_columns = {column["name"] for column in inspect(engine).get_columns("executoradmission")}
    assert "maintenance_target_sha" in admission_columns
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _MAINTENANCE_REVISION
    upgrade = _run_alembic(migration_database_url, "upgrade", _CONTROL_BOUNDARY_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    inspector = inspect(engine)
    assert "maintenance_target_sha" in {column["name"] for column in inspector.get_columns("executoradmission")}
    assert "active_epoch_id" in {column["name"] for column in inspector.get_columns("executoradmission")}
    assert {"migrationepoch", "migrationparticipant"} <= set(inspector.get_table_names())
    assert {"invocation_key_digest", "claim_receipt_at", "frozen_epoch_id"} <= {
        column["name"] for column in inspector.get_columns("executordispatch")
    }
    engine.dispose()


@pytest.mark.description(
    "A second upgrade blocks on the migration advisory lock and then completes at the head revision."
)
def test_upgrade_waits_for_the_migration_advisory_lock(migration_database_url: str) -> None:
    engine = create_engine(migration_database_url)
    with engine.connect() as lock_holder, ThreadPoolExecutor(max_workers=1) as executor:
        lock_holder.execute(
            text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
        )
        lock_holder.commit()
        upgrade = executor.submit(_run_alembic, migration_database_url, "upgrade", _MAINTENANCE_REVISION)
        deadline = monotonic() + 10
        try:
            while monotonic() < deadline:
                with engine.connect() as observer:
                    waiting = observer.execute(
                        text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)")
                    ).scalar_one()
                if waiting:
                    break
                if upgrade.done():
                    result = upgrade.result()
                    pytest.fail(f"Alembic exited before waiting for the migration lock: {result.stderr}")
                sleep(0.05)
            else:
                pytest.fail("Alembic did not wait for the migration advisory lock")
            assert not upgrade.done()
        finally:
            lock_holder.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
            )
            lock_holder.commit()

        result = upgrade.result(timeout=30)

    assert result.returncode == 0, result.stderr
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _MAINTENANCE_REVISION
    engine.dispose()


@pytest.mark.description("The current execution ownership migration refuses to downgrade.")
def test_current_execution_ownership_migration_rejects_downgrade(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _CURRENT_OWNERSHIP_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    org_id = uuid4()
    benchmark_id = uuid4()
    with Session(engine) as session:
        session.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "migration-test-org"},
        )
        session.commit()
        session.execute(
            text(
                "INSERT INTO executorrelease"
                " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                " readiness_metadata, created_at)"
                " VALUES (:id, :artifact_uri, :artifact_digest, '1', 'ACTIVE', true, '{}', :created_at)"
            ),
            {
                "id": "migration-test-release",
                "artifact_uri": "source:///artifacts/migration-test-release/src",
                "artifact_digest": "a" * 64,
                "created_at": datetime.now(UTC),
            },
        )
        arguments = AWSBenchmarkArguments(
            contract=AgentContractRequest(name="migration-test-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        )
        session.execute(
            text(
                "INSERT INTO benchmark"
                " (id, org_id, name, started_at, status, arguments, docent_reading_status,"
                " current_execution_release_id)"
                " VALUES (:id, :org_id, :name, now(), CAST(:status AS benchmarkstatus),"
                " CAST(:arguments AS json), CAST(:docent_reading_status AS docentreadingstatus), :release_id)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "migration-test-benchmark",
                "status": BenchmarkStatus.IN_PROGRESS.value,
                "arguments": arguments.model_dump_json(),
                "docent_reading_status": "IDLE",
                "release_id": "migration-test-release",
            },
        )
        session.commit()

    downgrade = _run_alembic(migration_database_url, "downgrade", _PREVIOUS_REVISION)

    assert downgrade.returncode != 0
    assert "current execution release ownership" in downgrade.stderr
    inspector = inspect(engine)
    assert "current_execution_release_id" in {column["name"] for column in inspector.get_columns("benchmark")}
    assert "ix_benchmark_current_execution_release_id" in {
        index["name"] for index in inspector.get_indexes("benchmark")
    }
    assert any(
        foreign_key["constrained_columns"] == ["current_execution_release_id"]
        for foreign_key in inspector.get_foreign_keys("benchmark")
    )
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        stored_owner = connection.execute(
            text("SELECT current_execution_release_id FROM benchmark WHERE id = :benchmark_id"),
            {"benchmark_id": benchmark_id},
        ).scalar_one()
    assert revision == _CURRENT_OWNERSHIP_REVISION
    assert stored_owner == "migration-test-release"
    engine.dispose()


@pytest.mark.description("The sandbox build reservation migration upgrades and downgrades cleanly.")
def test_sandbox_build_reservation_upgrade_and_downgrade(
    migration_database_url: str,
) -> None:
    def migrate(direction: str, revision: str) -> None:
        result = _run_alembic(migration_database_url, direction, revision)
        assert result.returncode == 0, result.stderr

    migrate("upgrade", _SANDBOX_BUILD_RESERVATION_PREDECESSOR)
    engine = create_engine(migration_database_url)
    attempt_started_at = datetime(2026, 9, 29, 12)
    org_id = uuid4()
    task_row_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": f"migration-reservation-{org_id}"},
        )
    benchmark_id = uuid4()
    arguments = AWSBenchmarkArguments(
        contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
        concurrency=1,
        queue_pool_id="pool_migration",
    )
    with Session(engine) as session:
        session.execute(
            text(
                "INSERT INTO benchmark (id, org_id, name, started_at, status, arguments) "
                "VALUES (:id, :org_id, :name, now(), CAST(:status AS benchmarkstatus), CAST(:arguments AS json))"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "migration reservation",
                "status": BenchmarkStatus.IN_PROGRESS.value,
                "arguments": arguments.model_dump_json(),
            },
        )
        session.execute(
            text(
                "INSERT INTO task (id, org_id, task_id, status, started_at, benchmark) "
                "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
            ),
            {
                "id": task_row_id,
                "org_id": org_id,
                "task_id": "reserved-task",
                "status": "BUILDING",
                "started_at": attempt_started_at,
                "benchmark": benchmark_id,
            },
        )
        session.commit()

    migrate("upgrade", _SANDBOX_BUILD_RESERVATION_REVISION)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO sandboxbuildreservation "
                "(task_row_id, attempt_started_at, reserved_at, pool_id, "
                "requested_vcpu, requested_memory, requested_disk) "
                "VALUES (:task_row_id, :attempt_started_at, :reserved_at, :pool_id, "
                ":requested_vcpu, :requested_memory, :requested_disk)"
            ),
            {
                "task_row_id": task_row_id,
                "attempt_started_at": attempt_started_at,
                "reserved_at": attempt_started_at,
                "pool_id": "pool_migration",
                "requested_vcpu": 1,
                "requested_memory": 2,
                "requested_disk": 3,
            },
        )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE task SET status = 'ERROR', finished_at = :finished_at WHERE id = :id"),
            {"id": task_row_id, "finished_at": attempt_started_at + timedelta(seconds=1)},
        )
        remaining = connection.execute(
            text("SELECT count(*) FROM sandboxbuildreservation WHERE task_row_id = :id"), {"id": task_row_id}
        ).scalar_one()
        assert remaining == 1

    migrate("downgrade", _SANDBOX_BUILD_RESERVATION_PREDECESSOR)
    assert "sandboxbuildreservation" not in inspect(engine).get_table_names()
    engine.dispose()


@pytest.mark.description("The combined migration preserves the head revision and supports admission writes.")
class TestReservedAdmissionMigration:
    @pytest.fixture
    def database(self, migration_database_url: str) -> Generator[Engine, None, None]:
        predecessor = _run_alembic(migration_database_url, "upgrade", _RESERVED_ADMISSION_PREDECESSOR)
        assert predecessor.returncode == 0, predecessor.stderr
        engine = create_engine(migration_database_url)
        try:
            yield engine
        finally:
            engine.dispose()

    @pytest.fixture
    def admission_rows(self, database: Engine) -> tuple[list[Task], ExecutorDispatch]:
        org_id = uuid4()
        with database.begin() as connection:
            connection.execute(
                text("INSERT INTO org (id, name) VALUES (:id, :name)"),
                {"id": org_id, "name": f"migration-admission-{org_id}"},
            )
        with Session(database, expire_on_commit=False) as session:
            release = ExecutorRelease(
                id="migration-admission-release",
                artifact_uri="source:///artifacts/migration-admission/src",
                artifact_digest="a" * 64,
                protocol_version="test",
            )
            session.execute(
                text(
                    "INSERT INTO executorrelease"
                    " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                    " readiness_metadata, created_at)"
                    " VALUES (:id, :uri, :digest, :protocol, 'CANDIDATE', true, '{}', :created_at)"
                ),
                {
                    "id": release.id,
                    "uri": release.artifact_uri,
                    "digest": release.artifact_digest,
                    "protocol": release.protocol_version,
                    "created_at": release.created_at,
                },
            )
            benchmark_id = uuid4()
            arguments = AWSBenchmarkArguments(
                contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
                concurrency=3,
                queue_pool_id="pool_migration",
            )
            session.execute(
                text(
                    "INSERT INTO benchmark (id, org_id, name, started_at, status, arguments) "
                    "VALUES (:id, :org_id, :name, now(), CAST(:status AS benchmarkstatus), "
                    "CAST(:arguments AS json))"
                ),
                {
                    "id": benchmark_id,
                    "org_id": org_id,
                    "name": "reserved admission migration",
                    "status": BenchmarkStatus.IN_PROGRESS.value,
                    "arguments": arguments.model_dump_json(),
                },
            )
            dispatch = ExecutorDispatch(
                benchmark_id=benchmark_id,
                kind=ExecutorDispatchKind.START,
                status=ExecutorDispatchStatus.RUNNING,
                executor_release_id=release.id,
                executor_artifact_uri=release.artifact_uri,
                executor_artifact_digest=release.artifact_digest,
                executor_protocol_version=release.protocol_version,
            )
            tasks = [
                Task(
                    org_id=org_id,
                    task_id=name,
                    benchmark=benchmark_id,
                    status=status,
                    started_at=datetime(2026, 10, 16, 12),
                )
                for name, status in (
                    ("building", TaskStatus.BUILDING),
                    ("ready", TaskStatus.PENDING),
                    ("rejected", TaskStatus.PENDING),
                )
            ]
            session.execute(
                text(
                    "INSERT INTO executordispatch (id, benchmark_id, kind, status, executor_release_id, "
                    "executor_artifact_uri, executor_artifact_digest, executor_protocol_version, created_at) "
                    "VALUES (:id, :benchmark_id, CAST(:kind AS executordispatchkind), "
                    "CAST(:status AS executordispatchstatus), :executor_release_id, "
                    ":executor_artifact_uri, :executor_artifact_digest, :executor_protocol_version, :created_at)"
                ),
                {
                    "id": dispatch.id,
                    "benchmark_id": benchmark_id,
                    "kind": dispatch.kind.value,
                    "status": dispatch.status.value,
                    "executor_release_id": release.id,
                    "executor_artifact_uri": release.artifact_uri,
                    "executor_artifact_digest": release.artifact_digest,
                    "executor_protocol_version": release.protocol_version,
                    "created_at": dispatch.created_at,
                },
            )
            session.execute(
                text(
                    "INSERT INTO task (id, org_id, task_id, status, started_at, benchmark) "
                    "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
                ),
                [
                    {
                        "id": task.id,
                        "org_id": task.org_id,
                        "task_id": task.task_id,
                        "status": task.status.value,
                        "started_at": task.started_at,
                        "benchmark": task.benchmark,
                    }
                    for task in tasks
                ],
            )
            session.commit()

            return tasks, dispatch

    @pytest.mark.description("The reserved admission migration upgrades and downgrades cleanly.")
    @pytest.mark.parametrize("populated", [False, True], ids=["empty-ledger", "pre-domain-ledger"])
    def test_upgrade_and_downgrade(
        self,
        migration_database_url: str,
        database: Engine,
        admission_rows: tuple[list[Task], ExecutorDispatch],
        populated: bool,
    ) -> None:
        tasks, dispatch = admission_rows
        building, ready, rejected = tasks
        original_columns = {
            column["name"]: (str(column["type"]), column["nullable"], column["default"])
            for column in inspect(database).get_columns("sandboxbuildreservation")
        }
        if populated:
            with database.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO sandboxbuildreservation "
                        "(task_row_id, attempt_started_at, reserved_at, pool_id, "
                        "requested_vcpu, requested_memory, requested_disk) "
                        "VALUES (:id, :attempt, :attempt, 'pool_migration', 1, 2, 20)"
                    ),
                    {"id": building.id, "attempt": building.started_at},
                )

        upgrade = _run_alembic(migration_database_url, "upgrade", _RESERVED_ADMISSION_REVISION)
        assert upgrade.returncode == 0, upgrade.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM sandboxbuildreservation")).scalar_one() == 0
        with database.begin() as connection:
            for attempt, released_at in (
                (building.started_at - timedelta(minutes=1), building.started_at),
                (building.started_at, None),
            ):
                connection.execute(
                    text(
                        "INSERT INTO sandboxbuildreservation "
                        "(task_row_id, attempt_started_at, reserved_at, released_at, pool_id, "
                        "target_id, sandbox_class, requested_vcpu, requested_memory, "
                        "requested_disk, requested_gpu) "
                        "VALUES (:task_row_id, :attempt, :attempt, :released_at, "
                        "'pool_migration', 'region-a', 'sysbox', 1.25, 2.25, 20.25, 0)"
                    ),
                    {"task_row_id": building.id, "attempt": attempt, "released_at": released_at},
                )
        with Session(database) as session:
            session.execute(
                text(
                    "INSERT INTO sandboxadmissioncandidate "
                    "(task_row_id, attempt_started_at, executor_dispatch_id, admission_ready, pool_id, "
                    "target_id, sandbox_class, requested_vcpu, requested_memory, requested_disk, requested_gpu) "
                    "VALUES (:id, :attempt, :dispatch, :ready, 'pool_migration', "
                    ":target, 'sysbox', :cpu, :memory, :disk, :gpu)"
                ),
                [
                    {
                        "id": ready.id,
                        "attempt": ready.started_at,
                        "dispatch": dispatch.id,
                        "ready": True,
                        "target": "region-a",
                        "cpu": 1.25,
                        "memory": 2.25,
                        "disk": 20.25,
                        "gpu": 0,
                    },
                    {
                        "id": rejected.id,
                        "attempt": rejected.started_at,
                        "dispatch": dispatch.id,
                        "ready": False,
                        "target": "region-b",
                        "cpu": 0,
                        "memory": 0,
                        "disk": 0,
                        "gpu": 1,
                    },
                ],
            )
            session.commit()

        current_head = _run_alembic(migration_database_url, "upgrade", "head")
        assert current_head.returncode == 0, current_head.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()
            )
            reservations = connection.execute(
                text(
                    "SELECT attempt_started_at, released_at, requested_vcpu, requested_memory, requested_disk "
                    "FROM sandboxbuildreservation ORDER BY attempt_started_at"
                )
            ).all()
            candidates = connection.execute(
                text(
                    "SELECT task_row_id, admission_ready, target_id, sandbox_class, "
                    "requested_vcpu, requested_memory, requested_disk, requested_gpu "
                    "FROM sandboxadmissioncandidate"
                )
            ).all()
            assert (
                connection.execute(
                    text(
                        "SELECT bool_and(readiness_checked_at IS NOT NULL AND readiness_requested_at IS NULL) "
                        "FROM sandboxadmissioncandidate"
                    )
                ).scalar_one()
                is True
            )

        assert [tuple(row) for row in reservations] == [
            (building.started_at - timedelta(minutes=1), building.started_at, 1.25, 2.25, 20.25),
            (building.started_at, None, 1.25, 2.25, 20.25),
        ]
        assert {row[0]: tuple(row[1:]) for row in candidates} == {
            ready.id: (True, "region-a", "sysbox", 1.25, 2.25, 20.25, 0),
            rejected.id: (False, "region-b", "sysbox", 0, 0, 0, 1),
        }

        downgrade = _run_alembic(migration_database_url, "downgrade", _RESERVED_ADMISSION_PREDECESSOR)
        assert downgrade.returncode == 0, downgrade.stderr

        inspector = inspect(database)
        assert {
            column["name"]: (str(column["type"]), column["nullable"], column["default"])
            for column in inspector.get_columns("sandboxbuildreservation")
        } == original_columns
        assert inspector.get_pk_constraint("sandboxbuildreservation")["constrained_columns"] == ["task_row_id"]
        assert "sandboxadmissioncandidate" not in inspector.get_table_names()
        with database.connect() as connection:
            remaining = connection.execute(
                text(
                    "SELECT task_row_id, attempt_started_at, requested_vcpu, requested_memory, requested_disk "
                    "FROM sandboxbuildreservation"
                )
            ).all()

        assert [tuple(row) for row in remaining] == [(building.id, building.started_at, 1, 2, 20)]

        repeated_upgrade = _run_alembic(migration_database_url, "upgrade", "head")
        assert repeated_upgrade.returncode == 0, repeated_upgrade.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM sandboxbuildreservation")).scalar_one() == 0
            statuses = {
                task_id: status for task_id, status in connection.execute(text("SELECT id, status FROM task")).all()
            }

        assert statuses == {
            building.id: TaskStatus.BUILDING,
            ready.id: TaskStatus.PENDING,
            rejected.id: TaskStatus.PENDING,
        }

    @pytest.mark.description(
        "The shared grant handoff migration keeps grant history and leaves no open build reservations."
    )
    def test_shared_grant_handoff_preserves_history_and_records_new_offers(
        self,
        migration_database_url: str,
        database: Engine,
        admission_rows: tuple[list[Task], ExecutorDispatch],
    ) -> None:
        tasks, dispatch = admission_rows
        building, ready, rejected = tasks
        upgrade = _run_alembic(migration_database_url, "upgrade", _SHARED_POOL_PREDECESSOR)
        assert upgrade.returncode == 0, upgrade.stderr
        with database.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO sandboxbuildreservation "
                    "(task_row_id, attempt_started_at, reserved_at, pool_id, target_id, sandbox_class, "
                    "requested_vcpu, requested_memory, requested_disk, requested_gpu) "
                    "VALUES (:id, :attempt, :attempt, 'pool_migration', 'region-a', 'sysbox', 1, 2, 3, 0)"
                ),
                {"id": building.id, "attempt": building.started_at},
            )

        upgrade = _run_alembic(migration_database_url, "upgrade", _SHARED_POOL_REVISION)
        assert upgrade.returncode == 0, upgrade.stderr
        inspector = inspect(database)
        columns = {column["name"]: column for column in inspector.get_columns("sandboxbuildreservation")}
        assert columns["executor_dispatch_id"]["nullable"]
        assert columns["accepted_at"]["nullable"]
        candidate_columns = {column["name"]: column for column in inspector.get_columns("sandboxadmissioncandidate")}
        assert not candidate_columns["readiness_checked_at"]["nullable"]
        assert candidate_columns["readiness_requested_at"]["nullable"]
        assert inspector.get_pk_constraint("sandboxbuildreservation")["constrained_columns"] == [
            "task_row_id",
            "attempt_started_at",
        ]
        assert any(
            key["constrained_columns"] == ["executor_dispatch_id"] and key["referred_table"] == "executordispatch"
            for key in inspector.get_foreign_keys("sandboxbuildreservation")
        )
        with database.begin() as connection:
            historical = connection.execute(
                text(
                    "SELECT reserved_at, accepted_at, executor_dispatch_id FROM sandboxbuildreservation "
                    "WHERE task_row_id = :id AND attempt_started_at = :attempt"
                ),
                {"id": building.id, "attempt": building.started_at},
            ).one()
            assert historical.accepted_at == historical.reserved_at
            assert historical.executor_dispatch_id is None
            assert (
                connection.execute(
                    text("UPDATE task SET status = 'BUILDING' WHERE id = :id RETURNING id"),
                    {"id": ready.id},
                ).scalar_one()
                == ready.id
            )
            grant = connection.execute(
                text(
                    "INSERT INTO sandboxbuildreservation "
                    "(task_row_id, attempt_started_at, reserved_at, executor_dispatch_id, pool_id, target_id, "
                    "sandbox_class, requested_vcpu, requested_memory, requested_disk, requested_gpu) "
                    "VALUES (:id, :attempt, :reserved_at, :dispatch, 'pool_migration', 'region-a', "
                    "'sysbox', 1, 2, 3, 0) RETURNING accepted_at, executor_dispatch_id"
                ),
                {
                    "id": ready.id,
                    "attempt": ready.started_at,
                    "reserved_at": datetime.now(UTC),
                    "dispatch": dispatch.id,
                },
            ).one()
            assert grant.accepted_at is None
            assert grant.executor_dispatch_id == dispatch.id
            connection.execute(
                text(
                    "INSERT INTO sandboxadmissioncandidate "
                    "(task_row_id, attempt_started_at, executor_dispatch_id, admission_ready, "
                    "readiness_checked_at, readiness_requested_at, pool_id, target_id, sandbox_class, "
                    "requested_vcpu, requested_memory, requested_disk, requested_gpu) "
                    "VALUES (:id, :attempt, :dispatch, false, :checked, :requested, "
                    "'pool_migration', 'region-a', 'sysbox', 1, 2, 3, 0)"
                ),
                {
                    "id": rejected.id,
                    "attempt": rejected.started_at,
                    "dispatch": dispatch.id,
                    "checked": datetime.now(UTC),
                    "requested": datetime.now(UTC),
                },
            )

        with database.begin() as connection:
            candidate = connection.execute(
                text(
                    "SELECT readiness_requested_at FROM sandboxadmissioncandidate "
                    "WHERE task_row_id = :id AND attempt_started_at = :attempt"
                ),
                {"id": rejected.id, "attempt": rejected.started_at},
            ).one()
            assert candidate.readiness_requested_at is not None
            assert (
                connection.execute(
                    text(
                        "DELETE FROM sandboxadmissioncandidate "
                        "WHERE task_row_id = :id AND attempt_started_at = :attempt RETURNING task_row_id"
                    ),
                    {"id": rejected.id, "attempt": rejected.started_at},
                ).scalar_one()
                == rejected.id
            )
            released = (
                connection.execute(
                    text(
                        "UPDATE sandboxbuildreservation SET released_at = :now "
                        "WHERE task_row_id IN (:building, :ready) RETURNING task_row_id"
                    ),
                    {"now": datetime.now(UTC), "building": building.id, "ready": ready.id},
                )
                .scalars()
                .all()
            )
            assert set(released) == {building.id, ready.id}
            stopped = (
                connection.execute(
                    text("UPDATE task SET status = 'STOPPED' WHERE id IN (:building, :ready, :rejected) RETURNING id"),
                    {"building": building.id, "ready": ready.id, "rejected": rejected.id},
                )
                .scalars()
                .all()
            )
            assert set(stopped) == {building.id, ready.id, rejected.id}
        downgrade = _run_alembic(migration_database_url, "downgrade", _SHARED_POOL_PREDECESSOR)
        assert downgrade.returncode == 0, downgrade.stderr
        columns_after = {column["name"] for column in inspect(database).get_columns("sandboxbuildreservation")}
        assert "accepted_at" not in columns_after
        assert "executor_dispatch_id" not in columns_after
        candidate_columns_after = {
            column["name"] for column in inspect(database).get_columns("sandboxadmissioncandidate")
        }
        assert "readiness_checked_at" not in candidate_columns_after
        assert "readiness_requested_at" not in candidate_columns_after
        with database.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM sandboxbuildreservation")).scalar_one() == 2
            assert (
                connection.execute(
                    text("SELECT count(*) FROM sandboxbuildreservation WHERE released_at IS NULL")
                ).scalar_one()
                == 0
            )


@pytest.mark.description(
    "Vals keeps the bucket it already uses. Tracker creates every other organization's bucket later."
)
def test_org_bucket_migration_records_the_deployment_bucket_for_vals_only(
    migration_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_S3_BUCKET", "agentic-harness-test")
    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            [{"id": uuid4(), "name": "vals.ai"}, {"id": uuid4(), "name": "acme.example"}],
        )

    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_REVISION)

    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        buckets = {row.name: row.storage_key for row in connection.execute(text("SELECT name, storage_key FROM org"))}
    assert buckets["vals.ai"] == "agentic-harness-test"
    assert buckets["acme.example"] is None
    downgrade = _run_alembic(migration_database_url, "downgrade", _ORG_BUCKET_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    engine.dispose()


@pytest.mark.description(
    "The flag is gone after upgrade. Downgrade restores it so the previous Tracker can still resume AWS runs."
)
def test_dropping_aws_managed_downgrades_with_every_aws_run_marked_managed(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    org_id = uuid4()
    contract = AgentContractRequest(name="migration-test-agent", install_cmd="true", run_cmd="true")
    runs = {
        "aws": AWSBenchmarkArguments(contract=contract, concurrency=1).model_dump_json(),
        "local": LocalBenchmarkArguments(
            contract=contract,
            concurrency=1,
            properties=LocalResources(data_root=Path("/tmp/valkyrie")),
            sandbox_provider="docker",
        ).model_dump_json(),
    }
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": "org"})
        for name, arguments in runs.items():
            connection.execute(
                text(
                    "INSERT INTO benchmark"
                    " (id, org_id, name, started_at, finished_at, status, arguments, docent_reading_status)"
                    " VALUES (:id, :org_id, :name, now(), now(), CAST(:status AS benchmarkstatus),"
                    " CAST(:arguments AS json), CAST('IDLE' AS docentreadingstatus))"
                ),
                {
                    "id": uuid4(),
                    "org_id": org_id,
                    "name": name,
                    "status": BenchmarkStatus.FINISHED.value,
                    "arguments": arguments,
                },
            )

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_AWS_MANAGED_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert "aws_managed" not in {column["name"] for column in inspect(engine).get_columns("benchmark")}

    downgrade = _run_alembic(migration_database_url, "downgrade", _ORG_BUCKET_REVISION)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        flags = dict(connection.execute(text("SELECT name, aws_managed FROM benchmark")).all())
    assert flags == {"aws": True, "local": False}
    engine.dispose()


@pytest.mark.description(
    "Project handles are unique per org while names may repeat, a run's last-appended link is its project even when link timestamps tie, and downgrade removes both tables."
)
def test_project_migration_matches_the_models_and_downgrades(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    for table in ("project", "run_project"):
        assert {column["name"]: column["nullable"] for column in inspector.get_columns(table)} == {
            column.name: column.nullable for column in SQLModel.metadata.tables[table].columns
        }
    assert [
        (constraint["name"], constraint["column_names"]) for constraint in inspector.get_unique_constraints("project")
    ] == [("unique_project_handle_per_org", ["org_id", "handle"])]
    assert [(index["name"], index["column_names"]) for index in inspector.get_indexes("run_project")] == [
        ("ix_run_project_benchmark_id", ["benchmark_id"])
    ]
    assert [
        (foreign_key["constrained_columns"], foreign_key["referred_table"], foreign_key["referred_columns"])
        for foreign_key in inspector.get_foreign_keys("project")
    ] == [(["org_id"], "org", ["id"])]
    assert sorted(
        (foreign_key["constrained_columns"], foreign_key["referred_table"], foreign_key["referred_columns"])
        for foreign_key in inspector.get_foreign_keys("run_project")
    ) == [(["benchmark_id"], "benchmark", ["id"]), (["project_id"], "project", ["id"])]
    assert [column["name"] for column in inspector.get_columns("run_project") if column.get("identity")] == ["id"]

    org_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": "org"})
    with Session(engine) as session:
        run, reversed_run = (
            Benchmark(
                id=uuid4(),
                org_id=org_id,
                name="projects",
                arguments=AWSBenchmarkArguments(
                    contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"), concurrency=1
                ),
            )
            for _ in range(2)
        )
        session.execute(
            text(
                "INSERT INTO benchmark (id, org_id, name, started_at, status, arguments) "
                "VALUES (:id, :org_id, :name, now(), CAST(:status AS benchmarkstatus), CAST(:arguments AS json))"
            ),
            [
                {
                    "id": benchmark.id,
                    "org_id": org_id,
                    "name": benchmark.name,
                    "status": BenchmarkStatus.IN_PROGRESS.value,
                    "arguments": benchmark.arguments.model_dump_json(),
                }
                for benchmark in (run, reversed_run)
            ],
        )
        router_bench = Project(
            org_id=org_id,
            handle="router-bench",
            name="Router Bench",
            spend_type=SpendType.DEVELOPMENT,
            subcategory="new_benchmark_generation",
        )
        google_report = Project(
            org_id=org_id,
            handle="google-report-xyz",
            name="Google Report XYZ",
            spend_type=SpendType.CUSTOMER,
            subcategory="report",
            customer="google",
            extra_tags={"engagement": "q4"},
        )
        session.add_all([router_bench, google_report])
        session.flush()
        moved_at = datetime.now(UTC)
        session.add_all(
            [
                RunProject(benchmark_id=run.id, project_id=router_bench.id, created_at=moved_at),
                RunProject(benchmark_id=run.id, project_id=google_report.id, created_at=moved_at),
                RunProject(benchmark_id=reversed_run.id, project_id=google_report.id, created_at=moved_at),
                RunProject(benchmark_id=reversed_run.id, project_id=router_bench.id, created_at=moved_at),
            ]
        )
        session.commit()
        reversed_current = ProjectRepository(session).get_for_run(reversed_run.id)
        assert reversed_current is not None
        assert reversed_current.handle == "router-bench"
        newest = ProjectRepository(session).get_for_run(run.id)
        assert newest is not None
        assert (newest.handle, newest.name, newest.spend_type, newest.extra_tags) == (
            "google-report-xyz",
            "Google Report XYZ",
            SpendType.CUSTOMER,
            {"engagement": "q4"},
        )
        session.add(
            Project(
                org_id=org_id,
                handle="router-bench-2",
                name="Router Bench 2",
                spend_type=SpendType.INTERNAL,
                subcategory="blog",
            )
        )
        session.commit()
        session.add(
            Project(
                org_id=org_id,
                handle="router-bench",
                name="Another name",
                spend_type=SpendType.INTERNAL,
                subcategory="blog",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    downgrade = _run_alembic(migration_database_url, "downgrade", _PROJECT_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    assert {"project", "run_project"}.isdisjoint(inspect(engine).get_table_names())
    with engine.connect() as connection:
        assert connection.execute(text("SELECT 1 FROM pg_type WHERE typname = 'spendtype'")).first() is None
    engine.dispose()


@pytest.mark.description(
    "Projects saved before the column exists read as unarchived, and downgrade drops only the column."
)
def test_project_archived_at_migration_keeps_existing_projects_unarchived_and_downgrades(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", "265bacf0f12a")
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    org_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": "archive-org"}
        )
        connection.execute(
            text(
                "INSERT INTO project (id, org_id, handle, name, spend_type, extra_tags, created_at) "
                "VALUES (:id, :org_id, 'kept', 'Kept', 'INTERNAL', '{}', now())"
            ),
            {"id": uuid4(), "org_id": org_id},
        )

    upgrade = _run_alembic(migration_database_url, "upgrade", "3f8b2c6d1e4a")
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        assert connection.execute(text("SELECT archived_at FROM project WHERE handle = 'kept'")).scalar_one() is None

    downgrade = _run_alembic(migration_database_url, "downgrade", "265bacf0f12a")
    assert downgrade.returncode == 0, downgrade.stderr
    assert "archived_at" not in {column["name"] for column in inspect(engine).get_columns("project")}
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM project")).scalar_one() == 1
    engine.dispose()


@pytest.mark.description(
    "Organizations saved before the column exists, vals.ai included, require no project. Downgrade drops only the column."
)
def test_org_requires_project_migration_leaves_every_org_off_and_downgrades(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", "3f8b2c6d1e4a")
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    with engine.begin() as connection:
        for name in ("vals.ai", "other-org"):
            connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": uuid4(), "name": name})

    upgrade = _run_alembic(migration_database_url, "upgrade", "3222f9e0a6ce")
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT name, requires_project FROM org ORDER BY name")).all()
    assert [tuple(row) for row in rows] == [("default", False), ("other-org", False), ("vals.ai", False)]

    downgrade = _run_alembic(migration_database_url, "downgrade", "3f8b2c6d1e4a")
    assert downgrade.returncode == 0, downgrade.stderr
    assert "requires_project" not in {column["name"] for column in inspect(engine).get_columns("org")}
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM org")).scalar_one() == 3
    engine.dispose()


def _insert_projects(engine: Engine, org_name: str, projects: list[tuple[str, str, bool]]) -> None:
    org_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": org_name})
        for handle, name, archived in projects:
            connection.execute(
                text(
                    "INSERT INTO project (id, org_id, handle, name, spend_type, extra_tags, created_at, archived_at) "
                    "VALUES (:id, :org_id, :handle, :name, 'INTERNAL', '{}', now(), "
                    "CASE WHEN :archived THEN now() END)"
                ),
                {"id": uuid4(), "org_id": org_id, "handle": handle, "name": name, "archived": archived},
            )


@pytest.mark.description(
    "Duplicate project names, archived ones included, stop the migration with a list of what to rename."
)
def test_unique_project_name_migration_lists_duplicates_and_adds_nothing(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _UNIQUE_PROJECT_NAME_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    _insert_projects(
        engine,
        "vals.ai",
        [("hy4", "Hy4", False), ("hy4-b", "hy4 ", False), ("swarms", "Swarms", True), ("swarms-2", "Swarms", True)],
    )
    _insert_projects(engine, "other-org", [("hy4", "Hy4", False)])

    upgrade = _run_alembic(migration_database_url, "upgrade", _UNIQUE_PROJECT_NAME_REVISION)

    assert upgrade.returncode == 1
    assert (
        "Rename these projects so each name is used once per organization, then rerun: "
        "org 'vals.ai' name 'hy4' held by hy4, hy4-b; "
        "org 'vals.ai' name 'swarms' held by swarms, swarms-2"
    ) in upgrade.stderr
    assert "uq_project_org_name" not in {index["name"] for index in inspect(engine).get_indexes("project")}
    engine.dispose()


@pytest.mark.description(
    "On distinct names the migration adds the per-org name index, which then refuses a case variant."
)
def test_unique_project_name_migration_adds_the_index_and_downgrades(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _UNIQUE_PROJECT_NAME_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    _insert_projects(engine, "vals.ai", [("hy4", "Hy4", False), ("swarms", "Swarms", True)])
    _insert_projects(engine, "other-org", [("hy4", "Hy4", False)])

    upgrade = _run_alembic(migration_database_url, "upgrade", _UNIQUE_PROJECT_NAME_REVISION)

    assert upgrade.returncode == 0, upgrade.stderr
    assert "uq_project_org_name" in {index["name"] for index in inspect(engine).get_indexes("project")}
    with pytest.raises(IntegrityError):
        _insert_projects(engine, "third-org", [("a", "Same", False), ("b", " same", False)])
    downgrade = _run_alembic(migration_database_url, "downgrade", _UNIQUE_PROJECT_NAME_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    assert "uq_project_org_name" not in {index["name"] for index in inspect(engine).get_indexes("project")}
    engine.dispose()


def _schema_snapshot(engine: Engine) -> dict[str, object]:
    inspector = inspect(engine)
    tables = sorted(inspector.get_table_names())
    return {
        table: (
            sorted(
                (column["name"], str(column["type"]), column["nullable"]) for column in inspector.get_columns(table)
            ),
            sorted(
                (key["name"], tuple(key["constrained_columns"]), key["referred_table"])
                for key in inspector.get_foreign_keys(table)
            ),
            sorted(
                (constraint["name"], tuple(constraint["column_names"]))
                for constraint in inspector.get_unique_constraints(table)
            ),
        )
        for table in tables
    }


@pytest.mark.description(
    "The epoch cleanup migration drops the epoch tables and columns, and its downgrade restores the previous schema."
)
def test_epoch_cleanup_migration_round_trips(migration_database_url: str) -> None:
    engine = create_engine(migration_database_url)
    upgrade = _run_alembic(migration_database_url, "upgrade", "a7d2c4e9f1b3")
    assert upgrade.returncode == 0, upgrade.stderr
    before = _schema_snapshot(engine)
    upgrade = _run_alembic(migration_database_url, "upgrade", "d4e8a1c6b9f2")
    assert upgrade.returncode == 0, upgrade.stderr
    inspector = inspect(engine)
    assert not {"migrationepoch", "migrationparticipant"} & set(inspector.get_table_names())
    assert "active_epoch_id" not in {column["name"] for column in inspector.get_columns("executoradmission")}
    assert not {
        "frozen_epoch_id",
        "frozen_remaining_seconds",
        "queued_park_invocation_digest",
        "superseded_by_dispatch_id",
        "successor_launch_claimed_at",
    } & {column["name"] for column in inspector.get_columns("executordispatch")}
    downgrade = _run_alembic(migration_database_url, "downgrade", "a7d2c4e9f1b3")
    assert downgrade.returncode == 0, downgrade.stderr
    assert _schema_snapshot(engine) == before
    engine.dispose()
