import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from projects.models import Base, Project
from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError

ALEMBIC_INI = Path(__file__).parent.parent / "alembic.ini"


@pytest.mark.description(
    "A new revision renders from the template, follows the current head, and a database upgrades to it."
)
def test_new_revision_renders_from_template(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    _ = shutil.copytree(ALEMBIC_INI.parent / "projects" / "migrations", migrations)
    config = Config(ALEMBIC_INI)
    config.set_main_option("script_location", str(migrations))
    head = ScriptDirectory.from_config(config).get_current_head()
    script = command.revision(config, message="next", rev_id="next")

    assert not isinstance(script, list) and script is not None
    assert script.down_revision == head
    bound = create_engine(f"sqlite:///{tmp_path / 'projects.db'}")
    with bound.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
    with bound.connect() as connection:
        assert MigrationContext.configure(connection).get_current_revision() == "next"


@pytest.mark.description(
    "Upgrading an empty database to head builds the schema the models declare, and refuses a second name that differs only in case or spaces."
)
def test_upgrade_matches_models(tmp_path: Path) -> None:
    bound = create_engine(f"sqlite:///{tmp_path / 'projects.db'}")
    config = Config(ALEMBIC_INI)
    with bound.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    with bound.connect() as connection:
        assert compare_metadata(MigrationContext.configure(connection), Base.metadata) == []

    org_id = uuid4()
    row: dict[str, object] = {
        "org_id": org_id,
        "spend_type": "development",
        "extra_tags": {},
        "created_at": datetime.now(UTC),
    }
    with bound.begin() as connection:
        _ = connection.execute(insert(Project), [row | {"id": uuid4(), "handle": "router", "name": "Router Bench"}])
    with pytest.raises(IntegrityError), bound.begin() as connection:
        _ = connection.execute(insert(Project), [row | {"id": uuid4(), "handle": "other", "name": " router bench "}])


@pytest.mark.description("At head, a handle and name are unique among live projects only, so a deleted row frees them.")
def test_deleted_rows_free_handle_and_name(tmp_path: Path) -> None:
    bound = create_engine(f"sqlite:///{tmp_path / 'projects.db'}")
    config = Config(ALEMBIC_INI)
    with bound.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    row: dict[str, object] = {
        "org_id": uuid4(),
        "handle": "router",
        "name": "Router Bench",
        "spend_type": "development",
        "extra_tags": {},
        "created_at": datetime.now(UTC),
    }
    with bound.begin() as connection:
        _ = connection.execute(insert(Project), [row | {"id": uuid4(), "deleted_at": datetime.now(UTC)}])
        _ = connection.execute(insert(Project), [row | {"id": uuid4()}])
    with pytest.raises(IntegrityError), bound.begin() as connection:
        _ = connection.execute(insert(Project), [row | {"id": uuid4(), "name": "Other"}])
