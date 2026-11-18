from logging.config import fileConfig

from alembic import context
from alembic.script import ScriptDirectory
from alembic.script.revision import RevisionError
from sqlalchemy import Connection, engine_from_config, inspect, pool, text
from sqlmodel import SQLModel

from tracker.config import build_database_url
from tracker.database.models import *  # noqa: F403

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

config.set_main_option("sqlalchemy.url", build_database_url())

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = SQLModel.metadata

_MIGRATION_ADVISORY_LOCK_ID = 0x56414C4B59524945
_PROJECT_DROP_REVISION = "f1d3b5a7c9e2"
_PROJECT_COPY_COUNTS = text(
    "SELECT (SELECT count(*) FROM project), "
    "(SELECT count(*) FROM project p JOIN project_backfill b ON b.project_id = p.id AND b.confirmed), "
    "(SELECT count(*) FROM project_cutover)"
)
_PROJECT_COPY_TABLES = ("project_backfill", "project_cutover")
_UNCOPIED_PROJECT_LIMIT = 20
_UNCOPIED_PROJECTS = text(
    "SELECT p.org_id, p.id FROM project p WHERE NOT EXISTS "
    "(SELECT 1 FROM project_backfill b WHERE b.project_id = p.id AND b.confirmed) "
    f"ORDER BY p.org_id, p.id LIMIT {_UNCOPIED_PROJECT_LIMIT}"
)
_ALL_PROJECTS = text(f"SELECT org_id, id FROM project ORDER BY org_id, id LIMIT {_UNCOPIED_PROJECT_LIMIT}")

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _upgrade_drops_projects(connection: Connection) -> bool:
    migration = context.get_context()
    destination = migration.opts.get("destination_rev")
    if destination is None or not inspect(connection).has_table("project"):
        return False
    script = ScriptDirectory.from_config(config)
    try:
        pending = script.iterate_revisions(destination, migration.get_current_heads())
        return any(step.revision == _PROJECT_DROP_REVISION for step in pending)
    except RevisionError:
        return False


def _refuse_incomplete_project_copy(connection: Connection) -> None:
    if not _upgrade_drops_projects(connection):
        return
    copy_tables = all(inspect(connection).has_table(table) for table in _PROJECT_COPY_TABLES)
    if copy_tables:
        projects, copied, cutovers = connection.execute(_PROJECT_COPY_COUNTS).one()
    else:
        projects, copied, cutovers = connection.execute(text("SELECT count(*) FROM project")).scalar_one(), 0, 0
    if projects and (copied != projects or cutovers != 1):
        uncopied = connection.execute(_UNCOPIED_PROJECTS if copy_tables else _ALL_PROJECTS).all()
        listed = ", ".join(f"{org_id}/{project_id}" for org_id, project_id in uncopied) or "none"
        raise RuntimeError(
            "Refusing to drop project tables until every project has a confirmed copy and there is one cutover row: "
            f"project={projects} confirmed_copies={copied} project_cutover={cutovers} "
            f"uncopied_org_id/id (first {_UNCOPIED_PROJECT_LIMIT}): {listed}"
        )


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        connection.execute(
            text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
        )
        connection.commit()
        try:
            context.configure(connection=connection, target_metadata=target_metadata)

            with context.begin_transaction():
                _refuse_incomplete_project_copy(connection)
                context.run_migrations()
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
            )
            connection.commit()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
