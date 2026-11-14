from logging.config import fileConfig

from alembic import context
from projects.database import engine
from projects.models import Base
from sqlalchemy import Connection

if context.config.config_file_name is not None:
    fileConfig(context.config.config_file_name, disable_existing_loggers=False)


def run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


bound = context.config.attributes.get("connection")
if isinstance(bound, Connection):
    run_migrations(bound)
else:
    with engine().connect() as connection:
        run_migrations(connection)
