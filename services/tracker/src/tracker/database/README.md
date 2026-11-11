# Database and migrations

The Tracker API and separate executor control service use the same PostgreSQL database through SQLModel.

- Alembic owns schema migrations; neither API nor control startup applies DDL.
- The explicit migration task runs before the services in Docker Compose, and the local Tracker target runs Alembic before starting control and the API.

## Add a table

Declare the model in `src/tracker/database/models.py` with `table=True`:

```python
class Benchmark(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: datetime | None = None
```

Then import it in `src/tracker/database/session.py` and add it to `_exposed_models`, so
SQLModel sees it.

## Generate and apply a migration

From `services/tracker`:

```bash
# Generate a migration; review it before applying
make migrate-gen

# Explicitly apply it (never by starting the API or control service)
uv run alembic upgrade head

# pytest-alembic checks, also run in CI
make test-alembic
```

New migrations land in `src/tracker/database/migrations/versions/`.

## Fix a migration that fails with `NameError`

Autogenerate sometimes refers to a custom column type by its module path without importing
it:

```text
sa.Column('arguments', tracker.database.models.BenchmarkArgumentsType(), ...),
NameError: name 'tracker' is not defined
```

Import the type at the top of the migration, use its short name in the column, then run
`uv run alembic upgrade head` again:

```python
from tracker.database.models import BenchmarkArgumentsType

sa.Column('arguments', BenchmarkArgumentsType(), nullable=True),
```
