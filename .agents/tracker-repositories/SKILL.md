---
name: tracker-repositories
description: Use when adding or changing Tracker code that reads or writes the database (routes, executor, scheduler, or the `tracker.services` modules they call); keeps SQLModel/SQLAlchemy queries out of those paths by pushing them into session-injected repositories in `services/tracker/src/tracker/repositories/`.
---

# Tracker repositories

New and changed Tracker code keeps ORM code out of the main paths, with no exceptions. Routers, `main.py`, the executor, the scheduler and the helpers they call don't build `select`/`update`/`delete` statements or call `session.add`/`delete`/`exec`; they call a repository method instead. Existing inline queries in those paths move into a repository when you change them. Each repository lives in `services/tracker/src/tracker/repositories/`, owns one API domain (projects, benchmarks, costs, ...), and wraps a `Session`.

## Rules

- The constructor is `def __init__(self, session: Session = Depends(get_session)) -> None` and stores `self._session`.
- The module exports `XRepositoryDependency = Annotated[XRepository, Depends(XRepository)]`. Do not add `get_x_repository` getters or lambdas; lambdas fail basedpyright strict.
- Routes and helper dependencies that only need the database take `XRepositoryDependency`, not a `Session` plus `XRepository(session)`. Existing routes still take a raw `Session` where they call `rollback`/`refresh`/`close` or pass it to `get_scoped` and model helpers (`Benchmark.fetch_task_state_counts`); new queries in those routes still go in a repository.
- Code outside FastAPI injection (`main.py` startup/recovery, executor, scheduler, `tracker.services` modules shared with them) constructs `XRepository(session)` and passes the repository, not the session, into shared helpers.
- Methods return models or plain values and raise domain errors from `tracker.exceptions` (for example `ProjectHandleTakenError`); routes map those to `HTTPException`. Repositories never raise `HTTPException`.
- Keep transaction ownership where it was. A method commits only when it is a complete unit of work (`ProjectRepository.create`); methods used inside a caller's transaction or lock (`BenchmarkRepository.stop_tasks`, `EgressPolicyRepository.revoke_attempts`) do not commit.
- Add a method to the domain's existing repository before creating a new one. Delete one-line wrappers a repository method makes redundant.

## Example

```python
# services/tracker/src/tracker/repositories/project.py
class ProjectRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def create(self, project: Project) -> Project:
        self._session.add(project)
        try:
            self._session.commit()
        except IntegrityError as error:
            self._session.rollback()
            raise ProjectHandleTakenError(project.handle) from error
        return project

    def get(self, org_id: UUID, handle: str) -> Project | None:
        return self._session.exec(
            select(Project).where(col(Project.org_id) == org_id, col(Project.handle) == handle)
        ).first()


ProjectRepositoryDependency = Annotated[ProjectRepository, Depends(ProjectRepository)]
```

```python
# services/tracker/src/tracker/api/projects.py
@router.post("", response_model=ProjectResponse)
def create_project(
    request: ProjectCreateRequest,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    project = Project(org_id=identity.org.id, created_by_email=identity.email, **request.model_dump())
    try:
        repository.create(project)
    except ProjectHandleTakenError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return ProjectResponse.from_row(project)
```

Outside a route, `ProjectRepository(session).save(project)`.

## Tests

Routes pick up the test session through the existing `dependency_overrides[get_session]`, so repositories need no overrides of their own; non-route tests construct `XRepository(session)`. Choose and run proof with the `writing-tests` skill.
