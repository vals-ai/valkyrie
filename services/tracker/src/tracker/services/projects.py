from uuid import UUID
import argparse

import httpx
from sqlmodel import Session

from tracker.clients.projects import ProjectServiceClient, ProjectServiceUnavailableError
from tracker.database.models import Project
from tracker.database.session import engine
from tracker.database import session as database_session
from tracker.logging import get_logger
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.project import ProjectRepository
from tracker.types import ProjectResponse


logger = get_logger(__name__)


def migrate_projects(*, reconcile: bool = False) -> None:
    with Session(database_session.engine, expire_on_commit=False) as session:
        repository = ProjectRepository(session)
        try:
            copied, skipped = repository.backfill(reconcile=reconcile)
        except (ProjectServiceUnavailableError, httpx.HTTPError):
            if reconcile:
                raise
            logger.warning(
                "Project service unavailable during startup backfill, deferring to deployment reconciliation"
            )
        else:
            logger.info(
                "Project service %s complete: copied=%s skipped=%s",
                "reconciliation" if reconcile else "backfill",
                copied,
                skipped,
            )
        if reconcile:
            repository.complete_cutover()
        else:
            repository.finish_startup_backfill()


def _gateway_ids(project: Project) -> tuple[UUID | None, UUID | None]:
    return project.gateway_request_id, project.gateway_key_id


def copy_projects(
    organizations: OrganizationRepository, projects: ProjectRepository, client: ProjectServiceClient
) -> dict[UUID, bool]:
    by_org: dict[UUID, list[Project]] = {org_id: [] for org_id in organizations.list_ids()}
    for project in projects.list_every():
        by_org[project.org_id].append(project)
    equal: dict[UUID, bool] = {}
    for org_id, rows in by_org.items():
        client.replace(org_id, rows)
        equal[org_id] = client.list_projects(org_id) == [ProjectResponse.from_row(row) for row in rows] and all(
            _gateway_ids(client.record(org_id, row.id)) == _gateway_ids(row) for row in rows
        )
    return equal


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", nargs="?", choices=("copy", "reconcile"), default="copy")
    args = parser.parse_args()
    if args.operation == "reconcile":
        migrate_projects(reconcile=True)
        return
    with Session(engine) as session:
        equal = copy_projects(
            OrganizationRepository(session), ProjectRepository(session), ProjectServiceClient.from_environment()
        )
    for org_id, same in equal.items():
        print(f"org {org_id}: {'equal' if same else 'DIFFERENT'}")
    print(f"{sum(equal.values())} of {len(equal)} orgs equal")
    raise SystemExit(0 if all(equal.values()) else 1)


if __name__ == "__main__":
    main()
