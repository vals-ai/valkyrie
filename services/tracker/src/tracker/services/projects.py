from uuid import UUID

from sqlmodel import Session

from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Project
from tracker.database.session import engine
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.project import ProjectRepository
from tracker.types import ProjectResponse


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
