from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.clients.projects import ProjectServiceClient, ProjectServiceUnavailableError, UnavailableAwareTransport
from tracker.database.models import Project, ProjectBackfill, ProjectCutover, RunProject
from tracker.domain import SpendType
from tracker.exceptions import ProjectMigrationConflictError
from tracker.repositories.project import ProjectRepository, project_hash
from tracker.services.projects import migrate_projects


class TestProjectBackfill:
    @pytest.mark.description(
        "A concurrent importer keeps the first saved project when the same ID is already inserted."
    )
    def test_concurrent_import(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.DEVELOPMENT)
        tracker_database.add(source)
        tracker_database.commit()

        def handle(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                project_service.seed(source)
                project_service.rows[str(source.id)]["name"] = "Concurrent service edit"
                return httpx.Response(409, request=request)
            return project_service.handle(request)

        client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(handle)))
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)

        assert ProjectRepository(tracker_database).backfill() == (0, 1)

        assert len(project_service.rows) == 1
        assert project_service.rows[str(source.id)]["name"] == "Concurrent service edit"

    @pytest.mark.description(
        "An unavailable service blocks an unconfirmed import but allows a confirmed project to reboot."
    )
    @pytest.mark.parametrize("status", [401, 500, 502, 503, 504, None])
    @pytest.mark.parametrize("confirmed", [None, False, True])
    def test_unavailable_service(
        self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch, status: int | None, confirmed: bool | None
    ) -> None:
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.DEVELOPMENT)
        tracker_database.add(source)
        tracker_database.commit()
        if confirmed is not None:
            tracker_database.add(
                ProjectBackfill(
                    project_id=source.id, org_id=source.org_id, source_hash=project_hash(source), confirmed=confirmed
                )
            )
            tracker_database.commit()

        def handle(request: httpx.Request) -> httpx.Response:
            if status is None:
                raise httpx.ConnectError("Project service unavailable", request=request)
            return httpx.Response(status, request=request)

        client = ProjectServiceClient(
            httpx.Client(base_url="http://projects", transport=UnavailableAwareTransport(httpx.MockTransport(handle)))
        )
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        warnings: list[str] = []
        monkeypatch.setattr("tracker.services.projects.logger.warning", warnings.append)

        if confirmed:
            migrate_projects()
        else:
            with pytest.raises(ProjectMigrationConflictError, match="no confirmed"):
                migrate_projects()

        assert tracker_database.get(ProjectCutover, 1) is None

        assert any("unavailable during startup backfill" in warning for warning in warnings)
        error = httpx.HTTPStatusError if status in (401, 500) else ProjectServiceUnavailableError
        with pytest.raises(error):
            migrate_projects(reconcile=True)

        assert tracker_database.get(Project, source.id) is not None

    @pytest.mark.description(
        "Repeated upgrades preserve archived projects, run links, service edits and deletions without duplicate projects."
    )
    def test_repeated_upgrade(self, tracker_database: Session, project_service: MockProjectRoutes) -> None:
        created_at = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
        projects = [
            Project(
                org_id=TEST_ORG_ID,
                handle=handle,
                name=handle.title(),
                spend_type=spend_type,
                created_at=created_at,
                created_by_email="owner@example.com",
                archived_at=created_at if handle == "archived" else None,
                gateway_request_id=UUID("00000000-0000-0000-0000-000000000001"),
                gateway_key_id=UUID("00000000-0000-0000-0000-000000000002"),
                extra_tags={"team": "migration"},
            )
            for handle, spend_type in (
                ("active", SpendType.DEVELOPMENT),
                ("archived", SpendType.CUSTOMER),
                ("deleted", SpendType.INTERNAL),
                ("edited", SpendType.INTERNAL),
            )
        ]
        for project in projects:
            tracker_database.add(project)
        run = make_benchmark(session=tracker_database)
        tracker_database.add(RunProject(benchmark_id=run.id, project_id=projects[0].id))
        tracker_database.commit()
        project_service.seed(*projects[2:])
        project_service.rows[str(projects[2].id)]["deleted_at"] = created_at.isoformat()
        project_service.rows[str(projects[3].id)]["name"] = "Newer service edit"
        repository = ProjectRepository(tracker_database)

        assert repository.backfill() == (2, 2)
        assert repository.backfill() == (0, 4)

        listed = {project.id: project for project in repository.list_all(TEST_ORG_ID, include_archived=True)}
        assert len(project_service.rows) == 4
        assert set(listed) == {projects[index].id for index in (0, 1, 3)}
        for project in projects[:2]:
            assert listed[project.id].model_dump() == project.model_dump()
        assert listed[projects[3].id].name == "Newer service edit"
        assert project_service.rows[str(projects[2].id)]["deleted_at"] == created_at.isoformat()
        linked = repository.get_for_run(run.id, strict=True)
        assert linked is not None
        assert linked.id == projects[0].id

    @pytest.mark.description("An empty Tracker project table needs no migration writes.")
    def test_empty_upgrade(self, tracker_database: Session) -> None:
        assert ProjectRepository(tracker_database).backfill() == (0, 0)

    @pytest.mark.description(
        "Startup skips a conflicting project and copies other rows while reconciliation still fails."
    )
    @pytest.mark.parametrize("field", ["handle", "name"])
    def test_collision(
        self, tracker_database: Session, project_service: MockProjectRoutes, field: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.DEVELOPMENT)
        target = Project(org_id=TEST_ORG_ID, handle="target", name="Target", spend_type=SpendType.DEVELOPMENT)
        setattr(target, field, getattr(source, field))
        tracker_database.add(source)
        later = Project(org_id=TEST_ORG_ID, handle="zzz-later", name="Later", spend_type=SpendType.INTERNAL)
        tracker_database.add(later)
        tracker_database.commit()
        project_service.seed(target)

        assert ProjectRepository(tracker_database).backfill() == (1, 1)

        assert project_service.rows[str(later.id)]["handle"] == "zzz-later"

        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        with pytest.raises(ProjectMigrationConflictError, match="no confirmed"):
            migrate_projects()

        with pytest.raises(ProjectMigrationConflictError, match="conflicts with a different project"):
            ProjectRepository(tracker_database).backfill(reconcile=True)

        assert set(project_service.rows) == {str(target.id), str(later.id)}

    @pytest.mark.description("Startup copies a late legacy row before confirming that every project has been imported.")
    def test_late_startup_copy(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        migrate_projects()
        source = Project(org_id=TEST_ORG_ID, handle="late", name="Late", spend_type=SpendType.DEVELOPMENT)
        tracker_database.add(source)
        tracker_database.commit()

        migrate_projects()

        baseline = tracker_database.get(ProjectBackfill, source.id)
        assert baseline is not None and baseline.confirmed
        assert project_service.rows[str(source.id)]["name"] == "Late"

    @pytest.mark.description(
        "A confirmed startup leaves cutover incomplete and a late unconfirmed project blocks startup during an outage."
    )
    def test_startup_does_not_complete_cutover(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.INTERNAL)
        tracker_database.add(source)
        tracker_database.commit()

        migrate_projects()

        baseline = tracker_database.get(ProjectBackfill, source.id)
        assert baseline is not None and baseline.confirmed
        assert tracker_database.get(ProjectCutover, 1) is None
        later = Project(org_id=TEST_ORG_ID, handle="later", name="Later", spend_type=SpendType.INTERNAL)
        tracker_database.add(later)
        tracker_database.commit()
        client = ProjectServiceClient(
            httpx.Client(
                base_url="http://projects",
                transport=UnavailableAwareTransport(
                    httpx.MockTransport(lambda request: httpx.Response(503, request=request))
                ),
            )
        )
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)

        with pytest.raises(ProjectMigrationConflictError, match="no confirmed"):
            migrate_projects()

        assert tracker_database.get(ProjectCutover, 1) is None
        assert str(later.id) not in project_service.rows

    @pytest.mark.description(
        "A completed cutover allows replacements to boot after a conflicting import and service outage."
    )
    @pytest.mark.parametrize("status", [500, 503])
    def test_completed_cutover_outage(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        monkeypatch: pytest.MonkeyPatch,
        status: int,
    ) -> None:
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        migrate_projects(reconcile=True)
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.INTERNAL)
        tracker_database.add(source)
        tracker_database.commit()
        target = Project(org_id=TEST_ORG_ID, handle="source", name="Target", spend_type=SpendType.INTERNAL)
        project_service.seed(target)
        with pytest.raises(ProjectMigrationConflictError, match="conflicts with a different project"):
            migrate_projects(reconcile=True)
        baseline = tracker_database.get(ProjectBackfill, source.id)
        assert baseline is not None and not baseline.confirmed

        client = ProjectServiceClient(
            httpx.Client(
                base_url="http://projects",
                transport=UnavailableAwareTransport(
                    httpx.MockTransport(lambda request: httpx.Response(status, request=request))
                ),
            )
        )
        warnings: list[str] = []
        monkeypatch.setattr(
            "tracker.repositories.project.logger.warning", lambda message, *args: warnings.append(message % args)
        )
        with monkeypatch.context() as outage:
            outage.setattr("tracker.repositories.project.project_service", lambda: client)
            migrate_projects()

        assert any("remains unconfirmed" in warning for warning in warnings)
        assert tracker_database.get(ProjectCutover, 1) is not None
        assert set(project_service.rows) == {str(target.id)}
        with pytest.raises(ProjectMigrationConflictError, match="conflicts with a different project"):
            migrate_projects(reconcile=True)

    @pytest.mark.description("Successful reconciliation persists the cutover marker without replacing it on a rerun.")
    def test_reconcile_completes_cutover(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())
        source = Project(org_id=TEST_ORG_ID, handle="source", name="Source", spend_type=SpendType.INTERNAL)
        tracker_database.add(source)
        tracker_database.commit()

        migrate_projects(reconcile=True)

        marker = tracker_database.get(ProjectCutover, 1)
        assert marker is not None
        completed_at = marker.completed_at
        assert str(source.id) in project_service.rows

        migrate_projects(reconcile=True)

        tracker_database.refresh(marker)
        assert marker.completed_at == completed_at

    @pytest.mark.description("Concurrent cutover completion keeps the marker committed by the winning task.")
    def test_concurrent_cutover(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        commit = tracker_database.commit

        def race() -> None:
            with Session(tracker_database.get_bind()) as writer:
                writer.add(ProjectCutover())
                writer.commit()
            commit()

        monkeypatch.setattr(tracker_database, "commit", race)

        ProjectRepository(tracker_database).complete_cutover()

        assert tracker_database.get(ProjectCutover, 1) is not None


class TestProjectReconciliation:
    @pytest.mark.description(
        "An unconfirmed import detects a legacy rename reversal after the winning create response is lost."
    )
    def test_unconfirmed_intent_survives_rename_reversal(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        original_hash = project_hash(legacy)
        pending: list[httpx.Request] = []

        def rename(name: str) -> None:
            with Session(tracker_database.get_bind(), expire_on_commit=False) as writer:
                source = writer.get(Project, legacy.id)
                assert source is not None
                source.name = name
                writer.commit()

        def handle(request: httpx.Request) -> httpx.Response:
            if request.method != "POST":
                return project_service.handle(request)
            if not pending:
                pending.append(request)
                rename("Intermediate")
                with Session(tracker_database.get_bind(), expire_on_commit=False) as second:
                    with pytest.raises(ProjectServiceUnavailableError):
                        ProjectRepository(second).backfill()
                rename("Original")
                return httpx.Response(409, request=request)
            assert project_service.handle(request).status_code == 201
            raise httpx.ReadError("Winning create response lost", request=request)

        client = ProjectServiceClient(
            httpx.Client(
                base_url="http://projects",
                transport=UnavailableAwareTransport(httpx.MockTransport(handle)),
            )
        )
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)
        repository = ProjectRepository(tracker_database)

        assert repository.backfill() == (0, 1)

        tracker_database.expire_all()
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        assert baseline.source_hash == original_hash
        assert baseline.confirmed is False
        assert legacy.name == "Original"
        assert project_service.rows[str(legacy.id)]["name"] == "Intermediate"
        assert repository.backfill() == (0, 1)
        with pytest.raises(ProjectMigrationConflictError, match="differing fields: name"):
            repository.backfill(reconcile=True)
        assert baseline.confirmed is False
        assert project_service.rows[str(legacy.id)]["name"] == "Intermediate"

        project_service.seed(legacy)
        assert repository.backfill(reconcile=True) == (0, 1)
        assert baseline.confirmed is True
        project_service.rows[str(legacy.id)]["name"] = "Legitimate service edit"
        assert repository.backfill(reconcile=True) == (0, 1)
        assert project_service.rows[str(legacy.id)]["name"] == "Legitimate service edit"

    @pytest.mark.description(
        "Two startup copies preserve the inserted snapshot when a legacy rename occurs between their create requests."
    )
    def test_two_startups_straddle_legacy_rename(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        original_hash = project_hash(legacy)
        pending: list[httpx.Request] = []
        responses: list[httpx.Response] = []

        def handle(request: httpx.Request) -> httpx.Response:
            if request.method != "POST":
                return project_service.handle(request)
            if not pending:
                pending.append(request)
                with Session(tracker_database.get_bind(), expire_on_commit=False) as writer:
                    source = writer.get(Project, legacy.id)
                    assert source is not None
                    source.name = "Late legacy rename"
                    writer.commit()
                with Session(tracker_database.get_bind(), expire_on_commit=False) as second:
                    assert ProjectRepository(second).backfill() == (0, 1)
                return responses[0]
            responses.append(project_service.handle(pending[0]))
            assert responses[0].status_code == 201
            return httpx.Response(409, request=request)

        client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(handle)))
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)
        repository = ProjectRepository(tracker_database)

        assert repository.backfill() == (1, 0)

        tracker_database.expire_all()
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        assert baseline.source_hash == original_hash
        assert legacy.name == "Late legacy rename"
        assert project_service.rows[str(legacy.id)]["name"] == "Original"
        with pytest.raises(ProjectMigrationConflictError, match="differing fields: name"):
            repository.backfill(reconcile=True)

    @pytest.mark.description(
        "A concurrent create preserves the previous baseline until the service row matches the intended copy."
    )
    @pytest.mark.parametrize("matches_source", [False, True])
    @pytest.mark.parametrize("reconcile", [False, True])
    def test_concurrent_create_preserves_baseline(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
        matches_source: bool,
        reconcile: bool,
    ) -> None:
        ProjectRepository(tracker_database).backfill()
        original = project_service.project(legacy.id)
        assert original is not None
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        original_hash = baseline.source_hash
        copied_at = baseline.copied_at
        del project_service.rows[str(legacy.id)]
        legacy.name = "Late legacy rename"
        tracker_database.commit()

        def handle(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                with Session(tracker_database.get_bind()) as observer:
                    stored = observer.get(ProjectBackfill, legacy.id)
                    assert stored is not None
                    assert stored.source_hash == original_hash
                project_service.seed(legacy if matches_source else original)
                return httpx.Response(409, request=request)
            return project_service.handle(request)

        client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(handle)))
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)
        repository = ProjectRepository(tracker_database)

        if reconcile and not matches_source:
            with pytest.raises(ProjectMigrationConflictError, match="differing fields: name"):
                repository.backfill(reconcile=True)
        else:
            assert repository.backfill(reconcile=reconcile) == (0, 1)

        if matches_source:
            assert baseline.source_hash == project_hash(legacy)
            assert project_service.rows[str(legacy.id)]["name"] == legacy.name
            assert repository.backfill(reconcile=True) == (0, 1)
        else:
            assert baseline.source_hash == original_hash
            assert baseline.copied_at == copied_at
            assert project_service.rows[str(legacy.id)]["name"] == original.name
            with pytest.raises(ProjectMigrationConflictError, match="differing fields: name"):
                repository.backfill(reconcile=True)

    @pytest.mark.description(
        "A failed create retains its intended hash so a lost response cannot hide a later legacy edit."
    )
    @pytest.mark.parametrize("service_committed", [False, True])
    def test_failed_create_intent(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
        service_committed: bool,
    ) -> None:
        failed = False

        def handle(request: httpx.Request) -> httpx.Response:
            nonlocal failed
            if request.method == "POST":
                with Session(tracker_database.get_bind()) as observer:
                    baseline = observer.get(ProjectBackfill, legacy.id)
                    assert baseline is not None
                    assert baseline.source_hash == original_hash
                if not failed:
                    failed = True
                    if service_committed:
                        assert project_service.handle(request).status_code == 201
                    raise httpx.ReadError("Create response lost", request=request)
            return project_service.handle(request)

        client = ProjectServiceClient(
            httpx.Client(
                base_url="http://projects",
                transport=UnavailableAwareTransport(httpx.MockTransport(handle)),
            )
        )
        monkeypatch.setattr("tracker.repositories.project.project_service", lambda: client)
        repository = ProjectRepository(tracker_database)
        original_hash = project_hash(legacy)

        with pytest.raises(ProjectServiceUnavailableError):
            repository.backfill()

        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        assert baseline.source_hash == original_hash
        assert (project_service.project(legacy.id) is not None) == service_committed
        legacy.name = "Late legacy rename"
        tracker_database.commit()

        if service_committed:
            assert repository.backfill() == (0, 1)
            with pytest.raises(ProjectMigrationConflictError, match="differing fields: name"):
                repository.backfill(reconcile=True)
            assert project_service.rows[str(legacy.id)]["name"] == "Original"
            assert baseline.source_hash == original_hash
        else:
            assert repository.backfill(reconcile=True) == (1, 0)
            assert project_service.rows[str(legacy.id)]["name"] == legacy.name
            assert baseline.source_hash == project_hash(legacy)

    @pytest.mark.description(
        "An already-applied legacy edit or deletion resumes reconciliation without repeating service writes."
    )
    @pytest.mark.parametrize("deleted", [False, True])
    def test_already_applied(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
        deleted: bool,
    ) -> None:
        repository = ProjectRepository(tracker_database)
        repository.backfill()
        project_id = legacy.id
        baseline = tracker_database.get(ProjectBackfill, project_id)
        assert baseline is not None
        original_hash = baseline.source_hash
        if deleted:
            tracker_database.delete(legacy)
            tracker_database.commit()
            project_service.rows[str(project_id)]["deleted_at"] = "2026-02-03T00:00:00Z"
        else:
            legacy.name = "Already applied"
            tracker_database.commit()
            project_service.seed(legacy)

        def unexpected_write(_client: ProjectServiceClient, _project: Project) -> None:
            pytest.fail("An already-applied change must not be written again")

        monkeypatch.setattr(ProjectServiceClient, "save", unexpected_write)
        monkeypatch.setattr(ProjectServiceClient, "delete", unexpected_write)

        repository.backfill(reconcile=True)

        assert baseline.source_hash == (original_hash if deleted else project_hash(legacy))
        copied_at = baseline.copied_at
        repository.backfill(reconcile=True)
        assert baseline.copied_at == copied_at

    @pytest.mark.description(
        "A baseline commit does not expire later projects before their full source hash is recorded."
    )
    def test_multiple_baseline_commits(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = [
            Project(org_id=TEST_ORG_ID, handle=handle, name=handle, spend_type=SpendType.INTERNAL)
            for handle in ("first", "second", "third")
        ]
        tracker_database.add_all(projects)
        tracker_database.commit()
        monkeypatch.setattr("tracker.database.session.engine", tracker_database.get_bind())

        migrate_projects()

        for project in projects:
            baseline = tracker_database.get(ProjectBackfill, project.id)
            assert baseline is not None
            assert baseline.source_hash == project_hash(project)
            assert project_service.project(project.id) is not None

    @pytest.fixture
    def legacy(self, tracker_database: Session) -> Project:
        project = Project(
            org_id=TEST_ORG_ID,
            handle="legacy",
            name="Original",
            spend_type=SpendType.DEVELOPMENT,
            created_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        )
        tracker_database.add(project)
        tracker_database.commit()
        return project

    @pytest.mark.description("Reconciliation copies a legacy project created after the startup snapshot.")
    def test_late_create(self, tracker_database: Session, project_service: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)
        assert repository.backfill() == (0, 0)
        project = Project(org_id=TEST_ORG_ID, handle="late", name="Late", spend_type=SpendType.INTERNAL)
        tracker_database.add(project)
        tracker_database.commit()

        assert repository.backfill(reconcile=True) == (1, 0)

        baseline = tracker_database.get(ProjectBackfill, project.id)
        assert baseline is not None
        assert baseline.source_hash == project_hash(project)
        assert project_service.project(project.id) is not None

    @pytest.mark.description("A late legacy edit fails without service writes and reconciles after a manual fix.")
    def test_late_edit(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        repository = ProjectRepository(tracker_database)
        repository.backfill()
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        original_hash = baseline.source_hash
        copied_at = baseline.copied_at
        before = dict(project_service.rows[str(legacy.id)])
        errors: list[str] = []
        monkeypatch.setattr("tracker.repositories.project.logger.error", errors.append)
        legacy.name = "Legacy edit"
        legacy.extra_tags = {"team": "legacy"}
        legacy.archived_at = datetime(2026, 2, 3, tzinfo=UTC)
        legacy.gateway_key_id = UUID("00000000-0000-0000-0000-000000000001")
        tracker_database.commit()

        with pytest.raises(ProjectMigrationConflictError, match="requires manual reconciliation"):
            repository.backfill(reconcile=True)

        assert project_service.rows[str(legacy.id)] == before
        assert baseline.source_hash == original_hash
        assert baseline.copied_at == copied_at
        assert len(errors) == 1
        assert str(legacy.id) in errors[0]
        for field in ("name", "extra_tags", "archived_at", "gateway_key_id"):
            assert field in errors[0]

        project_service.seed(legacy)

        repository.backfill(reconcile=True)

        assert baseline.source_hash == project_hash(legacy)
        copied_at = baseline.copied_at
        assert repository.backfill(reconcile=True) == (0, 1)
        assert baseline.copied_at == copied_at

    @pytest.mark.description("A late legacy deletion fails without service writes and passes after a manual deletion.")
    def test_late_delete(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        repository = ProjectRepository(tracker_database)
        repository.backfill()
        project_id = legacy.id
        baseline = tracker_database.get(ProjectBackfill, project_id)
        assert baseline is not None
        original_hash = baseline.source_hash
        copied_at = baseline.copied_at
        before = dict(project_service.rows[str(project_id)])
        errors: list[str] = []
        monkeypatch.setattr("tracker.repositories.project.logger.error", errors.append)
        tracker_database.delete(legacy)
        tracker_database.commit()

        with pytest.raises(ProjectMigrationConflictError, match="requires manual reconciliation"):
            repository.backfill(reconcile=True)

        assert project_service.rows[str(project_id)] == before
        assert baseline.source_hash == original_hash
        assert baseline.copied_at == copied_at
        assert len(errors) == 1
        assert str(project_id) in errors[0]
        assert "deleted_at" in errors[0]

        project_service.rows[str(project_id)]["deleted_at"] = "2026-02-03T00:00:00Z"

        assert repository.backfill(reconcile=True) == (0, 1)
        assert baseline.source_hash == original_hash
        assert baseline.copied_at == copied_at

    @pytest.mark.description("An unchanged legacy row leaves a service edit or tombstone and its baseline untouched.")
    @pytest.mark.parametrize("field,value", [("name", "Service edit"), ("deleted_at", "2026-02-03T00:00:00Z")])
    def test_service_edit(
        self, tracker_database: Session, project_service: MockProjectRoutes, legacy: Project, field: str, value: str
    ) -> None:
        repository = ProjectRepository(tracker_database)
        repository.backfill()
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        copied_at = baseline.copied_at
        project_service.rows[str(legacy.id)][field] = value

        assert repository.backfill(reconcile=True) == (0, 1)

        assert project_service.rows[str(legacy.id)][field] == value
        assert baseline.copied_at == copied_at

    @pytest.mark.description(
        "A legacy edit or deletion conflicts with a service edit and leaves the service and baseline intact."
    )
    @pytest.mark.parametrize("deleted", [False, True])
    @pytest.mark.parametrize("tombstone", [False, True])
    def test_conflict(
        self,
        tracker_database: Session,
        project_service: MockProjectRoutes,
        legacy: Project,
        deleted: bool,
        tombstone: bool,
    ) -> None:
        repository = ProjectRepository(tracker_database)
        repository.backfill()
        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        assert baseline is not None
        original_hash = baseline.source_hash
        project_id = legacy.id
        project_service.rows[str(project_id)]["name"] = "Service edit"
        if tombstone:
            project_service.rows[str(project_id)]["deleted_at"] = "2026-02-03T00:00:00Z"
        if deleted:
            tracker_database.delete(legacy)
        else:
            legacy.name = "Legacy edit"
        tracker_database.commit()

        if deleted and tombstone:
            repository.backfill(reconcile=True)
        else:
            with pytest.raises(ProjectMigrationConflictError):
                repository.backfill(reconcile=True)

        assert project_service.rows[str(project_id)]["name"] == "Service edit"
        assert baseline.source_hash == original_hash
        if tombstone:
            assert project_service.rows[str(project_id)]["deleted_at"] is not None

    @pytest.mark.description(
        "An equal pre-seeded row gains a baseline while a divergent pre-seeded row remains untracked."
    )
    @pytest.mark.parametrize("diverged", [False, True])
    def test_existing_baseline(
        self, tracker_database: Session, project_service: MockProjectRoutes, legacy: Project, diverged: bool
    ) -> None:
        project_service.seed(legacy)
        if diverged:
            project_service.rows[str(legacy.id)]["name"] = "Bench edit"

        assert ProjectRepository(tracker_database).backfill() == (0, 1)

        baseline = tracker_database.get(ProjectBackfill, legacy.id)
        if diverged:
            assert baseline is None
            assert project_service.rows[str(legacy.id)]["name"] == "Bench edit"
        else:
            assert baseline is not None
            assert baseline.source_hash == project_hash(legacy)
