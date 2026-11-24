from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from tests.factories import make_benchmark
from tracker.auth import RequestIdentity
from tracker.database.models import Task
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.services.datasets.artifacts import SAVED_ARTIFACTS_RUN
from tracker.services.datasets.ref import HostedDataset
from tracker.services.runs.regrade import regrade_run, regrade_tasks

STARTED_AT = datetime(2026, 11, 21, tzinfo=UTC)


def task(
    task_id: str,
    status: TaskStatus,
    *,
    evaluation_phase_token: UUID | None = None,
    eval_resume_state: dict[str, Any] | None = None,
) -> Task:
    return Task(
        org_id=uuid4(),
        benchmark=uuid4(),
        task_id=task_id,
        status=status,
        started_at=STARTED_AT,
        evaluation_phase_token=evaluation_phase_token,
        eval_resume_state=eval_resume_state,
    )


@pytest.mark.description(
    "A dataset run's regrade copies the tasks that reached grading and points them at its artifacts."
)
class TestDatasetRegrade:
    @pytest.mark.description(
        "Finished tasks and those that failed while grading are regraded, and the rest are skipped."
    )
    def test_tasks_that_reached_grading_are_regraded(self) -> None:
        source_run = uuid4()
        earlier = {SAVED_ARTIFACTS_RUN: str(uuid4())}

        tasks = regrade_tasks(
            [
                task("graded", TaskStatus.FINISHED),
                task("failed-grading", TaskStatus.ERROR, evaluation_phase_token=uuid4()),
                task("failed-agent", TaskStatus.ERROR),
                task("regraded-before", TaskStatus.ERROR, eval_resume_state=earlier),
            ],
            source_run,
        )

        assert tasks.eval_resume_states == {
            "graded": {SAVED_ARTIFACTS_RUN: str(source_run)},
            "failed-grading": {SAVED_ARTIFACTS_RUN: str(source_run)},
            "regraded-before": earlier,
        }
        assert tasks.skipped_task_ids == ["failed-agent"]

    @pytest.mark.description("A benchmark service run's regrade copies only tasks that saved evaluation state.")
    def test_a_service_run_copies_only_saved_states(self) -> None:
        tasks = regrade_tasks([task("graded", TaskStatus.FINISHED)])

        assert (tasks.eval_resume_states, tasks.skipped_task_ids) == ({}, ["graded"])

    @pytest.mark.description("The new run takes its benchmark, dataset and version from the pinned dataset.")
    def test_the_new_run_takes_the_pinned_dataset(self) -> None:
        source = make_benchmark(
            "ioi",
            status=BenchmarkStatus.FINISHED,
            dataset_ref=HostedDataset(benchmark="ioi", dataset="2024", dataset_id=uuid4(), revision=1),
        )
        target = HostedDataset(benchmark="ioi-fixed", dataset="2024", dataset_id=uuid4(), revision=2)

        run = regrade_run(
            source,
            regrade_tasks([task("graded", TaskStatus.FINISHED)], source.id),
            RequestIdentity(org=None),
            dataset_ref=target,
        )

        shown = run.arguments.with_dataset_metadata()
        assert run.name == "ioi-fixed"
        assert run.arguments.dataset_ref == target
        assert (run.arguments.dataset, run.arguments.dataset_version) == (None, None)
        assert shown.dataset_version is not None
        assert (shown.dataset, shown.dataset_version.id) == ("2024", "ioi-fixed/2024@2")
        assert run.arguments.task_ids == ["graded"]
