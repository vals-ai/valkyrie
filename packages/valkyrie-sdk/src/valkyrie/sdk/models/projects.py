"""Projects that runs are started in, and the spend fields every run in a project shares."""

from valkyrie.sdk.models._generated.projects import (
    NewProjectRequest,
    ProjectCreateRequest,
    ProjectResponse,
    ProjectsResponse,
    SpendType,
)

__all__ = [
    "SPEND_SUBCATEGORIES",
    "NewProjectRequest",
    "ProjectCreateRequest",
    "ProjectResponse",
    "ProjectsResponse",
    "SpendType",
]


SPEND_SUBCATEGORIES: dict[SpendType, tuple[str, ...]] = {
    SpendType.DEVELOPMENT: (
        "new_benchmark_generation",
        "new_benchmark_launch_runs",
        "post_launch_benchmark_update",
    ),
    SpendType.CUSTOMER: ("report", "paid_run", "eval_server", "platform"),
    SpendType.INTERNAL: (
        "unpaid_leaderboard_run",
        "other_unpaid_run",
        "fellows_interns",
        "blog",
        "miscellaneous",
    ),
}
