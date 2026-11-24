from uuid import UUID

from valkyrie.sdk.models import ProjectResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import one_line, done


def retag(run_id: UUID, project: str) -> ProjectResponse:
    response = call(lambda client: client.runs.retag(run_id, project))
    done(f"Run {run_id} is now in project {one_line(response.handle)}.")
    return response
