import re
from pathlib import Path

import pytest
from tracker.database.models import RunProject

_TRACKER_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_SQL_WRITE = re.compile(r'\b(insert\s+into|update|delete\s+from)\s+(?:"?\w+"?\.)?"?project\b"?', re.IGNORECASE)


@pytest.mark.description(
    "Tracker maps no `project` table and no Tracker module writes one in SQL, so the project service is the only project writer."
)
def test_tracker_has_no_project_table_or_writer() -> None:
    sources = [_TRACKER_ROOT / "main.py", *sorted((_TRACKER_ROOT / "src/tracker").rglob("*.py"))]
    writers = [path.name for path in sources if _PROJECT_SQL_WRITE.search(path.read_text())]
    assert ("project" in RunProject.metadata.tables, writers) == (False, [])
